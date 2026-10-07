//! One home writer, command service and guardian broker.
use crate::{
    calls::{self, Calls},
    dispatch::{Catalog, Hooks, InputSetter, ResourceSettings},
    execution::{CallLauncher, ExecutionHost},
    scheduler,
};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sluice_model::{
    RuntimeApi,
    commands::*,
    edit::{self, EditSnapshot, PlanEdit},
    error::PublicError,
    events::{ChangeBatch, ChangeCursor},
    gates::CachedResources,
    ids::*,
    plan::Plan,
    rpc::{self, JsonMap, RpcReply, RpcRequest, RpcResult, RunCapability},
};
use sluice_process::{
    guardian::{AdoptionAttempt, adopt_attempt},
    identity::ProcessIdentity,
    journal::{AttemptKey, CompletionJournal, PayloadResult},
    socket::{
        self, CoordinatorCommand, CoordinatorLink, CoordinatorReply, DurableAck, GuardianIdentity,
        Reply, Request, SubmissionSnapshot,
    },
};
use sluice_store::{
    ChangeKey, ReadPool, RetrySafety, StoreError, Writer, artifacts, attempts,
    plans::{self, PlanContext},
    projects, records, resources,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{UnixListener, UnixStream},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

pub fn executor() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}
fn storage(e: impl std::fmt::Display) -> PublicError {
    PublicError::Storage {
        message: e.to_string(),
    }
}
/// A request refused while it waited for the startup adoption pass: it never ran.
fn not_executed(why: &str) -> PublicError {
    PublicError::Busy {
        message: format!(
            "the coordinator is adopting runs and {why}; this request was not executed, so it is safe to retry"
        ),
        retryable: true,
    }
}
/// A request still unanswered when the coordinator stops.
fn stopping(scope: &RequestScope) -> PublicError {
    if scope.parked.load(std::sync::atomic::Ordering::SeqCst) {
        return not_executed("it stopped");
    }
    PublicError::Busy {
        message:
            "the coordinator stopped before answering; the request may still have taken effect"
                .into(),
        retryable: true,
    }
}
fn conflict(message: impl Into<String>) -> PublicError {
    PublicError::Conflict {
        message: message.into(),
        current_rev: None,
    }
}
fn data(value: impl Serialize) -> Result<CommandReply, PublicError> {
    Ok(CommandReply::Data(
        serde_json::to_value(value).map_err(storage)?.try_into()?,
    ))
}

struct Inner<H: ExecutionHost> {
    writer: Writer,
    reads: ReadPool,
    home: PathBuf,
    home_id: HomeId,
    catalog: Arc<Catalog>,
    host: Arc<H>,
    calls: Calls<Catalog, CallLauncher<H>>,
    frozen: std::sync::Mutex<FrozenFacts>,
    /// Whether `serve`'s startup adoption pass is done; see [`Coordinator::ready`].
    gate: tokio::sync::watch::Sender<Gate>,
    /// Slots for requests waiting on `ready`, so waiting writes never take
    /// the connection permits reads need.
    waiting: tokio::sync::Semaphore,
    /// Runs an adoption task is working on: one adopter per run.
    adopting: Arc<std::sync::Mutex<std::collections::HashSet<RunId>>>,
    /// Connection permits; a held watch gives its permit back (see `watches`).
    connections: Arc<tokio::sync::Semaphore>,
    /// Held watches that gave their connection permit back, per run.
    watches: Arc<std::sync::Mutex<std::collections::HashMap<RunId, usize>>>,
    /// Each project's plan as last compiled.
    plans: Arc<PlanCache>,
}
/// Where the startup adoption pass stands, for requests that wait on it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Gate {
    /// `serve`'s startup pass is running.
    Adopting,
    /// The pass is done, or the coordinator is not serving.
    Ready,
    /// `serve` stopped, or its startup pass failed, before the pass was done.
    Stopped,
}
/// One request's link to its connection, seen by `ready` through a task-local:
/// a request that is still waiting when its client hangs up is dropped unrun,
/// and a stop tells the client whether it ran.
#[derive(Default)]
struct RequestScope {
    gone: CancellationToken,
    parked: std::sync::atomic::AtomicBool,
}
tokio::task_local! {
    static REQUEST: Arc<RequestScope>;
}
/// Held watches per run that leave the connection pool; more count against it.
const WATCHES_PER_RUN: usize = 2;
/// Per-run adoptions in flight at once during an adoption pass.
pub const ADOPTION_PARALLELISM: usize = 16;
/// Connections the coordinator serves at once.
const CONNECTIONS: usize = 64;
/// Requests that may wait for the startup adoption pass at once; past this a
/// request is told to retry.
const WAITING_FOR_ADOPTION: usize = CONNECTIONS / 2;
/// Holds a run in the `adopting` set until its adoption task ends.
struct AdoptionClaim {
    adopting: Arc<std::sync::Mutex<std::collections::HashSet<RunId>>>,
    run: RunId,
}
impl AdoptionClaim {
    fn take(
        adopting: &Arc<std::sync::Mutex<std::collections::HashSet<RunId>>>,
        run: RunId,
    ) -> Option<Self> {
        adopting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(run)
            .then(|| Self {
                adopting: adopting.clone(),
                run,
            })
    }
}
impl Drop for AdoptionClaim {
    fn drop(&mut self) {
        self.adopting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.run);
    }
}
type StoredAttempt = (Option<String>, Option<String>, i64, i64, Option<String>);
/// Facts read out of attempts' frozen requests, which never change once
/// written: each run's capability and each recorded start's evidence. A
/// request can be hundreds of kilobytes; guardians that poll (older ones still
/// do, every 50 ms) would otherwise have it parsed on each request.
#[derive(Default)]
struct FrozenFacts {
    capabilities: std::collections::HashMap<AttemptId, RunCapability>,
    starts: std::collections::HashMap<(AttemptId, InvocationId), Value>,
}
impl FrozenFacts {
    /// Bounded: a coordinator sees few live attempts; past this it forgets.
    const LIMIT: usize = 4096;
    fn trim(&mut self) {
        if self.capabilities.len() + self.starts.len() > Self::LIMIT {
            self.capabilities.clear();
            self.starts.clear();
        }
    }
}
pub struct Coordinator<H: ExecutionHost> {
    inner: Arc<Inner<H>>,
}
impl<H: ExecutionHost> Clone for Coordinator<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<H: ExecutionHost> Coordinator<H> {
    /// Writer::open owns the home flock until the actor connection is closed.
    pub async fn open(home: PathBuf, catalog: Catalog, host: H) -> Result<Self, PublicError> {
        sluice_process::host::guard_scratch_home(&home)?;
        let path = home.clone();
        let writer = tokio::task::spawn_blocking(move || Writer::open(path))
            .await
            .map_err(storage)?
            .map_err(|e| e.into_public(false))?;
        let path = home.clone();
        let reads = tokio::task::spawn_blocking(move || ReadPool::open(path, 4))
            .await
            .map_err(storage)?
            .map_err(|e| e.into_public(true))?;
        let home_id = reads
            .snapshot(|sql| {
                let id: String =
                    sql.query_row("SELECT home_id FROM home_meta WHERE singleton=1", [], |r| {
                        r.get(0)
                    })?;
                calls::parse_id(id)
            })
            .await
            .map_err(|e| e.into_public(true))?;
        // The flock proves every earlier coordinator connection and lease is gone.
        writer.write(RetrySafety::Idempotent,|tx|{if tx.sql().execute("UPDATE maintenance SET scheduler_owner=NULL,scheduler_lease_until=NULL WHERE scheduler_owner IS NOT NULL",[])?>0{tx.changed(None,"scheduler");}Ok(())}).await?;
        let catalog = if host.composition_enabled() {
            let publication = crate::publication::Publication::new(
                crate::registry::FnRegistry::configured(home.clone())?,
                catalog,
            );
            Catalog(Default::default(), Some(publication))
        } else {
            catalog
        };
        let catalog = Arc::new(catalog);
        let host = Arc::new(host);
        let calls = Calls::new(
            writer.clone(),
            reads.clone(),
            catalog.clone(),
            Arc::new(CallLauncher {
                host: host.clone(),
                home: home_id,
                writer: writer.clone(),
            }),
        );
        let broker = Self {
            inner: Arc::new(Inner {
                writer,
                reads,
                home,
                home_id,
                catalog,
                host,
                calls,
                frozen: Default::default(),
                gate: tokio::sync::watch::Sender::new(Gate::Ready),
                waiting: tokio::sync::Semaphore::new(WAITING_FOR_ADOPTION),
                adopting: Default::default(),
                connections: Arc::new(tokio::sync::Semaphore::new(CONNECTIONS)),
                watches: Default::default(),
                plans: Default::default(),
            }),
        };
        artifacts::recover(broker.writer(), broker.home())
            .await
            .map_err(|e| e.into_public(false))?;
        broker.refresh_registry().await?;
        Ok(broker)
    }
    pub async fn refresh_registry(&self) -> Result<(), PublicError> {
        if let Some(publication) = &self.inner.catalog.1 {
            publication
                .refresh(self.writer(), self.projects().await?)
                .await?;
        }
        Ok(())
    }
    /// The read path's refresh: never waits behind a running republish and
    /// does not scan while the registry is current.
    async fn refresh_for_read(&self) -> Result<(), PublicError> {
        if let Some(publication) = &self.inner.catalog.1 {
            publication
                .refresh_if_stale(self.writer(), self.projects().await?)
                .await?;
        }
        Ok(())
    }
    /// Whether the startup adoption pass is done (always, for a coordinator
    /// that is not serving).
    pub fn adopted(&self) -> bool {
        *self.inner.gate.borrow() == Gate::Ready
    }
    /// Requests now waiting for the startup adoption pass, for diagnostics.
    pub fn waiting_for_adoption(&self) -> usize {
        WAITING_FOR_ADOPTION - self.inner.waiting.available_permits()
    }
    /// Connection permits in use, for diagnostics.
    pub fn connections_in_use(&self) -> usize {
        CONNECTIONS - self.inner.connections.available_permits()
    }
    /// Held watches that gave their connection permit back, for diagnostics.
    pub fn released_watches(&self) -> usize {
        self.inner
            .watches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .sum()
    }
    /// Wait until the startup adoption pass is done: writes are decided
    /// against reconciled runs, and nothing is admitted before it. A request
    /// that cannot wait (too many waiting, its client hung up, or the
    /// coordinator stopped first) is refused with a retryable `busy` and was
    /// not executed.
    async fn ready(&self) -> Result<(), PublicError> {
        let mut gate = self.inner.gate.subscribe();
        match *gate.borrow_and_update() {
            Gate::Ready => return Ok(()),
            Gate::Stopped => return Err(not_executed("it stopped")),
            Gate::Adopting => {}
        }
        let _slot = self
            .inner
            .waiting
            .try_acquire()
            .map_err(|_| not_executed("too many requests are waiting for it"))?;
        let scope = REQUEST.try_with(Arc::clone).unwrap_or_default();
        scope
            .parked
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let outcome = tokio::select! {
            gate = gate.wait_for(|gate| *gate != Gate::Adopting) => gate.map(|gate| *gate).unwrap_or(Gate::Stopped),
            _ = scope.gone.cancelled() => Gate::Stopped,
        };
        if outcome == Gate::Ready {
            scope
                .parked
                .store(false, std::sync::atomic::Ordering::SeqCst);
            return Ok(());
        }
        Err(not_executed(if scope.gone.is_cancelled() {
            "its client hung up"
        } else {
            "it stopped"
        }))
    }
    pub fn writer(&self) -> &Writer {
        &self.inner.writer
    }
    pub fn reads(&self) -> &ReadPool {
        &self.inner.reads
    }
    pub fn home(&self) -> &Path {
        &self.inner.home
    }
    pub fn home_id(&self) -> HomeId {
        self.inner.home_id
    }
    pub fn host(&self) -> &H {
        &self.inner.host
    }
    pub fn catalog(&self) -> &Catalog {
        &self.inner.catalog
    }
    pub(crate) fn plan_cache(&self) -> Arc<PlanCache> {
        self.inner.plans.clone()
    }
    pub fn calls(&self) -> &Calls<Catalog, CallLauncher<H>> {
        &self.inner.calls
    }
    pub async fn context(&self, project: ProjectId) -> Result<PlanContext, PublicError> {
        self.refresh_registry().await?;
        if let Some(publication) = &self.inner.catalog.1 {
            let errors = publication.problems(Some(project));
            if !errors.is_empty() {
                return Err(PublicError::Invalid {
                    message: "project registry blocked".into(),
                    errors,
                });
            }
        }
        let catalog = self.inner.catalog.clone();
        self.reads()
            .snapshot(move |sql| context(sql, project, &catalog))
            .await
            .map_err(|e| e.into_public(true))
    }
    pub async fn projects(&self) -> Result<Vec<ProjectId>, PublicError> {
        self.reads().snapshot(|sql|{let mut stmt=sql.prepare("SELECT project_id FROM projects WHERE deleted_at IS NULL ORDER BY created_at,project_id")?;let rows=stmt.query_map([],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;rows.into_iter().map(calls::parse_id).collect()}).await.map_err(|e|e.into_public(true))
    }
    pub async fn project_versions(&self) -> Result<BTreeMap<ProjectId, i64>, PublicError> {
        self.reads().snapshot(|sql|{let mut stmt=sql.prepare("SELECT p.project_id,coalesce(sum(v.version),0) FROM projects p LEFT JOIN change_versions v ON v.project_id=p.project_id AND v.view<>'progress' WHERE p.deleted_at IS NULL GROUP BY p.project_id")?;let rows=stmt.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?.collect::<Result<Vec<_>,_>>()?;rows.into_iter().map(|(id,n)|Ok((calls::parse_id(id)?,n))).collect()}).await.map_err(|e|e.into_public(true))
    }
    pub async fn capacity_resources(&self, project: ProjectId) -> Result<Vec<String>, PublicError> {
        self.reads()
            .snapshot(move |sql| {
                Ok(resources::capacity_observations(sql, project)?
                    .into_iter()
                    .map(|r| r.name)
                    .collect())
            })
            .await
            .map_err(|e| e.into_public(true))
    }
    pub async fn scheduler_owner(&self) -> Result<Option<String>, PublicError> {
        self.reads()
            .snapshot(|sql| {
                Ok(sql.query_row(
                    "SELECT scheduler_owner FROM maintenance WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .map_err(|e| e.into_public(true))
    }
    pub async fn acquire_scheduler(&self, owner: String) -> Result<(), PublicError> {
        if owner.is_empty() || owner.len() > 256 {
            return Err(conflict("invalid scheduler owner"));
        }
        self.writer().write(RetrySafety::NonIdempotent,move|tx|{let n=tx.sql().execute("UPDATE maintenance SET scheduler_owner=?1 WHERE singleton=1 AND scheduler_owner IS NULL",[owner])?;if n==0{return Err(conflict("scheduler lease already held").into());}tx.changed(None,"scheduler");Ok(())}).await
    }
    pub async fn release_scheduler(&self, owner: String) -> Result<(), PublicError> {
        self.writer().write(RetrySafety::Idempotent,move|tx|{if tx.sql().execute("UPDATE maintenance SET scheduler_owner=NULL,scheduler_lease_until=NULL WHERE singleton=1 AND scheduler_owner=?1",[owner])?>0{tx.changed(None,"scheduler");}Ok(())}).await
    }
    pub async fn command(&self, request: CommandRequest) -> Result<CommandReply, PublicError> {
        if served_while_adopting(&request) {
            self.refresh_for_read().await?;
        } else {
            self.ready().await?;
            self.refresh_registry().await?;
        }
        crate::drain::check_command(self.reads(), &request).await?;
        if let Some(reply) = calls::dispatch_p3_04(
            self.calls(),
            self.writer(),
            self.reads(),
            self.inner.catalog.clone(),
            request.clone(),
        )
        .await?
        {
            return Ok(reply);
        }
        if let Some(reply) = crate::dispatch_ext::dispatch_ext(self, request.clone()).await? {
            return Ok(reply);
        }
        let catalog = self.inner.catalog.clone();
        match request {
            CommandRequest::FnList { project } => {
                let id = crate::calls::resolve(self.reads(), project).await?;
                data(self.inner.catalog.1.as_ref().ok_or_else(|| conflict("registry unavailable"))?.listing(id))
            }
            CommandRequest::FnGet { name, project } => {
                let id = crate::calls::resolve(self.reads(), project).await?;
                data(self.inner.catalog.1.as_ref().ok_or_else(|| conflict("registry unavailable"))?.detail(id, &name)?)
            }
            CommandRequest::FnSave { manifest, main_py, project } => {
                let publication = self.inner.catalog.1.as_ref().ok_or_else(|| conflict("registry unavailable"))?;
                let saved = crate::registry::save(&publication.registry, self.writer(),
                    &serde_json::to_value(manifest).map_err(storage)?, &main_py, project).await?;
                self.refresh_registry().await?;
                data(json!({"name":saved.name,"scope":saved.scope.label(),"path":saved.path,"generation":saved.generation}))
            }
            CommandRequest::ProjectsList=>{let values=self.reads().snapshot(projects::list).await.map_err(|e|e.into_public(true))?;Ok(CommandReply::Projects(values))},
            CommandRequest::ProjectCreate{name,description,icon,resources,author}=>{
                let icon=icon.map(read_icon).transpose()?;
                let project=self.writer().write(RetrySafety::NonIdempotent,move|tx|projects::project_create(tx,projects::CreateProject{name,description,icon,resources:Some(serde_json::to_value(resources)?),author:author.unwrap_or_else(||"cli".into())},&projects::EmptyPlanInitializer,&ResourceSettings((*catalog).clone()))).await?;
                artifacts::recover(self.writer(),self.home()).await.map_err(|e|e.into_public(false))?; Ok(CommandReply::Project(ProjectIdentity{project_id:project.project_id,name:project.name}))
            },
            CommandRequest::PlanGet{project}=>self.reads().snapshot(move|sql|{let id=messages_project(sql,&project)?;let ctx=context(sql,id,&catalog)?;Ok(json!({"project":projects_identity(sql,id)?,"rev":ctx.revision,"plan":ctx.plan.document()}))}).await.map_err(|e|e.into_public(true)).and_then(data),
            CommandRequest::Status(query)=>self.reads().snapshot(move|sql|crate::status::status(sql,&catalog,query)).await.map_err(|e|e.into_public(true)).and_then(data),
            CommandRequest::BoardGet { project } => self.reads().snapshot(move |sql| {
                let p = projects::resolve(sql, &project)?;
                Ok(CommandReply::Board(sluice_model::commands::BoardView {
                    project: ProjectIdentity { project_id: p.project_id, name: p.name },
                    rev: p.board_rev,
                    program: p.board,
                }))
            }).await.map_err(|e| e.into_public(true)),
            command if project_mutation(&command) => {
                let command = match command {
                    CommandRequest::ProjectUpdate(mut update) => {
                        update.icon = update.icon.map(|icon| read_icon(icon).map(IconUpload::from)).transpose()?;
                        CommandRequest::ProjectUpdate(update)
                    }
                    command => command,
                };
                let reply = if edit_project(&command).is_some() {
                    self.plan_edit(command).await?
                } else {
                    let home = self.home().to_owned();
                    self.writer().write(RetrySafety::NonIdempotent, move |tx| mutate_project(tx, &catalog, &home, command)).await?
                };
                if matches!(reply, CommandReply::Project(_)) { artifacts::recover(self.writer(), self.home()).await.map_err(|e|e.into_public(false))?; }
                Ok(reply)
            },
            CommandRequest::StepSubmit(request)=>self.writer().write(RetrySafety::NonIdempotent,move|tx|{let version=attempts::step_submit(tx,request)?;if version.is_none(){return Err(conflict("stale submission").into());}Ok(CommandReply::Ack)}).await,
            CommandRequest::Submission{run}=>data(self.submissions(run).await?),
            CommandRequest::StepProgress(request)=>self.writer().write(RetrySafety::Idempotent,move|tx|attempts::step_progress(tx,request)).await.and_then(data),
            CommandRequest::StepSettle(request)=>self.step_settle(request).await,
            CommandRequest::Ask(_) | CommandRequest::Say(_) | CommandRequest::Reply(_) => {
                let post = message_post(request)?;
                let plans = self.inner.plans.clone();
                self.writer().write(RetrySafety::NonIdempotent, move |tx| post_message(tx, &catalog, &plans, post).map(CommandReply::Receipt)).await
            }
            CommandRequest::MessagePost(request) => {
                let post = bridge_post(request)?;
                let plans = self.inner.plans.clone();
                self.writer().write(RetrySafety::NonIdempotent, move |tx| Ok(CommandReply::Posted { id: post_message(tx, &catalog, &plans, post)?.id })).await
            }
            CommandRequest::Messages(request)=>self.reads().snapshot(move|sql|{let id=messages_project(sql,&request.project)?;let identity=if request.owner {sluice_store::messages::OWNER_STREAM} else {sluice_store::messages::ORCHESTRATOR_STREAM};let messages=sluice_store::messages::messages(sql,id,request.view,request.thread.as_deref(),request.since,identity)?;Ok(CommandReply::Messages(MessagePage{project:projects_identity(sql,id)?,last_id:messages.last().map(|m|m.id),messages}))}).await.map_err(|e|e.into_public(true)),
            CommandRequest::AcquireLease(request)=>self.writer().write(RetrySafety::Idempotent,move|tx|{let lease=resources::request_lease_keyed(tx,request.run,&request.resource,request.amount,&format!("callback/{}/{}",request.run,request.request_id))?;let state=resources::leases(tx.sql(),run_project(tx.sql(),request.run)?)?.into_iter().find(|l|l.id==lease).ok_or_else(||conflict("lease missing"))?.state;Ok(CommandReply::Lease{lease,state})}).await,
            CommandRequest::ReleaseLease(request)=>self.writer().write(RetrySafety::Idempotent,move|tx|{resources::release_lease(tx,request.lease,request.run)?;Ok(CommandReply::Ack)}).await,
            CommandRequest::RegisterCompletionAction(request)=>self.writer().write(RetrySafety::Idempotent,move|tx|{if !attempts::register_completion_action(tx,request,&Hooks)?{return Err(conflict("stale action registration").into());}Ok(CommandReply::Ack)}).await,
            CommandRequest::LogRead(request)=>self.reads().snapshot(move|sql|{let project=request.project.as_ref().map(|p|messages_project(sql,p)).transpose()?;Ok(CommandReply::Records(records::read_records(sql,project,&records::RecordFilter::from(&request))?.into_page()?))}).await.map_err(|e|e.into_public(true)),
            _ => Err(PublicError::not_implemented("command dispatch extension")),
        }
    }
    /// `step_settle` (SPEC §7.5): settle a step that is finishing (its one current run has a
    /// valid submission) on that submission. Only a bare agent fn's step: the outputs are the
    /// ones its done signal would have given, derived from what its supervisor checkpointed
    /// and git now, and checked against the run's frozen schema before anything is written.
    /// Its guardian then stops the agent as a cancel does, and the completion succeeds with
    /// them.
    pub async fn step_settle(&self, request: StepSettle) -> Result<CommandReply, PublicError> {
        let catalog = self.inner.catalog.clone();
        let StepSettle {
            project,
            step,
            reason,
            author,
        } = request;
        let refuse = |message: String| PublicError::Invalid {
            errors: vec![message.clone()],
            message,
        };
        let id = step.clone();
        let (project, identity, frozen, submission) = self
            .reads()
            .snapshot(move |sql| {
                let project = messages_project(sql, &project)?;
                let ctx = context(sql, project, &catalog)?;
                let spec = ctx.plan.steps().get(&id).ok_or_else(|| PublicError::NotFound {
                    message: format!("no step {id}"),
                })?;
                let status = plans::read_state(sql, project)?.status(&id);
                if status != StepStatus::Running {
                    return Err(refuse(format!(
                        "step {id} is {}, not finishing: step_settle settles a running step whose run has submitted",
                        serde_json::to_value(&status)?.as_str().unwrap_or("not running")
                    ))
                    .into());
                }
                if spec.scatter.is_some() {
                    return Err(refuse(format!(
                        "step {id} is scattered and step_settle settles one run: step_cancel it, then step_set_output its outputs"
                    ))
                    .into());
                }
                let Some((identity, frozen)) = attempts::current_run(sql, project, &id)? else {
                    return Err(refuse(format!("step {id} has no current run to settle")).into());
                };
                let submission: Option<String> = sql
                    .query_row(
                        "SELECT outputs FROM submissions WHERE run_id=?1",
                        [identity.run.to_string()],
                        |r| r.get(0),
                    )
                    .optional()?;
                let Some(submission) = submission else {
                    return Err(refuse(format!(
                        "step {id} is running and its run {} has not submitted: it is not finishing (step_cancel stops it)",
                        identity.run
                    ))
                    .into());
                };
                let submission: JsonMap = serde_json::from_str(&submission)?;
                Ok((project, identity, frozen, submission))
            })
            .await
            .map_err(|e| e.into_public(true))?;
        let name = frozen["declaration"]["run"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let by_hand = "step_cancel it, then step_set_output the outputs it should have";
        if !sluice_agents::AGENT_BUILTINS.contains(&name.as_str()) {
            return Err(refuse(format!(
                "step {step} runs {name}, not an agent fn: its own outputs are the fn's to return and sluice cannot derive them from the submission; {by_hand}"
            )));
        }
        let inputs: JsonMap = serde_json::from_value(frozen["inputs"].clone()).map_err(storage)?;
        let fields = submission
            .0
            .iter()
            .map(|(name, value)| (name.clone(), value.as_value().clone()))
            .collect();
        let run_dir = self.home().join("runs").join(identity.run.to_string());
        let outputs = sluice_agents::settled_outputs(&name, &inputs, &run_dir, &fields)
            .await
            .map_err(|why| {
                refuse(format!(
                    "cannot derive the outputs step {step}'s done signal would give: {why}; {by_hand}"
                ))
            })?;
        attempts::check_frozen_outputs(&frozen, &outputs).map_err(|e| match e.into_public(false) {
            PublicError::Invalid { errors, .. } => PublicError::Invalid {
                message: format!(
                    "the outputs derived for step {step} do not fit its run's outputs; {by_hand}"
                ),
                errors,
            },
            other => other,
        })?;
        let run = identity.run;
        let author = author.unwrap_or_else(|| "cli".into());
        let settled = outputs.clone();
        self.writer()
            .write(RetrySafety::NonIdempotent, move |tx| {
                attempts::settle(tx, &identity, &settled, author, reason)
            })
            .await?;
        data(json!({"project":project,"step":step,"run":run,"outputs":outputs}))
    }
    pub async fn submissions(&self, run: RunId) -> Result<SubmissionSnapshot, PublicError> {
        self.reads()
            .snapshot(move |sql| {
                let row: Option<(i64, String)> = sql
                    .query_row(
                        "SELECT version,outputs FROM submissions WHERE run_id=?1",
                        [run.to_string()],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                Ok(match row {
                    Some((version, fields)) => SubmissionSnapshot {
                        version: Some(version as u64),
                        fields: serde_json::from_str(&fields)?,
                    },
                    None => SubmissionSnapshot {
                        version: None,
                        fields: JsonMap::default(),
                    },
                })
            })
            .await
            .map_err(|e| e.into_public(true))
    }
    async fn authenticate(
        &self,
        id: &AttemptKey,
        capability: Option<&RunCapability>,
    ) -> Result<(), PublicError> {
        if id.home != self.home_id() {
            return Err(conflict("wrong home identity"));
        }
        let id = id.clone();
        let capability = capability.cloned();
        let known = self.frozen().capabilities.get(&id.attempt).cloned();
        let cached = known.is_some();
        let attempt = id.attempt;
        let expected = self.reads().snapshot(move|sql|{
            // Only the capability is read out of the frozen request, and only
            // once per attempt: every guardian request authenticates.
            let row:Option<StoredAttempt>=sql.query_row("SELECT r.project_id,r.step_id,r.generation,r.work_generation,CASE WHEN ?3 THEN NULL WHEN r.step_id IS NULL THEN a.request->'$.function.bundle.capability' ELSE a.request->'$.provenance.runtime.capability' END FROM runs r JOIN attempts a USING(attempt_id) WHERE r.run_id=?1 AND r.attempt_id=?2",(id.run.to_string(),id.attempt.to_string(),cached),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
            let Some((project,step,generation,work,saved))=row else{return Err(conflict("unknown attempt").into());};
            let expected:RunCapability=match known{Some(known)=>known,None=>serde_json::from_str(saved.as_deref().unwrap_or("null"))?};
            if project!=id.project.map(|p|p.to_string()) || step!=id.step.as_ref().map(ToString::to_string) || generation as u64!=id.generation.0 || work as u64!=id.work.0 || capability.as_ref()!=Some(&expected){return Err(conflict("attempt identity or capability mismatch").into());}Ok(expected)
        }).await.map_err(|e|e.into_public(false))?;
        if !cached {
            let mut frozen = self.frozen();
            frozen.trim();
            frozen.capabilities.insert(attempt, expected);
        }
        Ok(())
    }
    fn frozen(&self) -> std::sync::MutexGuard<'_, FrozenFacts> {
        self.inner.frozen.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub async fn guardian(
        &self,
        command: CoordinatorCommand,
        capability: Option<&RunCapability>,
    ) -> Result<CoordinatorReply, PublicError> {
        let id = guardian_key(&command).clone();
        self.authenticate(&id, capability).await?;
        match command {
            CoordinatorCommand::Claim(g) => {
                // Either name: a guardian an older release launched may claim after a deploy.
                if sluice_process::systemd::TransientService::adopt(id.run, &g.unit).is_err()
                    || g.process.pid == 0
                    || g.socket_challenge.is_empty()
                {
                    return Err(conflict("invalid guardian identity"));
                }
                let home = self.home_id();
                let accepted=crate::install::admission_write(self.writer(),RetrySafety::Idempotent,move|tx|{
                    let (phase,cancel):(String,bool)=tx.sql().query_row("SELECT phase,cancel_requested FROM attempts WHERE attempt_id=?1",[id.attempt.to_string()],|r|Ok((r.get(0)?,r.get(1)?)))?;
                    if phase=="terminal" || cancel {return Ok(false);}
                    let stored=stored_guardian(tx.sql(),&id,home)?;
                    if let Some(old)=stored{return Ok(old==g);}
                    let identity=store_guardian(&g);
                    if id.step.is_some(){attempts::claim(tx,&step_identity(&id)?,identity)}else{
                        let n=tx.sql().execute("UPDATE attempts SET phase='claimed' WHERE attempt_id=?1 AND phase='reserved' AND spawn_attempted=1 AND cancel_requested=0",[id.attempt.to_string()])?;
                        if n>0{persist_guardian(tx,&id,identity)?;tx.changed(id.project,"calls");}Ok(n>0)
                    }
                }).await?;
                Ok(CoordinatorReply::Claimed(accepted))
            }
            CoordinatorCommand::Started {
                invocation,
                executor,
                ..
            } => {
                self.started(id, invocation, executor).await?;
                Ok(CoordinatorReply::Started)
            }
            CoordinatorCommand::CancelIntent(_) => {
                let cancelled = self
                    .reads()
                    .snapshot(move |sql| {
                        Ok(sql.query_row(
                            "SELECT cancel_requested FROM attempts WHERE attempt_id=?1",
                            [id.attempt.to_string()],
                            |r| r.get(0),
                        )?)
                    })
                    .await
                    .map_err(|e| e.into_public(true))?;
                Ok(CoordinatorReply::CancelIntent(cancelled))
            }
            CoordinatorCommand::Messages {
                after,
                through,
                limit,
                ..
            } => {
                if limit == 0 || limit > 128 || after.0 < 0 {
                    return Err(conflict("invalid delivery window"));
                }
                Ok(CoordinatorReply::Messages(
                    self.offer_messages(id, after, through, limit).await?,
                ))
            }
            CoordinatorCommand::Watch { after, wait_ms, .. } => {
                if after.0 < 0 {
                    return Err(conflict("invalid delivery window"));
                }
                self.watch(id, after, Duration::from_millis(wait_ms)).await
            }
            CoordinatorCommand::DeliverAck { ack, .. } => {
                // A guardian repeats an ack only while unsure it was taken.
                let check = id.clone();
                let message = ack.message;
                let recorded = self.reads().snapshot(move |sql| {
                    let Some(project) = check.project else { return Ok(true) };
                    Ok(sql.query_row(
                        "SELECT EXISTS(SELECT 1 FROM message_deliveries WHERE project_id=?1 AND run_id=?2 AND message_id=?3 AND acknowledged_at IS NOT NULL)",
                        (project.to_string(), check.run.to_string(), message.0), |r| r.get::<_, bool>(0),
                    )?)
                }).await.map_err(|e| e.into_public(true))?;
                if recorded {
                    let invocation = ack.invocation;
                    let check = id.clone();
                    self.reads()
                        .snapshot(move |sql| require_invocation(sql, &check, invocation))
                        .await
                        .map_err(|e| e.into_public(false))?;
                    return Ok(CoordinatorReply::Ack);
                }
                self.writer()
                    .write(RetrySafety::Idempotent, move |tx| {
                        require_invocation(tx.sql(), &id, ack.invocation)?;
                        if let Some(project) = id.project {
                            let recorded: bool = tx.sql().query_row(
                                "SELECT EXISTS(SELECT 1 FROM message_deliveries WHERE project_id=?1 AND run_id=?2 AND message_id=?3 AND acknowledged_at IS NOT NULL)",
                                (project.to_string(), id.run.to_string(), ack.message.0), |r| r.get(0),
                            )?;
                            if recorded { return Ok(()); }
                            ensure_current(tx.sql(), &id)?;
                            sluice_store::messages::acknowledge_delivery(
                                tx,
                                project,
                                id.run,
                                ack.message,
                            )?;
                        }
                        Ok(())
                    })
                    .await?;
                Ok(CoordinatorReply::Ack)
            }
            CoordinatorCommand::Submissions(_) => Ok(CoordinatorReply::Submissions(
                self.submissions(id.run).await?,
            )),
            CoordinatorCommand::Complete(journal) => {
                Ok(CoordinatorReply::Completed(self.complete(*journal).await?))
            }
            CoordinatorCommand::Callback { request, .. } => {
                if request.protocol != rpc::PROTOCOL_VERSION
                    || request.run_capability.as_ref() != capability
                {
                    return Err(conflict("callback capability mismatch"));
                }
                self.callback(id, *request)
                    .await
                    .map(|r| CoordinatorReply::Callback(Box::new(r)))
            }
        }
    }
    /// Hold a guardian's watch until cancellation is requested or a message
    /// after `after` is addressed to its step (offered as `Messages` would),
    /// or until `wait` (at most `MAX_WATCH`) passes. It reads only when the
    /// project's log or messages commit: a cancel and a message both append to
    /// the log. A run that is no longer current is offered nothing, as before.
    async fn watch(
        &self,
        id: AttemptKey,
        after: MessageId,
        wait: Duration,
    ) -> Result<CoordinatorReply, PublicError> {
        let deadline = tokio::time::Instant::now() + wait.min(socket::MAX_WATCH);
        let keys = vec![
            ChangeKey::new(id.project, "log"),
            ChangeKey::new(id.project, "messages"),
        ];
        let mut changes = self
            .reads()
            .subscribe(self.writer(), keys)
            .await
            .map_err(|e| e.into_public(true))?;
        loop {
            let check = id.clone();
            let (cancelled, pending) = self
                .reads()
                .snapshot(move |sql| {
                    let cancelled: bool = sql.query_row(
                        "SELECT cancel_requested FROM attempts WHERE attempt_id=?1",
                        [check.attempt.to_string()],
                        |r| r.get(0),
                    )?;
                    let (Some(project), Some(step)) = (check.project, &check.step) else {
                        return Ok((cancelled, false));
                    };
                    let pending = !cancelled
                        && current_run(sql, &check)?
                        && sql.query_row(
                            "SELECT EXISTS(SELECT 1 FROM messages WHERE project_id=?1 AND \"to\"=?2 AND id>?3)",
                            (project.to_string(), step.as_str(), after.0),
                            |r| r.get::<_, bool>(0),
                        )?;
                    Ok((cancelled, pending))
                })
                .await
                .map_err(|e| e.into_public(true))?;
            if cancelled || pending {
                let messages = if pending {
                    match self.offer_messages(id.clone(), after, None, 128).await {
                        Ok(messages) => messages,
                        Err(PublicError::Conflict { .. }) => Vec::new(),
                        Err(e) => return Err(e),
                    }
                } else {
                    Vec::new()
                };
                if cancelled || !messages.is_empty() {
                    return Ok(CoordinatorReply::Watched {
                        cancelled,
                        messages,
                    });
                }
            }
            tokio::select! {
                changed = changes.wait() => { changed.map_err(|e| e.into_public(true))?; }
                _ = tokio::time::sleep_until(deadline) => {
                    return Ok(CoordinatorReply::Watched { cancelled: false, messages: Vec::new() });
                }
            }
        }
    }
    /// Offer the messages after `after` addressed to the attempt's step,
    /// recording each delivery. Reads first: the write runs only for new ones.
    async fn offer_messages(
        &self,
        id: AttemptKey,
        after: MessageId,
        through: Option<MessageId>,
        limit: u16,
    ) -> Result<Vec<socket::DeliveryMessage>, PublicError> {
        let check = id.clone();
        let pending = self
            .reads()
            .snapshot(move |sql| {
                ensure_current(sql, &check)?;
                let (Some(project), Some(step)) = (check.project, check.step) else {
                    return Ok(false);
                };
                Ok(sql.query_row(
                    "SELECT EXISTS(SELECT 1 FROM messages WHERE project_id=?1 AND \"to\"=?2 AND id>?3 AND (?4 IS NULL OR id<=?4))",
                    (project.to_string(), step.as_str(), after.0, through.map(|m| m.0)),
                    |r| r.get::<_, bool>(0),
                )?)
            })
            .await
            .map_err(|e| e.into_public(false))?;
        if !pending {
            return Ok(Vec::new());
        }
        let messages = self.writer().write(RetrySafety::Idempotent,move|tx|{
                    ensure_current(tx.sql(), &id)?;
                    let Some(project)=id.project else{return Ok(vec![]);};let Some(step)=id.step else{return Ok(vec![]);};
                    let mut q=tx.sql().prepare("SELECT id FROM messages WHERE project_id=?1 AND \"to\"=?2 AND id>?3 AND (?4 IS NULL OR id<=?4) ORDER BY id LIMIT ?5")?;
                    let ids=q.query_map((project.to_string(),step.as_str(),after.0,through.map(|m|m.0),limit),|r|r.get::<_,i64>(0))?.collect::<Result<Vec<_>,_>>()?;drop(q);
                    let mut out=vec![];for message in ids{
                        tx.sql().execute("INSERT INTO message_deliveries(project_id,run_id,message_id,assigned_at) VALUES (?1,?2,?3,strftime('%Y-%m-%dT%H:%M:%fZ','now')) ON CONFLICT DO NOTHING",(project.to_string(),id.run.to_string(),message))?;
                        out.push(socket::DeliveryMessage{id:MessageId(message),body:serde_json::to_value(sluice_store::messages::message(tx.sql(),project,MessageId(message))?)?.try_into()?});
                    }
                    if !out.is_empty(){tx.changed(Some(project),"messages");}Ok(out)
                }).await?;
        Ok(messages)
    }
    async fn record_start(
        &self,
        id: AttemptKey,
        invocation: InvocationId,
        executor: ProcessIdentity,
    ) -> Result<bool, PublicError> {
        // A guardian repeats a start only while unsure it was taken. A start
        // already recorded is answered from a read of that one entry, not by
        // parsing the whole frozen request inside the writer.
        let check = id.clone();
        let evidence = json!({"invocation":invocation,"executor":executor});
        let key = (id.attempt, invocation);
        let known = self.frozen().starts.get(&key).cloned();
        if known.as_ref().is_some_and(|old| *old != evidence) {
            return Err(conflict("start evidence changed"));
        }
        let cached = known.is_some();
        let compare = evidence.clone();
        let recorded = self.reads().snapshot(move |sql| {
            let row: Option<(String, bool, Option<String>)> = sql.query_row(
                "SELECT a.phase,a.cancel_requested,CASE WHEN ?8 THEN NULL ELSE (SELECT value FROM json_each(a.request,'$.runtime_starts') WHERE json_extract(value,'$.invocation')=json_extract(?7,'$')) END FROM attempts a JOIN runs r USING(attempt_id) WHERE r.run_id=?1 AND a.attempt_id=?2 AND r.project_id IS ?3 AND r.step_id IS ?4 AND r.generation=?5 AND r.work_generation=?6",
                (check.run.to_string(), check.attempt.to_string(), check.project.map(|p|p.to_string()), check.step.as_ref().map(ToString::to_string), i64::try_from(check.generation.0).map_err(|_| conflict("generation overflow"))?, i64::try_from(check.work.0).map_err(|_| conflict("work generation overflow"))?, serde_json::to_string(&compare["invocation"])?, cached),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            ).optional()?;
            let Some((phase, cancelled, old)) = row else { return Ok(None) };
            if !cached {
                let Some(old) = old else { return Ok(None) };
                if serde_json::from_str::<Value>(&old)? != compare {
                    return Err(conflict("start evidence changed").into());
                }
            }
            Ok(Some(phase != "terminal" && !cancelled && current_run(sql, &check)?))
        }).await.map_err(|e| e.into_public(false))?;
        if let Some(open) = recorded {
            if !cached {
                let mut frozen = self.frozen();
                frozen.trim();
                frozen.starts.insert(key, evidence);
            }
            return Ok(open);
        }
        self.writer().write(RetrySafety::Idempotent, move |tx| {
            let (phase, cancelled, raw): (String, bool, String) = tx.sql().query_row(
                "SELECT a.phase,a.cancel_requested,a.request FROM attempts a JOIN runs r USING(attempt_id) WHERE r.run_id=?1 AND a.attempt_id=?2 AND r.project_id IS ?3 AND r.step_id IS ?4 AND r.generation=?5 AND r.work_generation=?6",
                (id.run.to_string(), id.attempt.to_string(), id.project.map(|p|p.to_string()), id.step.as_ref().map(ToString::to_string), i64::try_from(id.generation.0).map_err(|_| conflict("generation overflow"))?, i64::try_from(id.work.0).map_err(|_| conflict("work generation overflow"))?),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            let mut request: Value = serde_json::from_str(&raw)?;
            let evidence = json!({"invocation":invocation,"executor":executor});
            let starts = request.as_object_mut().ok_or_else(|| conflict("frozen request shape"))?
                .entry("runtime_starts").or_insert(json!([])).as_array_mut()
                .ok_or_else(|| conflict("start evidence shape"))?;
            if let Some(old) = starts.iter().find(|v| v["invocation"] == json!(invocation)) {
                if *old != evidence { return Err(conflict("start evidence changed").into()); }
                return Ok(phase != "terminal" && !cancelled && current_run(tx.sql(), &id)?);
            }
            ensure_current(tx.sql(), &id)?;
            if starts.len() >= 1024 { return Err(conflict("too many invocation starts").into()); }
            if phase == "claimed" {
                // Start evidence describes an executor already granted by the OS.
                // Cancellation closes dispatch, but cannot erase that history.
                if id.step.is_some() && !cancelled {
                    if !attempts::started(tx, &step_identity(&id)?, &mut Hooks)? {
                        return Err(conflict("start refused").into());
                    }
                } else {
                    tx.sql().execute("UPDATE attempts SET phase='executing' WHERE attempt_id=?1 AND phase='claimed'", [id.attempt.to_string()])?;
                    tx.sql().execute("UPDATE runs SET started_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE run_id=?1", [id.run.to_string()])?;
                    if let Some(project) = id.project.filter(|_| id.step.is_some()) {
                        sluice_store::messages::advance_cursor(tx, project, id.run)?;
                    }
                }
            } else if phase != "executing" {
                return Err(conflict("attempt cannot acknowledge start").into());
            }
            starts.push(evidence);
            tx.sql().execute("UPDATE attempts SET request=?2 WHERE attempt_id=?1", (id.attempt.to_string(), request.to_string()))?;
            tx.changed(id.project, "status");
            Ok(!cancelled)
        }).await
    }
    async fn started(
        &self,
        id: AttemptKey,
        invocation: InvocationId,
        executor: ProcessIdentity,
    ) -> Result<(), PublicError> {
        if self.record_start(id, invocation, executor).await? {
            Ok(())
        } else {
            Err(PublicError::Cancelled {
                message: "start recorded; dispatch closed".into(),
            })
        }
    }
    pub(crate) async fn callback(
        &self,
        id: AttemptKey,
        request: RpcRequest,
    ) -> Result<CommandReply, PublicError> {
        // The bearer grants callbacks for this admitted identity only. Never expose
        // arbitrary project editing, calls or privileged capacity admission to it.
        match &request.command {
            CommandRequest::StepSubmit(s)
                if Some(s.project) == id.project
                    && Some(&s.step) == id.step.as_ref()
                    && s.run == id.run => {}
            CommandRequest::Submission { run } if *run == id.run => {}
            // Its own step's progress: the store checks the run is that step's current one.
            CommandRequest::StepProgress(p)
                if Some(&p.step) == id.step.as_ref() && p.run == id.run => {}
            CommandRequest::MessagePost(m)
                if m.run == Some(id.run)
                    && m.project
                        == ProjectSelector::Id(
                            id.project
                                .ok_or_else(|| conflict("callback needs a project"))?,
                        )
                    && m.from.as_deref() == id.step.as_ref().map(StepId::as_str) => {}
            command @ (CommandRequest::Ask(_)
            | CommandRequest::Say(_)
            | CommandRequest::Reply(_))
                if run_speaks(
                    command,
                    id.run,
                    id.project
                        .ok_or_else(|| conflict("callback needs a project"))?,
                ) => {}
            CommandRequest::AcquireLease(l) if l.run == id.run && id.project.is_some() => {}
            CommandRequest::ReleaseLease(l) if l.run == id.run => {}
            CommandRequest::RegisterCompletionAction(a)
                if a.run == id.run && Some(a.project) == id.project => {}
            command if project_mutation(command) => {}
            _ => return Err(conflict("callback command is outside the run authority")),
        }
        // A run's plan edits and section-lease requests are decided against
        // reconciled runs, like a user's; its other callbacks are its own.
        if project_mutation(&request.command)
            || matches!(request.command, CommandRequest::AcquireLease(_))
        {
            self.ready().await?;
        }
        let key = request.request_id.0;
        let command = request.command;
        let encoded = serde_json::to_value(&command).map_err(storage)?;
        let refresh = matches!(command, CommandRequest::AcquireLease(_));
        let mut tries = 0;
        let reply = loop {
            let catalog = self.inner.catalog.clone();
            let plans = self.inner.plans.clone();
            let home = self.home().to_owned();
            // A plan edit is prepared outside the writer; if what it was prepared from
            // has changed by the time the writer takes it, it is prepared again, and after
            // EDIT_TRIES such tries it is prepared in the writer.
            let outside = if tries < EDIT_TRIES {
                OutsideEdit::prepare(self, &command).await
            } else {
                None
            };
            let (id, command) = (id.clone(), command.clone());
            let attempt = self
                .callback_attempt(
                    id.clone(),
                    key.clone(),
                    encoded.clone(),
                    refresh,
                    move |tx| callback_mutation(tx, &id, command, &catalog, &plans, &home, outside),
                )
                .await?;
            match attempt {
                Some((reply, plan)) => {
                    // The plan an edit committed, kept rather than freed in the writer.
                    if let Some(plan) = plan {
                        self.inner.plans.put(plan);
                    }
                    break reply;
                }
                None => tries += 1,
            }
        };
        if matches!(reply, CommandReply::Project(_)) {
            artifacts::recover(self.writer(), self.home())
                .await
                .map_err(|e| e.into_public(false))?;
        }
        Ok(reply)
    }
    async fn callback_transaction<F>(
        &self,
        id: AttemptKey,
        key: String,
        command: Value,
        refresh: bool,
        mutate: F,
    ) -> Result<CommandReply, PublicError>
    where
        F: FnOnce(&mut sluice_store::WriteTransaction<'_>) -> sluice_store::Result<CommandReply>
            + Send
            + 'static,
    {
        self.callback_attempt(id, key, command, refresh, move |tx| {
            Ok(Some((mutate(tx)?, ())))
        })
        .await?
        .map(|(reply, ())| reply)
        .ok_or_else(|| PublicError::Storage {
            message: "callback gave no reply".into(),
        })
    }
    /// `callback_transaction` for a mutation that can find what it prepared outside the
    /// writer changed: it answers None, nothing is written or cached, and the caller
    /// prepares it again. Beside its reply (the one cached) it may hand back what it is
    /// done with, to be freed or kept outside the writer; a cached reply has none.
    async fn callback_attempt<F, T>(
        &self,
        id: AttemptKey,
        key: String,
        command: Value,
        refresh: bool,
        mutate: F,
    ) -> Result<Option<(CommandReply, T)>, PublicError>
    where
        F: FnOnce(
                &mut sluice_store::WriteTransaction<'_>,
            ) -> sluice_store::Result<Option<(CommandReply, T)>>
            + Send
            + 'static,
        T: Default + Send + 'static,
    {
        self.writer()
            .write(RetrySafety::Idempotent, move |tx| {
                let raw: String = tx.sql().query_row(
                    "SELECT request FROM attempts WHERE attempt_id=?1",
                    [id.attempt.to_string()],
                    |r| r.get(0),
                )?;
                let mut frozen: Value = serde_json::from_str(&raw)?;
                let cache = frozen
                    .as_object_mut()
                    .ok_or_else(|| conflict("attempt request shape"))?
                    .entry("runtime_callbacks")
                    .or_insert(json!({}))
                    .as_object_mut()
                    .ok_or_else(|| conflict("callback cache shape"))?;
                if let Some(saved) = cache.get(&key) {
                    if saved["command"] != command {
                        return Err(conflict("callback request ID reused").into());
                    }
                    if !refresh || !current_run(tx.sql(), &id)? {
                        return Ok(Some((
                            serde_json::from_value(saved["reply"].clone())?,
                            T::default(),
                        )));
                    }
                }
                ensure_current(tx.sql(), &id)?;
                if cache.len() >= 16384 {
                    return Err(conflict("callback cache limit").into());
                }
                let Some((reply, done)) = mutate(tx)? else {
                    return Ok(None);
                };
                cache.insert(key, json!({"command":command,"reply":reply}));
                tx.sql().execute(
                    "UPDATE attempts SET request=?2 WHERE attempt_id=?1",
                    (id.attempt.to_string(), frozen.to_string()),
                )?;
                tx.changed(id.project, "status");
                Ok(Some((reply, done)))
            })
            .await
    }
    /// A plan edit command: prepared outside the writer and committed in it while what it
    /// was prepared from holds; after EDIT_TRIES tries that found it changed, prepared and
    /// committed in the writer.
    async fn plan_edit(&self, command: CommandRequest) -> Result<CommandReply, PublicError> {
        let catalog = self.inner.catalog.clone();
        let home = self.home().to_owned();
        for _ in 0..EDIT_TRIES {
            let Some(outside) = OutsideEdit::prepare(self, &command).await else {
                break;
            };
            let catalog = catalog.clone();
            if let Some((reply, plan)) = self
                .writer()
                .write(RetrySafety::NonIdempotent, move |tx| {
                    commit_outside(tx, &catalog, outside)
                })
                .await?
            {
                if let Some(plan) = plan {
                    self.inner.plans.put(plan);
                }
                return Ok(reply);
            }
        }
        self.writer()
            .write(RetrySafety::NonIdempotent, move |tx| {
                mutate_project(tx, &catalog, &home, command)
            })
            .await
    }
    async fn helper(
        &self,
        forwarded: crate::compose::ForwardHelper,
        capability: Option<&RunCapability>,
    ) -> Result<CommandReply, PublicError> {
        use crate::python::{HelperCommand, HelperExtension};
        let id = forwarded.identity;
        self.authenticate(&id, capability).await?;
        let request = forwarded.request;
        if request.protocol != rpc::PROTOCOL_VERSION
            || request.run_capability.as_ref() != capability
        {
            return Err(conflict("helper capability mismatch"));
        }
        match request.command {
            HelperCommand::Runtime(command) => {
                self.callback(
                    id,
                    RpcRequest {
                        protocol: request.protocol,
                        request_id: request.request_id,
                        run_capability: request.run_capability,
                        command,
                    },
                )
                .await
            }
            HelperCommand::Extension(HelperExtension::RetryOnFailure(action)) => {
                if Some(action.project) != id.project
                    || action.run != id.run
                    || action.message.len() > 8192
                    || action.message.trim().is_empty()
                {
                    return Err(conflict("retry action outside run authority"));
                }
                let encoded = json!({"retry_on_failure":action});
                self.callback_transaction(
                    id.clone(),
                    request.request_id.0,
                    encoded,
                    false,
                    move |tx| {
                        let saved: Option<String> = tx.sql().query_row(
                            "SELECT completion_action FROM runs WHERE run_id=?1",
                            [id.run.to_string()],
                            |r| r.get(0),
                        )?;
                        let target = if let Some(saved) = saved {
                            let saved: Value = serde_json::from_str(&saved)?;
                            if saved["target"]["step"] != json!(action.step)
                                || saved["message"] != json!(action.message)
                            {
                                return Err(conflict("completion action changed").into());
                            }
                            serde_json::from_value(saved["target"].clone())?
                        } else {
                            attempts::completion_target(tx, action.project, &action.step)?
                                .ok_or_else(|| conflict("target has no completed result"))?
                        };
                        if !attempts::register_completion_action(
                            tx,
                            RegisterCompletionAction {
                                project: action.project,
                                run: action.run,
                                target,
                                message: action.message,
                                author: action.author,
                            },
                            &Hooks,
                        )? {
                            return Err(conflict("stale completion registration").into());
                        }
                        Ok(CommandReply::Ack)
                    },
                )
                .await
            }
            HelperCommand::Extension(HelperExtension::Tool(tool)) => {
                if tool.name.starts_with("message.") {
                    let project = id
                        .project
                        .ok_or_else(|| conflict("message fn needs a project"))?;
                    let context = self.context(project).await?;
                    let ctx = crate::builtins::messages::MessageCtx {
                        project,
                        step: id.step.clone(),
                        run: Some(id.run),
                        writer: self.writer().clone(),
                        reads: self.reads().clone(),
                        plan_inputs: Arc::new(InputSetter(context)),
                        cancel: CancellationToken::new(),
                        mutation_guard: Some(Arc::new({
                            let id = id.clone();
                            move |tx| ensure_current(tx.sql(), &id)
                        })),
                    };
                    // A cancel appends to the project's log: recheck it on log
                    // commits instead of every 100 ms.
                    let mut changes = self
                        .reads()
                        .subscribe(self.writer(), vec![ChangeKey::new(Some(project), "log")])
                        .await
                        .map_err(|e| e.into_public(true))?;
                    let operation =
                        crate::builtins::messages::dispatch(&tool.name, &tool.args, &ctx);
                    // The operation is polled alongside the cancel check, never left parked
                    // while the check awaits a read: a parked operation keeps the read pool
                    // permit it has been granted, and with as many waiting asks as the pool
                    // has connections every read in the coordinator waited forever.
                    let cancelled = async {
                        loop {
                            if matches!(
                                self.guardian(
                                    CoordinatorCommand::CancelIntent(id.clone()),
                                    capability
                                )
                                .await?,
                                CoordinatorReply::CancelIntent(true)
                            ) {
                                return Ok::<_, PublicError>(());
                            }
                            changes.wait().await.map_err(|e| e.into_public(true))?;
                        }
                    };
                    tokio::select! {
                        result = operation => return result.map_err(storage).and_then(data),
                        cancelled = cancelled => {
                            cancelled?;
                            ctx.cancel.cancel();
                            return Err(PublicError::Cancelled {
                                message: "message wait cancelled".into(),
                            });
                        }
                    }
                }
                let author = id.step.as_ref().map(|step| format!("step:{step}"));
                let mut tool = tool;
                // `ctx.tool("step_progress", {...})` is about the run's own step by default.
                if tool.name == "step_progress" {
                    let text = |s: String| sluice_model::rpc::JsonValue::try_from(Value::String(s));
                    let args = &mut tool.args.0;
                    if let Some(step) = &id.step
                        && !args.contains_key("step")
                    {
                        args.insert("step".into(), text(step.to_string())?);
                    }
                    if !args.contains_key("run") {
                        args.insert("run".into(), text(id.run.to_string())?);
                    }
                }
                let mut command = crate::compose::decode_tool(tool, author.as_deref())?;
                // Named callbacks use the same command adapters with the run's
                // immutable project and submission identity.
                match &mut command {
                    CommandRequest::MessagePost(m) => {
                        m.run = Some(id.run);
                        m.from = id.step.as_ref().map(ToString::to_string);
                    }
                    CommandRequest::Ask(m) => (m.run, m.owner) = (Some(id.run), false),
                    CommandRequest::Say(m) => (m.run, m.owner) = (Some(id.run), false),
                    CommandRequest::Reply(m) => (m.run, m.owner) = (Some(id.run), false),
                    _ => {}
                }
                let reply = if matches!(
                    &command,
                    CommandRequest::StepSubmit(_)
                        | CommandRequest::StepProgress(_)
                        | CommandRequest::Submission { .. }
                        | CommandRequest::MessagePost(_)
                        | CommandRequest::Ask(_)
                        | CommandRequest::Say(_)
                        | CommandRequest::Reply(_)
                        | CommandRequest::AcquireLease(_)
                        | CommandRequest::ReleaseLease(_)
                        | CommandRequest::RegisterCompletionAction(_)
                ) || project_mutation(&command)
                {
                    self.callback(
                        id,
                        RpcRequest {
                            protocol: request.protocol,
                            request_id: request.request_id,
                            run_capability: request.run_capability,
                            command,
                        },
                    )
                    .await?
                } else {
                    // Public project tools use the public command service. The
                    // helper bearer remains confined to its admitted project.
                    let encoded = serde_json::to_value(&command).map_err(storage)?;
                    let selector = encoded["args"]
                        .get("project")
                        .filter(|p| !p.is_null())
                        .ok_or_else(|| conflict("tool requires the run's project"))?;
                    let selector: ProjectSelector =
                        serde_json::from_value(selector.clone()).map_err(storage)?;
                    let project = crate::calls::resolve(self.reads(), Some(selector)).await?;
                    if project != id.project {
                        return Err(conflict("tool project differs from run"));
                    }
                    if !matches!(
                        command,
                        CommandRequest::PlanGet { .. }
                            | CommandRequest::BoardGet { .. }
                            | CommandRequest::Status(_)
                            | CommandRequest::Messages(_)
                            | CommandRequest::LogRead(_)
                            | CommandRequest::StepWait(_)
                            | CommandRequest::FnList { .. }
                            | CommandRequest::FnGet { .. }
                            | CommandRequest::CallStatus { .. }
                    ) {
                        return Err(conflict("tool mutation is outside the run authority"));
                    }
                    self.command(command).await?
                };
                data(crate::compose::reply_value(reply)?)
            }
        }
    }
    pub async fn complete(&self, journal: CompletionJournal) -> Result<DurableAck, PublicError> {
        journal.validate(&journal.identity).map_err(storage)?;
        if journal.identity.home != self.home_id() {
            return Err(conflict("completion home mismatch"));
        }
        let id = &journal.identity;
        // Real hosts independently recheck containment. Fake hosts own their proof.
        if !self.host().cleanup_valid(&journal).await? {
            return Err(conflict("payload cleanup is not proven"));
        }
        for start in &journal.starts {
            self.record_start(id.clone(), start.invocation, start.executor.clone())
                .await?;
        }
        let snapshot = self.submissions(id.run).await?;
        if snapshot.version != journal.submission_version || snapshot.fields != journal.submissions
        {
            return Err(conflict("completion submissions changed"));
        }
        let completion_id = journal.completion_id.clone();
        let run = id.run;
        if id.step.is_some() {
            let catalog = self.inner.catalog.clone();
            self.writer()
                .write(RetrySafety::Idempotent, move |tx| {
                    let id = step_identity(&journal.identity)?;
                    let prior: Option<String> = tx.sql().query_row(
                        "SELECT completion_id FROM runs WHERE run_id=?1 AND attempt_id=?2 AND project_id=?3 AND step_id=?4 AND generation=?5 AND work_generation=?6",
                        (id.run.to_string(), id.attempt.to_string(), id.project.to_string(), id.step.as_str(), i64::try_from(id.generation.0).map_err(|_| conflict("generation overflow"))?, i64::try_from(id.work.0).map_err(|_| conflict("work generation overflow"))?), |r| r.get(0),
                    )?;
                    if let Some(prior) = prior {
                        return if prior == journal.completion_id { Ok(()) } else { Err(conflict("stale completion").into()) };
                    }
                    let raw: String = tx.sql().query_row(
                        "SELECT request FROM attempts WHERE attempt_id=?1",
                        [id.attempt.to_string()],
                        |r| r.get(0),
                    )?;
                    let mut frozen: Value = serde_json::from_str(&raw)?;
                    let admitted: crate::execution::FrozenPlan = serde_json::from_value(
                        frozen["provenance"]["runtime"]["completion"].clone(),
                    )?;
                    let admitted = admitted.context(id.project)?;
                    let current = match context(tx.sql(), id.project, &catalog) {
                        Ok(ctx) => Ok(ctx),
                        Err(StoreError::Public(error)) => Err(error.to_string()),
                        Err(error) => return Err(error),
                    };
                    frozen["runtime_reconciliation_error"] =
                        current.as_ref().err().map_or(Value::Null, |e| json!(e));
                    tx.sql().execute(
                        "UPDATE attempts SET request=?2 WHERE attempt_id=?1",
                        (id.attempt.to_string(), frozen.to_string()),
                    )?;
                    for ack in &journal.delivery_acks {
                        require_invocation(tx.sql(), &journal.identity, ack.invocation)?;
                        sluice_store::messages::acknowledge_delivery(
                            tx,
                            id.project,
                            id.run,
                            ack.message,
                        )?;
                    }
                    let (kind, outputs) = completion_kind(journal.result);
                    if attempts::complete_frozen(
                        tx,
                        attempts::CompletionContext {
                            admitted: &admitted,
                            current: current.as_ref().map_err(String::as_str),
                        },
                        attempts::Complete {
                            identity: id,
                            completion_id: journal.completion_id,
                            kind,
                            outputs,
                            processes_gone: true,
                            submission_version: journal.submission_version,
                        },
                        &mut Hooks,
                    )?
                    .is_none()
                    {
                        return Err(conflict("stale completion").into());
                    }
                    Ok(())
                })
                .await?;
        } else {
            let (_, outputs) = completion_kind(journal.result.clone());
            let error = match journal.result {
                PayloadResult::Succeeded(_) => None,
                PayloadResult::Failed(e) => Some(e),
                PayloadResult::Cancelled(message) => Some(PublicError::Cancelled { message }),
                PayloadResult::Rejected(message) => Some(PublicError::FnFailure { message }),
                PayloadResult::Lost(message) | PayloadResult::Unknown(message) => {
                    Some(PublicError::ProcessLost { message })
                }
            };
            if !calls::complete(
                self.writer(),
                calls::CallCompletion {
                    call: run,
                    attempt: id.attempt,
                    project: id.project,
                    completion_id: completion_id.clone(),
                    outputs,
                    error,
                    processes_gone: true,
                },
            )
            .await?
            {
                return Err(conflict("stale call completion"));
            }
        }
        Ok(DurableAck { run, completion_id })
    }
    /// One adoption pass over every nonterminal attempt, at most
    /// [`ADOPTION_PARALLELISM`] runs at a time.
    pub async fn adopt(&self) -> Result<(), PublicError> {
        self.adopt_bounded(ADOPTION_PARALLELISM).await
    }
    /// An adoption pass with at most `parallelism` runs in flight.
    pub async fn adopt_bounded(&self, parallelism: usize) -> Result<(), PublicError> {
        self.adopt_until(parallelism, &CancellationToken::new())
            .await
    }
    /// Each run's adoption touches only that run (its run dir, unit, cgroup and
    /// attempt rows) and never launches, so runs adopt concurrently. A run's
    /// failure is deferred to the next pass and does not stop the others; a
    /// run another pass is adopting is skipped. `stop` ends the pass before the
    /// next run starts, letting those in flight finish.
    async fn adopt_until(
        &self,
        parallelism: usize,
        stop: &CancellationToken,
    ) -> Result<(), PublicError> {
        let home = self.home().to_path_buf();
        let home_id = self.home_id();
        let attempts=self.reads().snapshot(move|sql|{
            let mut q=sql.prepare("SELECT r.run_id,r.attempt_id,r.project_id,r.step_id,r.generation,r.work_generation,a.request,r.unit_name,r.cgroup FROM runs r JOIN attempts a USING(attempt_id) WHERE a.phase<>'terminal'")?;
            let mut rows=q.query([])?;let mut out=vec![];
            while let Some(row)=rows.next()?{
                let run:RunId=calls::parse_id(row.get(0)?)?;let id=AttemptKey{home:home_id,run,attempt:calls::parse_id(row.get(1)?)?,project:row.get::<_,Option<String>>(2)?.map(calls::parse_id).transpose()?,step:row.get::<_,Option<String>>(3)?.map(calls::parse_id).transpose()?,generation:StepGeneration(row.get::<_,i64>(4)? as u64),work:WorkGeneration(row.get::<_,i64>(5)? as u64)};
                let raw:String=row.get(6)?;let frozen:Value=serde_json::from_str(&raw)?;let capability=serde_json::from_value(if id.step.is_some(){frozen["provenance"]["runtime"]["capability"].clone()}else{frozen["function"]["bundle"]["capability"].clone()})?;
                let guardian=stored_guardian(sql,&id,home_id)?;let cgroup:Option<String>=row.get(8)?;
                out.push(AdoptionAttempt{identity:id.clone(),guardian,run_dir:home.join("runs").join(run.to_string()),unit:row.get::<_,Option<String>>(7)?.unwrap_or_else(||sluice_process::systemd::TransientService::for_launch(run).name().into()),service_cgroup:cgroup.map(|c|c.strip_suffix("/control").unwrap_or(&c).to_string()),capability});
            }Ok(out)
        }).await.map_err(|e|e.into_public(true))?;
        let mut running = JoinSet::new();
        for attempt in attempts {
            while running.len() >= parallelism.max(1) {
                adoption_ended(running.join_next().await);
            }
            if stop.is_cancelled() {
                break;
            }
            let Some(claim) = AdoptionClaim::take(&self.inner.adopting, attempt.identity.run)
            else {
                continue;
            };
            let broker = self.clone();
            running.spawn(async move {
                let _claim = claim;
                let link = LocalLink {
                    broker: broker.clone(),
                    capability: attempt.capability.clone(),
                };
                if let Err(e) = adopt_attempt(&attempt, &link, broker.host()).await {
                    tracing::warn!(run=%attempt.identity.run,error=%e,"adoption deferred");
                }
            });
        }
        while let Some(ended) = running.join_next().await {
            adoption_ended(Some(ended));
        }
        Ok(())
    }
    pub async fn serve(&self, stop: CancellationToken) -> Result<(), PublicError> {
        use std::os::unix::fs::PermissionsExt;
        let path = self.home().join("coordinator.sock");
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(storage(e)),
        }
        let listener = UnixListener::bind(&path).map_err(storage)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(storage)?;
        // Serve at once. Reads, install control, the scheduler lease and the
        // runs' own guardians are answered during the startup pass; other
        // commands wait for it (`ready`), and the scheduler, which does all
        // admission, starts only after it.
        self.inner.gate.send_replace(Gate::Adopting);
        let serving = stop.child_token();
        let mut startup = tokio::spawn({
            let broker = self.clone();
            let stop = serving.clone();
            async move { broker.adopt_until(ADOPTION_PARALLELISM, &stop).await }
        });
        let mut adopting = true;
        let mut scheduler_task = None;
        let mut failure = None;
        let mut clients = JoinSet::new();
        loop {
            tokio::select! {
                _=serving.cancelled()=>break,
                adopted=&mut startup, if adopting=>{
                    adopting=false;
                    match adopted.map_err(storage).and_then(|r| r) {
                        Ok(())=>{
                            self.inner.gate.send_replace(Gate::Ready);
                            scheduler_task=Some(tokio::spawn(scheduler::run(self.clone(),serving.child_token())));
                        }
                        Err(e)=>{failure=Some(e);break;}
                    }
                },
                Some(result)=clients.join_next()=>{if let Err(e)=result{tracing::error!(error=%e,"coordinator client task failed");}},
                accepted=listener.accept()=>{let (stream,_)=accepted.map_err(storage)?;let Ok(permit)=self.inner.connections.clone().try_acquire_owned()else{drop(stream);continue;};let broker=self.clone();let stop=serving.child_token();clients.spawn(async move{if let Err(e)=broker.connection(stream,stop,permit).await{tracing::debug!(error=%e,"socket client closed");}});}
            }
        }
        // Requests still waiting for the pass are refused, not left waiting.
        self.inner.gate.send_if_modified(|gate| {
            let adopting = *gate == Gate::Adopting;
            if adopting {
                *gate = Gate::Stopped;
            }
            adopting
        });
        serving.cancel();
        drop(listener);
        if adopting {
            // The pass stops before its next run; those in flight finish.
            if let Ok(Err(e)) = startup.await {
                tracing::warn!(error=%e, "startup adoption ended early");
            }
        }
        while clients.join_next().await.is_some() {}
        self.calls().close().await;
        if let Some(task) = scheduler_task {
            task.await.map_err(storage)??;
        }
        match std::fs::remove_file(path) {
            Ok(()) => {}
            // A removed home took its socket with it.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(storage(e)),
        }
        self.writer()
            .shutdown()
            .await
            .map_err(|e| e.into_public(false))?;
        failure.map_or(Ok(()), Err)
    }
    async fn connection(
        &self,
        mut stream: UnixStream,
        stop: CancellationToken,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), PublicError> {
        let request: Request<Value> =
            tokio::time::timeout(Duration::from_secs(5), socket::read_frame(&mut stream))
                .await
                .map_err(storage)?
                .map_err(storage)?;
        let route = Route::of(&request.command);
        let request_id = request.request_id.clone();
        // A request that cannot be decoded is answered with its error, never dropped.
        let incoming = match Incoming::decode(request.protocol, &request) {
            Ok(incoming) => incoming,
            Err(error) => {
                tracing::warn!(request = %request_id.0, %error, "coordinator refused a request");
                return route
                    .refuse(&mut stream, request_id, error)
                    .await
                    .map_err(storage);
            }
        };
        let capability = request.run_capability;
        match incoming {
            Incoming::Helper(forwarded) => {
                let broker = self.clone();
                let scope = Arc::new(RequestScope::default());
                let work = crate::contain::contained(
                    "helper",
                    REQUEST.scope(scope.clone(), async move {
                        broker.helper(forwarded.helper, capability.as_ref()).await
                    }),
                );
                let result = answer(&mut stream, &stop, scope, work).await;
                write_reply(
                    &mut stream,
                    &RpcReply {
                        protocol: 1,
                        request_id,
                        result: match result {
                            Ok(v) => RpcResult::Ok(Box::new(v)),
                            Err(e) => RpcResult::Error(e),
                        },
                    },
                )
                .await
                .map_err(storage)?;
            }
            Incoming::Runtime(command) => match command {
                RuntimeCommand::AcquireScheduler { owner } => {
                    let result = self.acquire_scheduler(owner.clone()).await;
                    let accepted = result.is_ok();
                    let response = Reply {
                        protocol: 1,
                        request_id,
                        result: result.map(|()| CommandReply::Ack),
                    };
                    if let Err(e) = write_reply(&mut stream, &response).await {
                        if accepted {
                            self.release_scheduler(owner).await?;
                        }
                        return Err(storage(e));
                    }
                    if accepted {
                        use tokio::io::AsyncReadExt;
                        let mut byte = [0; 1];
                        tokio::select! {_=stream.read(&mut byte)=>{},_=stop.cancelled()=>{}};
                        self.release_scheduler(owner).await?;
                    }
                }
                RuntimeCommand::Changes(cursor) => {
                    let result = self.changes(cursor).await;
                    write_reply(
                        &mut stream,
                        &Reply {
                            protocol: 1,
                            request_id,
                            result,
                        },
                    )
                    .await
                    .map_err(storage)?;
                }
            },
            Incoming::Guardian(command) => {
                let held = matches!(command, CoordinatorCommand::Watch { .. });
                // A held watch idles for up to a minute, one per live guardian:
                // once authenticated it gives its connection permit back (at
                // most `WATCHES_PER_RUN` per run), so the number of live runs
                // never decides whether reads are served.
                let mut _released = None;
                if held
                    && self
                        .authenticate(guardian_key(&command), capability.as_ref())
                        .await
                        .is_ok()
                {
                    _released = WatchSlot::take(&self.inner.watches, guardian_key(&command).run);
                    if _released.is_some() {
                        drop(permit);
                    }
                }
                let broker = self.clone();
                let scope = Arc::new(RequestScope::default());
                let handler = crate::contain::contained(
                    "guardian callback",
                    REQUEST.scope(scope.clone(), async move {
                        broker.guardian(command, capability.as_ref()).await
                    }),
                );
                let result = if held {
                    // A held watch ends when its guardian hangs up or the
                    // coordinator stops; it changes nothing, so dropping it is safe.
                    use tokio::io::AsyncReadExt;
                    let mut byte = [0; 1];
                    tokio::select! {
                        result = handler => result,
                        _ = stream.read(&mut byte) => return Ok(()),
                        _ = stop.cancelled() => return Ok(()),
                    }
                } else {
                    answer(&mut stream, &stop, scope, handler).await
                };
                write_reply(
                    &mut stream,
                    &Reply {
                        protocol: 1,
                        request_id,
                        result,
                    },
                )
                .await
                .map_err(storage)?;
            }
            Incoming::Command(command) => {
                let broker = self.clone();
                let scope = Arc::new(RequestScope::default());
                let work = crate::contain::contained(
                    "command",
                    REQUEST.scope(scope.clone(), async move { broker.command(command).await }),
                );
                let reply = answer(&mut stream, &stop, scope, work).await;
                let result = match reply {
                    Ok(reply) => RpcResult::Ok(Box::new(reply)),
                    Err(e) => RpcResult::Error(e),
                };
                write_reply(
                    &mut stream,
                    &RpcReply {
                        protocol: 1,
                        request_id,
                        result,
                    },
                )
                .await
                .map_err(storage)?;
            }
        }
        Ok(())
    }
}

/// Which reply envelope a request's sender reads: helpers and public commands read an
/// `RpcReply`, runtime and guardian callers a `Reply`.
#[derive(Clone, Copy)]
enum Route {
    Rpc,
    Reply,
}
impl Route {
    fn of(command: &Value) -> Self {
        if command.get("runtime").is_some() || command.get("method").is_some() {
            Self::Reply
        } else {
            Self::Rpc
        }
    }
    async fn refuse(
        self,
        stream: &mut UnixStream,
        request_id: sluice_model::rpc::RequestId,
        error: PublicError,
    ) -> std::io::Result<()> {
        match self {
            Self::Rpc => {
                write_reply(
                    stream,
                    &RpcReply {
                        protocol: 1,
                        request_id,
                        result: RpcResult::Error(error),
                    },
                )
                .await
            }
            Self::Reply => {
                write_reply(
                    stream,
                    &Reply::<Value> {
                        protocol: 1,
                        request_id,
                        result: Err(error),
                    },
                )
                .await
            }
        }
    }
}

/// A request decoded for its route before anything runs.
enum Incoming {
    Helper(crate::compose::HelperWire),
    Runtime(RuntimeCommand),
    Guardian(CoordinatorCommand),
    Command(CommandRequest),
}
impl Incoming {
    fn decode(protocol: u16, request: &Request<Value>) -> Result<Self, PublicError> {
        if protocol != rpc::PROTOCOL_VERSION {
            return Err(conflict("unsupported protocol"));
        }
        let command = &request.command;
        let bytes = serde_json::to_vec(command).map_err(storage)?;
        Ok(if command.get("helper").is_some() {
            Self::Helper(rpc::decode_json(&bytes)?)
        } else if command.get("runtime").is_some() {
            Self::Runtime(rpc::decode_json(&bytes)?)
        } else if command.get("method").is_some() {
            Self::Guardian(rpc::decode_json(&bytes)?)
        } else {
            if request.run_capability.is_some() {
                return Err(conflict("use authenticated guardian callback"));
            }
            Self::Command(rpc::decode_json(&bytes)?)
        })
    }
}

/// Run one request for a connected client. Its scope learns when the client
/// hangs up (a request still waiting for the startup pass is then dropped
/// unrun; one already running finishes as before), and a stop answers with an
/// error that says whether the request ran.
async fn answer<T>(
    stream: &mut UnixStream,
    stop: &CancellationToken,
    scope: Arc<RequestScope>,
    work: impl std::future::Future<Output = Result<T, PublicError>>,
) -> Result<T, PublicError> {
    use tokio::io::AsyncReadExt;
    tokio::pin!(work);
    let mut byte = [0; 1];
    let mut watching = true;
    loop {
        tokio::select! {
            result = &mut work => return result,
            read = stream.read(&mut byte), if watching => {
                watching = false;
                if matches!(read, Ok(0) | Err(_)) {
                    scope.gone.cancel();
                }
            }
            _ = stop.cancelled() => return Err(stopping(&scope)),
        }
    }
}
/// A held watch that gave its connection permit back, counted per run.
struct WatchSlot {
    watches: Arc<std::sync::Mutex<std::collections::HashMap<RunId, usize>>>,
    run: RunId,
}
impl WatchSlot {
    fn take(
        watches: &Arc<std::sync::Mutex<std::collections::HashMap<RunId, usize>>>,
        run: RunId,
    ) -> Option<Self> {
        let mut held = watches.lock().unwrap_or_else(|e| e.into_inner());
        let count = held.entry(run).or_default();
        if *count >= WATCHES_PER_RUN {
            return None;
        }
        *count += 1;
        Some(Self {
            watches: watches.clone(),
            run,
        })
    }
}
impl Drop for WatchSlot {
    fn drop(&mut self) {
        let mut held = self.watches.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = held.get_mut(&self.run) {
            *count -= 1;
            if *count == 0 {
                held.remove(&self.run);
            }
        }
    }
}
fn adoption_ended(ended: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(e)) = ended {
        tracing::warn!(error=%e, "adoption task failed; deferred to the next pass");
    }
}
/// Commands answered while the startup adoption pass runs: each is one read
/// snapshot. Everything else, including commands added later, waits for it.
fn served_while_adopting(command: &CommandRequest) -> bool {
    matches!(
        command,
        CommandRequest::ProjectsList
            | CommandRequest::Status(_)
            | CommandRequest::PlanGet { .. }
            | CommandRequest::BoardGet { .. }
            | CommandRequest::PlanHistory { .. }
            | CommandRequest::PlanView { .. }
            | CommandRequest::StepContext { .. }
            | CommandRequest::RecipeList { .. }
            | CommandRequest::FnList { .. }
            | CommandRequest::FnGet { .. }
            | CommandRequest::CallStatus { .. }
            | CommandRequest::Verify { .. }
            | CommandRequest::Messages(_)
            | CommandRequest::LogRead(_)
            | CommandRequest::LogWait(_)
            | CommandRequest::StepWait(_)
            | CommandRequest::Query(_)
            | CommandRequest::Docs { .. }
            | CommandRequest::Submission { .. }
    )
}
struct LocalLink<H: ExecutionHost> {
    broker: Coordinator<H>,
    capability: RunCapability,
}
impl<H: ExecutionHost> CoordinatorLink for LocalLink<H> {
    async fn request(&self, command: CoordinatorCommand) -> Result<CoordinatorReply, PublicError> {
        self.broker.guardian(command, Some(&self.capability)).await
    }
}
impl<H: ExecutionHost> RuntimeApi for Coordinator<H> {
    async fn command(&self, request: CommandRequest) -> Result<CommandReply, PublicError> {
        Coordinator::command(self, request).await
    }
    async fn changes(&self, cursor: ChangeCursor) -> Result<ChangeBatch, PublicError> {
        self.reads()
            .snapshot(move |sql| {
                let mut events = vec![];
                for project in &cursor.projects {
                    events.extend(
                        records::read_records(
                            sql,
                            Some(*project),
                            &records::RecordFilter {
                                since: Some(cursor.after),
                                limit: 1000,
                                ..Default::default()
                            },
                        )?
                        .into_page()?
                        .records,
                    );
                }
                events.sort_by_key(|r| r.seq.0);
                events.truncate(1000);
                let after = events.last().map_or(cursor.after, |r| r.seq);
                Ok(ChangeBatch {
                    records: events,
                    cursor: ChangeCursor {
                        after,
                        projects: cursor.projects,
                    },
                })
            })
            .await
            .map_err(|e| e.into_public(true))
    }
}
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "runtime",
    content = "args",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RuntimeCommand {
    AcquireScheduler { owner: String },
    Changes(ChangeCursor),
}
pub use crate::client::CoordinatorClient;

/// ask, say and reply as one store post.
fn message_post(command: CommandRequest) -> Result<sluice_store::messages::Post, PublicError> {
    use sluice_store::messages::Post;
    match command {
        CommandRequest::Ask(a) => Post::try_from(a),
        CommandRequest::Say(s) => Post::try_from(s),
        CommandRequest::Reply(r) => Post::try_from(r),
        _ => return Err(conflict("not a message command")),
    }
    .map_err(|e| e.into_public(false))
}
/// The retired message_post, translated for the runs of older releases that send it.
fn bridge_post(request: MessagePost) -> Result<sluice_store::messages::Post, PublicError> {
    tracing::warn!(
        run = ?request.run,
        "message_post is deprecated: a run of an older release posted; translating it to ask, say or reply"
    );
    sluice_store::messages::bridge(request).map_err(|e| e.into_public(false))
}
/// Post a message in its writer transaction. The project's plan must compile, as an
/// answer may set one of its inputs: the plan compiled last for its revision is taken
/// when there is one, rather than compiled again while every other write waits.
fn post_message(
    tx: &mut sluice_store::WriteTransaction<'_>,
    catalog: &Catalog,
    plans: &PlanCache,
    post: sluice_store::messages::Post,
) -> sluice_store::Result<MessageReceipt> {
    let project = messages_project(tx.sql(), &post.project)?;
    let (revision, plan) = plans.compiled(
        tx.sql(),
        project,
        catalog,
        &catalog.for_project(Some(project)),
    )?;
    let inputs = CompiledInputs {
        project,
        revision,
        plan,
    };
    Ok(sluice_store::messages::post(tx, post, &inputs)?.receipt)
}
/// `InputSetter` over a compiled plan that may be shared: the context it sets an input
/// with is built only when an answer sets one.
struct CompiledInputs {
    project: ProjectId,
    revision: Revision,
    plan: Arc<Plan>,
}
impl sluice_store::messages::PlanInputSetter for CompiledInputs {
    fn set_input(
        &self,
        tx: &mut sluice_store::WriteTransaction<'_>,
        input: sluice_store::messages::InputAnswer<'_>,
    ) -> sluice_store::Result<()> {
        InputSetter(PlanContext {
            project: self.project,
            revision: self.revision,
            plan: (*self.plan).clone(),
        })
        .set_input(tx, input)
    }
}
/// A callback's message speaks only as its own run, in its own project.
fn run_speaks(command: &CommandRequest, run: RunId, project: ProjectId) -> bool {
    let (selector, speaker, owner) = match command {
        CommandRequest::Ask(m) => (&m.project, m.run, m.owner),
        CommandRequest::Say(m) => (&m.project, m.run, m.owner),
        CommandRequest::Reply(m) => (&m.project, m.run, m.owner),
        _ => return false,
    };
    !owner && speaker == Some(run) && *selector == ProjectSelector::Id(project)
}
pub(crate) fn context(
    sql: &Connection,
    project: ProjectId,
    catalog: &Catalog,
) -> sluice_store::Result<PlanContext> {
    let (rev, doc): (i64, String) = sql.query_row(
        "SELECT rev,doc FROM plans WHERE project_id=?1",
        [project.to_string()],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let doc: JsonMap = serde_json::from_str(&doc)?;
    let catalog = catalog.for_project(Some(project));
    let plan = Plan::parse_owned(doc, &catalog).map_err(|errors| PublicError::Invalid {
        message: "invalid stored plan".into(),
        errors: errors.into_iter().map(|e| e.to_string()).collect(),
    })?;
    Ok(PlanContext {
        project,
        revision: Revision(rev as u64),
        plan,
    })
}
fn resource_limits(
    sql: &Connection,
    project: ProjectId,
) -> sluice_store::Result<indexmap::IndexMap<String, sluice_model::plan::ResourceLimit>> {
    Ok(resources::declarations(sql, project)?
        .into_iter()
        .map(|(n, r)| {
            (
                n,
                match r.declaration {
                    resources::Capacity::Fixed(n) => sluice_model::plan::ResourceLimit::Fixed(n),
                    resources::Capacity::Function(_) => sluice_model::plan::ResourceLimit::Dynamic,
                },
            )
        })
        .collect())
}
fn messages_project(
    sql: &Connection,
    selector: &ProjectSelector,
) -> sluice_store::Result<ProjectId> {
    sluice_store::messages::resolve_project(sql, selector)
}
fn projects_identity(sql: &Connection, id: ProjectId) -> sluice_store::Result<ProjectIdentity> {
    let p = projects::resolve(sql, &ProjectSelector::Id(id))?;
    Ok(ProjectIdentity {
        project_id: id,
        name: p.name,
    })
}
fn edit_project(command: &CommandRequest) -> Option<ProjectSelector> {
    Some(match command {
        CommandRequest::PlanPatch(r) => r.project.clone(),
        CommandRequest::StepAdd(r) => r.project.clone(),
        CommandRequest::StepUpdate(r) => r.project.clone(),
        CommandRequest::StepRemove(r) => r.project.clone(),
        CommandRequest::StepPause(r) => r.project.clone(),
        CommandRequest::StepSetInput(r) => r.project.clone(),
        CommandRequest::EdgeAdd(r) | CommandRequest::EdgeRemove(r) => r.project.clone(),
        CommandRequest::UnitTag(r) => r.project.clone(),
        _ => return None,
    })
}
fn step_identity(id: &AttemptKey) -> sluice_store::Result<attempts::AttemptIdentity> {
    Ok(attempts::AttemptIdentity {
        project: id.project.ok_or_else(|| conflict("missing project"))?,
        step: id.step.clone().ok_or_else(|| conflict("missing step"))?,
        generation: id.generation,
        work: id.work,
        run: id.run,
        attempt: id.attempt,
    })
}
async fn write_reply<T: Serialize>(stream: &mut UnixStream, reply: &T) -> std::io::Result<()> {
    tokio::time::timeout(Duration::from_secs(5), socket::write_frame(stream, reply))
        .await
        .map_err(std::io::Error::other)?
}

fn require_invocation(
    sql: &Connection,
    id: &AttemptKey,
    invocation: InvocationId,
) -> sluice_store::Result<()> {
    let raw: String = sql.query_row(
        "SELECT request FROM attempts WHERE attempt_id=?1",
        [id.attempt.to_string()],
        |r| r.get(0),
    )?;
    let request: Value = serde_json::from_str(&raw)?;
    if !request["runtime_starts"]
        .as_array()
        .is_some_and(|starts| starts.iter().any(|s| s["invocation"] == json!(invocation)))
    {
        return Err(conflict("delivery acknowledgement has no admitted invocation").into());
    }
    Ok(())
}

fn guardian_key(command: &CoordinatorCommand) -> &AttemptKey {
    match command {
        CoordinatorCommand::Claim(g) => &g.identity,
        CoordinatorCommand::Started { identity, .. }
        | CoordinatorCommand::Messages { identity, .. }
        | CoordinatorCommand::Watch { identity, .. }
        | CoordinatorCommand::DeliverAck { identity, .. }
        | CoordinatorCommand::Callback { identity, .. } => identity,
        CoordinatorCommand::CancelIntent(id) | CoordinatorCommand::Submissions(id) => id,
        CoordinatorCommand::Complete(j) => &j.identity,
    }
}
fn store_guardian(g: &GuardianIdentity) -> attempts::GuardianIdentity {
    attempts::GuardianIdentity {
        unit_name: g.unit.clone(),
        boot_id: g.process.boot_id.clone(),
        pid: g.process.pid,
        start: g.process.start_time.to_string(),
        cgroup: g.process.cgroup.clone(),
        socket_challenge: g.socket_challenge.clone(),
    }
}
fn persist_guardian(
    tx: &mut sluice_store::WriteTransaction<'_>,
    id: &AttemptKey,
    g: attempts::GuardianIdentity,
) -> sluice_store::Result<()> {
    tx.sql().execute("UPDATE runs SET unit_name=?2,boot_id=?3,guardian_pid=?4,guardian_start=?5,cgroup=?6,socket_challenge=?7 WHERE run_id=?1",(id.run.to_string(),g.unit_name,g.boot_id,g.pid,g.start,g.cgroup,g.socket_challenge))?;
    Ok(())
}
fn stored_guardian(
    sql: &Connection,
    id: &AttemptKey,
    _home: HomeId,
) -> sluice_store::Result<Option<GuardianIdentity>> {
    let row:Option<(String,String,u32,String,String,String)>=sql.query_row("SELECT unit_name,boot_id,guardian_pid,guardian_start,cgroup,socket_challenge FROM runs WHERE run_id=?1 AND guardian_pid IS NOT NULL",[id.run.to_string()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    row.map(|(unit, boot_id, pid, start, cgroup, socket_challenge)| {
        Ok(GuardianIdentity {
            identity: id.clone(),
            process: ProcessIdentity {
                pid,
                start_time: start
                    .parse()
                    .map_err(|e| StoreError::InvalidDatabase(format!("{e}")))?,
                boot_id,
                cgroup,
            },
            unit,
            socket_challenge,
        })
    })
    .transpose()
}
fn completion_kind(result: PayloadResult) -> (attempts::CompletionKind, JsonMap) {
    use attempts::CompletionKind as K;
    match result {
        PayloadResult::Succeeded(outputs) => (K::Succeeded, outputs),
        PayloadResult::Failed(e) => (K::Failed(e), JsonMap::default()),
        PayloadResult::Rejected(message) => (K::Rejected { message }, JsonMap::default()),
        PayloadResult::Cancelled(message) => (K::Cancelled { message }, JsonMap::default()),
        PayloadResult::Lost(message) => (K::Lost { message }, JsonMap::default()),
        PayloadResult::Unknown(message) => (K::Unknown { message }, JsonMap::default()),
    }
}

fn run_project(sql: &Connection, run: RunId) -> sluice_store::Result<ProjectId> {
    let id: String = sql.query_row(
        "SELECT project_id FROM runs WHERE run_id=?1",
        [run.to_string()],
        |r| r.get(0),
    )?;
    calls::parse_id(id)
}

fn check_expected_revision(
    request: &impl Serialize,
    current: Revision,
) -> sluice_store::Result<()> {
    if serde_json::to_value(request)?["expected_rev"]
        .as_u64()
        .is_some_and(|rev| rev != current.0)
    {
        return Err(PublicError::Conflict {
            message: "plan revision changed".into(),
            current_rev: Some(current),
        }
        .into());
    }
    Ok(())
}

/// Callback lifetime is checked by the writer alongside every new mutation.
fn current_run(sql: &Connection, id: &AttemptKey) -> sluice_store::Result<bool> {
    let current: bool = sql.query_row(
        "SELECT EXISTS(SELECT 1 FROM runs r JOIN attempts a USING(attempt_id)
         LEFT JOIN steps s ON s.project_id=r.project_id AND s.step_id=r.step_id
         LEFT JOIN calls c ON c.run_id=r.run_id
         WHERE r.run_id=?1 AND r.attempt_id=?2 AND r.project_id IS ?3 AND r.step_id IS ?4
         AND r.generation=?5 AND r.work_generation=?6 AND a.phase<>'terminal' AND r.finished_at IS NULL
         AND ((r.step_id IS NOT NULL AND s.generation=r.generation AND s.work_generation=r.work_generation
               AND EXISTS(SELECT 1 FROM json_each(s.run_ids) WHERE value=r.run_id))
              OR (r.step_id IS NULL AND c.call_id=r.run_id AND c.status='running')))",
        (id.run.to_string(), id.attempt.to_string(), id.project.map(|p| p.to_string()),
         id.step.as_ref().map(ToString::to_string), i64::try_from(id.generation.0).map_err(|_| conflict("generation overflow"))?, i64::try_from(id.work.0).map_err(|_| conflict("work generation overflow"))?),
        |r| r.get(0),
    )?;
    Ok(current)
}
fn ensure_current(sql: &Connection, id: &AttemptKey) -> sluice_store::Result<()> {
    if !current_run(sql, id)? {
        return Err(conflict("callback run is no longer current").into());
    }
    Ok(())
}

/// What a run's callback does in its writer transaction: None when the plan edit it
/// prepared outside the writer finds what it was prepared from changed (nothing is
/// written); else the reply, with the plan an edit committed.
fn callback_mutation(
    tx: &mut sluice_store::WriteTransaction<'_>,
    id: &AttemptKey,
    command: CommandRequest,
    catalog: &Catalog,
    plans: &PlanCache,
    home: &Path,
    outside: Option<OutsideEdit>,
) -> sluice_store::Result<Option<(CommandReply, Option<CachedPlan>)>> {
    let mut committed = None;
    let reply = match command.clone() {
        CommandRequest::StepSubmit(s) => {
            if attempts::step_submit(tx, s)?.is_none() {
                return Err(conflict("stale submission").into());
            }
            CommandReply::Ack
        }
        CommandRequest::StepProgress(p) => {
            if Some(messages_project(tx.sql(), &p.project)?) != id.project {
                return Err(conflict("tool project differs from run").into());
            }
            CommandReply::Data(sluice_model::rpc::JsonValue::try_from(
                attempts::step_progress(tx, p)?,
            )?)
        }
        CommandRequest::Submission { run } => {
            let row: Option<String> = tx
                .sql()
                .query_row(
                    "SELECT outputs FROM submissions WHERE run_id=?1",
                    [run.to_string()],
                    |r| r.get(0),
                )
                .optional()?;
            CommandReply::Data(serde_json::from_str(row.as_deref().unwrap_or("{}"))?)
        }
        CommandRequest::MessagePost(m) => CommandReply::Posted {
            id: post_message(tx, catalog, plans, bridge_post(m)?)?.id,
        },
        command @ (CommandRequest::Ask(_) | CommandRequest::Say(_) | CommandRequest::Reply(_)) => {
            CommandReply::Receipt(post_message(tx, catalog, plans, message_post(command)?)?)
        }
        CommandRequest::AcquireLease(l) => {
            let lease = resources::request_lease_keyed(
                tx,
                id.run,
                &l.resource,
                l.amount,
                &format!("callback/{}/{}", id.run, l.request_id),
            )?;
            resources::grant_leases(tx, run_project(tx.sql(), l.run)?)?;
            let state = resources::leases(tx.sql(), run_project(tx.sql(), l.run)?)?
                .into_iter()
                .find(|l| l.id == lease)
                .ok_or_else(|| conflict("lease missing"))?
                .state;
            CommandReply::Lease { lease, state }
        }
        CommandRequest::ReleaseLease(l) => {
            resources::release_lease(tx, l.lease, id.run)?;
            CommandReply::Ack
        }
        CommandRequest::RegisterCompletionAction(a) => {
            if !attempts::register_completion_action(tx, a, &Hooks)? {
                return Err(conflict("stale completion action").into());
            }
            CommandReply::Ack
        }
        command if project_mutation(&command) => {
            let encoded = serde_json::to_value(&command)?;
            let selector: ProjectSelector =
                serde_json::from_value(encoded["args"]["project"].clone())?;
            if Some(messages_project(tx.sql(), &selector)?) != id.project {
                return Err(conflict("tool project differs from run").into());
            }
            match outside {
                Some(outside) => match commit_outside(tx, catalog, outside)? {
                    Some((reply, plan)) => {
                        committed = plan;
                        reply
                    }
                    None => return Ok(None),
                },
                None => mutate_project(tx, catalog, home, command)?,
            }
        }
        _ => return Err(conflict("unsupported callback").into()),
    };
    Ok(Some((reply, committed)))
}

/// How many times a plan edit is prepared outside the writer before it is prepared in it.
pub(crate) const EDIT_TRIES: usize = 3;

/// A plan edit prepared from a read snapshot, before its writer transaction: compiled,
/// patched, validated, simulated and worked out down to the rows it writes, so the writer
/// holds no other write back while a plan of thousands of steps is compiled. The writer
/// takes this outcome (what to write, or the refusal) only when the snapshot's rows (the
/// plan's revision, the project's pause, inputs, step rows and resources: `Witness`) and
/// the project's fn signatures are as they were here: it is then the outcome it would
/// have worked out itself.
pub(crate) struct OutsideEdit {
    selector: ProjectSelector,
    project: ProjectId,
    mark: sluice_store::RowMark,
    witness: plans::Witness,
    signatures: indexmap::IndexMap<String, sluice_model::plan::FnSignature>,
    outcome: sluice_store::Result<Staged>,
}
enum Staged {
    Preview(EditPreview),
    Effect {
        effect: Box<plans::EditEffect>,
        /// The plan it commits, compiled.
        plan: Option<CachedPlan>,
        inputs: Option<edit::InputChanges>,
        prune: Option<sluice_model::units::PruneSet>,
    },
}
impl OutsideEdit {
    /// Prepare a plan edit command from a read snapshot; None for any other command, or
    /// when the snapshot cannot be read (the writer then prepares it, and refuses it, alone).
    pub(crate) async fn prepare<H: ExecutionHost>(
        broker: &Coordinator<H>,
        command: &CommandRequest,
    ) -> Option<Self> {
        let selector = edit_project(command)?;
        let edit = PlanEdit::try_from(command.clone()).ok()?;
        let (catalog, home, cache) = (
            broker.inner.catalog.clone(),
            broker.home().to_owned(),
            broker.inner.plans.clone(),
        );
        // Before the snapshot begins: every commit it cannot see is published after this.
        let mark = broker.writer().row_mark();
        broker
            .reads()
            .snapshot(move |sql| {
                let id = messages_project(sql, &selector)?;
                let signatures = catalog.for_project(Some(id));
                let (revision, plan) = cache.compiled(sql, id, &catalog, &signatures)?;
                let (witness, state) = plans::Witness::read_with_state(sql, id)?;
                let limits = resource_limits(sql, id)?;
                let prepared = edit::prepare_edit(
                    &EditSnapshot {
                        revision,
                        plan: &plan,
                        state: &state,
                        signatures: &signatures,
                        recipes: &crate::dispatch_ext::load_recipes(&home, id)?,
                        resources: &CachedResources::default(),
                        limits: &limits,
                        prune_eligible: None,
                    },
                    edit,
                );
                let outcome = prepared.map_err(Into::into).and_then(|prepared| {
                    if prepared.dry_run {
                        return Ok(Staged::Preview(prepared.preview));
                    }
                    let (inputs, prune) = (prepared.inputs.clone(), prepared.prune.clone());
                    let mut effect = plans::edit_effect(
                        sql,
                        id,
                        prepared,
                        plans::Current {
                            revision,
                            document: plan.document(),
                            state: &state,
                            prepared_with: true,
                        },
                    )?;
                    let plan = effect.take_plan().map(|(revision, plan)| {
                        CachedPlan::new(id, revision, signatures.0.clone(), plan)
                    });
                    Ok(Staged::Effect {
                        effect: Box::new(effect),
                        plan,
                        inputs,
                        prune,
                    })
                });
                Ok(Self {
                    selector,
                    project: id,
                    mark,
                    witness,
                    signatures: signatures.0,
                    outcome,
                })
            })
            .await
            .ok()
    }
}
/// Commit an edit prepared outside the writer, with the plan it commits, or None when
/// what it was prepared from has changed (nothing is written).
fn commit_outside(
    tx: &mut sluice_store::WriteTransaction<'_>,
    catalog: &Catalog,
    outside: OutsideEdit,
) -> sluice_store::Result<Option<(CommandReply, Option<CachedPlan>)>> {
    crate::drain::ensure_admission(tx, &crate::drain::Admission::Plan)?;
    let id = messages_project(tx.sql(), &outside.selector)?;
    if id != outside.project
        || outside.signatures != catalog.for_project(Some(id)).0
        || !outside.witness.holds_since(tx, id, outside.mark)?
    {
        return Ok(None);
    }
    Ok(Some(match outside.outcome? {
        Staged::Preview(preview) => (CommandReply::Preview(preview), None),
        Staged::Effect {
            effect,
            plan,
            inputs,
            prune,
        } => (
            edit_reply(plans::commit_effect(tx, *effect)?, inputs, prune),
            plan,
        ),
    }))
}

/// A project's plan compiled at a revision with some fn signatures. A revision fixes the
/// plan's document (every write of it moves the revision on), so the same revision and
/// signatures compile the same plan.
pub(crate) struct CachedPlan {
    project: ProjectId,
    revision: Revision,
    signatures: indexmap::IndexMap<String, sluice_model::plan::FnSignature>,
    plan: Arc<Plan>,
}
impl CachedPlan {
    pub(crate) fn new(
        project: ProjectId,
        revision: Revision,
        signatures: indexmap::IndexMap<String, sluice_model::plan::FnSignature>,
        plan: Plan,
    ) -> Self {
        Self {
            project,
            revision,
            signatures,
            plan: Arc::new(plan),
        }
    }
}
/// Each project's plan as last compiled: the one an edit committed, or the one compiled
/// from the store for an edit or a message. An edit or a message at the same revision
/// then takes it instead of compiling the stored plan again.
#[derive(Default)]
pub(crate) struct PlanCache(std::sync::Mutex<std::collections::HashMap<ProjectId, CachedPlan>>);
impl PlanCache {
    /// The project's plan at its current revision, compiled with `signatures` (its view
    /// of `catalog`): the cached one when it matches, else compiled from the store.
    pub(crate) fn compiled(
        &self,
        sql: &Connection,
        project: ProjectId,
        catalog: &Catalog,
        signatures: &Catalog,
    ) -> sluice_store::Result<(Revision, Arc<Plan>)> {
        let rev: i64 = sql.query_row(
            "SELECT rev FROM plans WHERE project_id=?1",
            [project.to_string()],
            |r| r.get(0),
        )?;
        let revision = Revision(rev as u64);
        if let Some(cached) = self.lock().get(&project)
            && cached.revision == revision
            && cached.signatures == signatures.0
        {
            return Ok((revision, cached.plan.clone()));
        }
        let ctx = context(sql, project, catalog)?;
        let plan = Arc::new(ctx.plan);
        if ctx.revision == revision {
            self.put(CachedPlan {
                project,
                revision,
                signatures: signatures.0.clone(),
                plan: plan.clone(),
            });
        }
        Ok((ctx.revision, plan))
    }
    pub(crate) fn put(&self, plan: CachedPlan) {
        // Freeing a compiled plan of thousands of steps takes milliseconds: not on an
        // async worker, nor in the writer.
        if let Some(replaced) = self.lock().insert(plan.project, plan) {
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => drop(runtime.spawn_blocking(move || drop(replaced))),
                Err(_) => drop(std::thread::spawn(move || drop(replaced))),
            }
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<ProjectId, CachedPlan>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn project_mutation(command: &CommandRequest) -> bool {
    matches!(
        command,
        CommandRequest::ProjectUpdate(_)
            | CommandRequest::BoardSet(_)
            | CommandRequest::BoardSlotSet(_)
            | CommandRequest::StepRetry(_)
            | CommandRequest::StepCancel(_)
            | CommandRequest::StepSetOutput(_)
            | CommandRequest::PlanSetInput(_)
    ) || edit_project(command).is_some()
}
fn mutate_project(
    tx: &mut sluice_store::WriteTransaction<'_>,
    catalog: &Catalog,
    home: &std::path::Path,
    command: CommandRequest,
) -> sluice_store::Result<CommandReply> {
    match command {
        CommandRequest::ProjectUpdate(update) => {
            let project = projects::project_update(
                tx,
                &update.project,
                projects::UpdateProject {
                    new_name: update.new_name,
                    description: update.description,
                    icon: update.icon.map(projects::Icon::try_from).transpose()?,
                    resources: update.resources.map(serde_json::to_value).transpose()?,
                    paused: update.paused,
                    archived: update.archived,
                    expected_settings_rev: update.expected_settings_rev,
                    reason: update.reason,
                    author: update.author.unwrap_or_else(|| "cli".into()),
                },
                &ResourceSettings(catalog.clone()),
            )?;
            Ok(CommandReply::Project(ProjectIdentity {
                project_id: project.project_id,
                name: project.name,
            }))
        }
        CommandRequest::BoardSet(request) => {
            let rev = projects::board_set(
                tx,
                &request.project,
                projects::SetBoard {
                    program: request.program,
                    expected_rev: request.expected_rev,
                    reason: request.reason,
                    author: request.author.unwrap_or_else(|| "cli".into()),
                },
            )?;
            Ok(CommandReply::BoardRev { rev })
        }
        CommandRequest::BoardSlotSet(request) => {
            let key = request.key.clone();
            let change = projects::board_slot_set(
                tx,
                &request.project,
                projects::SetBoardSlot {
                    key: request.key,
                    markdown: request.markdown,
                    author: request.author.unwrap_or_else(|| "cli".into()),
                },
            )?;
            Ok(CommandReply::Data(
                sluice_model::rpc::JsonValue::try_from(serde_json::json!({
                    "key": key,
                    "updated_at": change.slot.as_ref().map(|s| s.at.clone()),
                    "cleared": change.slot.is_none(),
                    "changed": change.changed,
                }))
                .map_err(|e| PublicError::Storage {
                    message: e.to_string(),
                })?,
            ))
        }
        CommandRequest::StepRetry(request) => {
            crate::drain::ensure_admission(tx, &crate::drain::Admission::Plan)?;
            let id = messages_project(tx.sql(), &request.project)?;
            let ctx = context(tx.sql(), id, catalog)?;
            check_expected_revision(&request, ctx.revision)?;
            Ok(CommandReply::Retry(plans::step_retry(
                tx, &ctx, request, &mut Hooks,
            )?))
        }
        CommandRequest::StepCancel(request) => {
            let id = messages_project(tx.sql(), &request.project)?;
            let ctx = context(tx.sql(), id, catalog)?;
            check_expected_revision(&request, ctx.revision)?;
            plans::step_cancel(tx, &ctx, request)?;
            Ok(CommandReply::Ack)
        }
        CommandRequest::StepSetOutput(request) => {
            let id = messages_project(tx.sql(), &request.project)?;
            let ctx = context(tx.sql(), id, catalog)?;
            plans::step_set_output(tx, &ctx, request)?;
            Ok(CommandReply::Ack)
        }
        CommandRequest::PlanSetInput(request) => {
            crate::drain::ensure_admission(tx, &crate::drain::Admission::Plan)?;
            let id = messages_project(tx.sql(), &request.project)?;
            let ctx = context(tx.sql(), id, catalog)?;
            if request.edit.dry_run {
                return Err(PublicError::not_implemented("input dry run").into());
            }
            if request.edit.expected.is_some_and(|r| r != ctx.revision) {
                return Err(conflict("plan revision changed").into());
            }
            plans::set_input(
                tx,
                &ctx,
                &request.name,
                request.value,
                request.edit.author.unwrap_or_default(),
                request.edit.reason,
            )?;
            Ok(CommandReply::Ack)
        }
        other => {
            crate::drain::ensure_admission(tx, &crate::drain::Admission::Plan)?;
            let project =
                edit_project(&other).ok_or_else(|| conflict("unsupported project mutation"))?;
            let id = messages_project(tx.sql(), &project)?;
            let ctx = context(tx.sql(), id, catalog)?;
            let state = plans::read_state(tx.sql(), id)?;
            let prepared = edit::prepare_edit(
                &EditSnapshot {
                    revision: ctx.revision,
                    plan: &ctx.plan,
                    state: &state,
                    signatures: &catalog.for_project(Some(id)),
                    recipes: &crate::dispatch_ext::load_recipes(home, id)?,
                    resources: &CachedResources::default(),
                    limits: &resource_limits(tx.sql(), id)?,
                    prune_eligible: None,
                },
                PlanEdit::try_from(other)?,
            )?;
            if prepared.dry_run {
                return Ok(CommandReply::Preview(prepared.preview));
            }
            let (inputs, prune) = (prepared.inputs.clone(), prepared.prune.clone());
            Ok(edit_reply(
                plans::apply_edit(tx, id, prepared)?,
                inputs,
                prune,
            ))
        }
    }
}

/// A project icon argument, with a path read here, before any write transaction.
fn read_icon(icon: IconUpload) -> Result<projects::Icon, PublicError> {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    projects::Icon::from_upload(icon, home.as_deref()).map_err(|e| e.into_public(false))
}

/// The edit result, with `step_set_input`'s per-step report or `plan_prune`'s units.
pub(crate) fn edit_reply(
    result: EditResult,
    inputs: Option<edit::InputChanges>,
    prune: Option<sluice_model::units::PruneSet>,
) -> CommandReply {
    if let Some(inputs) = inputs {
        return CommandReply::Inputs(InputEditResult {
            edit: result,
            changed: inputs.changed,
            running: inputs.running,
            unsupported: inputs.unsupported,
        });
    }
    if let Some(prune) = prune {
        return CommandReply::Pruned(PruneResult {
            edit: result,
            units: prune.units,
            kept: prune
                .kept
                .into_iter()
                .map(|(unit, holder)| {
                    let (step, output) = match holder {
                        sluice_model::units::PruneHolder::Step(step) => (Some(step), None),
                        sluice_model::units::PruneHolder::PlanOutput(name) => (None, Some(name)),
                    };
                    KeptUnit { unit, step, output }
                })
                .collect(),
        });
    }
    CommandReply::Edit(result)
}
