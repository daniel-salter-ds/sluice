//! Execution from the reservation's published bundle and authenticated helper bridge.
use crate::{
    execution::Launch,
    publication::{FrozenExecution, declarations},
    python::*,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sluice_model::{commands::*, error::PublicError, ids::*, rpc::*};
use sluice_process::socket;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;

pub(crate) fn failure(e: impl std::fmt::Display) -> PublicError {
    PublicError::FnFailure {
        message: e.to_string(),
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservationContext {
    pub execution: FrozenExecution,
    pub project_name: String,
    pub outputs: JsonMap,
    #[serde(default)]
    pub extra_inputs: JsonMap,
}
impl ReservationContext {
    pub async fn from_call(
        writer: &sluice_store::Writer,
        _home: &Path,
        call: &crate::calls::AdmittedCall,
    ) -> Result<Option<Self>, PublicError> {
        let Some(execution) = call.function.bundle.0.get("execution") else {
            return Ok(None);
        };
        let execution = decode_json(&serde_json::to_vec(execution).map_err(failure)?)?;
        let project = call.project;
        let project_name = writer
            .write(sluice_store::RetrySafety::Idempotent, move |tx| {
                Ok(project
                    .map(|p| {
                        sluice_store::projects::resolve(tx.sql(), &ProjectSelector::Id(p))
                            .map(|p| p.name.to_string())
                    })
                    .transpose()?
                    .unwrap_or_default())
            })
            .await?;
        Ok(Some(Self {
            execution,
            project_name,
            outputs: JsonMap::default(),
            extra_inputs: JsonMap::default(),
        }))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForwardHelper {
    pub identity: sluice_process::journal::AttemptKey,
    pub request: HelperRequest,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelperWire {
    pub helper: ForwardHelper,
}

/// Decodes a tool's flat public arguments (MCP's `decode_tool`: name, arguments, client name
/// for the author) into its command.
pub type ToolDecoder = fn(
    &str,
    serde_json::Map<String, serde_json::Value>,
    Option<&str>,
) -> Result<CommandRequest, PublicError>;
static TOOL_DECODER: std::sync::OnceLock<ToolDecoder> = std::sync::OnceLock::new();
/// The MCP adapter lives above this crate, so the coordinator mode installs it at start;
/// `ctx.tool` then takes exactly the arguments MCP and `sluice tool` take.
pub fn install_tool_decoder(decoder: ToolDecoder) {
    let _ = TOOL_DECODER.set(decoder);
}
/// A helper's `ctx.tool(name, args)`: the flat MCP arguments, authored as `author` unless
/// they name one.
pub fn decode_tool(tool: ToolRequest, author: Option<&str>) -> Result<CommandRequest, PublicError> {
    let decode = TOOL_DECODER
        .get()
        .ok_or_else(|| failure("this process has no tool decoder"))?;
    let args = match serde_json::to_value(tool.args).map_err(failure)? {
        serde_json::Value::Object(args) => args,
        _ => return Err(failure("tool arguments must be an object")),
    };
    decode(&tool.name, args, author)
}
/// A command reply as MCP returns it: the reply's data, `{"ok": true}` for an acknowledgement.
pub fn reply_value(reply: CommandReply) -> Result<serde_json::Value, PublicError> {
    if let CommandReply::Ack = reply {
        return Ok(json!({"ok":true}));
    }
    let value = serde_json::to_value(reply).map_err(|e| PublicError::Storage {
        message: e.to_string(),
    })?;
    Ok(value
        .get("data")
        .cloned()
        .unwrap_or(serde_json::Value::Null))
}

pub struct Dispatcher {
    pub home: PathBuf,
    pub launch: Launch,
    pub context: ReservationContext,
    pub cancel: CancellationToken,
    pub agents: crate::agent_factory::RunAgents,
    sidecar: tokio::sync::Mutex<Option<crate::sidecar::Sidecar>>,
}
impl Dispatcher {
    pub fn new(home: PathBuf, launch: Launch, context: ReservationContext) -> Self {
        let cancel = CancellationToken::new();
        let agents = crate::agent_factory::RunAgents::new(
            home.clone(),
            launch.clone(),
            context.clone(),
            cancel.clone(),
        );
        Self {
            home,
            launch,
            context,
            cancel,
            agents,
            sidecar: tokio::sync::Mutex::new(None),
        }
    }
    pub async fn callback(&self, request: HelperRequest) -> Result<CommandReply, PublicError> {
        if request.protocol != PROTOCOL_VERSION
            || request.run_capability.as_ref() != Some(&self.launch.capability)
        {
            return Err(failure("helper capability mismatch"));
        }
        if let HelperCommand::Runtime(CommandRequest::Builtin { invocation }) = &request.command {
            if invocation.run != self.launch.identity.run
                || invocation.attempt != self.launch.identity.attempt
                || invocation.project != self.launch.invocation.project
                || invocation.step != self.launch.identity.step
            {
                return Err(failure("builtin differs from outer run"));
            }
            let mut sidecar = self
                .sidecar
                .try_lock()
                .map_err(|_| failure("concurrent agent composition refused"))?;
            if sidecar.is_none() {
                *sidecar = Some(crate::sidecar::Sidecar::start(self).await?);
            }
            return sidecar
                .as_mut()
                .expect("started sidecar")
                .invoke(request)
                .await;
        }
        self.forward(&request).await.map_err(failure)?
    }
    /// A helper request sent on to the coordinator: the outer error is the exchange
    /// failing (no coordinator, or one that went away without answering), the inner
    /// result its answer.
    async fn forward(
        &self,
        request: &HelperRequest,
    ) -> std::io::Result<Result<CommandReply, PublicError>> {
        let mut stream = UnixStream::connect(self.home.join("coordinator.sock")).await?;
        socket::write_frame(
            &mut stream,
            &socket::Request {
                protocol: PROTOCOL_VERSION,
                request_id: request.request_id.clone(),
                run_capability: Some(self.launch.capability.clone()),
                command: HelperWire {
                    helper: ForwardHelper {
                        identity: self.launch.identity.clone(),
                        request: request.clone(),
                    },
                },
            },
        )
        .await?;
        let reply: RpcReply = socket::read_frame(&mut stream).await?;
        if reply.request_id != request.request_id || reply.protocol != PROTOCOL_VERSION {
            return Ok(Err(failure("helper reply identity mismatch")));
        }
        Ok(match reply.result {
            RpcResult::Ok(reply) => Ok(*reply),
            RpcResult::Error(e) => Err(e),
        })
    }
    async fn python(&self, invocation: &FnInvocation) -> Result<JsonMap, PublicError> {
        let run_dir = self.home.join("runs").join(invocation.run.to_string());
        let helper_socket = run_dir.join("helper.sock");
        let bin = std::env::current_exe().map_err(failure)?;
        let helper = std::env::var_os("SLUICE_PYTHON_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| bin.parent().unwrap_or(Path::new(".")).join("python"));
        let mut config = PythonConfig::new(helper, bin);
        // The run's .env secrets layer over the inherited environment (SPEC §5.4).
        config.environment.extend(
            crate::dotenv::run_environment(&self.home, Some(invocation.project))
                .into_iter()
                .map(|(k, v)| (k.into(), v.into())),
        );
        if let Some(uv) = std::env::var_os("SLUICE_UV_BIN") {
            config.uv = uv.into();
        }
        let execution = &self.context.execution;
        let host = PythonHost {
            config,
            bundle: PinnedPythonFn {
                bundle_dir: execution.fn_dir.clone().unwrap_or_else(|| run_dir.clone()),
                sibling_helper_root: execution.bundle_root.clone(),
            },
            context: PythonContext {
                home: self.home.clone(),
                run_dir: run_dir.clone(),
                project_dir: self
                    .home
                    .join("projects")
                    .join(invocation.project.to_string()),
                project: self.context.project_name.clone(),
                prev_run: self.launch.prev_run,
                extra_inputs: self.context.extra_inputs.clone(),
                outputs: self.context.outputs.clone(),
                returns: declarations(&execution.outputs),
                control_socket: Some(helper_socket),
                run_capability: Some(self.launch.capability.clone()),
            },
            cancellation: self.cancel.clone(),
        };
        if invocation.name.starts_with("inline.") {
            crate::inline::invoke_inline(&host, invocation).await
        } else {
            host.execute(invocation).await
        }
        .map_err(PythonError::into_public)
    }
    pub async fn execute(
        self: &Arc<Self>,
        invocation: FnInvocation,
    ) -> Result<JsonMap, PublicError> {
        if sluice_agents::AGENT_BUILTINS.contains(&invocation.name.as_str()) {
            return self.agents.invoke(invocation).await;
        }
        if self.context.execution.fn_dir.is_some() || invocation.name.starts_with("inline.") {
            let path = self
                .home
                .join("runs")
                .join(invocation.run.to_string())
                .join("helper.sock");
            let listener = UnixListener::bind(&path).map_err(failure)?;
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .map_err(failure)?;
            let dispatcher = self.clone();
            let server = tokio::spawn(async move {
                let mut clients = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        _ = dispatcher.cancel.cancelled() => break,
                        Some(_) = clients.join_next(), if !clients.is_empty() => {},
                        accepted = listener.accept(), if clients.len() < 8 => {
                            let Ok((mut stream,_)) = accepted else { break; };
                            let dispatcher = dispatcher.clone();
                            clients.spawn(async move {
                                let request: HelperRequest = socket::read_frame(&mut stream).await?;
                                let result = dispatcher.callback(request.clone()).await;
                                socket::write_frame(&mut stream, &RpcReply { protocol: 1, request_id: request.request_id, result: match result { Ok(v) => RpcResult::Ok(Box::new(v)), Err(e) => RpcResult::Error(e) } }).await
                            });
                        }
                    }
                }
                clients.abort_all();
                while clients.join_next().await.is_some() {}
            });
            let mut result = self.python(&invocation).await;
            if let Some(sidecar) = self.sidecar.lock().await.take()
                && let Err(error) = sidecar.close().await
            {
                result = Err(error);
            }
            self.cancel.cancel();
            server.await.map_err(failure)?;
            std::fs::remove_file(path).map_err(failure)?;
            return result;
        }
        builtin(self, invocation).await
    }
}
async fn builtin(
    dispatcher: &Dispatcher,
    invocation: FnInvocation,
) -> Result<JsonMap, PublicError> {
    let policy = crate::builtins::find(&invocation.name)
        .map(|d| d.retry)
        .unwrap_or(crate::builtins::descriptor::DEFAULT_RETRY);
    let backoff = std::env::var("SLUICE_BACKOFF")
        .ok()
        .map(|v| v.parse::<f64>().map_err(failure))
        .transpose()?;
    if backoff.is_some_and(|v| !v.is_finite() || v < 0.0) {
        return Err(failure("invalid SLUICE_BACKOFF"));
    }
    let delay = backoff
        .map(std::time::Duration::try_from_secs_f64)
        .transpose()
        .map_err(failure)?
        .unwrap_or(policy.backoff);
    for attempt in 0..=policy.retries {
        match builtin_once(dispatcher, invocation.clone()).await {
            Err(PublicError::Transient { .. }) if attempt < policy.retries => {
                tokio::select! {
                    _ = dispatcher.cancel.cancelled() => return Err(PublicError::Cancelled { message: "builtin cancelled during backoff".into() }),
                    _ = tokio::time::sleep(delay) => {},
                }
            }
            result => return result,
        }
    }
    unreachable!("retry loop returns its final outcome")
}
fn builtin_failure(error: crate::builtins::FnFailure) -> PublicError {
    match error {
        crate::builtins::FnFailure::Transient(message) => PublicError::Transient { message },
        error => failure(error),
    }
}
/// How long a waiting message builtin keeps asking a coordinator that is away.
const RESUME_FOR: std::time::Duration = std::time::Duration::from_secs(600);
async fn builtin_once(
    dispatcher: &Dispatcher,
    invocation: FnInvocation,
) -> Result<JsonMap, PublicError> {
    let name = invocation.name.as_str();
    let mut environment = std::env::vars().collect::<BTreeMap<_, _>>();
    // .env secrets over the inherited environment, never over the run's own SLUICE_* values.
    for (key, value) in crate::dotenv::run_environment(&dispatcher.home, Some(invocation.project)) {
        if !(key.starts_with("SLUICE_") && environment.contains_key(&key)) {
            environment.insert(key, value);
        }
    }
    let context = crate::builtins::BuiltinCtx::new(environment.clone());
    if name.starts_with("core.") {
        return crate::builtins::core::dispatch(name, &invocation.inputs, &context)
            .await
            .map_err(builtin_failure);
    }
    if name.starts_with("git.") || name.starts_with("gh.") {
        let ctx = crate::builtins::git::BuiltinCtx::new(
            environment,
            dispatcher
                .home
                .join("runs")
                .join(invocation.run.to_string()),
        );
        return if name.starts_with("git.") {
            crate::builtins::git::dispatch(name, &invocation.inputs, &ctx).await
        } else {
            crate::builtins::gh::dispatch(name, &invocation.inputs, &ctx).await
        }
        .map_err(|error| match error {
            crate::builtins::git::FnFailure::Transient(message) => {
                PublicError::Transient { message }
            }
            error => failure(error),
        });
    }
    if name.starts_with("message.") {
        // A waiting ask asked again takes up its own question, and message.wait only
        // reads, so one the coordinator dropped (a restart) is asked again once it is
        // back instead of failing the step.
        let resumable = name == "message.wait"
            || (name == "message.ask"
                && invocation.inputs.0.get("wait").map(JsonValue::as_value)
                    == Some(&serde_json::Value::Bool(true)));
        let request = HelperRequest {
            protocol: 1,
            request_id: RequestId(InvocationId::new().to_string()),
            run_capability: Some(dispatcher.launch.capability.clone()),
            command: HelperCommand::Extension(HelperExtension::Tool(ToolRequest {
                name: name.into(),
                args: invocation.inputs,
            })),
        };
        let until = tokio::time::Instant::now() + RESUME_FOR;
        let reply = loop {
            let answer = dispatcher.forward(&request).await;
            let dropped = matches!(
                answer,
                Err(_)
                    | Ok(Err(PublicError::Busy {
                        retryable: true,
                        ..
                    }))
            );
            if !resumable || !dropped || tokio::time::Instant::now() >= until {
                break answer.map_err(failure)?;
            }
            tokio::select! {
                _ = dispatcher.cancel.cancelled() => return Err(PublicError::Cancelled { message: "message builtin cancelled while the coordinator was away".into() }),
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            }
        };
        return reply.and_then(|r| match r {
            CommandReply::Data(v) => decode_json(&serde_json::to_vec(&v).map_err(failure)?),
            _ => Err(failure("message builtin reply")),
        });
    }
    crate::builtins::dispatch(name, &invocation.inputs, &context)
        .await
        .map_err(builtin_failure)
}
