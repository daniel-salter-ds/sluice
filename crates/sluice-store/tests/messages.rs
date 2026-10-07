#[path = "../../../tests/support/home.rs"]
mod home;
use home::ScratchHome;
use rusqlite::params;
use serde_json::{Value, json};
use sluice_model::{
    commands::{Delivery, Message, MessageAnswer, MessageVerb, QuestionState},
    error::PublicError,
    events::{Event, NotificationOutcome},
    ids::{AttemptId, MessageId, ProjectId, ProjectSelector, Revision, RunId},
    rpc::JsonValue,
    types::{Type, check_value_at},
};
use sluice_store::{ReadPool, Result, RetrySafety, WriteTransaction, Writer, messages::*};

struct Fixture {
    _home: ScratchHome,
    writer: Writer,
    reads: ReadPool,
    project: ProjectId,
}
impl Fixture {
    async fn new() -> Self {
        let home = ScratchHome::new().unwrap();
        assert!(home.root().exists());
        assert_eq!(ScratchHome::validate(home.path()).unwrap(), home.path());
        let writer = Writer::open(home.path()).unwrap();
        let project = ProjectId::new();
        writer.write(RetrySafety::NonIdempotent,move |tx| {
            tx.sql().execute("INSERT INTO projects(project_id,name,created_at) VALUES (?1,'p','now')",[project.to_string()])?;
            tx.sql().execute("INSERT INTO plans(project_id,rev,doc) VALUES (?1,1,'{}')",[project.to_string()])?;
            tx.sql().execute("INSERT INTO steps(project_id,step_id,position,declaration) VALUES (?1,'work',0,'{}'),(?1,'other',1,'{}')",[project.to_string()])?;
            tx.changed(Some(project),"plan");Ok(())
        }).await.unwrap();
        let reads = ReadPool::open(home.path(), 2).unwrap();
        Self {
            _home: home,
            writer,
            reads,
            project,
        }
    }
    async fn post(&self, post: Post) -> std::result::Result<Message, PublicError> {
        self.posted(post).await.map(|p| p.message)
    }
    async fn posted(&self, post: Post) -> std::result::Result<Posted, PublicError> {
        self.writer
            .write(RetrySafety::NonIdempotent, move |tx| {
                sluice_store::messages::post(tx, post, &TestInputs)
            })
            .await
    }
    async fn claimed(&self, id: MessageId) -> Option<String> {
        self.reads
            .snapshot(move |c| {
                Ok(
                    c.query_row("SELECT claimed_by FROM messages WHERE id=?1", [id.0], |r| {
                        r.get(0)
                    })?,
                )
            })
            .await
            .unwrap()
    }
    async fn question(&self, id: MessageId) -> Question {
        let p = self.project;
        self.reads
            .snapshot(move |sql| question(sql, p, id))
            .await
            .unwrap()
    }
    async fn run(&self, step: &str, item: i64, generation: i64, prev: Option<RunId>) -> RunId {
        let p = self.project;
        let step = step.to_owned();
        let run = RunId::new();
        let attempt = AttemptId::new();
        self.writer.write(RetrySafety::NonIdempotent,move |tx| {
            tx.sql().execute("INSERT INTO attempts(attempt_id,project_id,step_id,generation,item_index,phase,request,inputs_hash,created_at) VALUES (?1,?2,?3,?4,?5,'executing','{}','same','now')",params![attempt.to_string(),p.to_string(),step,generation,item])?;
            tx.sql().execute("INSERT INTO runs(run_id,project_id,attempt_id,step_id,generation,item_index,prev_run,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,'now')",params![run.to_string(),p.to_string(),attempt.to_string(),step,generation,item,prev.map(|r|r.to_string())])?;
            tx.changed(Some(p),"status");Ok(())
        }).await.unwrap();
        run
    }
    async fn stop(&self, run: RunId) {
        let p = self.project;
        self.writer.write(RetrySafety::NonIdempotent,move |tx| {
            tx.sql().execute("UPDATE attempts SET phase='terminal' WHERE attempt_id=(SELECT attempt_id FROM runs WHERE run_id=?1)",[run.to_string()])?;
            tx.sql().execute("UPDATE runs SET finished_at='now' WHERE run_id=?1",[run.to_string()])?;
            tx.changed(Some(p),"status");Ok(())
        }).await.unwrap();
    }
    async fn ask(&self, run: RunId, title: &str) -> AskResult {
        let post = asking(self.project, "question")
            .title(title)
            .speaker(Speaker::Run(run));
        self.writer
            .write(RetrySafety::NonIdempotent, move |tx| {
                ask_waiting(tx, post, &TestInputs)
            })
            .await
            .unwrap()
    }
    async fn input(&self, typ: Value) {
        let p = self.project;
        self.writer
            .write(RetrySafety::NonIdempotent, move |tx| {
                tx.sql().execute(
                    "INSERT INTO inputs(project_id,name,position,declaration) VALUES (?1,'n',0,?2)",
                    params![p.to_string(), typ.to_string()],
                )?;
                tx.changed(Some(p), "plan");
                Ok(())
            })
            .await
            .unwrap();
    }
    async fn count(&self, table: &'static str) -> i64 {
        self.reads
            .snapshot(move |c| {
                Ok(c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
            })
            .await
            .unwrap()
    }
}
/// The orchestrator tells the owner something.
fn draft(project: ProjectId, body: &str) -> Post {
    Post {
        project: ProjectSelector::Id(project),
        speaker: Speaker::Orchestrator,
        body: body.into(),
        verb: Verb::Say {
            to: "owner".into(),
            data: None,
        },
    }
}
/// The orchestrator asks the owner.
fn asking(project: ProjectId, body: &str) -> Post {
    Post {
        verb: Verb::Ask {
            to: "owner".into(),
            title: None,
            ui: None,
            input: None,
            data: None,
        },
        ..draft(project, body)
    }
}
/// The owner replies.
fn reply(project: ProjectId, id: MessageId, body: &str) -> Post {
    Post {
        speaker: Speaker::Owner,
        verb: Verb::Reply {
            to_message: id,
            answer: None,
        },
        ..draft(project, body)
    }
}
fn answer(project: ProjectId, id: MessageId, value: Value) -> Post {
    reply(project, id, "").answer(MessageAnswer {
        action: "submit".into(),
        params: Some(serde_json::from_value(json!({"value":7})).unwrap()),
        values: Some(serde_json::from_value(json!({"value":value})).unwrap()),
    })
}
trait Draft {
    fn to(self, to: &str) -> Self;
    fn input(self, input: &str) -> Self;
    fn title(self, title: &str) -> Self;
    fn speaker(self, speaker: Speaker) -> Self;
    fn answer(self, answer: MessageAnswer) -> Self;
}
impl Draft for Post {
    fn to(mut self, recipient: &str) -> Self {
        match &mut self.verb {
            Verb::Ask { to, .. } | Verb::Say { to, .. } => *to = recipient.into(),
            Verb::Reply { .. } => panic!("a reply's recipient is derived"),
        }
        self
    }
    fn input(mut self, name: &str) -> Self {
        if let Verb::Ask { input, .. } = &mut self.verb {
            *input = Some(name.into());
        }
        self
    }
    fn title(mut self, text: &str) -> Self {
        if let Verb::Ask { title, .. } = &mut self.verb {
            *title = Some(text.into());
        }
        self
    }
    fn speaker(mut self, speaker: Speaker) -> Self {
        self.speaker = speaker;
        self
    }
    fn answer(mut self, given: MessageAnswer) -> Self {
        if let Verb::Reply { answer, .. } = &mut self.verb {
            *answer = Some(given);
        }
        self
    }
}
fn waiting(result: AskResult) -> Message {
    match result {
        AskResult::Waiting(p) => p.message,
        _ => panic!("expected waiting"),
    }
}
struct TestInputs;
impl PlanInputSetter for TestInputs {
    fn set_input(&self, tx: &mut WriteTransaction<'_>, update: InputAnswer<'_>) -> Result<()> {
        let raw: String = tx.sql().query_row(
            "SELECT declaration FROM inputs WHERE project_id=?1 AND name=?2",
            params![update.project.to_string(), update.name],
            |r| r.get(0),
        )?;
        let typ: Type = serde_json::from_str(&raw)?;
        check_value_at(
            &typ,
            update.value.as_value(),
            &format!("inputs.{}", update.name),
        )
        .map_err(|errors| PublicError::Invalid {
            message: "answer does not fit".into(),
            errors: errors.into_iter().map(|e| e.to_string()).collect(),
        })?;
        tx.sql().execute(
            "UPDATE inputs SET value=?1,generation=generation+1 WHERE project_id=?2 AND name=?3",
            params![
                serde_json::to_string(update.value)?,
                update.project.to_string(),
                update.name
            ],
        )?;
        tx.sql().execute(
            "UPDATE plans SET rev=rev+1 WHERE project_id=?1",
            [update.project.to_string()],
        )?;
        let rev: i64 = tx.sql().query_row(
            "SELECT rev FROM plans WHERE project_id=?1",
            [update.project.to_string()],
            |r| r.get(0),
        )?;
        tx.append_record(
            Some(update.project),
            Event::PlanInput {
                rev: Revision(rev as u64),
                author: update.author.into(),
                reason: update.reason.into(),
                name: update.name.into(),
                value: update.value.clone(),
            },
        )?;
        tx.changed(Some(update.project), "plan");
        Ok(())
    }
}

#[tokio::test]
async fn messages_have_record_ids_derived_threads_and_exact_optional_fields() {
    let f = Fixture::new().await;
    let mut post = asking(f.project, "hello").title("Title");
    if let Verb::Ask { data, ui, .. } = &mut post.verb {
        *data = Some(JsonValue::try_from(json!({"x":[null,1]})).unwrap());
        *ui = Some("root = Button()".into());
    }
    let m = f.post(post).await.unwrap();
    assert_eq!(m.thread, "owner");
    assert_eq!(m.verb, MessageVerb::Ask);
    assert_eq!(m.from, "orchestrator");
    assert_eq!(m.state, Some(QuestionState::Open));
    let p = f.project;
    let id = m.id;
    let (stored, event) = f
        .reads
        .snapshot(move |c| {
            let payload: String =
                c.query_row("SELECT payload FROM records WHERE seq=?1", [id.0], |r| {
                    r.get(0)
                })?;
            Ok((message(c, p, id)?, serde_json::from_str::<Event>(&payload)?))
        })
        .await
        .unwrap();
    assert_eq!(stored, m);
    // The record never carries a question's state.
    let mut recorded = m.clone();
    recorded.state = None;
    assert_eq!(event, Event::Message(Box::new(recorded)));
}
#[tokio::test]
async fn replies_inherit_thread_and_go_to_the_parent_sender() {
    let f = Fixture::new().await;
    let q = f
        .post(
            asking(f.project, "question")
                .to("work")
                .speaker(Speaker::Owner),
        )
        .await
        .unwrap();
    assert_eq!(q.thread, "step-work");
    let r = f
        .post(reply(f.project, q.id, "yes").speaker(Speaker::Orchestrator))
        .await
        .unwrap();
    assert_eq!(r.thread, q.thread);
    assert_eq!(r.to, Some(q.from));
    assert_eq!(r.verb, MessageVerb::Reply);
    assert_eq!(r.to_message, Some(q.id));
    assert_eq!(f.question(q.id).await.state, QuestionState::Answered);
    let q = f.question(q.id).await.message;
    assert_eq!(q.answered_by, Some(r.id));
}
#[tokio::test]
async fn unknown_recipient_unknown_parent_and_cross_project_reply_write_nothing() {
    let f = Fixture::new().await;
    let m = f.post(draft(f.project, "a")).await.unwrap();
    for to in ["", "nobody", "cli", "Work"] {
        assert!(matches!(
            f.post(draft(f.project, "b").to(to)).await,
            Err(PublicError::Invalid { .. })
        ));
        assert!(matches!(
            f.post(asking(f.project, "b").to(to)).await,
            Err(PublicError::Invalid { .. })
        ));
    }
    assert!(matches!(
        f.post(draft(f.project, "self").to("orchestrator")).await,
        Err(PublicError::Invalid { .. })
    ));
    assert!(matches!(
        f.post(reply(f.project, MessageId(999), "x")).await,
        Err(PublicError::NotFound { .. })
    ));
    let p = ProjectId::new();
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute(
                "INSERT INTO projects(project_id,name,created_at) VALUES (?1,'q','now')",
                [p.to_string()],
            )?;
            tx.changed(Some(p), "projects");
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        f.post(reply(p, m.id, "b")).await,
        Err(PublicError::NotFound { .. })
    ));
    assert_eq!(f.count("messages").await, 1);
    assert_eq!(f.count("records").await, 1);
}
#[tokio::test]
async fn a_reply_to_a_note_or_a_closed_question_is_just_a_message() {
    let f = Fixture::new().await;
    let note = f.post(draft(f.project, "fyi")).await.unwrap();
    let r = f.post(reply(f.project, note.id, "thanks")).await.unwrap();
    assert_eq!(r.verb, MessageVerb::Reply);
    assert_eq!(r.state, None);
    assert!(matches!(
        f.post(answer(f.project, note.id, json!(1))).await,
        Err(PublicError::Invalid { .. })
    ));
    let q = f.post(asking(f.project, "q")).await.unwrap();
    let close = reply(f.project, q.id, "").answer(MessageAnswer {
        action: "close".into(),
        params: None,
        values: None,
    });
    f.post(close).await.unwrap();
    f.post(reply(f.project, q.id, "after all")).await.unwrap();
    assert_eq!(f.question(q.id).await.state, QuestionState::Closed);
    let blank = reply(f.project, q.id, " ");
    assert!(matches!(
        f.post(blank).await,
        Err(PublicError::Invalid { .. })
    ));
}
#[tokio::test]
async fn first_answer_wins_later_plain_reply_sets_no_input_and_stale_ui_conflicts() {
    let f = Fixture::new().await;
    f.input(json!("int")).await;
    let q = f.post(asking(f.project, "q").input("n")).await.unwrap();
    let first = f.post(answer(f.project, q.id, json!(3))).await.unwrap();
    f.post(reply(f.project, q.id, "later invalid integer"))
        .await
        .unwrap();
    assert!(matches!(
        f.post(answer(f.project, q.id, json!(9))).await,
        Err(PublicError::Conflict { .. })
    ));
    assert_eq!(f.question(q.id).await.reply.unwrap().id, first.id);
    let value: String = f
        .reads
        .snapshot(|c| Ok(c.query_row("SELECT value FROM inputs", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(value, "3");
    assert_eq!(f.count("messages").await, 3);
}
#[tokio::test]
async fn invalid_typed_answer_leaves_question_input_revision_records_and_notifications_unchanged() {
    let f = Fixture::new().await;
    f.input(json!("int")).await;
    let q = f.post(asking(f.project, "q").input("n")).await.unwrap();
    let records = f.count("records").await;
    let wake = f.writer.subscribe();
    assert!(matches!(
        f.post(answer(f.project, q.id, json!("three"))).await,
        Err(PublicError::Invalid { .. })
    ));
    assert_eq!(f.question(q.id).await.state, QuestionState::Open);
    assert_eq!(f.count("records").await, records);
    assert_eq!(f.count("messages").await, 1);
    assert!(!wake.has_changed().unwrap());
    let (value, rev): (Option<String>, i64) = f
        .reads
        .snapshot(|c| {
            Ok(c.query_row(
                "SELECT value,(SELECT rev FROM plans) FROM inputs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(value, None);
    assert_eq!(rev, 1);
}
#[tokio::test]
async fn null_precedence_and_nullable_inputs_use_the_shared_type_contract() {
    for typ in [json!("Any"), json!("int?")] {
        let f = Fixture::new().await;
        f.input(typ).await;
        for value in [json!(null), json!(4)] {
            let q = f.post(asking(f.project, "q").input("n")).await.unwrap();
            f.post(answer(f.project, q.id, value.clone()))
                .await
                .unwrap();
            let stored: String = f
                .reads
                .snapshot(|c| Ok(c.query_row("SELECT value FROM inputs", [], |r| r.get(0))?))
                .await
                .unwrap();
            assert_eq!(serde_json::from_str::<Value>(&stored).unwrap(), value);
        }
    }
}
#[tokio::test]
async fn plain_body_and_params_fallback_set_input_and_name_the_answering_author() {
    let f = Fixture::new().await;
    f.input(json!("string")).await;
    for structured in [false, true] {
        let q = f
            .post(asking(f.project, "q").input("n").title("Which word?"))
            .await
            .unwrap();
        let mut r = reply(f.project, q.id, "body");
        if structured {
            r = r.answer(MessageAnswer {
                action: "submit".into(),
                values: None,
                params: Some(serde_json::from_value(json!({"value":"param"})).unwrap()),
            });
        }
        f.post(r).await.unwrap();
        let payload: String = f
            .reads
            .snapshot(|c| {
                Ok(c.query_row(
                    "SELECT payload FROM records WHERE kind='plan.input' ORDER BY seq DESC LIMIT 1",
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap();
        let event: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(event["author"], "owner");
        assert_eq!(event["reason"], format!("message {}: Which word?", q.id.0));
        assert_eq!(event["value"], if structured { "param" } else { "body" });
    }
}
#[tokio::test]
async fn missing_answer_value_and_null_required_input_are_atomic() {
    let f = Fixture::new().await;
    f.input(json!("int")).await;
    let q = f.post(asking(f.project, "q").input("n")).await.unwrap();
    let r = reply(f.project, q.id, "").answer(MessageAnswer {
        action: "submit".into(),
        params: None,
        values: None,
    });
    for r in [r, answer(f.project, q.id, json!(null))] {
        assert!(matches!(f.post(r).await, Err(PublicError::Invalid { .. })));
    }
    assert_eq!(f.question(q.id).await.state, QuestionState::Open);
    assert_eq!(f.count("records").await, 2); // the question and its notify reservation
}
#[tokio::test]
async fn close_resolves_without_setting_input() {
    let f = Fixture::new().await;
    f.input(json!("int")).await;
    let q = f.post(asking(f.project, "q").input("n")).await.unwrap();
    let mut r = answer(f.project, q.id, json!("invalid"));
    if let Verb::Reply { answer, .. } = &mut r.verb {
        answer.as_mut().unwrap().action = "close".into();
    }
    f.post(r).await.unwrap();
    assert_eq!(f.question(q.id).await.state, QuestionState::Closed);
    assert_eq!(f.question(q.id).await.message.answered_by, None);
    assert_eq!(f.count("records").await, 3);
    assert!(matches!(
        f.post(answer(f.project, q.id, json!(1))).await,
        Err(PublicError::Conflict { .. })
    ));
}
#[tokio::test]
async fn errors_after_the_setter_wrote_still_roll_back_the_whole_transaction() {
    struct Fault;
    impl PlanInputSetter for Fault {
        fn set_input(&self, tx: &mut WriteTransaction<'_>, u: InputAnswer<'_>) -> Result<()> {
            TestInputs.set_input(tx, u)?;
            Err(PublicError::Invalid {
                message: "fault after write".into(),
                errors: vec![],
            }
            .into())
        }
    }
    let f = Fixture::new().await;
    f.input(json!("int")).await;
    let q = f.post(asking(f.project, "q").input("n")).await.unwrap();
    let r = answer(f.project, q.id, json!(4));
    assert!(
        f.writer
            .write(RetrySafety::NonIdempotent, move |tx| {
                sluice_store::messages::post(tx, r, &Fault)
            })
            .await
            .is_err()
    );
    assert_eq!(f.count("records").await, 2);
    assert_eq!(f.question(q.id).await.state, QuestionState::Open);
    let rev: i64 = f
        .reads
        .snapshot(|c| Ok(c.query_row("SELECT rev FROM plans", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(rev, 1);
}
#[tokio::test]
async fn unknown_input_project_and_blank_bodies_leave_no_rows() {
    let f = Fixture::new().await;
    assert!(matches!(
        f.post(asking(f.project, "q").input("missing")).await,
        Err(PublicError::NotFound { .. })
    ));
    assert!(matches!(
        f.post(draft(f.project, " ")).await,
        Err(PublicError::Invalid { .. })
    ));
    assert!(matches!(
        f.post(asking(f.project, "")).await,
        Err(PublicError::Invalid { .. })
    ));
    assert!(f.post(draft(ProjectId::new(), "q")).await.is_err());
    assert_eq!(f.count("records").await, 0);
}
#[tokio::test]
async fn waiting_and_cancellation_are_derived_and_retry_adopts_open_question() {
    let f = Fixture::new().await;
    let r = f.run("work", -1, 1, None).await;
    let q = waiting(f.ask(r, "Title").await);
    assert!(f.question(q.id).await.waiting);
    assert_eq!(q.thread, "step-work");
    assert_eq!(waiting(f.ask(r, "Title").await).id, q.id);
    let p = f.project;
    f.writer.write(RetrySafety::NonIdempotent,move |tx|{tx.sql().execute("UPDATE attempts SET cancel_requested=1 WHERE attempt_id=(SELECT attempt_id FROM runs WHERE run_id=?1)",[r.to_string()])?;tx.changed(Some(p),"status");Ok(())}).await.unwrap();
    assert!(!f.question(q.id).await.waiting);
    assert!(f.question(q.id).await.stopped.unwrap().contains("cancel"));
    f.stop(r).await;
    let next = f.run("work", -1, 1, Some(r)).await;
    assert_eq!(waiting(f.ask(next, "Title").await).id, q.id);
    assert!(f.question(q.id).await.waiting);
    assert_eq!(f.question(q.id).await.message.run, Some(next));
    assert_eq!(f.count("messages").await, 1);
}
#[tokio::test]
async fn an_absent_asker_answer_is_claimed_once_and_never_reused() {
    let f = Fixture::new().await;
    let r = f.run("work", -1, 1, None).await;
    let q = waiting(f.ask(r, "Title").await);
    f.stop(r).await;
    let a = f.post(reply(f.project, q.id, "yes")).await.unwrap();
    let next = f.run("work", -1, 1, Some(r)).await;
    match f.ask(next, "Title").await {
        AskResult::Answered { question, reply } => {
            assert_eq!(question.id, q.id);
            assert_eq!(reply.id, a.id);
            assert_eq!(f.claimed(reply.id).await, Some(next.to_string()));
        }
        _ => panic!("expected answer"),
    }
    let p = f.project;
    f.writer
        .write(RetrySafety::Idempotent, move |tx| {
            claim_answer(tx, p, q.id, next)
        })
        .await
        .unwrap();
    f.stop(next).await;
    let third = f.run("work", -1, 1, Some(next)).await;
    assert_ne!(waiting(f.ask(third, "Title").await).id, q.id);
}
#[tokio::test]
async fn delivered_answer_cannot_be_claimed_by_another_run() {
    let f = Fixture::new().await;
    let r = f.run("work", -1, 1, None).await;
    let q = waiting(f.ask(r, "Title").await);
    f.post(reply(f.project, q.id, "yes")).await.unwrap();
    let p = f.project;
    f.writer
        .write(RetrySafety::Idempotent, move |tx| {
            claim_answer(tx, p, q.id, r)
        })
        .await
        .unwrap();
    let other = f.run("other", -1, 1, None).await;
    assert!(matches!(
        f.writer
            .write(RetrySafety::NonIdempotent, move |tx| claim_answer(
                tx, p, q.id, other
            ))
            .await,
        Err(PublicError::Conflict { .. })
    ));
}
#[tokio::test]
async fn scatter_items_steps_titles_and_reintroduced_generations_have_separate_lineages() {
    let f = Fixture::new().await;
    let r = f.run("work", 0, 1, None).await;
    let q = waiting(f.ask(r, "Title").await);
    f.stop(r).await;
    for (step, item, generation, title) in [
        ("work", 1, 1, "Title"),
        ("other", 0, 1, "Title"),
        ("work", 0, 2, "Title"),
        ("work", 0, 1, "Other title"),
    ] {
        let run = f.run(step, item, generation, None).await;
        assert_ne!(waiting(f.ask(run, title).await).id, q.id);
        f.stop(run).await;
    }
    let retry = f.run("work", 0, 1, Some(r)).await;
    assert_eq!(waiting(f.ask(retry, "Title").await).id, q.id);
}
#[tokio::test]
async fn only_latest_question_is_considered_and_closed_question_is_not_adopted() {
    let f = Fixture::new().await;
    let r = f.run("work", -1, 1, None).await;
    let old = waiting(f.ask(r, "Title").await);
    let new = f
        .post(
            asking(f.project, "new")
                .title("Title")
                .speaker(Speaker::Run(r)),
        )
        .await
        .unwrap();
    let close = reply(f.project, new.id, "").answer(MessageAnswer {
        action: "close".into(),
        params: None,
        values: None,
    });
    f.post(close).await.unwrap();
    f.stop(r).await;
    let retry = f.run("work", -1, 1, Some(r)).await;
    let next = waiting(f.ask(retry, "Title").await);
    assert_ne!(next.id, old.id);
    assert_ne!(next.id, new.id);
}
#[tokio::test]
async fn notify_is_reserved_once_for_questions_only_and_result_never_replays() {
    let f = Fixture::new().await;
    let q = f.post(asking(f.project, "q")).await.unwrap();
    f.post(draft(f.project, "note")).await.unwrap();
    assert_eq!(f.count("notification_attempts").await, 1);
    let p = f.project;
    let a = f
        .reads
        .snapshot(move |c| notify_attempt(c, p, q.id))
        .await
        .unwrap();
    assert_eq!(a.outcome, NotificationOutcome::Reserved);
    for _ in 0..2 {
        f.writer
            .write(RetrySafety::Idempotent, move |tx| {
                notify_result(
                    tx,
                    p,
                    q.id,
                    a.attempt,
                    NotificationOutcome::Uncertain,
                    Some("crash".into()),
                    None,
                )
            })
            .await
            .unwrap();
    }
    assert!(matches!(
        f.writer
            .write(RetrySafety::NonIdempotent, move |tx| notify_result(
                tx,
                p,
                q.id,
                a.attempt,
                NotificationOutcome::Dispatched,
                None,
                None
            ))
            .await,
        Err(PublicError::Conflict { .. })
    ));
    let count: i64 = f
        .reads
        .snapshot(|c| {
            Ok(c.query_row(
                "SELECT count(*) FROM records WHERE kind='project.notify'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}
#[tokio::test]
async fn delivery_reservation_does_not_consume_and_actual_start_advances_once() {
    let f = Fixture::new().await;
    let m = f
        .post(draft(f.project, "feedback").to("work"))
        .await
        .unwrap();
    let run = f.run("work", -1, 1, None).await;
    let p = f.project;
    let range = f
        .writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            assign_run_range(tx, p, run, 0, None)
        })
        .await
        .unwrap();
    assert_eq!(range.messages, vec![m.id]);
    assert_eq!(range.after, MessageId(0));
    assert!(matches!(
        f.writer
            .write(RetrySafety::NonIdempotent, move |tx| advance_cursor(
                tx, p, run
            ))
            .await,
        Err(PublicError::Conflict { .. })
    ));
    f.stop(run).await;
    let retry = f.run("work", -1, 1, Some(run)).await;
    assert_eq!(
        f.writer
            .write(RetrySafety::NonIdempotent, move |tx| assign_run_range(
                tx, p, retry, 0, None
            ))
            .await
            .unwrap(),
        range
    );
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute(
                "UPDATE runs SET started_at='now' WHERE run_id=?1",
                [retry.to_string()],
            )?;
            advance_cursor(tx, p, retry)?;
            acknowledge_delivery(tx, p, retry, m.id)?;
            Ok(())
        })
        .await
        .unwrap();
    f.stop(retry).await;
    let next_run = f.run("work", -1, 1, Some(retry)).await;
    let next = f
        .writer
        .write(RetrySafety::Idempotent, move |tx| {
            assign_run_range(tx, p, next_run, m.id.0, None)
        })
        .await
        .unwrap();
    assert_eq!(next.after, m.id);
    assert!(next.messages.is_empty());
    let acknowledged: bool = f
        .reads
        .snapshot(move |c| {
            Ok(c.query_row(
                "SELECT acknowledged_at IS NOT NULL FROM message_deliveries WHERE run_id=?1",
                [retry.to_string()],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert!(acknowledged);
}
#[tokio::test]
async fn scatter_windows_and_not_started_item_survive_shared_cursor_advance() {
    let f = Fixture::new().await;
    let m = f
        .post(draft(f.project, "feedback").to("work"))
        .await
        .unwrap();
    let r0 = f.run("work", 0, 1, None).await;
    let r1 = f.run("work", 1, 1, None).await;
    let p = f.project;
    let window = f
        .writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            let range = assign_run_range(tx, p, r0, 0, None)?;
            let exact = sluice_store::attempts::AssignedRange {
                after: range.after.0,
                through: range.through.0,
            };
            assign_run_range(tx, p, r1, 0, Some(&exact))?;
            Ok(range)
        })
        .await
        .unwrap();
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute(
                "UPDATE runs SET started_at='now' WHERE run_id=?1",
                [r0.to_string()],
            )?;
            advance_cursor(tx, p, r0)
        })
        .await
        .unwrap();
    f.stop(r1).await;
    let retry = f.run("work", 1, 1, Some(r1)).await;
    // Explicit retry enters a new work generation while retaining the item lineage.
    f.writer.write(RetrySafety::NonIdempotent,move |tx| {
        tx.sql().execute("UPDATE runs SET work_generation=2 WHERE run_id=?1",[retry.to_string()])?;
        tx.sql().execute("UPDATE attempts SET work_generation=2 WHERE attempt_id=(SELECT attempt_id FROM runs WHERE run_id=?1)",[retry.to_string()])?;
        tx.changed(Some(p),"status"); Ok(())
    }).await.unwrap();
    let next = f
        .writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            assign_run_range(tx, p, retry, 0, None)
        })
        .await
        .unwrap();
    assert_eq!(next, window);
    assert_eq!(next.messages, vec![m.id]);
    assert_eq!(f.count("message_deliveries").await, 3);
}
#[tokio::test]
async fn live_feed_starts_at_exact_window_end_and_generation_fences_old_start() {
    let f = Fixture::new().await;
    let first = f.post(draft(f.project, "first").to("work")).await.unwrap();
    let run = f.run("work", -1, 1, None).await;
    let p = f.project;
    let assigned = f
        .writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            assign_run_range(tx, p, run, 0, None)
        })
        .await
        .unwrap();
    let late = f.post(draft(p, "late").to("work")).await.unwrap();
    assert_eq!(assigned.through, first.id);
    assert!(late.id > assigned.through);
    assert!(
        f.writer
            .write(RetrySafety::NonIdempotent, move |tx| {
                tx.sql().execute(
                    "UPDATE runs SET started_at='now' WHERE run_id=?1",
                    [run.to_string()],
                )?;
                tx.sql().execute(
                    "UPDATE steps SET generation=2 WHERE project_id=?1 AND step_id='work'",
                    [p.to_string()],
                )?;
                advance_cursor(tx, p, run)
            })
            .await
            .is_err()
    );
    let cursor: i64 = f
        .reads
        .snapshot(|c| {
            Ok(c.query_row(
                "SELECT delivery_cursor FROM steps WHERE step_id='work'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(cursor, 0);
}
#[tokio::test]
async fn simultaneous_ui_answers_set_input_only_once() {
    let f = Fixture::new().await;
    f.input(json!("int")).await;
    let q = f.post(asking(f.project, "q").input("n")).await.unwrap();
    let (a, b) = tokio::join!(
        f.post(answer(f.project, q.id, json!(1))),
        f.post(answer(f.project, q.id, json!(2)))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(f.count("messages").await, 2);
    let rev: i64 = f
        .reads
        .snapshot(|c| Ok(c.query_row("SELECT rev FROM plans", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(rev, 2);
}

#[tokio::test]
async fn acknowledgement_recorded_after_a_cancel_still_claims_the_delivered_answer() {
    let f = Fixture::new().await;
    let run = f.run("work", -1, 1, None).await;
    let q = waiting(f.ask(run, "Title").await);
    let a = f.post(reply(f.project, q.id, "yes")).await.unwrap();
    let p = f.project;
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            assign_run_range(tx, p, run, 0, None)?;
            tx.sql().execute(
                "UPDATE runs SET started_at='now' WHERE run_id=?1",
                [run.to_string()],
            )?;
            advance_cursor(tx, p, run)?;
            // The run was cancelled after it was handed the answer; its completion
            // journal records the acknowledgement afterwards.
            tx.sql().execute(
                "UPDATE attempts SET cancel_requested=1 WHERE attempt_id=(SELECT attempt_id FROM runs WHERE run_id=?1)",
                [run.to_string()],
            )?;
            acknowledge_delivery(tx, p, run, a.id)?;
            Ok(())
        })
        .await
        .unwrap();
    let answer = f.question(q.id).await.reply.unwrap();
    assert_eq!(f.claimed(answer.id).await, Some(run.to_string()));
}

#[tokio::test]
async fn delivery_acknowledgement_claims_answer_and_claim_outlives_log_trimming() {
    let f = Fixture::new().await;
    let run = f.run("work", -1, 1, None).await;
    let q = waiting(f.ask(run, "Title").await);
    let a = f.post(reply(f.project, q.id, "yes")).await.unwrap();
    let p = f.project;
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            assign_run_range(tx, p, run, 0, None)?;
            tx.sql().execute(
                "UPDATE runs SET started_at='now' WHERE run_id=?1",
                [run.to_string()],
            )?;
            advance_cursor(tx, p, run)?;
            acknowledge_delivery(tx, p, run, a.id)?;
            for _ in 0..12 {
                tx.append_record(
                    Some(p),
                    Event::ProjectUpdate {
                        fields: vec![],
                        author: "test".into(),
                        reason: None,
                    },
                )?;
            }
            sluice_store::records::trim_to(tx, Some(p), 10, 9)?;
            Ok(())
        })
        .await
        .unwrap();
    let answer = f.question(q.id).await.reply.unwrap();
    assert_eq!(f.claimed(answer.id).await, Some(run.to_string()));
    let retained: i64 = f
        .reads
        .snapshot(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM records WHERE seq IN (?1,?2)",
                params![q.id.0, a.id.0],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(retained, 0);
    f.stop(run).await;
    let retry = f.run("work", -1, 1, Some(run)).await;
    assert_ne!(waiting(f.ask(retry, "Title").await).id, q.id);
}

#[tokio::test]
async fn sender_and_thread_are_derived_from_the_speaker_and_recipient() {
    let f = Fixture::new().await;
    let run = f.run("work", -1, 1, None).await;
    for (speaker, to, from, thread) in [
        (Speaker::Owner, "orchestrator", "owner", "owner"),
        (Speaker::Orchestrator, "owner", "orchestrator", "owner"),
        (Speaker::Owner, "work", "owner", "step-work"),
        (Speaker::Orchestrator, "other", "orchestrator", "step-other"),
        (Speaker::Run(run), "orchestrator", "work", "step-work"),
        (Speaker::Run(run), "owner", "work", "step-work"),
        (Speaker::Run(run), "other", "work", "step-work"),
        (Speaker::Sluice, "owner", "sluice", "owner"),
    ] {
        let m = f
            .post(draft(f.project, "note").to(to).speaker(speaker))
            .await
            .unwrap();
        assert_eq!((m.from.as_str(), m.thread.as_str()), (from, thread));
        assert_eq!(m.to.as_deref(), Some(to));
        assert_eq!(
            m.run,
            match speaker {
                Speaker::Run(run) => Some(run),
                _ => None,
            }
        );
    }
    assert!(matches!(
        f.post(
            draft(f.project, "self")
                .to("work")
                .speaker(Speaker::Run(run))
        )
        .await,
        Err(PublicError::Invalid { .. })
    ));
    assert!(matches!(
        f.post(draft(f.project, "x").speaker(Speaker::Run(RunId::new())))
            .await,
        Err(PublicError::NotFound { .. })
    ));
}
#[tokio::test]
async fn rows_and_records_stored_before_the_verbs_read_in_the_current_shape() {
    let f = Fixture::new().await;
    let p = f.project;
    let read = f
        .writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            let project = p.to_string();
            tx.sql().execute("INSERT INTO messages(id,project_id,thread,\"from\",\"to\",body,needs_reply,at) VALUES (1,?1,'m1','cli',NULL,'old question',1,'then'),(2,?1,'m1','owner','cli','old reply',0,'then'),(3,?1,'m1','cli','owner','old note',0,'then')",[&project])?;
            tx.sql().execute("UPDATE messages SET reply_to=1 WHERE id=2", [])?;
            tx.sql().execute("UPDATE messages SET resolved_by=2 WHERE id=1", [])?;
            tx.changed(Some(p), "messages");
            let legacy = json!({"kind":"message","id":2,"thread":"m1","from":"owner","to":"cli","title":null,"body":"old reply","needs_reply":false,"reply_to":1,"answer":null,"ui":null,"input":null,"data":null,"run":null,"posted_at":"then","claimed_by":null});
            let event: Event = serde_json::from_value(legacy).unwrap();
            Ok((
                message(tx.sql(), p, MessageId(1))?,
                message(tx.sql(), p, MessageId(2))?,
                message(tx.sql(), p, MessageId(3))?,
                event,
            ))
        })
        .await
        .unwrap();
    let (question, reply, note, event) = read;
    assert_eq!(question.verb, MessageVerb::Ask);
    assert_eq!(question.state, Some(QuestionState::Answered));
    assert_eq!(question.answered_by, Some(MessageId(2)));
    assert_eq!(reply.verb, MessageVerb::Reply);
    assert_eq!(reply.to_message, Some(MessageId(1)));
    assert_eq!(note.verb, MessageVerb::Say);
    assert_eq!(note.state, None);
    let Event::Message(legacy) = event else {
        panic!("message record")
    };
    assert_eq!(legacy.verb, MessageVerb::Reply);
    assert_eq!(legacy.to_message, Some(MessageId(1)));
    let fields: Vec<String> = serde_json::to_value(Event::Message(legacy))
        .unwrap()
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert!(
        !fields
            .iter()
            .any(|k| k == "needs_reply" || k == "reply_to" || k == "claimed_by")
    );
}
#[tokio::test]
async fn stopped_reason_follows_cancelled_failed_and_removed_step_truth() {
    let f = Fixture::new().await;
    let run = f.run("work", -1, 1, None).await;
    let q = waiting(f.ask(run, "Title").await);
    f.stop(run).await;
    let p = f.project;
    f.writer.write(RetrySafety::NonIdempotent,move |tx| {tx.sql().execute("UPDATE steps SET status='failed',error='{\"error\":\"cancelled\",\"message\":\"stop\"}' WHERE project_id=?1 AND step_id='work'",[p.to_string()])?;tx.changed(Some(p),"status");Ok(())}).await.unwrap();
    assert_eq!(
        f.question(q.id).await.stopped,
        Some("work is cancelled".into())
    );
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute(
                "UPDATE steps SET error=NULL WHERE project_id=?1 AND step_id='work'",
                [p.to_string()],
            )?;
            tx.changed(Some(p), "status");
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        f.question(q.id).await.stopped,
        Some("work is failed".into())
    );
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute(
                "DELETE FROM steps WHERE project_id=?1 AND step_id='work'",
                [p.to_string()],
            )?;
            tx.changed(Some(p), "status");
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        f.question(q.id).await.stopped,
        Some("work is not in the plan".into())
    );
}

#[tokio::test]
async fn repeated_range_request_keeps_the_frozen_window_and_leaves_late_messages_for_live_feed() {
    let f = Fixture::new().await;
    f.post(draft(f.project, "first").to("work")).await.unwrap();
    let run = f.run("work", -1, 1, None).await;
    let p = f.project;
    let first = f
        .writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            assign_run_range(tx, p, run, 0, None)
        })
        .await
        .unwrap();
    let late = f.post(draft(p, "late").to("work")).await.unwrap();
    let exact = sluice_store::attempts::AssignedRange {
        after: first.after.0,
        through: first.through.0,
    };
    let again = f
        .writer
        .write(RetrySafety::Idempotent, move |tx| {
            assign_run_range(tx, p, run, exact.after, Some(&exact))
        })
        .await
        .unwrap();
    assert_eq!(again, first);
    assert!(late.id > first.through);
}

/// A run listens by its frozen reservation, not by its fn's name or how its guardian
/// asks for messages; a run an older release reserved listens once it has taken one.
#[tokio::test]
async fn receipts_read_listening_from_the_runs_reservation() {
    let f = Fixture::new().await;
    let p = f.project;
    let set = |sql: &'static str| {
        let w = f.writer.clone();
        async move {
            w.write(RetrySafety::NonIdempotent, move |tx| {
                tx.sql().execute(sql, [p.to_string()])?;
                tx.changed(Some(p), "status");
                Ok(())
            })
            .await
            .unwrap()
        }
    };
    let receipt = || async {
        let r = f.posted(draft(p, "hi").to("work")).await.unwrap().receipt;
        (r.delivery, r.run)
    };
    set("UPDATE steps SET status='running',declaration='{\"run\":\"lash.worker\",\"in\":{}}' WHERE project_id=?1 AND step_id='work'").await;
    let run = f.run("work", -1, 1, None).await;
    // Reserved by this release: a pack fn that takes `listen` listens.
    set("UPDATE attempts SET request='{\"listens\":true}' WHERE project_id=?1 AND step_id='work'")
        .await;
    assert_eq!(receipt().await, (Delivery::Delivered, Some(run)));
    set("UPDATE attempts SET request='{\"listens\":false}' WHERE project_id=?1 AND step_id='work'")
        .await;
    assert_eq!(receipt().await, (Delivery::NoLiveRun, None));
    // Reserved by an older release: not an agent fn by name, and nothing taken yet.
    set("UPDATE attempts SET request='{}' WHERE project_id=?1 AND step_id='work'").await;
    assert_eq!(receipt().await, (Delivery::NoLiveRun, None));
    // Once it has acknowledged a message on its live feed, it listens.
    let sent = f.post(draft(p, "taken").to("work")).await.unwrap();
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute("INSERT INTO message_deliveries(project_id,run_id,message_id,assigned_at,acknowledged_at) VALUES (?1,?2,?3,'now','now')", params![p.to_string(), run.to_string(), sent.id.0])?;
            tx.changed(Some(p), "messages");
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(receipt().await, (Delivery::Delivered, Some(run)));
    // Submitted: the run's agent is gone, and the step takes no more messages while the
    // run finishes.
    f.writer
        .write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute(
                "UPDATE steps SET run_ids=json_array(?2) WHERE project_id=?1 AND step_id='work'",
                params![p.to_string(), run.to_string()],
            )?;
            tx.sql().execute(
                "INSERT INTO submissions(run_id,project_id,step_id,outputs,at) VALUES (?1,?2,'work','{}','now')",
                params![run.to_string(), p.to_string()],
            )?;
            tx.changed(Some(p), "status");
            Ok(())
        })
        .await
        .unwrap();
    let before = f.count("messages").await;
    assert!(matches!(
        f.posted(draft(p, "hi").to("work")).await,
        Err(PublicError::Conflict { message, .. }) if message.contains("has submitted its outputs")
    ));
    assert_eq!(f.count("messages").await, before);
    // Finished: a settled step takes no messages.
    f.stop(run).await;
    set("UPDATE steps SET status='succeeded' WHERE project_id=?1 AND step_id='work'").await;
    assert!(matches!(
        f.posted(draft(p, "hi").to("work")).await,
        Err(PublicError::Conflict { message, .. }) if message.contains("settled")
    ));
}

#[tokio::test]
async fn receipts_follow_the_recipient_and_its_step_runs() {
    let f = Fixture::new().await;
    let p = f.project;
    let set = |sql: &'static str| {
        let w = f.writer.clone();
        async move {
            w.write(RetrySafety::NonIdempotent, move |tx| {
                tx.sql().execute(sql, [p.to_string()])?;
                tx.changed(Some(p), "status");
                Ok(())
            })
            .await
            .unwrap()
        }
    };
    let receipt = |to: &'static str| f.posted(draft(p, "hi").to(to));
    for to in ["owner"] {
        let r = receipt(to).await.unwrap().receipt;
        assert_eq!(
            (r.delivery, r.run, r.thread.as_str()),
            (Delivery::Delivered, None, "owner")
        );
    }
    let r = f
        .posted(draft(p, "hi").to("orchestrator").speaker(Speaker::Owner))
        .await
        .unwrap()
        .receipt;
    assert_eq!(r.delivery, Delivery::Delivered);
    // A pending step runs later: its next run is assigned the message.
    let r = receipt("work").await.unwrap().receipt;
    assert_eq!((r.delivery, r.run), (Delivery::Queued, None));
    assert_eq!((r.to.as_str(), r.thread.as_str()), ("work", "step-work"));
    // A live run of an agent step that listens is handed it.
    set("UPDATE steps SET status='running',declaration='{\"run\":\"agent.run\",\"in\":{}}' WHERE project_id=?1 AND step_id='work'").await;
    let run = f.run("work", -1, 1, None).await;
    let r = receipt("work").await.unwrap().receipt;
    assert_eq!((r.delivery, r.run), (Delivery::Delivered, Some(run)));
    // One that does not listen keeps it for a later run.
    set("UPDATE steps SET declaration='{\"run\":\"agent.run\",\"in\":{\"listen\":{\"default\":false}}}' WHERE project_id=?1 AND step_id='work'").await;
    assert_eq!(
        receipt("work").await.unwrap().receipt.delivery,
        Delivery::NoLiveRun
    );
    // A run reserved but not started takes it when it starts.
    set("UPDATE attempts SET phase='claimed' WHERE project_id=?1 AND step_id='work'").await;
    let r = receipt("work").await.unwrap().receipt;
    assert_eq!((r.delivery, r.run), (Delivery::Queued, Some(run)));
    f.stop(run).await;
    // Paused: no live or upcoming run, kept for a later one.
    set("UPDATE steps SET status='pending',paused='true' WHERE project_id=?1 AND step_id='work'")
        .await;
    let r = receipt("work").await.unwrap().receipt;
    assert_eq!((r.delivery, r.run), (Delivery::NoLiveRun, None));
    // Settled: refused and not stored. An answer to a question it asked before it
    // settled is still taken, and so is closing one; a later reply to it is refused.
    let q = f
        .post(asking(p, "q").to("orchestrator").speaker(Speaker::Run(run)))
        .await
        .unwrap();
    let other = f
        .post(
            asking(p, "other")
                .to("orchestrator")
                .speaker(Speaker::Run(run)),
        )
        .await
        .unwrap();
    for status in ["succeeded", "failed", "stale", "skipped"] {
        let w = f.writer.clone();
        w.write(RetrySafety::NonIdempotent, move |tx| {
            tx.sql().execute(
                "UPDATE steps SET status=?2,paused=NULL WHERE project_id=?1 AND step_id='work'",
                params![p.to_string(), status],
            )?;
            tx.changed(Some(p), "status");
            Ok(())
        })
        .await
        .unwrap();
        let before = f.count("messages").await;
        for refused in [
            f.posted(draft(p, "hi").to("work")).await,
            f.posted(asking(p, "why").to("work")).await,
        ] {
            assert!(
                matches!(&refused, Err(PublicError::Conflict { message, .. }) if message.contains("step work is settled")),
                "{status}: {refused:?}"
            );
        }
        assert_eq!(f.count("messages").await, before, "{status}");
    }
    let answered = f
        .posted(reply(p, q.id, "answer").speaker(Speaker::Orchestrator))
        .await
        .unwrap();
    assert_eq!(
        (answered.receipt.to.as_str(), answered.receipt.delivery),
        ("work", Delivery::NoLiveRun)
    );
    assert_eq!(f.question(q.id).await.state, QuestionState::Answered);
    let before = f.count("messages").await;
    let refused = f
        .posted(reply(p, q.id, "again").speaker(Speaker::Orchestrator))
        .await;
    assert!(
        matches!(&refused, Err(PublicError::Conflict { message, .. }) if message.contains("step work is settled")),
        "{refused:?}"
    );
    assert_eq!(f.count("messages").await, before);
    let closed = f
        .posted(
            reply(p, other.id, "")
                .answer(MessageAnswer {
                    action: "close".into(),
                    params: None,
                    values: None,
                })
                .speaker(Speaker::Orchestrator),
        )
        .await
        .unwrap();
    assert_eq!(closed.receipt.to, "work");
    assert_eq!(f.question(other.id).await.state, QuestionState::Closed);
    // A reply to a removed step's message is kept, with nobody to run it.
    set("DELETE FROM steps WHERE project_id=?1 AND step_id='work'").await;
    let r = f
        .posted(reply(p, q.id, "answer").speaker(Speaker::Orchestrator))
        .await
        .unwrap()
        .receipt;
    assert_eq!((r.to.as_str(), r.delivery), ("work", Delivery::NoLiveRun));
    assert!(matches!(
        receipt("work").await,
        Err(PublicError::Invalid { .. })
    ));
}
