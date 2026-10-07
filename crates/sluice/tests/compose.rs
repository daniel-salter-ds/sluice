//! Composition acceptance uses absolute binaries and private scratch service units.
#[path = "../../../tests/support/executable.rs"]
mod executable;
#[allow(dead_code)]
#[path = "../../../tests/support/units.rs"]
mod units;
use serde_json::{Value, json};
use sluice_model::{
    commands::{CommandReply, Delivery, MessageReceipt},
    ids::ProjectId,
    rpc::{RpcReply, RpcResult, decode_json, encode_frame},
};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    os::unix::{fs::symlink, net::UnixStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
struct Gate {
    temp: tempfile::TempDir,
    home: PathBuf,
    broker: Option<Child>,
    env: BTreeMap<String, String>,
    project: ProjectId,
}
impl Gate {
    fn new() -> Self {
        Self::configured(|_| {})
    }
    fn configured(configure: impl FnOnce(&mut Self)) -> Self {
        let temp = tempfile::Builder::new()
            .prefix("sluice-test-compose-")
            .tempdir_in("/tmp")
            .unwrap();
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let bin = temp.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        for (name, path) in [
            ("uv", uv()),
            ("python3", PathBuf::from("/usr/bin/python3")),
            ("bash", PathBuf::from("/usr/bin/bash")),
            ("git", PathBuf::from("/usr/bin/git")),
        ] {
            symlink(path, bin.join(name)).unwrap();
        }
        let fake = bin.join("fake-engine");
        executable::write(&fake, FAKE);
        let script = home.join("fake-script.json");
        std::fs::write(&script, b"{}").unwrap();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let mut env = BTreeMap::new();
        for (n, p) in [
            ("SLUICE_HOME", home.clone()),
            ("PATH", bin.clone()),
            ("SLUICE_HOST_PATH", bin),
            ("SLUICE_PYTHON_DIR", repo.join("python")),
            ("SLUICE_UV_BIN", uv()),
            ("SLUICE_FAKE_ENGINE_BIN", fake),
            ("SLUICE_FAKE_ENGINE_SCRIPT", script),
            (
                "SLUICE_TEST_AGENT_TRANSIENT_MARKER",
                home.join("turn-committed"),
            ),
        ] {
            env.insert(n.into(), p.to_string_lossy().into_owned());
        }
        env.insert("SLUICE_FIXTURE".into(), "1".into());
        env.insert("SLUICE_BACKOFF".into(), "0".into());
        env.insert("COMPOSITION_ENV_FIXTURE".into(), "frozen-launch".into());
        let mut gate = Self {
            temp,
            home,
            broker: None,
            env,
            project: ProjectId::new(),
        };
        configure(&mut gate);
        gate.boot();
        let CommandReply::Project(p)=gate.rpc(json!({"command":"project_create","args":{"name":"compose","description":"composition fixture","icon":null,"resources":{"section":1},"author":"test"}})) else {panic!("project")};
        gate.project = p.project_id;
        gate
    }
    fn boot(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.home.join("broker.log"))
            .unwrap();
        self.broker = Some(
            Command::new(env!("CARGO_BIN_EXE_sluice"))
                .arg("coordinator")
                .envs(&self.env)
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        self.wait(|g| UnixStream::connect(g.home.join("coordinator.sock")).is_ok());
    }
    fn wait(&self, mut condition: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !condition(self) {
            assert!(
                Instant::now() < deadline,
                "timed out, broker: {}, status: {}",
                std::fs::read_to_string(self.home.join("broker.log")).unwrap_or_default(),
                if self.home.join("coordinator.sock").exists() {
                    self.status()
                } else {
                    Value::Null
                }
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    fn selector(&self) -> Value {
        json!({"kind":"id","value":self.project})
    }
    fn rpc(&self, command: Value) -> CommandReply {
        match self.try_rpc(command) {
            Ok(reply) => reply,
            Err(e) => panic!("command failed: {e:?}"),
        }
    }
    fn try_rpc(&self, command: Value) -> Result<CommandReply, sluice_model::error::PublicError> {
        let request = json!({"protocol":1,"request_id":"compose-test","run_capability":null,"command":command});
        let mut stream = UnixStream::connect(self.home.join("coordinator.sock")).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        stream.write_all(&encode_frame(&request).unwrap()).unwrap();
        let reply: RpcReply = decode_json(&read(&mut stream)).unwrap();
        match reply.result {
            RpcResult::Ok(v) => Ok(*v),
            RpcResult::Error(e) => Err(e),
        }
    }
    fn data(&self, command: Value) -> Value {
        let CommandReply::Data(v) = self.rpc(command) else {
            panic!("data")
        };
        v.into_value()
    }
    fn status(&self) -> Value {
        self.data(json!({"command":"status","args":{"project":self.selector(),"selection":{"steps":null,"tags":null},"all":true}}))
    }
    fn plan(&self, steps: Value) {
        self.rpc(json!({"command":"plan_patch","args":{"project":self.selector(),"rev":1,"ops":[{"op":"replace","path":"","value":{"inputs":{},"steps":steps,"outputs":{}}}],"start":true,"dry_run":false,"reason":"fixture","author":"test"}}));
    }
    fn lease(&self) -> UnixStream {
        let mut stream = UnixStream::connect(self.home.join("coordinator.sock")).unwrap();
        stream.write_all(&encode_frame(&json!({"protocol":1,"request_id":"scheduler","run_capability":null,"command":{"runtime":"acquire_scheduler","args":{"owner":"compose"}}})).unwrap()).unwrap();
        let reply: Value = decode_json(&read(&mut stream)).unwrap();
        assert!(reply["result"].get("Ok").is_some(), "{reply}");
        stream
    }
    fn function(&self, name: &str, inputs: Value, outputs: Value, code: &str) {
        let dir = self
            .home
            .join("projects")
            .join(self.project.to_string())
            .join("fns")
            .join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("fn.json"),
            json!({"name":name,"inputs":inputs,"outputs":outputs,"open":true}).to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.join("main.py"),
            format!(
                "# /// script\n# requires-python = \">=3.12\"\n# dependencies = []\n# ///\n{code}"
            ),
        )
        .unwrap();
    }
    fn script(&self, value: Value) {
        std::fs::write(self.home.join("fake-script.json"), value.to_string()).unwrap();
    }
    fn run(&self, step: &str) -> String {
        self.status()["steps"][step]["run_ids"][0]
            .as_str()
            .unwrap()
            .into()
    }
    fn terminal(&self, step: &str) -> Value {
        self.wait(|g| {
            matches!(
                g.status()["steps"][step]["status"].as_str(),
                Some("succeeded" | "failed" | "skipped" | "stale")
            )
        });
        self.status()["steps"][step].clone()
    }
    fn events(&self) -> Vec<Value> {
        std::fs::read_to_string(self.home.join("fake-events.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
    /// The orchestrator tells `step` something: its receipt.
    fn say(&self, step: &str, body: &str) -> MessageReceipt {
        let CommandReply::Receipt(receipt) = self
            .rpc(json!({"command":"say","args":{"project":self.selector(),"body":body,"to":step}}))
        else {
            panic!("say")
        };
        receipt
    }
    fn post(&self, step: &str, body: &str) -> i64 {
        self.say(step, body).id.0
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        let _ = &self.temp;
        if std::thread::panicking() {
            let evidence = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/g3-fix-evidence")
                .join(self.temp.path().file_name().unwrap());
            if let Ok(runs) = std::fs::read_dir(self.home.join("runs")) {
                for run in runs.flatten() {
                    let dest = evidence.join(run.file_name());
                    let _ = std::fs::create_dir_all(&dest);
                    for name in [
                        "native.json",
                        "stderr-tail.log",
                        "engine-environments.jsonl",
                        "fixture-events.jsonl",
                        "fixture-errors.log",
                        "devin-hooks.jsonl",
                        "devin.log",
                        "fixture-hook-replies.jsonl",
                        "app-server.log",
                        "codex-wire.jsonl",
                    ] {
                        let _ = std::fs::copy(run.path().join(name), dest.join(name));
                    }
                }
            }
        }
        if let Some(mut child) = self.broker.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        units::stop_home_units(&self.home);
    }
}
fn uv() -> PathBuf {
    let output = Command::new("/bin/bash")
        .args(["-lc", "command -v uv"])
        .output()
        .unwrap();
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .canonicalize()
        .unwrap()
}
fn read(stream: &mut UnixStream) -> Vec<u8> {
    let mut size = [0; 4];
    stream.read_exact(&mut size).unwrap();
    let mut body = vec![0; u32::from_be_bytes(size) as usize];
    stream.read_exact(&mut body).unwrap();
    body
}
fn bindings(values: Value) -> Value {
    Value::Object(
        values
            .as_object()
            .unwrap()
            .iter()
            .map(|(n, v)| (n.clone(), json!({"default":v})))
            .collect(),
    )
}
fn agent(g: &Gate) -> Value {
    json!({"run":"agent.run","in":bindings(json!({"engine":"fake","cwd":g.temp.path(),"spec":"Submit summary and finish"})),"outputs":{"summary":"string"}})
}
/// Point `g` at the fake `engine` CLI, run through a wrapper that records its environment
/// and commits once per internal attempt; `native-fixture.json` is its script.
fn native_engine(g: &mut Gate, engine: &str) {
    let root = g.temp.path().to_path_buf();
    let owner = root.join("owner");
    for suffix in [
        ".codex",
        ".claude",
        ".config/devin",
        ".local/share",
        ".cache",
        ".local/state",
    ] {
        std::fs::create_dir_all(owner.join(suffix)).unwrap();
    }
    std::fs::write(owner.join(".codex/config.toml"), "").unwrap();
    std::fs::write(owner.join(".codex/auth.json"), "fake credential").unwrap();
    std::fs::write(owner.join(".config/devin/config.json"), "{}").unwrap();
    let cwd = root.join("work");
    std::fs::create_dir(&cwd).unwrap();
    for args in [
        vec!["init", "-q"],
        vec!["config", "user.name", "Scratch"],
        vec!["config", "user.email", "scratch@example.invalid"],
        vec!["commit", "-q", "--allow-empty", "-m", "Initialize scratch"],
    ] {
        assert!(
            Command::new("/usr/bin/git")
                .args(args)
                .current_dir(&cwd)
                .status()
                .unwrap()
                .success()
        );
    }
    let bin = root.join("bin");
    symlink("/usr/bin/stty", bin.join("stty")).unwrap();
    let path = bin.to_string_lossy().into_owned();
    // Deliberately contaminate PATH. Engines must restore the explicitly pinned host PATH.
    g.env
        .insert("PATH".into(), format!("/missing-virtualenv/bin:{path}"));
    g.env.insert("SLUICE_HOST_PATH".into(), path);
    for (key, suffix) in [
        ("HOME", ""),
        ("CODEX_HOME", ".codex"),
        ("CLAUDE_CONFIG_DIR", ".claude"),
        ("XDG_CONFIG_HOME", ".config"),
        ("XDG_DATA_HOME", ".local/share"),
        ("XDG_CACHE_HOME", ".cache"),
        ("XDG_STATE_HOME", ".local/state"),
    ] {
        g.env.insert(
            key.into(),
            owner.join(suffix).to_string_lossy().into_owned(),
        );
    }
    g.env.insert("PYTHONHOME".into(), "/missing-python".into());
    g.env.insert("CLAUDECODE".into(), "parent".into());
    g.env.insert("SLUICE_BACKOFF".into(), "1".into());
    g.env.insert("SLUICE_AGENT_POLL_S".into(), "0.02".into());
    g.env.insert("SLUICE_AGENT_SETTLE_S".into(), "0.1".into());
    g.env
        .insert("SLUICE_AGENT_GRACE_MIN".into(), "0.003".into());
    g.env.insert("LANG".into(), "C.UTF-8".into());
    g.env.insert("TERM".into(), "xterm-256color".into());
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    g.env.insert(
        "SLUICE_TMUX_PREFIX".into(),
        workspace
            .join("target/private-tmux")
            .to_string_lossy()
            .into_owned(),
    );
    let config = root.join("native-fixture.json");
    std::fs::write(&config, json!({"prompts":root.join("devin-prompts.jsonl"),"turns":[{"busy_s":0.05,"busy_ms":50,"reply":"first"},{"busy_s":0.05,"busy_ms":50,"reply":"continued"}]}).to_string()).unwrap();
    g.env.insert(
        "SLUICE_FAKE_CLAUDE".into(),
        config.to_string_lossy().into_owned(),
    );
    g.env
        .insert("FAKE_DEVIN".into(), config.to_string_lossy().into_owned());
    g.env.insert("SLUICE_CODEX_FIXTURE".into(), "tui".into());
    let executable = bin.join(engine);
    let script = format!(
        "#!/bin/sh\nset -e\ncase \"$1\" in --version|--help|models|debug) exec '{}' {engine} \"$@\" ;; esac\n/usr/bin/python3 - <<'PY'\n{NATIVE_ENV_PROBE}\nPY\n{}\nexec '{}' {engine} \"$@\" 2>>\"$SLUICE_RUN_DIR/fixture-errors.log\"\n",
        Path::new(env!("CARGO_BIN_EXE_fixture")).display(),
        if engine == "codex" {
            "case \"$1\" in -c) exec /usr/bin/sleep 600 ;; esac"
        } else {
            ""
        },
        Path::new(env!("CARGO_BIN_EXE_fixture")).display()
    );
    executable::write(&executable, script);
    g.env.insert(
        format!("SLUICE_{}_BIN", engine.to_uppercase()),
        executable.to_string_lossy().into_owned(),
    );
}
fn native_factory(engine: &str) {
    use sluice_agents::{
        delivery::DeliveryState,
        engines::InputId,
        supervisor::{Checkpoint, State},
    };
    let g = Gate::configured(|g| {
        native_engine(g, engine);
        std::fs::write(
            g.home.join("turn-committed"),
            "inject after a completed turn",
        )
        .unwrap();
    });
    // .env secrets reach the engine's pane: the project's over the home's.
    std::fs::write(
        g.home.join(".env"),
        "DOTENV_AGENT=home\nDOTENV_AGENT_HOME=home\n",
    )
    .unwrap();
    let project_dir = g.home.join("projects").join(g.project.to_string());
    std::fs::create_dir_all(&project_dir).unwrap();
    std::fs::write(project_dir.join(".env"), "DOTENV_AGENT=project\n").unwrap();
    let cwd = g.temp.path().join("work");
    let baseline = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&cwd)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    // Devin runs a fusion object; Codex and Claude their defaults. Each id is checked against
    // the fixture CLI's listing and lands in the result's model.
    let (model, resolved) = match engine {
        "devin" => (
            json!({"type":"fusion","main":{"model":"claude-opus-5-5","effort":"high","fast":true},"sidekick":{"model":"swe-2","effort":"high"}}),
            "fusion-claude-opus-5-5-high-fast-sidekick-swe-2-high",
        ),
        "codex" => (Value::Null, "gpt-6.1-sol@high"),
        _ => (Value::Null, "opus@high"),
    };
    g.plan(json!({"work":{"run":"agent.run","in":bindings(json!({"engine":engine,"cwd":cwd,"spec":"Complete a fake turn","model":model})),"outputs":{"word":"string"}}}));
    let _lease = g.lease();
    g.wait(|g| g.status()["steps"]["work"]["run_ids"][0].is_string());
    let run = g.run("work");
    let directory = g.home.join("runs").join(&run);
    g.wait(|_| {
        Checkpoint::read(&directory)
            .ok()
            .flatten()
            .is_some_and(|c| c.state == State::Backoff)
    });
    let first = Checkpoint::read(&directory).unwrap().unwrap();
    let session = first.session.clone().unwrap();
    // A shutdown callback already accepted by the guardian can outlive the old pane. Nobody
    // waits for its reply, so the guardian's next exchange removes it: read it as it lands.
    let stale_reply = (engine == "claude").then(|| {
        let journal = directory.join("engine-hooks");
        std::fs::create_dir_all(&journal).unwrap();
        std::fs::write(journal.join("stale.request.json"), json!({"run":run,"engine":"claude","event":"SessionEnd","payload":{"hook_event_name":"SessionEnd","session_id":session,"cwd":cwd,"transcript_path":g.temp.path().join("owner/.claude/projects/fixture").join(format!("{session}.jsonl"))}}).to_string()).unwrap();
        let path = journal.join("stale.reply.json");
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                if let Ok(bytes) = std::fs::read(&path) {
                    return Some(bytes);
                }
                if Instant::now() >= deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    });
    g.wait(|_| {
        Checkpoint::read(&directory)
            .ok()
            .flatten()
            .is_some_and(|c| {
                c.internal_attempt == 2
                    && c.delivery.entries.iter().any(|e| {
                        e.id == InputId::Continue { attempt: 2 }
                            && e.state == DeliveryState::Acknowledged
                    })
            })
    });
    if engine == "devin" {
        let addressed = g.post("work", "Addressed input before feedback retry");
        g.wait(|_| {
            Checkpoint::read(&directory)
                .ok()
                .flatten()
                .is_some_and(|c| {
                    c.delivery.entries.iter().any(|e| {
                        e.id == InputId::Message {
                            id: sluice_model::ids::MessageId(addressed),
                        } && e.state == DeliveryState::Acknowledged
                    })
                })
        });
    }
    g.rpc(json!({"command":"step_submit","args":{"project":g.project,"step":"work","run":run,"outputs":{"word":"blue"},"author":"fixture"}}));
    let done = g.terminal("work");
    assert_eq!(done["status"], "succeeded", "{engine}: {done}");
    assert_eq!(done["outputs"]["word"], "blue", "{engine}: {done}");
    assert_eq!(done["outputs"]["model"], resolved, "{engine}: {done}");
    if engine == "devin" {
        let config: Value =
            serde_json::from_slice(&std::fs::read(directory.join("devin-config.json")).unwrap())
                .unwrap();
        assert_eq!(config["agent"]["model"], resolved);
    }
    assert_eq!(
        done["outputs"]["session"],
        session.as_str(),
        "{engine}: {done}"
    );
    let checkpoint = Checkpoint::read(&directory).unwrap().unwrap();
    assert_eq!(checkpoint.internal_attempt, 2);
    assert_eq!(checkpoint.session.as_deref(), Some(session.as_str()));
    assert_eq!(checkpoint.head_before.as_deref(), Some(baseline.as_str()));
    assert_eq!(
        checkpoint
            .delivery
            .entries
            .iter()
            .find(|e| e.id == InputId::Task)
            .unwrap()
            .tries,
        1
    );
    let count = Command::new("/usr/bin/git")
        .args(["rev-list", "--count", &format!("{baseline}..HEAD")])
        .current_dir(&cwd)
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&count.stdout).trim(), "2");
    let environments: Vec<Value> =
        std::fs::read_to_string(directory.join("engine-environments.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
    assert!(environments.len() >= 2);
    for env in environments {
        for key in [
            "HOME",
            "PATH",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_CACHE_HOME",
            "XDG_STATE_HOME",
            "LANG",
            "TERM",
        ] {
            let expected = if key == "PATH" {
                &g.env["SLUICE_HOST_PATH"]
            } else {
                &g.env[key]
            };
            assert_eq!(
                env[key].as_str(),
                Some(expected.as_str()),
                "{engine}: {key}"
            );
        }
        for (key, value) in [
            ("SLUICE_HOME", g.home.to_string_lossy().into_owned()),
            ("SLUICE_BIN", env!("CARGO_BIN_EXE_sluice").into()),
            ("SLUICE_PROJECT_ID", g.project.to_string()),
            ("SLUICE_PROJECT", "compose".into()),
            (
                "SLUICE_PROJECT_DIR",
                g.home
                    .join("projects")
                    .join(g.project.to_string())
                    .to_string_lossy()
                    .into_owned(),
            ),
            (
                "SLUICE_CONTROL_SOCKET",
                directory
                    .join("control.sock")
                    .to_string_lossy()
                    .into_owned(),
            ),
            ("SLUICE_STEP", "work".into()),
            ("SLUICE_RUN_ID", run.clone()),
            ("SLUICE_RUN_DIR", directory.to_string_lossy().into_owned()),
            ("SLUICE_FN_DIR", directory.to_string_lossy().into_owned()),
            ("SLUICE_PREV_RUN", "".into()),
        ] {
            assert_eq!(env[key], value, "{engine}: {key}");
        }
        assert_eq!(env["DOTENV_AGENT"], "project", "{engine}");
        assert_eq!(env["DOTENV_AGENT_HOME"], "home", "{engine}");
        assert_eq!(env["callback_capability"], true);
        assert_eq!(env["unrelated"], false);
        assert_eq!(env["contaminated"], false);
        let key = match engine {
            "codex" => "CODEX_HOME",
            "claude" => "CLAUDE_CONFIG_DIR",
            _ => "XDG_DATA_HOME",
        };
        if engine == "codex" {
            assert!(
                Path::new(env[key].as_str().unwrap())
                    .starts_with(g.home.join("codex-native-homes"))
            );
        } else {
            assert_eq!(env[key], g.env[key]);
        }
    }
    if let Some(stale_reply) = stale_reply {
        let reply: Value = serde_json::from_slice(
            &stale_reply
                .join()
                .unwrap()
                .expect("the stale hook was answered"),
        )
        .unwrap();
        assert!(
            reply["Err"].is_object(),
            "stale hook must not enter the resumed adapter"
        );
        let prompts = std::fs::read_to_string(directory.join("fixture-prompts.jsonl")).unwrap();
        assert_eq!(
            prompts
                .lines()
                .filter(|line| line.contains("Your task is in"))
                .count(),
            1
        );
    }
    assert!(std::os::unix::net::UnixStream::connect(directory.join("tmux.sock")).is_err());
    if engine == "devin" {
        // Port p5-05's addressed-input/feedback sequence and force the missing Stop.
        let config = g.temp.path().join("native-fixture.json");
        std::fs::write(config, json!({"prompts":g.temp.path().join("devin-prompts.jsonl"),"turns":[{"reply":"task done"},{"exit_before_stop":true}]}).to_string()).unwrap();
        g.rpc(json!({"command":"step_retry","args":{"project":g.selector(),"selection":{"steps":["work"],"tags":null},"message":"Resume after addressed input","reason":"fixture feedback","author":"fixture"}}));
        g.wait(|g| {
            g.status()["steps"]["work"]["run_ids"][0]
                .as_str()
                .is_some_and(|id| id != run)
        });
        let feedback_run = g.run("work");
        let failed = g.terminal("work");
        assert_eq!(failed["status"], "failed", "{failed}");
        assert_eq!(
            failed["error"]["error"], "exited_without_submit",
            "{failed}"
        );
        assert_eq!(failed["error"]["session"], session.as_str(), "{failed}");
        let cp = Checkpoint::read(&g.home.join("runs").join(feedback_run))
            .unwrap()
            .unwrap();
        assert_eq!(cp.session.as_deref(), Some(session.as_str()));
        assert!(
            cp.delivery
                .entries
                .iter()
                .any(|e| matches!(e.id, InputId::Message { .. })
                    && e.state == DeliveryState::Acknowledged)
        );
    }
}
const NATIVE_ENV_PROBE: &str = r#"import json, os, pathlib, subprocess
run = pathlib.Path(os.environ['SLUICE_RUN_DIR'])
keys = ['HOME','PATH','XDG_CONFIG_HOME','XDG_DATA_HOME','XDG_CACHE_HOME','XDG_STATE_HOME','LANG','TERM','SLUICE_HOME','SLUICE_BIN','SLUICE_PROJECT_ID','SLUICE_PROJECT','SLUICE_STEP','SLUICE_RUN_ID','SLUICE_RUN_DIR','SLUICE_PROJECT_DIR','SLUICE_CONTROL_SOCKET','SLUICE_FN_DIR','SLUICE_PREV_RUN','CODEX_HOME','CLAUDE_CONFIG_DIR','DOTENV_AGENT','DOTENV_AGENT_HOME']
snapshot = {key: os.environ.get(key) for key in keys}
snapshot['callback_capability'] = bool(os.environ.get('SLUICE_RUN_CAPABILITY'))
snapshot['unrelated'] = 'COMPOSITION_ENV_FIXTURE' in os.environ
snapshot['contaminated'] = any(key in os.environ for key in ['PYTHONHOME','VIRTUAL_ENV','PYTHONPATH','CLAUDECODE'])
with (run / 'engine-environments.jsonl').open('a') as f:
    f.write(json.dumps(snapshot) + '\n')
attempt = json.loads((run / 'native.json').read_text())['internal_attempt']
marker = pathlib.Path.cwd() / ('commit-' + str(attempt))
if not marker.exists():
    marker.write_text('fake turn')
    subprocess.run(['/usr/bin/git','add',marker.name], check=True)
    subprocess.run(['/usr/bin/git','commit','-qm','Record fake invocation ' + str(attempt)], check=True)
if 'CODEX_HOME' in os.environ:
    tmp = pathlib.Path(os.environ['CODEX_HOME']) / 'tmp/arg0/fake'
    tmp.mkdir(parents=True, exist_ok=True)
    link = tmp / 'apply_patch'
    if not link.is_symlink(): link.symlink_to('/usr/bin/true')
"#;
#[test]
fn composed_codex_environment_and_same_run_retry() {
    native_factory("codex");
}
#[test]
fn composed_claude_environment_and_stale_shutdown_retry() {
    native_factory("claude");
}
#[test]
fn composed_devin_environment_and_resumed_permission_mode() {
    native_factory("devin");
}
/// A Python fn that runs a Devin agent (as lash.worker does) gives it the invocation
/// directory `runs/<run>/invocations/<invocation>`, where the control socket's absolute path
/// is longer than a Unix socket address holds. Devin's hooks must still reach the guardian,
/// or its task is never acknowledged ("Devin prompt acceptance unknown"). The fixture, like
/// the real CLI, carries on past a hook command that fails.
#[test]
fn composed_devin_hooks_reach_the_guardian_from_a_python_fn() {
    let g = Gate::configured(|g| native_engine(g, "devin"));
    let root = g.temp.path();
    std::fs::write(
        root.join("native-fixture.json"),
        json!({"prompts":root.join("devin-prompts.jsonl"),"hook_failures":"continue","turns":[{"busy_ms":50,"reply":"fixture done"}]}).to_string(),
    )
    .unwrap();
    g.function(
        "custom.devin",
        json!({"cwd":"string"}),
        json!({"session":"string","final":"string"}),
        r#"from sluice_fn import run

def main(inp, ctx):
    out = ctx.builtin('agent.run', {'engine':'devin', 'cwd':inp['cwd'], 'spec':'Complete a fake turn'})
    return {'session': out['session'], 'final': out['final']}
run(main)
"#,
    );
    g.plan(json!({"work":{"run":"custom.devin","in":bindings(json!({"cwd":root.join("work")}))}}));
    let _lease = g.lease();
    let done = g.terminal("work");
    let invocations = g.home.join("runs").join(g.run("work")).join("invocations");
    let invocation = std::fs::read_dir(&invocations)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let errors = std::fs::read_to_string(invocation.join("fixture-errors.log")).unwrap_or_default();
    assert_eq!(done["status"], "succeeded", "{done}\n{errors}");
    assert_eq!(done["outputs"]["final"], "fixture done", "{done}");
    // The condition the live home hit: a path no Unix socket address can hold.
    let socket = invocation.join("control.sock");
    assert!(socket.as_os_str().len() >= 108, "{}", socket.display());
    assert!(!errors.contains("hook command exited"), "{errors}");
    let hooks = std::fs::read_to_string(invocation.join("devin-hooks.jsonl")).unwrap();
    for event in ["SessionStart", "UserPromptSubmit", "Stop"] {
        assert!(hooks.contains(&format!("\"{event}\"")), "{event}: {hooks}");
    }
}
/// A stored plan still holding the retired forms (a string `model`, an `effort` input) keeps
/// validating; each such step fails at launch with kind `Invalid` and the object to use. An
/// object whose id the engine does not list fails the same way, naming the nearest ids.
#[test]
fn retired_model_forms_validate_in_the_plan_and_fail_at_launch() {
    let g = Gate::configured(|g| native_engine(g, "devin"));
    let cwd = g.temp.path().join("work");
    let step = |run: &str, extra: Value| {
        let mut inputs = json!({"cwd":cwd,"spec":"Never runs"});
        inputs
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!({"run":run,"in":bindings(inputs)})
    };
    g.plan(json!({
        "run-sol": step("agent.run", json!({"engine":"codex","model":"sol","effort":"xhigh"})),
        "codex-sol": step("agent.codex", json!({"model":"sol","effort":"high"})),
        "devin-fusion": step("agent.devin", json!({"model":"fusion"})),
        "devin-effort": step("agent.devin", json!({"model":{"type":"normal","model":"swe-2","effort":"high"},"effort":"max"})),
        "devin-unknown": step("agent.devin", json!({"model":{"type":"normal","model":"swe-2","effort":"hgh"}})),
    }));
    let problems = g.data(json!({"command":"verify","args":{"project":g.selector()}}));
    assert_eq!(problems, json!([]), "{problems}");
    let _lease = g.lease();
    for (step, message) in [
        (
            "run-sol",
            r#"model must be a JSON object, not "sol", and effort is no longer an input: drop effort and set model to {"type":"normal","model":"sol","effort":"xhigh"}"#,
        ),
        (
            "codex-sol",
            r#"set model to {"type":"normal","model":"sol","effort":"high"}"#,
        ),
        (
            "devin-fusion",
            r#"model must be a JSON object, not "fusion"; use {"type":"fusion","main":{"model":"claude-opus-5-5","effort":"high"},"sidekick":{"model":"swe-2","effort":"high"}}"#,
        ),
        (
            "devin-effort",
            r#"effort is no longer an input: drop it and set model to {"type":"normal","model":"swe-2","effort":"max"}"#,
        ),
        (
            "devin-unknown",
            "devin has no model swe-2-hgh (composed from model",
        ),
    ] {
        let result = g.terminal(step);
        assert_eq!(result["status"], "failed", "{step}: {result}");
        assert_eq!(
            result["error"]["error"], "agent_failure",
            "{step}: {result}"
        );
        assert_eq!(result["error"]["kind"], "Invalid", "{step}: {result}");
        let text = result["error"]["message"].as_str().unwrap();
        assert!(text.contains(message), "{step}: {text}");
    }
    let unknown = g.status()["steps"]["devin-unknown"]["error"]["message"].clone();
    assert!(
        unknown.as_str().unwrap().contains("nearest: swe-2-high"),
        "{unknown}"
    );
    // Nothing reached the engine: no session started for any of them.
    assert!(!g.temp.path().join("devin-prompts.jsonl").exists());
}
#[test]
fn builtin_python_tools_sections_and_inline_execute_through_guardian() {
    let g = Gate::new();
    g.function(
        "custom.work",
        json!({"value":"int"}),
        json!({"answer":"int"}),
        r#"from sluice_fn import run, sh

def main(inp, ctx):
    with ctx.acquire('section', timeout=5):
        reply = ctx.tool('status', {})
        assert reply['project']['project_id'] == ctx.project_id
        assert ctx.project_dir.name == ctx.project_id
        assert 'type' in ctx.extra_inputs['topic']
        assert ctx.outputs['tag']['doc'] == 'A tag'
        assert sh(['python3', '-c', 'print(7)']).stdout.strip() == '7'
    with ctx.acquire('section', timeout=5):
        answer = inp['value'] + 1
    def acquire(key):
        return ctx.callback('acquire_lease', {'run':ctx.run_id, 'resource':'section', 'amount':1, 'priority':0, 'request_id':key})['data']
    first = acquire('first')
    waiting = acquire('second')
    assert first['state'] == 'held' and waiting['state'] == 'waiting'
    ctx.callback('release_lease', {'run':ctx.run_id, 'lease':first['lease']})
    granted = acquire('second')
    assert granted['lease'] == waiting['lease'] and granted['state'] == 'held'
    ctx.callback('release_lease', {'run':ctx.run_id, 'lease':granted['lease']})
    return {'answer':answer, 'tag':'yes'}
run(main)
"#,
    );
    g.plan(json!({"seed":{"run":"core.echo","in":{"value":{"default":4}}},"python":{"run":"custom.work","in":{"value":{"source":"seed/value"},"topic":{"default":"guide"}},"outputs":{"tag":{"type":"string","doc":"A tag"}}},"inline":{"run":"inline.python","in":{"code":{"default":"out = n + 1"},"n":{"source":"python/answer"}}}}));
    let _lease = g.lease();
    let value = g.terminal("inline");
    assert_eq!(value["status"], "succeeded", "{}", g.status());
    assert_eq!(value["outputs"]["value"], 6);
    assert_eq!(g.status()["steps"]["python"]["status"], "succeeded");
    let run = g.run("python");
    let db = rusqlite::Connection::open(g.home.join("sluice.db")).unwrap();
    let count: i64 = db
        .query_row(
            "SELECT count(*) FROM leases WHERE run_id=?1 AND kind='section' AND state='released'",
            [run],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 4);
}
#[test]
fn fake_agent_submits_delivers_once_and_feedback_resumes_previous_session() {
    let g = Gate::new();
    g.script(json!({"outputs":{"summary":"fake output"},"wait_message":true,"marker_transient_once":true}));
    g.plan(json!({"work":agent(&g),"after":{"run":"core.external","after":["work"],"outputs":{"ok":"boolean"}}}));
    // A step that will run once `work` is done: its next run is assigned the message.
    let queued = g.say("after", "for the next run");
    assert_eq!((queued.delivery, queued.run), (Delivery::Queued, None));
    let _lease = g.lease();
    g.wait(|g| g.home.join("fake-events.jsonl").exists());
    // The live fake agent listens: it is handed the message on its live feed.
    let receipt = g.say("work", "first live message");
    assert_eq!(receipt.delivery, Delivery::Delivered, "{receipt:?}");
    assert_eq!(receipt.run.map(|r| r.to_string()), Some(g.run("work")));
    assert_eq!(receipt.thread, "step-work");
    let id = receipt.id.0;
    let value = g.terminal("work");
    assert_eq!(value["status"], "succeeded", "{value}");
    assert_eq!(value["outputs"]["summary"], "fake output");
    let first = g.run("work");
    // Settled: a message to it is refused; a retry's message reaches its next run.
    let refused = g.try_rpc(
        json!({"command":"say","args":{"project":g.selector(),"body":"after the fact","to":"work"}}),
    );
    assert!(
        matches!(&refused, Err(sluice_model::error::PublicError::Conflict { message, .. }) if message.contains("settled")),
        "{refused:?}"
    );
    let delivered = g
        .events()
        .into_iter()
        .filter(|e| {
            (e["command"]["command"] == "deliver_text" || e["command"]["command"] == "steer")
                && e["command"]["id"]["kind"] == "message"
                && e["command"]["id"]["id"] == id
        })
        .count();
    assert_eq!(delivered, 1, "events: {:?}", g.events());
    g.script(json!({"outputs":{"summary":"resumed output"}}));
    g.rpc(json!({"command":"step_retry","args":{"project":g.selector(),"selection":{"steps":["work"],"tags":null},"message":"Continue the existing session","reason":"feedback","author":"test"}}));
    g.wait(|g| g.status()["steps"]["work"]["run_ids"][0] != first);
    let second = g.terminal("work");
    assert_eq!(second["status"], "succeeded", "{second}");
    assert_eq!(second["outputs"]["summary"], "resumed output");
    assert_eq!(second["outputs"]["session"], value["outputs"]["session"]);
    assert!(
        g.events()
            .iter()
            .any(|e| e["command"]["command"] == "resume")
    );
    let run = g.run("work");
    let launch: Value = serde_json::from_slice(
        &std::fs::read(g.home.join("runs").join(run).join("runtime.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(launch["prev_run"], first);
}
/// An agent's task starts with its step's previous attempt (the first says so) and the git
/// status of its cwd when it launched.
#[test]
fn an_agent_task_starts_with_its_previous_attempt_and_its_dirty_work_tree() {
    let g = Gate::new();
    let repo = g.temp.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    for args in [
        &["init", "-q"][..],
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "first",
        ],
    ] {
        assert!(
            Command::new("/usr/bin/git")
                .current_dir(&repo)
                .args(args)
                .status()
                .unwrap()
                .success()
        );
    }
    std::fs::write(repo.join("left.txt"), "left by hand").unwrap();
    g.script(json!({"fatal":true}));
    g.plan(json!({"work":{"run":"agent.run","in":bindings(json!({"engine":"fake","cwd":repo,"spec":"Submit summary and finish"})),"outputs":{"summary":"string"}}}));
    let _lease = g.lease();
    let failed = g.terminal("work");
    assert_eq!(failed["status"], "failed", "{failed}");
    let first = g.run("work");
    let task =
        |run: &str| std::fs::read_to_string(g.home.join("runs").join(run).join("task.md")).unwrap();
    let text = task(&first);
    let note = text.find("## Previous attempt").expect("note");
    assert!(
        note < text.find("Submit summary and finish").unwrap(),
        "{text}"
    );
    assert!(
        text.contains("None: this is the first attempt at this step."),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "The working directory {} has 1 uncommitted path now: ?? left.txt.",
            repo.display()
        )),
        "{text}"
    );
    g.script(json!({"outputs":{"summary":"done"}}));
    g.rpc(json!({"command":"step_retry","args":{"project":g.selector(),"selection":{"steps":["work"],"tags":null},"message":null,"reason":"again","author":"test"}}));
    g.wait(|g| g.status()["steps"]["work"]["run_ids"][0] != first.as_str());
    let second = g.run("work");
    assert_eq!(g.terminal("work")["status"], "succeeded");
    let text = task(&second);
    assert!(
        text.contains(&format!(
            "This is attempt 2 at this step. The previous one (run {first}, "
        )),
        "{text}"
    );
    assert!(text.contains(") failed: agent_failure"), "{text}");
    assert!(text.contains("It submitted nothing"), "{text}");
}
#[test]
fn python_agent_wrapper_retries_transient_in_same_run_and_lands_outer_outputs() {
    let g = Gate::new();
    g.script(json!({"outputs":{"summary":"from agent"},"marker_transient_once":true}));
    g.function("custom.worker",json!({"cwd":"string"}),json!({"session":"string"}),r#"from sluice_fn import run, Transient
import fcntl

def main(inp, ctx):
    try:
        result = ctx.builtin('agent.run', {'engine':'fake', 'cwd':inp['cwd'], 'spec':ctx.header('Do the work')})
    except Transient:
        locks = list((ctx.home / 'locks').glob('session-*.lock'))
        assert len(locks) == 1
        with locks[0].open('r+') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                raise AssertionError('session lock released during helper retry')
            except BlockingIOError:
                (ctx.home / 'lock-retained').write_text('yes')
        raise
    fields = ctx.submission()
    return {'session': result['session'], 'summary':fields['summary']}
run(main, retries=1, backoff=0)
"#);
    g.plan(json!({"work":{"run":"custom.worker","in":bindings(json!({"cwd":g.temp.path()})),"outputs":{"summary":"string"}}}));
    let _lease = g.lease();
    let value = g.terminal("work");
    assert_eq!(value["status"], "succeeded", "{value}");
    assert_eq!(value["outputs"]["summary"], "from agent");
    let events = g.events();
    assert!(events.iter().any(|e| e["command"]["command"] == "resume"));
    assert_eq!(
        events
            .iter()
            .filter(|e| e["command"]["command"] == "deliver_text"
                && e["command"]["id"]["kind"] == "task")
            .count(),
        1
    );
    let run = g.run("work");
    let db = rusqlite::Connection::open(g.home.join("sluice.db")).unwrap();
    let count: i64 = db
        .query_row("SELECT count(*) FROM runs WHERE step_id='work'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
    assert!(g.home.join("lock-retained").exists());
    assert!(g.home.join("turn-committed.injected").exists());
    assert!(!g.home.join("turn-committed").exists());
    let launch: Value = serde_json::from_slice(
        &std::fs::read(g.home.join("runs").join(&run).join("launch.json")).unwrap(),
    )
    .unwrap();
    assert!(
        events
            .iter()
            .all(|e| e["cgroup"] != launch["executor"]["cgroup"])
    );
    assert_eq!(
        events
            .iter()
            .map(|e| e["pid"].as_u64().unwrap())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
    let checkpoint: Value = serde_json::from_slice(
        &std::fs::read(g.home.join("runs").join(run).join("native.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(checkpoint["internal_attempt"], 2);
    let groups = events
        .iter()
        .map(|e| e["cgroup"].as_str().unwrap().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(groups.len(), 1);
    assert!(groups.iter().all(|p| p.contains("/payload/")));
}
#[test]
fn rejected_python_registers_action_and_rearms_target_atomically() {
    let g = Gate::new();
    g.function(
        "custom.land",
        json!({}),
        json!({}),
        r#"from sluice_fn import run, Rejected

def main(inp, ctx):
    if (ctx.home / 'rejected-once').exists():
        return {}
    (ctx.home / 'rejected-once').write_text('yes')
    ctx.retry_on_failure('work', 'Fix the review')
    raise Rejected('Review refused')
run(main)
"#,
    );
    g.plan(json!({"work":{"run":"core.echo","in":{"value":{"default":1}}},"land":{"run":"custom.land","after":["work"]}}));
    let lease = g.lease();
    g.wait(|g| g.home.join("runs").exists());
    g.wait(|g|match g.rpc(json!({"command":"log_read","args":{"project":g.selector(),"since_seq":null,"kinds":["run.completion_action"],"threads":null,"limit":100}})) { CommandReply::Records(page) => !page.records.is_empty(), _ => false });
    drop(lease);
    let db = rusqlite::Connection::open(g.home.join("sluice.db")).unwrap();
    let (action,finished):(String,Option<String>)=db.query_row("SELECT completion_action,finished_at FROM runs WHERE step_id='land' ORDER BY created_at LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert!(finished.is_some());
    assert!(action.contains("Fix the review"));
    let count: i64 = db
        .query_row(
            "SELECT count(*) FROM messages WHERE body='Fix the review'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let (result, outcome): (String, String) = db.query_row("SELECT result,action_outcome FROM runs WHERE step_id='land' ORDER BY created_at LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    let result: Value = serde_json::from_str(&result).unwrap();
    let outcome: Value = serde_json::from_str(&outcome).unwrap();
    assert_eq!(result["status"], "failed");
    assert_eq!(outcome["outcome"], "applied");
    let work: i64 = db
        .query_row(
            "SELECT work_generation FROM steps WHERE step_id='work'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(work >= 2);
}
#[test]
fn terminal_agent_error_preserves_kind_message_and_session_through_python() {
    let g = Gate::new();
    g.script(json!({"fatal":true}));
    g.function("custom.failure",json!({"cwd":"string"}),json!({}),r#"from sluice_fn import run

def main(inp, ctx):
    return ctx.builtin('agent.run', {'engine':'fake', 'cwd':inp['cwd'], 'spec':'Fail with a typed error'})
run(main)
"#);
    g.plan(json!({"work":{"run":"custom.failure","in":bindings(json!({"cwd":g.temp.path()}))}}));
    let _lease = g.lease();
    let result = g.terminal("work");
    assert_eq!(result["status"], "failed", "{result}");
    assert_eq!(result["error"]["error"], "agent_failure");
    assert_eq!(result["error"]["kind"], "EngineExited");
    assert_eq!(result["error"]["message"], "fixture terminal error");
    assert_eq!(result["error"]["session"], "fake-session");
}
#[test]
fn compiled_builtin_retries_transient_within_one_reserved_run() {
    let g = Gate::new();
    let fake = g.temp.path().join("bin/gh");
    executable::write(
        &fake,
        r#"#!/usr/bin/python3
import os, pathlib, sys, json
assert os.environ['COMPOSITION_ENV_FIXTURE'] == 'frozen-launch'
p = pathlib.Path(os.environ['SLUICE_HOME']) / 'gh-ran'
n = int(p.read_text()) + 1 if p.exists() else 1
p.write_text(str(n))
if n == 1: sys.exit(1)
print(json.dumps({'state':'OPEN','headRefOid':'fixture-sha','url':'fixture-url','statusCheckRollup':[]}))
"#,
    );
    g.plan(json!({"wait":{"run":"gh.pr_wait","in":bindings(json!({"path":g.temp.path(),"pr":"1","until":"checks"}))}}));
    let _lease = g.lease();
    let result = g.terminal("wait");
    assert_eq!(result["status"], "succeeded", "{result}");
    assert_eq!(result["outputs"]["sha"], "fixture-sha");
    assert_eq!(std::fs::read_to_string(g.home.join("gh-ran")).unwrap(), "2");
    let db = rusqlite::Connection::open(g.home.join("sluice.db")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM runs WHERE step_id='wait'", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap(),
        1
    );
}
fn sequential_messages(live: bool) {
    // Ported from execution review's sequential_message_delivery probe.
    let g = Gate::configured(|g| {
        g.env.insert("SLUICE_AGENT_SETTLE_S".into(), "0.1".into());
        g.env.insert("SLUICE_AGENT_NUDGES".into(), "1".into());
    });
    // The first agent stops without submitting (a run submits once, and submitting ends
    // the agent); the fn continues its session with a second one, as a wrapper continues
    // after a wall cap.
    g.script(json!({"outputs":{"summary":"done"},"wait_message":live,"no_submit":true}));
    g.function("custom.sequential", json!({"cwd":"string"}), json!({"session":"string"}), r#"from sluice_fn import run, AgentFailure
import json, os, pathlib
def main(inp, ctx):
    try:
        ctx.builtin('agent.run', {'engine':'fake', 'cwd':inp['cwd'], 'spec':'First task'})
        raise RuntimeError('the first agent was to stop without submitting')
    except AgentFailure as failure:
        assert failure.kind == 'ExitedWithoutSubmit', failure.kind
        session = failure.session
    pathlib.Path(os.environ['SLUICE_FAKE_ENGINE_SCRIPT']).write_text(json.dumps({'outputs':{'summary':'done'}}))
    result = ctx.builtin('agent.run', {'engine':'fake', 'cwd':inp['cwd'], 'spec':'Second task', 'session':session})
    return {'session':result['session']}
run(main)
"#);
    g.plan(json!({"work":{"run":"custom.sequential","in":bindings(json!({"cwd":g.temp.path()})),"outputs":{"summary":"string"}}}));
    let assigned = (!live).then(|| g.post("work", "Apply this once"));
    let _lease = g.lease();
    let message = assigned.unwrap_or_else(|| {
        g.wait(|g| {
            g.home.join("fake-events.jsonl").exists()
                && g.events()
                    .iter()
                    .any(|e| e["command"]["id"]["kind"] == "task")
        });
        g.post("work", "Apply this live message once")
    });
    let result = g.terminal("work");
    assert_eq!(result["status"], "succeeded", "{result}");
    let messages: Vec<_> = g
        .events()
        .into_iter()
        .filter(|e| e["command"]["id"]["kind"] == "message")
        .collect();
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert_eq!(messages[0]["command"]["id"]["id"], message);
    let events = g.events();
    let pids: std::collections::BTreeSet<_> =
        events.iter().map(|e| e["pid"].as_u64().unwrap()).collect();
    assert_eq!(pids.len(), 2);
    assert_eq!(
        events
            .iter()
            .filter(|e| e["command"]["id"]["kind"] == "task")
            .count(),
        2
    );
}
#[test]
fn sequential_composition_retains_acknowledged_assigned_messages() {
    sequential_messages(false);
}
#[test]
fn sequential_composition_retains_acknowledged_live_messages() {
    sequential_messages(true);
}
const FAKE: &str = r#"#!/usr/bin/python3
import os, sys, json, socket, struct, pathlib
home = pathlib.Path(os.environ['SLUICE_HOME'])
config = json.loads(pathlib.Path(sys.argv[2]).read_text())
run = os.environ['SLUICE_RUN_ID']
state = {'status':'starting','turns_started':0,'turns_completed':0,'waiting':None,'background_work':[],'compactions':0,'final_text':'fake done','session_id':'fake-session','acknowledged':[],'not_accepted':[],'progress':0,'error':None}
messages = False
submitted = False
transient = False
sessions = home / 'fake-sessions.json'
data = json.loads(sessions.read_text()) if sessions.exists() else {}
data['fake-session'] = os.getcwd()
sessions.write_text(json.dumps(data))
def rpc(command):
    raw = json.dumps({'protocol':1,'request_id':str(state['progress']),'run_capability':os.environ['SLUICE_RUN_CAPABILITY'],'command':command}).encode()
    with socket.socket(socket.AF_UNIX) as s:
        s.connect(os.environ['SLUICE_CONTROL_SOCKET']); s.sendall(struct.pack('>I',len(raw))+raw)
        def read(n):
            out=b''
            while len(out)<n:
                part=s.recv(n-len(out))
                if not part: raise RuntimeError('callback closed')
                out+=part
            return out
        size=struct.unpack('>I',read(4))[0]; answer=json.loads(read(size))
        assert answer['result']['status']=='ok', answer
for line in sys.stdin:
    req=json.loads(line)
    cgroup=pathlib.Path('/proc/self/cgroup').read_text().strip().split('::')[-1]
    with (home/'fake-events.jsonl').open('a') as f: f.write(json.dumps({'run':run,'pid':os.getpid(),'cgroup':cgroup,**req})+'\n')
    error=None
    if req['operation']=='command':
        cmd=req['command']
        if cmd['command'] in ('start_fresh','resume'): state['status']='idle'
        if cmd['command'] in ('deliver_text','steer'):
            if cmd['id']['kind']=='message': messages=True
            state['acknowledged'].append(cmd['id']);state['turns_started']+=1;state['status']='busy';state['progress']+=1
        if cmd['command']=='request_exit': state['status']='exited'
    if req['operation']=='observe' and state['turns_started']:
        if config.get('fatal'):
            state['error']={'kind':'fatal','message':'fixture terminal error'}
        elif config.get('marker_transient_once') and not (home/'turn-committed.injected').exists():
            (home/'turn-committed').write_text(run)
            state['status']='idle';state['turns_completed']=state['turns_started'];state['progress']+=1
        elif config.get('transient_once') and not (home/'transient-ran').exists():
            (home/'transient-ran').write_text(run)
            state['error']={'kind':'transient','message':'fixture rate limit'}
        elif not config.get('wait_message') or messages:
            if not submitted and not config.get('no_submit'):
                rpc({'command':'step_submit','args':{'project':os.environ['SLUICE_PROJECT_ID'],'step':os.environ['SLUICE_STEP'],'run':run,'outputs':config.get('outputs',{}),'author':'fixture'}})
                submitted=True
            state['error']=None;state['status']='idle';state['turns_completed']=state['turns_started'];state['progress']+=1
    print(json.dumps({'observation':state,'outcome':'acknowledged','error':error,'hook':None}),flush=True)
"#;
#[test]
fn dotenv_secrets_reach_fn_runs_but_never_records_or_logs() {
    const SECRET: &str = "s3cr3t-dotenv-value-7f1c";
    let g = Gate::configured(|g| {
        std::fs::write(
            g.home.join(".env"),
            "# home secrets\nDOTENV_SHARED=home\nDOTENV_HOME_ONLY='from home'\n",
        )
        .unwrap();
        g.env.insert("DOTENV_SHARED".into(), "coordinator".into());
        g.env.insert("DOTENV_INHERITED".into(), "inherited".into());
    });
    let project_dir = g.home.join("projects").join(g.project.to_string());
    std::fs::create_dir_all(&project_dir).unwrap();
    std::fs::write(
        project_dir.join(".env"),
        format!("export DOTENV_SHARED=project\nDOTENV_TOKEN=\"{SECRET}\"\n"),
    )
    .unwrap();
    g.function(
        "custom.env",
        json!({}),
        json!({"shared":"string","home_only":"string","inherited":"string","token":"string","step":"string"}),
        r#"from sluice_fn import run
import hashlib, os

def main(inp, ctx):
    return {
        'shared': os.environ['DOTENV_SHARED'],
        'home_only': os.environ['DOTENV_HOME_ONLY'],
        'inherited': os.environ['DOTENV_INHERITED'],
        'token': hashlib.sha256(os.environ['DOTENV_TOKEN'].encode()).hexdigest(),
        'step': os.environ['SLUICE_STEP'],
    }
run(main)
"#,
    );
    g.plan(json!({
        "python":{"run":"custom.env"},
        "inline":{"run":"inline.python","in":{"code":{"default":"import hashlib, os\nout = hashlib.sha256(os.environ['DOTENV_TOKEN'].encode()).hexdigest()"}}}
    }));
    let _lease = g.lease();
    let digest = {
        use std::fmt::Write;
        let output = Command::new("/usr/bin/python3")
            .args([
                "-c",
                &format!("import hashlib; print(hashlib.sha256(b'{SECRET}').hexdigest())"),
            ])
            .output()
            .unwrap();
        let mut text = String::new();
        write!(text, "{}", String::from_utf8(output.stdout).unwrap().trim()).unwrap();
        text
    };
    let python = g.terminal("python");
    assert_eq!(python["status"], "succeeded", "{python}");
    let outputs = &python["outputs"];
    // The project's .env wins over the home's, which wins over the inherited environment.
    assert_eq!(outputs["shared"], "project");
    assert_eq!(outputs["home_only"], "from home");
    assert_eq!(outputs["inherited"], "inherited");
    assert_eq!(outputs["token"], digest.as_str());
    // A .env never overrides the run's own SLUICE_* variables.
    assert_eq!(outputs["step"], "python");
    let inline = g.terminal("inline");
    assert_eq!(inline["status"], "succeeded", "{inline}");
    assert_eq!(inline["outputs"]["value"], digest.as_str());
    // The value itself is in no record, row, log or run file.
    let mut checked = 0;
    let mut scan = vec![g.home.clone()];
    while let Some(path) = scan.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap().flatten() {
                scan.push(entry.path());
            }
            continue;
        }
        if path.file_name().is_some_and(|n| n == ".env") || !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap_or_default();
        assert!(
            !bytes.windows(SECRET.len()).any(|w| w == SECRET.as_bytes()),
            "secret leaked into {}",
            path.display()
        );
        checked += 1;
    }
    assert!(checked > 3, "scanned {checked} files");
}
/// Steps blocked on waiting asks hold nothing other requests need: with more of them
/// than the coordinator has read connections, it keeps answering while they wait.
#[test]
fn waiting_asks_do_not_wedge_the_coordinator() {
    const ASKS: usize = 8;
    let g = Gate::new();
    let steps = (0..ASKS)
        .map(|i| {
            let ask =
                json!({"to":"owner","title":format!("Merge {i}?"),"body":"Approve?","wait":true});
            (
                format!("ask{i}"),
                json!({"run":"message.ask","in":bindings(ask)}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    g.plan(Value::Object(steps));
    let _lease = g.lease();
    let open = |g: &Gate| {
        let CommandReply::Messages(page) = g.rpc(json!({"command":"messages","args":{"project":g.selector(),"view":"questions","thread":null,"since":null,"owner":true}})) else {
            panic!("messages")
        };
        page.messages
    };
    g.wait(|g| open(g).len() == ASKS);
    let answers_promptly = |g: &Gate| {
        let until = Instant::now() + Duration::from_secs(20);
        while Instant::now() < until {
            let started = Instant::now();
            g.status();
            g.rpc(json!({"command":"say","args":{"project":g.selector(),"body":"still here","to":"owner"}}));
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the coordinator took {:?} to answer",
                started.elapsed()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    answers_promptly(&g);
    for q in open(&g) {
        g.rpc(json!({"command":"reply","args":{"project":g.selector(),"to_message":q.id,"body":"","answer":{"action":"approve","params":null,"values":null},"owner":true}}));
    }
    for i in 0..ASKS {
        let step = g.terminal(&format!("ask{i}"));
        assert_eq!(step["status"], "succeeded", "{step}");
        assert_eq!(
            step["outputs"]["reply"]["answer"]["action"], "approve",
            "{step}"
        );
    }
}
#[test]
fn owner_question_runs_the_configured_notify_command_once() {
    let g = Gate::configured(|g| {
        let out = g.home.join("notified.jsonl");
        std::fs::write(
            g.home.join("config.json"),
            json!({"fn_dirs":[],"notify":{"command":["/bin/sh","-c","/usr/bin/cat >> \"$1\" && echo >> \"$1\"","notify",out],"timeout_s":10}}).to_string(),
        )
        .unwrap();
    });
    let CommandReply::Receipt(MessageReceipt { id, .. }) = g.rpc(json!({"command":"ask","args":{"project":g.selector(),"body":"Merge the release branch?","title":"Release","to":"owner"}})) else {panic!("ask")};
    g.rpc(
        json!({"command":"say","args":{"project":g.selector(),"body":"just a note","to":"owner"}}),
    );
    let read = |g: &Gate| -> Vec<Value> {
        std::fs::read_to_string(g.home.join("notified.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    };
    // No scheduler lease is held: notification does not wait for one.
    g.wait(|g| !read(g).is_empty());
    let db = rusqlite::Connection::open(g.home.join("sluice.db")).unwrap();
    g.wait(|_| {
        db.query_row(
            "SELECT outcome FROM notification_attempts WHERE message_id=?1",
            [id.0],
            |r| r.get::<_, String>(0),
        )
        .unwrap()
            == "dispatched"
    });
    let sent = read(&g);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0]["id"], id.0);
    assert_eq!(sent[0]["title"], "Release");
    assert_eq!(sent[0]["verb"], "ask");
    assert_eq!(sent[0]["project"], "compose");
    assert_eq!(sent[0]["project_id"], json!(g.project));
}

/// `ctx.tool` takes the flat arguments MCP takes and returns the plain result MCP returns,
/// authored as the step.
#[test]
fn python_ctx_tool_takes_flat_mcp_arguments_and_returns_plain_results() {
    let g = Gate::new();
    g.function(
        "custom.tools",
        json!({}),
        json!({"done":"boolean"}),
        r#"from sluice_fn import run, CallbackError

def main(inp, ctx):
    status = ctx.tool('status', {'steps': 'later'})
    assert list(status['steps']) == ['later'] and 'reply' not in status, status
    plan = ctx.tool('plan_get', {})
    assert 'later' in plan['plan']['steps'], plan
    paused = ctx.tool('step_pause', {'steps': 'later', 'reason': 'hold'})
    assert paused['rev'] == plan['rev'] + 1 and 'preview' in paused, paused
    said = ctx.tool('say', {'to': 'later', 'body': 'later, then'})
    assert said['to'] == 'later' and said['thread'] == 'step-tools', said
    ok = ctx.tool('step_set_output', {'step': 'later', 'outputs': {'value': 5}, 'force': True,
                                      'reason': 'by hand'})
    assert ok == {'ok': True}, ok
    waited = ctx.tool('step_wait', {'steps': 'later', 'until': 'succeeded', 'timeout': 5})
    assert waited['met'] is True and waited['steps'] == {'later': 'succeeded'}, waited
    done = ctx.tool('log_read', {'kinds': ['step.status'], 'statuses': ['succeeded'], 'limit': 50})
    assert [r['seq'] for r in done['records'] if r['step'] == 'later'][-1] <= waited['seq'], done
    try:
        ctx.tool('say', {'to': 'later', 'body': 'too late'})
        raise AssertionError('a settled step took a message')
    except CallbackError as error:
        assert error.error == 'conflict' and 'settled' in error.message, error.message
    log = ctx.tool('log_read', {'limit': 3})
    assert sorted(log) == ['last_seq', 'records'], log
    fns = ctx.tool('fn_list', {})
    assert any(f['name'] == 'custom.tools' for f in fns), fns
    asked = ctx.tool('ask', {'to': 'orchestrator', 'body': 'which?'})
    assert asked['thread'] == 'step-tools' and asked['delivery'] == 'delivered', asked
    try:
        ctx.tool('say', {'to': 'nobody', 'body': 'x'})
        raise AssertionError('an unknown recipient was accepted')
    except CallbackError as error:
        assert error.error == 'invalid', error.message
    try:
        ctx.tool('status', {'selection': {'steps': ['later'], 'tags': None}})
        raise AssertionError('the wire shape was accepted')
    except CallbackError as error:
        assert error.error == 'bad_request' and 'selection' in error.message, error.message
    return {'done': True}
run(main)
"#,
    );
    g.plan(json!({"tools":{"run":"custom.tools","in":{}},
                  "later":{"run":"core.echo","after":["tools"],"in":{"value":{"default":1}}}}));
    let _lease = g.lease();
    let value = g.terminal("tools");
    assert_eq!(value["status"], "succeeded", "{value}");
    let later = &g.status()["steps"]["later"];
    assert_eq!(later["status"], "succeeded", "{later}");
    assert_eq!(later["outputs"]["value"], 5);
    let db = rusqlite::Connection::open(g.home.join("sluice.db")).unwrap();
    let (author, reason): (String, String) = db
        .query_row(
            "SELECT author, reason FROM plan_edits WHERE project_id=?1 ORDER BY rev DESC LIMIT 1",
            [g.project.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((author.as_str(), reason.as_str()), ("step:tools", "hold"));
    // The fn's messages speak as its step, with its run.
    let (from, run): (String, String) = db
        .query_row(
            "SELECT \"from\", run_id FROM messages WHERE project_id=?1 AND needs_reply=1",
            [g.project.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((from.as_str(), run), ("tools", g.run("tools")));
}

/// `ctx.progress` publishes a running step's latest values without finishing it: they merge,
/// are checked against the step's outputs, show in the `query` tool's `steps`, feed no reader,
/// and wake neither `log_wait` nor `next`. The step's result is what its run returns.
#[test]
fn python_ctx_progress_publishes_values_that_are_never_final_and_wake_nothing() {
    let g = Gate::new();
    let dir = g.temp.path().join("rolling");
    std::fs::create_dir(&dir).unwrap();
    g.function(
        "custom.rolling",
        json!({"dir":"string"}),
        json!({"red":"int","head":"string"}),
        r#"from sluice_fn import run, CallbackError
from pathlib import Path
import time

def wait(path):
    while not path.exists():
        time.sleep(0.02)

def main(inp, ctx):
    root = Path(inp['dir'])
    first = ctx.progress(red=3)
    assert first['progress'] == {'red': 3} and first['run'] == ctx.run_id, first
    second = ctx.progress({'head': 'abc'})
    assert second['progress'] == {'red': 3, 'head': 'abc'}, second
    via = ctx.tool('step_progress', {'outputs': {'note': 'via tool'}})
    assert via['progress'] == {'red': 3, 'head': 'abc', 'note': 'via tool'}, via
    for bad, word in [({'red': 'three'}, 'outputs.red'), ({'nope': 1}, 'outputs.nope')]:
        try:
            ctx.progress(bad)
            raise AssertionError('progress that does not fit was accepted')
        except CallbackError as error:
            assert error.error == 'invalid' and any(word in e for e in error.errors), error.errors
    try:
        ctx.tool('step_progress', {'step': 'later', 'outputs': {'value': 1}})
        raise AssertionError("another step's progress was accepted")
    except CallbackError as error:
        assert error.error == 'conflict', error.message
    (root / 'published').touch()
    wait(root / 'again')
    ctx.progress(red=1)
    (root / 'published-again').touch()
    wait(root / 'finish')
    return {'red': 0, 'head': 'def'}
run(main)
"#,
    );
    g.plan(json!({
        "rolling":{"run":"custom.rolling","in":{"dir":{"default":dir}},"outputs":{"note":"string?"}},
        "later":{"run":"core.echo","in":{"value":{"source":"rolling/red"}}}}));
    let _lease = g.lease();
    let wait_file = |name: &str| {
        let path = dir.join(name);
        g.wait(|_| path.exists());
    };
    wait_file("published");
    let status = g.status();
    assert_eq!(status["steps"]["rolling"]["status"], "running", "{status}");
    assert_eq!(status["steps"]["later"]["status"], "pending", "{status}");
    let query = |g: &Gate| {
        g.data(json!({"command":"query","args":{"sql":"SELECT json_extract(progress, '$.red'), json_extract(progress, '$.note'), progress_at IS NOT NULL, progress_run FROM steps WHERE project_id = ? AND step_id = 'rolling'","params":[g.project.to_string()],"limit":10}}))
    };
    assert_eq!(
        query(&g)["rows"][0],
        json!([3, "via tool", 1, g.run("rolling")])
    );
    // Waits standing while more progress lands wake for none of it.
    let CommandReply::Records(page) = g.rpc(
        json!({"command":"log_read","args":{"project":g.selector(),"since_seq":null,"kinds":null,"threads":null,"limit":1}}),
    ) else {
        panic!("records")
    };
    let since = page.last_seq.0;
    let started = Instant::now();
    let (log_wait, next) = std::thread::scope(|scope| {
        let log_wait = scope.spawn(|| {
            g.rpc(json!({"command":"log_wait","args":{"read":{"project":g.selector(),"since_seq":since,"kinds":null,"threads":null,"limit":50},"timeout_seconds":3,"questions_only":false}}))
        });
        let next = scope.spawn(|| {
            g.rpc(json!({"command":"next","args":{"projects":[g.selector()],"since_seq":since,"me":"orchestrator","timeout_seconds":3,"all":false,"settle_seconds":1,"settle_max_seconds":1,"settles":"short"}}))
        });
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(dir.join("again"), b"").unwrap();
        wait_file("published-again");
        (log_wait.join().unwrap(), next.join().unwrap())
    });
    assert!(started.elapsed() >= Duration::from_millis(2500));
    let CommandReply::Records(page) = log_wait else {
        panic!("log_wait: {log_wait:?}")
    };
    assert!(page.records.is_empty(), "{:?}", page.records);
    let CommandReply::Next(next) = next else {
        panic!("next: {next:?}")
    };
    assert!(next.records.is_empty() && next.timed_out, "{next:?}");
    assert_eq!(query(&g)["rows"][0][0], 1);
    // The step's result is what its run returns; the reader gets that, never the progress.
    std::fs::write(dir.join("finish"), b"").unwrap();
    let rolling = g.terminal("rolling");
    assert_eq!(rolling["status"], "succeeded", "{rolling}");
    assert_eq!(rolling["outputs"]["red"], 0);
    let later = g.terminal("later");
    assert_eq!(later["outputs"]["value"], 0, "{later}");
    // Kept after the run, with its time, until the next run starts.
    assert_eq!(query(&g)["rows"][0][0], 1);
}
