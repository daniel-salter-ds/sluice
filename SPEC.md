# sluice: specification

sluice runs plans: graphs of typed function calls. Orchestrators (agents over MCP, people at a
shell or the dashboard) edit a project's plan through typed tools; a per-home coordinator
validates every edit, schedules ready steps and runs each one inside a supervised systemd unit.
The plan document borrows CWL's shapes (`inputs`/`outputs`/`steps`, `run`, `source`/`default`,
`scatter`, CWL type spellings) without aiming for CWL compliance.

This file is the contract of the shipped build. When the code and this file disagree, fix one
of them in the same change. `DESIGN.md` covers how the dashboard looks; `docs/agent/*.md` are
the agent-facing topics the `docs` tool serves.

## 1. Concepts

- **Home:** one directory holding a database and everything a set of projects needs (§2.3). One
  coordinator process owns each home's writes.
- **Project:** a name (renameable), an immutable UUIDv7 id, a description, an optional icon,
  named resources, paused and archived flags, and an optional board (an OpenUI Lang program the
  dashboard draws beside the plan, `docs("board")`). Each project has exactly one plan and its own
  functions and recipes.
- **Function (fn):** a named unit with typed `inputs` and `outputs`. Built-in fns are compiled
  into sluice (§16); user fns are Python (`fn.json` plus `main.py`, §5). An **open** fn (every
  agent fn) also takes extra inputs a step binds and outputs a step declares.
- **Plan:** typed plan `inputs`, named plan `outputs`, and `steps`. Each step runs one fn and
  binds its inputs to plan inputs, other steps' outputs, literals or files. Edited only through
  typed edits; each edit gets a new revision (`rev`) and a history entry.
- **Unit:** the steps sharing one `unit:<name>` tag; an untagged step is a unit of one (§6.7).
- **Run:** one execution of a step (one per scattered item) or of a call, in its own transient
  systemd unit under a guardian (§2.5, §7).
- **Message:** a row on a project thread: a question (`ask`), a note (`say`) or a `reply`, to
  a step, the orchestrator or the owner. Open questions to `owner` are the inbox (§8).
- **Log:** each project's ordered records of what happened (edits, manual values, status
  changes, calls, messages, …), each with a home-wide `seq` (§9). History, not truth: `status`
  and `plan_get` are the current state.

## 2. Installation, home and processes

### 2.1 Home resolution

Every mode resolves its home once, at startup: `SLUICE_HOME` when it is set, else the home the
installation has selected (§2.2; installation directory `SLUICE_INSTALL_DIR`, default
`~/.local/share/sluice/install`). When neither exists the command fails with an error saying
so. The installation control directory must lie outside the home.

### 2.2 Installation and releases

An installation lives under a prefix (default `~/.local/share/sluice`):

```
bin/sluice                  the launcher: a small native program (built by build-release)
install/                    the control directory (SLUICE_INSTALL_DIR overrides it)
  install.lock              flock: shared for admission decisions, exclusive for fence/select/unfence
  selection.json            {generation, release_path, home_path}: the selected release and home
  fence.json                {generation, reason, since} while the installation is fenced
  generation.json           the installation generation, advanced by every fence/select
  entry                     symlink to the selected release's bin/sluice
  services.json             what scripts/deploy started: {name: {unit, release}}
releases/<git-sha>-<sha256>/
  bin/sluice                the release binary
  python/                   sluice_fn (the fn helper, §5.4) and inline_python.py
  tmux/                     the private tmux 3.7c build and its tmux-manifest.json
  assets/                   the dashboard assets (the binary also embeds them)
  manifest.json             release_id, git_sha, guardian protocol, sha256 of every file,
                            the private tmux manifest, the build toolchain
```

The **launcher** execs `<install>/entry --installation-entry <install> <args…>`; the release
binary reads `selection.json` and execs the selected release's `bin/sluice` with `SLUICE_HOME`
set to the selected home and `SLUICE_INSTALL_DIR` to the control directory. A release binary
re-execs itself once with `SLUICE_PYTHON_DIR=<release>/python`,
`SLUICE_TMUX_PREFIX=<release>/tmux` and `PYTHONDONTWRITEBYTECODE=1`. Agent docs and dashboard
assets are compiled into the binary.

`sluice install <command>` prints the installation status as JSON
`{generation, selection, fence}`:

| command | effect |
|---|---|
| `install status` | read only |
| `install fence <reason>` | writes `fence.json` under the exclusive lock; while fenced, every coordinator activation and admission write is refused (`maintenance: <reason> (generation n)`), except a coordinator started with `--maintenance` |
| `install select <release_dir> <home>` | verifies the release's manifest (when it has one) and records the selection |
| `install unfence` | verifies the selected release and removes the fence |

A home that is not the installation's selected home refuses admission (`maintenance: stale
selected home`). Without a release and without `SLUICE_INSTALL_DIR` (a source build), the
installation is the sibling directory `<home>.sluice-install`.

**`scripts/build-release <prefix>`** runs `cargo build --workspace --bins --release --locked`
with its target dir under `<prefix>/.build/target`, stages `bin/sluice`, `python/`, the private
tmux (built once by `scripts/build-private-tmux` and cached under `<prefix>/.build`) and
`assets/`, writes `manifest.json`, moves the stage to `releases/<release_id>` (an existing
release id must have identical metadata), compiles the launcher into `<prefix>/bin/sluice` and
prints the release path. It refuses a prefix on `PATH`, `/`, or `~/.local/bin`.

**`scripts/deploy [REF] [--prefix DIR] [--skip-compat REASON]`** (REF defaults to `origin/main`)
installs one commit:

1. `git archive` the commit into a temporary source tree and run `build-release` there;
2. `scripts/compat-check` the new release: on a scratch copy of the home's database, with its
   coordinator cut off from systemd, every release an unfinished run is pinned to (plus the
   selected one and the candidate) runs `log_read`, `status`, `say`, a stale `step_submit` and
   `step_context`; a storage or schema error, a crash, a timeout or a submit not refused as
   stale stops the deploy unless `--skip-compat` gives a reason. Each deploy's outcome and any
   skip reason are appended to `<install>/deploy.log`;
3. `install fence "deploy <sha>"`;
4. stop the services recorded in `services.json` and the home's auto-started coordinator unit;
5. `install select <release> <home>`;
6. start three transient user units, `sluice-<sha16(install dir)>-coordinator`
   (`coordinator --maintenance`), `-serve` (`serve --no-runner --port 3065`, or
   `SLUICE_DEPLOY_PORT`) and `-loop` (`loop`), each with `SLUICE_HOME`, `SLUICE_INSTALL_DIR`,
   `PATH` and `HOME` set, recording them in `services.json`; after the coordinator it waits
   until the coordinator is ready to serve its socket;
7. check that every unit is active and run `sluice doctor --json`;
8. `install unfence`;
9. prune releases: keep the newest three plus every release a live process runs from or an
   unfinished run in the home records.

**`scripts/ship [REF] [--dry-run]`** takes a gated branch to a verified live deploy: it refuses a
dirty tree, rebases onto `origin/main` (re-running `scripts/check` only when main changed a non-`*.md`
file the branch changed, else building), pushes with a bounded retry, deploys `origin/main` to the
selected home, checks that every run live before the deploy is still running or finished with a
result, that the three units are active and that the dashboard answers, and prints one line.

Any failure after the fence leaves the installation fenced and says so. Running steps survive a
deploy: each run's guardian stays pinned to its own release, and the new coordinator adopts it
(§7.9).

### 2.3 Home layout

```
config.json                 {"fn_dirs": [], "http": {"host": "127.0.0.1", "port": 3065},
                            "log_max": 10000}; written when missing, read on each use; a key
                            of the wrong shape is ignored (with a warning) for its default:
                            fn_dirs    directories (relative to the home) whose fn dirs join
                                       the global scope
                            http       serve's host and port when the command line gives none
                            log_max    records each log keeps; past it the coordinator trims
                                       the oldest down to 90% of it (every 30 s)
                            unread_alert_min  minutes; when set, the "nobody reading" owner
                                       question (§8)
                            notify     {command: [argv…], timeout_s: 30}: owner notification
                                       (§8)
.env                        secrets for every run in the home (§5.4)
sluice.db                   the database (§3)
coordinator.sock            the coordinator's Unix socket (owner only)
coordinator.lock            flock held by the running coordinator
fns/                        global user fns; fns/generations/ holds immutable published
                            copies that runs are pinned to
recipes/<name>.json         global recipes (§6.8)
runs/<run id>/              one run's directory: its control socket, invocation and delivery
                            records, messages.json (the messages assigned to it), stderr and
                            the engine's own files
locks/session-<key>.lock    agent session locks
projects/<project id>/
  fns/                      the project's fns
  generations/<n>/          immutable published copies of the project's fns
  recipes/<name>.json       the project's recipes
  .env                      secrets for this project's runs, over the home's (§5.4)
  icons/<generation>        the project's image icon
```

### 2.4 Coordinator

The coordinator (`sluice coordinator`) owns the home: it holds `coordinator.lock`, opens the
single SQLite writer, serves `coordinator.sock`, publishes the fn registry, runs reconciliation
and, while some client holds the **scheduler lease**, admits and launches work.

- Any CLI command that needs it, `serve` and `loop` connect to the socket and, when nothing
  answers, start the coordinator as the transient user unit `sluice-coordinator-<sha16(home)>`
  (`systemd-run --user --collect --service-type=exec -p Restart=no`) and wait up to 10 s for
  its socket. Activation is refused while the installation is fenced.
- `coordinator --maintenance` may start while the installation is fenced; `scripts/deploy` uses
  it.
- The scheduler lease is one per home: `serve` takes it unless `--no-runner`, `loop` takes it,
  and a second holder is refused (`conflict`, "scheduler lease already held"). The lease lasts
  as long as the holder's connection. Without a holder nothing new starts; running work goes
  on.
- Every request on the socket is one length-prefixed JSON frame (§12.1). A request it cannot
  decode, or one whose handler panics, gets a logged error reply; the connection stays usable.

### 2.5 Runs and the guardian

Each run is a transient systemd user unit, `sluice-run-<run id>.service`, started with
`Delegate=yes`, `KillMode=control-group` and `Restart=no`. Inside it the **guardian**
(`sluice guardian --run --attempt --socket`) proves its identity to the coordinator, splits the
unit's cgroup into `control` (itself) and `payload/<invocation>` leaves, starts the payload
through the `payload-exec` launcher, serves the run's control socket (callbacks, engine hooks,
delivery acknowledgements), holds one watch on the coordinator for cancellation and messages,
and reports the start and the completion until the coordinator acknowledges them. A payload is
a Python fn (§5.4), an agent session in the private tmux, or a built-in fn.

An agent's supervisor acknowledges each live message its engine accepted to the guardian, which
answers once the acknowledgement is durable in the run's `delivery.json` (a redelivery after a
restart skips it). The guardian tells the coordinator afterwards, beside its loop and again
until the coordinator takes it, and the completion carries every acknowledgement, so a slow or
stalled coordinator never holds the answer.

The guardian hands each engine hook to the agent supervisor through the run's
`engine-hooks/` journal and waits up to three seconds for its decision. The journal keeps only
the hooks in flight, however many a run has: the supervisor removes a hook's request once its
reply is durable, the guardian the reply once it has read it, and each side clears what a crash
left on its next pass. Past 1024 hooks in flight a hook is refused.

Stopping a run sends TERM to its processes, waits five seconds, then kills the payload cgroup
recursively and proves it empty before any resource it held is released. The guardian, not the
coordinator, owns the payload: a coordinator restart or a deploy leaves running payloads alone.

Host prerequisites (cgroup v2, a systemd user manager with delegation, `pidfd_open`, a boot id)
are checked by `sluice doctor`; see `docs/rust/host-prerequisites.md`.

### 2.6 Maintenance modes

The home has one maintenance mode: `normal` or `drain`.

- **drain** (`drain` tool, `sluice drain`): pauses the selected projects (default every
  project not archived) that are not paused already, records them and the drain's author as
  owner, and rejects new plan work and user calls home-wide (`busy`, "drain rejects new plan
  work and user calls"): plan edits, retries, input sets and `fn_call`. Running steps and calls
  finish. Draining again with another author is a `conflict`. `release` unpauses exactly the
  recorded projects and returns to `normal`.

## 3. Storage

`sluice.db` is SQLite in WAL mode at schema 1, created from `migrations/0001.sql` (23 STRICT
tables). The schema version changes only for a change older binaries cannot read: a run's pinned
`sluice` reads the database itself and refuses any other version. Columns added later keep the
version; the coordinator's writer adds any that are missing, and any view added later (and
marks a home left at the interim board schema 2 as 1), in one transaction when it opens the
home or a restore, before anything else touches it, and readers refuse a home still missing a
column. New state is never a new table: every release counts the home's 23 tables and refuses
any other number, so a pinned one could not read a home with a 24th. A board's slots are
therefore the column `projects.board_slots` (a JSON object of key to `{markdown, at,
author}`) and the view `board_slots`. Only
the coordinator writes, through one writer task; reads use a pool of read-only connections and
one snapshot per answer. Every logical change (an edit and its records, a status change and
its records, a message and its record) commits in one transaction.

Tables: `home_meta`, `projects`, `plans`, `plan_edits`, `inputs`, `steps`, `attempts`, `runs`,
`submissions`, `calls`, `step_results`, `resources`, `leases`, `messages`,
`question_attachments`, `message_deliveries`, `readers`, `records`, `change_versions`,
`maintenance`, `artifact_jobs`, `sessions`, `notification_attempts`.

A step's row also carries its progress (§6.4): `progress` (the JSON object of latest values),
`progress_at` (when it was last set) and `progress_run` (the run that set it), all null until a
run publishes some and cleared when the step's next run starts. `steps` is readable by `query`
as it is, so `SELECT json_extract(progress, '$.red'), progress_at FROM steps WHERE project_id =
? AND step_id = 'tests-main'` reads a rolling step's latest count.

Views for agents' queries (§12.4 `query`): `outcomes` (removed steps' results), `log` (each
record as the log tools return it), `step_changes` (`step.status` records as rows), `edits`
(`plan_edits`), `questions` (the questions, `ask`s, plus derived `state`
open|answered|closed and `waiting`) and `board_slots` (`project_id, key, markdown, updated_at,
author`: each live project's board slots). Public tables are keyed by the immutable `project_id`,
never by name.

Ids: projects, runs, attempts, results and invocations are UUIDv7; message ids and record
seqs share one increasing integer sequence.

## 4. Types

Written inline, compared structurally:

| form | meaning |
|---|---|
| `"string"`, `"int"`, `"float"`, `"boolean"`, `"Any"` | primitives; `Any` accepts anything |
| `"T?"`, e.g. `"string?"`, `"string[]?"` | optional: T or null; an optional input may be left unbound |
| `["null", T]` | optional form for any T |
| `"T[]"` | array of T |
| `{"type": "array", "items": T}` | array of T |
| `{"type": "enum", "symbols": ["a", "b"]}` | one of these strings |
| `{"type": "record", "fields": {"f": T}}` | object with these fields |

An output **fits** an input when either is `Any`; same primitive, or `int` into `float`; an
enum into `string` or into an enum with every symbol; arrays of fitting items; a record that
has every required field of the input record with fitting types (extra fields are fine). An
optional value does not fit a required input. Runtime values are checked against types with
path-bearing errors (`report.outcome: expected one of [done, blocked], got "ok"`).

Plan inputs and step-declared outputs may be written `{"type": T, "doc": "..."}`. An extra
input of an open fn's step takes its source's type: a ref's type; for a list source, an array
of the refs' type (`Any[]` when they differ); `Any` for a `default`; `string` for a `file`; the
item type for the scatter input.

## 5. Functions

### 5.1 Scopes

- **builtin:** compiled into the binary (§16).
- **global:** `<home>/fns/` and every directory in `config.json`'s `fn_dirs`.
- **project:** `projects/<id>/fns/`.

A project sees builtin, global and its own fns. Names never collide: a global fn may not reuse
a builtin name, and a project fn may not reuse a builtin or global one. A fn dir is an
immediate subdirectory holding `fn.json` whose `name` matches the directory. The registry is
rescanned when fn files change (a watcher on the fn scopes) and republished only when it
changed; each publication is an immutable generation that runs are pinned to.

A project whose own scope has a problem (a collision, a bad `fn.json`) is **blocked**: plan
edits, manual values and new runs of that project fail with `invalid` ("project registry
blocked", listing the problems). Problems elsewhere leave the broken fn out of lookup.
`verify` reports every problem.

### 5.2 fn.json

```json
{"name": "text.upper", "doc": "Upper-case a string.",
 "inputs": {"text": "string"}, "outputs": {"text": "string"}}
```

Keys: `name` (dotted lowercase: two or more `[a-z][a-z0-9_]*` parts), `doc`, `inputs`,
`outputs`, `open` (boolean), `submits`, `icon`; any other key is a problem. `submits` (only
with `open: true`) names outputs every step of the fn declares as if it listed them itself,
each a type or `{"type", "doc"}`, none named like a fn output.

**Icon:** one file `icon.svg`, `icon.png` or `icon.webp` in the fn dir (at most 256 KiB, its
content the type its name says), or `"icon": "<text>"` (at most 16 characters, no control
characters). The file wins. Two icon files, a bad file or a bad text is a problem. An SVG
icon is drawn single-colour in `currentColor` on a 16×16 grid.

### 5.3 Saving

`fn_save(fn, main_py, project?)` validates the manifest, refuses a name that collides, writes
`fns/<name>/fn.json` and `main.py` into the project's (or the global) scope, republishes the
registry and returns `{name, scope, path, generation}`.

### 5.4 Python fn contract

A user fn runs as `uv run --no-project --quiet <bundle>/main.py`, where the bundle is the fn's
published generation. `main.py` declares any dependencies in a PEP 723 block. The process
runs in the run's payload cgroup, in the fn's published directory, with:

- `PYTHONPATH`: the release's `python/` directory (the `sluice_fn` helper) plus the published
  scope directory holding the fn, so it can import modules kept beside it (e.g. `_lib/`);
- environment: the coordinator's environment plus `SLUICE_HOME`, `SLUICE_BIN`,
  `SLUICE_PROJECT_ID`, `SLUICE_PROJECT` (the name at launch), `SLUICE_STEP`, `SLUICE_RUN_ID`,
  `SLUICE_RUN_DIR`, `SLUICE_PROJECT_DIR`, `SLUICE_FN_DIR`, `SLUICE_PREV_RUN`,
  `SLUICE_CONTROL_SOCKET`, `SLUICE_RUN_CAPABILITY`, and `SLUICE_HOST_PATH`,
  `SLUICE_HOST_PYTHONPATH`, `SLUICE_HOST_VIRTUAL_ENV` (the values before sluice changed them).
  Secrets come from `.env` files, loaded at each run's launch: `<home>/.env`, then
  `<home>/projects/<id>/.env`, each over what came before, then the `SLUICE_*` variables above
  over both. A line is `KEY=value` (`export ` prefix, `#` comments and one pair of quotes
  allowed); other lines are skipped and reported by `verify`. Built-in fns and agent sessions
  get the same values (an agent session over its engine's allowlisted environment). Values are
  never logged or recorded.
- stdin: one JSON envelope `{"protocol": 1, "inputs": {...}, "context": {...}}`. `context`
  holds `home`, `run_dir`, `project_dir`, `project`, `project_id`, `step`, `run_id`,
  `attempt_id`, `invocation_id`, `fn_dir`, `prev_run`, `extra_inputs` (an open fn's
  step: `{name: {"type"}}`), `outputs` (the outputs the step declares: `{name: {"type",
  "doc"}}`), `returns`, `control_socket` and `run_capability`.
- stdout: exactly one JSON document, `{"ok": true, "outputs": {...}}` or `{"ok": false,
  "error": {"kind", "message"}}` (or an `agent_failure` error object), at most 16 MiB. Anything
  else, a non-zero exit without a result, or outputs that fail their types fails the step;
  the error carries the stderr tail (2 KiB).

The helper, `sluice_fn` (standard library only):

```python
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
from sluice_fn import run, sh, Transient

def main(inp, ctx):
    ctx.log("upper-casing")
    return {"text": inp["text"].upper()}

if __name__ == "__main__":
    run(main)
```

- `run(main, retries=0, backoff=30)` reads the envelope, calls `main(inputs, ctx)`, writes the
  result and exits. `Transient` raised by `main` is retried within the same run up to
  `retries` times, `backoff` seconds apart (`SLUICE_BACKOFF` overrides it); `ctx.attempt`
  counts the calls. SIGTERM and SIGINT raise `Cancelled`. `main` returning `None` means `{}`;
  anything but a dict is an error.
- Error kinds written: `Rejected` → `rejected` (refused on purpose; a registered completion
  action may follow, below), `Transient` → `transient` (retries exhausted), `Cancelled` →
  `cancelled`, `AgentFailure(kind, message, session)` → an `agent_failure` error, anything
  else → `fn_failure` with the exception's text. The traceback goes to stderr.
- `log(msg)` / `ctx.log`: stderr. `sh(argv, cwd, check, env, timeout, input)` runs a command
  (raises `ShError` on a non-zero exit when `check`); `stream(argv, on_line, …)` runs one and
  hands each line to `on_line`; `child_env(extra)` is the environment for child tools (host
  `PATH`, `PYTHONPATH`, `VIRTUAL_ENV` restored, uv and agent-nesting variables removed).
- `ctx` attributes: `project_id`, `project`, `step`, `run_id`, `attempt_id`, `invocation_id`,
  `run_dir`, `home`, `fn_dir`, `project_dir`, `prev_run`, `extra_inputs`, `outputs`,
  `attempt`.
- `ctx.callback(command, args)` sends one command to the run's control socket with the run
  capability and returns the reply; an error reply raises `Transient`, `AgentFailure`,
  `Cancelled` or `CallbackError` (`error`, `message`, `errors`, `current_rev`, `retryable`).
- `ctx.tool(name, args)` calls a named tool for the run's own project (`project` defaults to
  it): the reads `status`, `plan_get`, `messages`, `log_read`, `fn_list`, `fn_get` and
  `call_status`; `step_wait`; `step_progress` (its own step's); `ask`, `say` and `reply`, `message.ask`, `message.say`,
  `message.reply` and `message.wait` (as the step, with its run); and the project's
  mutations (`project_update`, the edit tools, retry, cancel, manual values). `args` are the
  tool's flat MCP arguments (§12.2) and the result is the tool's MCP result (`{"ok": true}` for
  an acknowledgement); an unspecified author is `step:<step>`. Another project, or any other
  tool, is refused (`conflict`).
- `ctx.builtin(name, inputs)` runs a builtin fn inside this run (same run and attempt, a new
  invocation) and returns its outputs.
- `ctx.submission()` returns the run's submission, if any; `ctx.submit(outputs)` submits the
  step's declared outputs, once (§6.4).
- `ctx.progress(**fields)` (or `ctx.progress({...})`) publishes the step's latest values
  while it runs, through `ctx.tool("step_progress", …)`, and returns its reply (§6.4).
- `ctx.retry_on_failure(step, message)` registers a completion action: if this run ends
  `rejected`, `step` (which must have a completed result) is retried with `message` (≤ 8 KiB)
  as feedback. Registering the same action again is a no-op; a different one is an error.
- `with ctx.acquire(resource, amount=1, timeout=None):` holds a section lease on a project
  resource (§7.6) for the block; `timeout` raises `TimeoutError`. An undeclared resource or an
  amount over a fixed capacity raises `CallbackError` (`bad_request`).
- `ctx.header(text)` appends the step's declared outputs to an agent task.

## 6. Plans

### 6.1 Document

```json
{
  "inputs":  {"repo": "string", "tasks": {"type": "string[]", "doc": "One task per item"}},
  "outputs": {"notes": {"source": "notes/final"}},
  "steps": {
    "work":  {"run": "agent.run", "scatter": "spec", "tags": ["unit:build"],
              "in": {"engine": {"default": "devin"}, "cwd": {"source": "repo"},
                     "spec": {"source": "tasks"}}},
    "gate":  {"run": "core.collect", "tags": ["unit:build", "exit"],
              "in": {"items": {"source": ["work/final"]}}},
    "notes": {"run": "agent.run", "after": ["unit:build"],
              "in": {"engine": {"default": "claude"}, "cwd": {"source": "repo"},
                     "spec": {"source": "gate/items.0"}}}
  }
}
```

A new project's plan is `{"inputs": {}, "outputs": {}, "steps": {}}` at rev 1. Project names,
step ids, plan input and output names match `^[a-z0-9][a-z0-9_-]*$`; a project name may not
look like a UUID; `owner` and `orchestrator` are not step ids. Plan inputs and steps share one
namespace.

Step keys: `run` (the fn), `in` (bindings), `scatter`, `doc`, `outputs` (declared, open fns
only), `paused` (`true` or a reason string), `after` (gate entries), `tags`, `needs`,
`priority`. Plan outputs are `{"source": "<ref>"}`.

### 6.2 Bindings

- `{"default": <json>}`: a literal.
- `{"source": "<ref>"}`: a plan input name, or `<step>/<output>` with optional `.field` or
  `.0` path segments.
- `{"source": ["<ref>", …]}`: fan-in, an array of the values in order.
- `{"file": "/abs/path"}`: the file's UTF-8 text, a `string`, read when the step starts (every
  start); a missing file fails the step and `verify` warns about one missing now. Only the path
  feeds the inputs hash.

Unbound optional fn inputs are null. A source binding is a **handoff**: the step waits for its
sources to succeed and is skipped when a source step is skipped.

### 6.3 Scatter

`"scatter": "<input>"` runs the step once per item of that input's array (each item must fit
the input's type); every output becomes an array in item order. The step succeeds when every
item does and fails if any fails, keeping the per-item results so a retry with unchanged inputs
and item count re-runs only the failed items. A scattered step holds its `needs` once.

### 6.4 Open fns: extra inputs, declared outputs, submission

A step whose fn is open may bind extra inputs (any id-shaped name) and declare `outputs`
(`{name: type | {"type", "doc"}}`, none named like a fn output); `submits` from fn.json join
them. Refs to declared outputs validate like any output. While the step runs, whoever does the
work calls `step_submit(project, step, run, outputs)`: checked against the declared outputs
(every required one, fitting types, no others; `invalid` lists each mismatch and nothing is
stored), refused unless the run is current. A valid submission is a `step.submit` record and
the agent's done signal: its supervisor stops the session at once (§15), so the agent
invocation returns its result (`session`, `final`, `git`) straight away. A step that runs an
agent fn itself then completes with it; a fn that composes one (`ctx.builtin("agent.run")`)
gets that result back, finishes, and completes the step with what it returns. Either way the
run's completion is the step's result: the fn's outputs, with the submission joined in (a
returned value wins), validated against the fn's outputs and the declared ones. A run submits
once: a second submission is refused (`conflict`). A run that ends without submitting a
required declared output (and without returning it) fails the step with
`exited_without_submit` (`session` when the agent's is known), unless it failed otherwise
first.

A running step whose current runs have all stored a valid submission is **finishing** until
its run ends: its agent's work is done, messages to it are refused (§8) and so are a second
submission and `step_set_output`. `status` (both views), `step_context` and `call_status`
(for a call's run that has submitted) show it as `finishing: {since, submission_seq, release}`:
the time the (latest) submission was stored, its `step.submit` record's seq (null once the log
has trimmed it), and the run's pinned release in short form (a `<git-sha>-<sha256>` release id
as its git sha's first 12 characters; any other id whole). With the done signal finishing
lasts as long as stopping the session takes. A run whose guardian is pinned to a release from
before the done signal does not end its agent on submit: that supervisor waits until the agent
has been idle through its grace, so the step can stay finishing for a long time; `step_settle`
(§7.5) settles it on its submission. No `grace_until` is shown: when the grace started (the
agent's last turn going idle) lives only in that supervisor's memory, and the grace itself in
its environment, so sluice cannot compute it honestly.

**Progress.** A running step that never finishes (a rolling test run of main, say) still has
results worth showing. Its current run publishes them with `step_progress(project, step, run,
outputs)` (`ctx.progress(**fields)` in a fn; in a run `project`, `step` and `run` default to
its own, §14): each field is one of the step's outputs (its fn's or declared) and must fit its
type; an unknown field or a value that does not fit is `invalid` with each problem, and
nothing is stored. Fields merge over the run's earlier progress. It is refused (`conflict`)
unless the step is running and `run` is its current run, and (`invalid`) for a scattered step.
Progress is never final: it feeds no input, handoff or gate, never finishes or settles the
step, is not a submission (a run that publishes progress still submits, or returns its
outputs, as before) and joins no result. It writes no log record, so it wakes no `next`,
`log_wait` or `step_wait` (§10); its commit touches a change view of its own (`progress`) that
nothing waits on and the scheduler's project versions leave out. It is kept, with its time
and run, until the step's next run is reserved, which clears it: after the run ends it stays
visible, as progress, not as outputs. The reply is `{project, step, run, progress, at}`, the
merged values and when they were set. The dashboard (§13) shows a step's progress while it is
fresher than its outputs: unless the step has succeeded (or gone stale since) with a result
recorded at or after the progress was set.

### 6.5 Work done outside sluice

A step running `core.external` (open, no inputs or outputs of its own) is never started: once
ready it waits (wait reason `external: set its outputs with step_set_output`) until
`step_set_output` settles it or `step_cancel` fails it. It may not scatter, and `fn_call`
refuses it. Patching a step's `run` to `core.external` and retrying it moves its work out: the
old fn's inputs become extra inputs, and outputs dependents read must be declared.

### 6.6 Gates: `after`

`after` is a list of gate entries. A step is ready when every handoff source has succeeded and
every entry is satisfied.

| entry | satisfied when | skips the step when |
|---|---|---|
| `s` (a step) | `s` succeeded | `s` was skipped |
| `s?` | `s` succeeded or was skipped | never |
| `r` (a boolean ref or plan input) | the value is `true` | the value is `false` or null, or its step was skipped |
| `!r` | the value is `false` | the value is `true` or null, or its step was skipped |
| `unit:u` | every exit step of unit `u` succeeded | any exit step was skipped |
| `unit:u?` | every exit step succeeded or was skipped | never |

- An entry whose step is pending, running, failed or stale is unsatisfied: the step waits.
- A ref entry must be typed `boolean`, `boolean?` or `Any`; an `Any` value that is not a
  boolean fails the step (`after: <ref> is 3, not a boolean`). `?` is refused on refs and `!`
  on step and unit entries. A unit entry naming a unit with no steps is refused.
- A skipped step is decided again whenever its reasons change and goes back to `pending`. Skip
  reasons: `<ref> is false|true|null`, `step <s> was skipped`, `unit <u> was skipped (exit step
  <s>)`. Wait text: `after <entry> (<status>)`.
- Gates decide starts only: they never stop running work, are not evaluated for paused steps,
  and never feed the inputs hash.
- Entries keep their order and are deduplicated. Cycles count every entry; a unit entry counts
  as edges to the unit's exit steps.

### 6.7 Units

A unit is its tag: steps tagged `unit:<name>` form unit `<name>` (at most one `unit:` tag per
step); an untagged step is a unit of one named by its id. Edges may cross units. A unit is
**done** when every step succeeded or was skipped. Its **exit steps** are its steps tagged
`exit`, else its sinks over the edges inside the unit; a unit may not depend on its own exits.
Its **entry steps** (derived, never stored) are its steps with no dependency inside the unit.
`unit:` and `exit` are reserved tags. A unit is **settled** when none of its steps is running
and every pending one is external or not ready.

### 6.8 Recipes

A recipe is `recipes/<name>.json` in the home (global) or in `projects/<id>/` (the project's
wins on a name clash): `{"name", "doc"?, "params"?: {name: type | {"type", "doc"}}, "steps"}`,
`name` matching the file. `unit` (a step id) is always a param. In every step id and string,
`{param}` is replaced by the value (a non-string as JSON); a string that is exactly `{param}`
becomes the value with its type; `{{`/`}}` are literal braces; an unknown `{x}` is an error. A
recipe step with `when` is broken.

`unit_add(project, recipe, unit, params, after?, inputs?, tags?, start?)` checks the params,
expands the recipe, tags every new step `unit:<unit>` (then the recipe's tags, then `tags`;
`unit:` tags in `tags` are refused), and in the same edit appends `after` entries per suffix
(`"*"` means the unit's entry steps) and binds `inputs` per suffix to `{"default": value}`. A
suffix is a recipe step's id without the leading `<unit>-`. An unknown suffix, an input the
step's fn does not declare and the recipe does not bind, or a step id the plan already has is
refused before anything is written.

### 6.9 Validation

Every edit is validated whole: ids valid; every `run` visible to the project; required inputs
bound, no unknown inputs (extra inputs and declared outputs only on open fns); refs naming a
plan input or a step output (paths through record types, anything under `Any`); every source
fitting its input (list sources element-wise, the scatter input item-wise); literals passing
their types; gate entries well formed; `needs` naming declared resources within any fixed
capacity; no cycles. Errors are a list with paths
(`steps.notes.in.cwd: repo is int, which does not fit string: int is not string`).

### 6.10 Edits

Every edit tool produces RFC 6902 ops against the plan document, validates the result, and
commits the new plan, a `plan_edits` row and a `plan.edit` record `{rev, author, reason, ops}`
in one transaction. The reply is the **edit result** `{project: {project_id, name}, rev,
preview, steps?}`, `preview` being `{ops, would_start, would_queue, would_skip, would_stale,
errors}` and `steps` the steps the edit was about (`unit_add`'s new steps, `unit_tag`'s unit,
`step_pause`'s selection, `plan_prune`'s removed steps).
With `dry_run: true` the reply is the preview alone and nothing is written. The simulation uses
cached capacities and never runs fns; `core.external` steps never appear in `would_start`.

- `plan_patch` requires the current `rev`; the other edit tools take an optional `rev` and
  otherwise apply to the current plan. A stale `rev` is `conflict` with `current_rev`.
- A running step may change only `paused` and `tags`.
- `start: false` (`plan_patch`, `step_add`, `unit_add`) adds steps with `"paused": true`
  unless a step sets `paused` itself.
- An edit that changes nothing (an edge already there, tags or pauses as they are, a prune
  that removes nothing, a patch that yields the same plan) commits nothing: no rev, record or
  history row. Its reply is the edit result with the current `rev` and empty `preview.ops`.
  `step_set_input` refuses one instead (`bad_request`).
- Removing a finished step keeps its result as an `outcomes` row; a pending step leaves none.

## 7. Running

### 7.1 Step status

`pending`, `running`, `succeeded`, `failed`, `stale`, `skipped`. A succeeded step may be
`manual` (set by hand). A scattered step also has `done`, `total` and `instances`. A step's
`error` is a structured error object (§12.2).

A step is **ready** when it is pending, not paused, its project is not paused, every plan input
it reads has a value, every handoff source succeeded and every gate is satisfied. A ready step
starts at once unless it has `needs` (§7.6); a ready `core.external` step only waits.

A step is **settled** when nothing more happens to it until someone edits the plan, retries,
cancels, sets an input or sets its outputs by hand: its status is `succeeded`, `failed`,
`stale` or `skipped`, or it is pending and **held**. A pending step is held when it or its
project is paused, when it is a ready `core.external` step, or when it is not ready and every
step it depends on (handoff sources and gate steps, a unit gate's exit steps) that has not
succeeded or been skipped is itself settled; what it waits for is then a failed, stale or held
step, or a plan input with no value. A running step is not settled, nor is a pending step that
is ready (queued on resources included), about to be skipped or failed by the scheduler, or
waiting on a step that is running or can still start. A unit's settling (§6.7, `unit.settled`)
is looser: it counts every pending step that is not ready, whatever it waits on.

### 7.2 Admission and launch

While the scheduler lease is held the coordinator, on every relevant change, settles skips,
marks staleness, and admits ready steps in priority order (higher first, ties in plan order).
Admission reserves an attempt and its runs in one transaction (status `running`, fresh run ids,
the messages assigned to each run, `prev_run`), then launches each run's unit. A reservation
whose launch fails before the payload starts fails that run with `process_lost`; it is never
started twice. Built-in fns that need no process run inside the coordinator.

### 7.3 Staleness

When a step starts or is set by hand it records `inputs_hash`, a hash of the canonical JSON of
its bound inputs (an unbound optional input is left out; a file binding by path). A succeeded
step becomes `stale` when a step it reads is stale or its inputs, once all available, hash
differently; it becomes `succeeded` again if they hash as recorded. Stale steps keep their
outputs, never re-run by themselves, and block their readers. A step set with `force` while its
inputs were not ready records unknown inputs and turns stale once they are all there. Gates
never cause staleness.

### 7.4 Retry and re-arm

`step_retry(project, steps?, tags?, message?, reason?)` takes steps that are `succeeded`,
`failed` or `stale` (any other selected step refuses the whole call). Each goes back to
`pending` and gets a `step.retry` record with its new work generation. A succeeded step keeps
showing its old outputs until the new run ends; its dependents go stale only if the result
differs. A failed scattered step keeps its succeeded items when its inputs hash and item count
are unchanged.

Retrying also **re-arms** the blocked region: from each retried step it walks dependents
through handoffs and gates, passing through failed, stale and pending steps (failed and stale
ones go back to `pending`, cancelled failures included) and stopping at succeeded, running and
skipped steps. The reply is `{project, steps, rearmed, stopped_at}`. `message` (1 to 65536
bytes) is posted to each retried step's thread in the same transaction, so its next run is
assigned it. Pauses are kept.

**The previous attempt.** Each run of a step is an attempt at it; a retry, a re-arm, a
send-back (a retry with a message, or a `Rejected` completion action) or any other relaunch
starts a new one, and its `prev_run` (§7.8) names the one before. sluice derives a note about
that previous attempt from what it keeps anyway (the run, its result, its submission, the
`step.cancel` or `step.settle` record) and stores nothing new. The note says:

- which attempt this is (1 for the first: "None: this is the first attempt at this step.");
- how the previous one ended: succeeded, failed (its error kind, agent kind and message),
  cancelled (by whom and why, while the log still has the `step.cancel` record), lost, or
  settled on its submission (by whom and why);
- its submission's fields, or when it submitted nothing its outputs, every string cut to 200
  characters and the line to 1,200;
- when its outputs include a `git` value (the agent fns' result), the head it ended at, the
  commits since its baseline and whether it left uncommitted changes;
- when the step's inputs bind a `cwd` that is a git work tree, `git status --porcelain` of it
  now: how many entries and the first 20 (`M path`, `?? path`). git runs with a 3-second limit;
  git absent, failing or too slow is said in the note (`unavailable: <why>`) and never fails
  the launch or the call.

`step_context` carries it as `attempt: {number, previous?, worktree?, note}` (`previous`:
`{run, started, finished, ended, error?, by?, reason?, submitted?, outputs?, git?}`;
`worktree`: `{cwd, git: "clean" | "dirty" | "unavailable", count?, paths?, reason?}`; `note`:
the text), for a running step about its current run, otherwise about the attempt its next run
would follow; a scattered step has none. Every agent's task starts with `note` (§15), computed
at launch.

### 7.5 Cancel

`step_cancel(project, steps?, tags?, reason?, expected_rev?)` asks running steps to stop (their
guardians stop the payload) and fails a pending `core.external` step at once. A cancelled step
fails with the error `{"error": "cancelled", "message": <reason>}` and a `step.cancel` record.
Reply `{"ok": true}`.

`step_settle(project, step, reason?)` settles a finishing step (§6.4) on its submission, for
the owner or the orchestrator (a run's callbacks may not). It is refused (`invalid`, saying
why) unless the step is running, not scattered, and its one current run has stored a valid
submission, and unless the step runs an agent fn itself (`agent.run`, `agent.codex`,
`agent.devin`, `agent.claude`, `agent.review`, `decide.llm`). Its outputs are the ones the done
signal would have given: the submission plus the agent fn's own, derived from what the run's
supervisor checkpointed (`runs/<run>/native.json`: the session, the agent's last message, the
git baseline, read by field so an older release's checkpoint reads too), the git facts of its
`cwd` now, the model its `model` input composes (a retired string `model` leaves `model` out,
as the run's frozen outputs then lack it) and, for `agent.codex` and `agent.devin`, the engine
log as their `log` output. They are checked against the run's frozen outputs before anything
is written; when they cannot be derived or do not fit, the call is refused (`invalid`) with
the way by hand. A fn that composes an agent (`ctx.builtin("agent.run")`) completes with what
it returns, which sluice cannot derive from the submission: it is refused with the same way by
hand, `step_cancel` and then `step_set_output` with the outputs it should have. Otherwise one
transaction records the outputs on the attempt, sets its cancel intent (the guardian stops the
payload as for a cancel) and writes a `step.settle` record; a second `step_settle` is a
`conflict`. The run's completion then succeeds with those outputs, whatever it reports
(cancelled, lost or failed), unless it succeeded on its own first, which keeps its own result.
Reply `{project, step, run, outputs}`.

### 7.6 Resources and leases

A project declares resources: `{"lane": 4}` or `{"lane": {"capacity": 4}}` (a fixed integer
≥ 0), or `{"cpu": {"capacity_fn": "<fn>"}}`, a fn the project sees that takes no required input
and returns `{capacity: int}`. While the scheduler lease is held, every capacity fn is called as
a direct call (author `capacity`) every 10 s; a good value replaces the cached one and is
recorded as `project.capacity`, a failure keeps the last good value and records the error.
Before its first value a capacity fn's resource admits only needs of 0.

A step's `needs` (`{resource: n}`) is held while it runs (a scattered step once). A ready step
with `needs` starts only when every named resource has `capacity - held >= need`; otherwise it
stays pending and queued (`step.queued` record when its shortfall changes; wait reason
`queued: needs lane 1 (4/4 held)`). Lowering a capacity never stops running work; a resource
that a step needs or a lease holds cannot be removed.

A **section lease** (`ctx.acquire`, §5.4) waits for and holds an amount of a resource inside a
running step's run; grants follow the step's `priority`, then arrival. Held leases count in
the same totals as `needs`. A run that ends releases its leases. Grants and releases are
`step.lease` records.

### 7.7 Manual values

- `plan_set_input(project, name, value)`: sets a declared plan input (type-checked), a
  `plan.input` record; readers that already ran go stale when it changes.
- `step_set_input(project, steps?, tags?, inputs)`: binds the named inputs of the selected
  steps to `{"default": value}` in one edit, skipping running steps and steps lacking an input;
  a succeeded step whose binding changes goes stale. The reply is the edit result plus
  `changed` (the steps changed), `running` (selected, running, left alone) and `unsupported`
  (`[{step, inputs}]`, selected but lacking those inputs).
- `step_set_output(project, step, outputs, force?, reason?)`: marks a non-running step
  `succeeded` with `manual: true`, outputs checked against the step's outputs (arrays for a
  scattered step). Without `force`, refused (`invalid`, "step gates or inputs are not ready")
  while a gate is unsatisfied or something it reads is not ready. A pause, the step's or its
  project's, does not block it: pausing holds back a launch, so a paused step can be given its
  result without ever starting. A `step.output` record.

### 7.8 Message delivery and `prev_run`

Each step keeps a delivery cursor: the last message id addressed to it that a run of it was
given. Reserving a run assigns it every message to the step after the cursor and records the
range on the run; the live feed continues from the end of that range, and the cursor advances
once the run has started. Every run of a scattered step gets the step's messages.
`runs/<run>/messages.json` shows the assigned range. Each run records `prev_run`, the step's (or
item's) previous run, seen as `SLUICE_PREV_RUN` and `ctx.prev_run`. Agent fns resume the
previous session when a bound `session` says so, or when `session` is unbound, the run was
assigned messages, `prev_run` recorded a session, and the engine and cwd are unchanged;
`session: ""` always starts fresh.

### 7.9 Adoption and completion

A coordinator that starts finds runs whose guardians are still alive and watches them
(`run.adopt` outcome `watching`), finishes runs that completed while it was away (`finished`),
and fails runs whose guardian and payload are gone without a completion (`lost`, error
`process_lost`). A live run that no step or call references is stopped (`run.orphan`). A
completion is journalled by the guardian and acknowledged durably by the coordinator.

A starting coordinator answers at once. Its first adoption pass adopts runs concurrently, at
most 16 at a time. During that pass it serves reads, the scheduler lease and the runs' own
guardians; `install` commands never wait for the coordinator. Every other command, and a run's
own plan edits and section-lease requests, wait until the pass is done. A run's message that
answers a question is served at once, even when it sets a plan input. A waiting request is
refused with `busy` (retryable, "… this request was not executed, so it is safe to retry") when
32 requests already wait, when its client hangs up, or when the coordinator stops or its pass
fails first. Nothing is admitted or launched before the pass is done. A run whose adoption fails
stays as it was and is tried again by the next pass, every 30 s.

### 7.10 Outcomes

Removing a finished step from the plan (any edit, including `plan_prune`) stamps its result row
`removed_at`; the `outcomes` view shows these rows (`result_id, project_id, step_id,
generation, work_generation, attempt_id, unit, declaration, inputs, inputs_hash, status,
outputs, error, manual, run_ids, recorded_at, removed_at`). They are never trimmed and go with
the project.

## 8. Messages

Three verbs post a message, each with a required recipient; the thread and the sender are
derived, never given:

- `ask(project, to, body, title?, ui?, input?, data?)`: a question that needs a reply.
- `say(project, to, body, data?)`: a note; no reply is expected.
- `reply(project, to_message, body="", answer?)`: a reply to that message.

Each takes `run?` too: the run that speaks (agent and fn callers; `sluice tool` fills it in a
run, §14). The dashboard speaks as the owner (`owner: true`, which MCP and `sluice
tool` do not take).

- `from` is derived: a run's step for a step's run (`orchestrator` for a call's run), `owner`
  for the dashboard, `orchestrator` for MCP and command-line callers with no run identity,
  `sluice` for the coordinator's own alerts.
- `to`, for `ask` and `say`, is a step id in the project's current plan, `orchestrator` or
  `owner`. Anything else (missing, empty, an unknown or removed step, another name, the sender
  itself) is `invalid` and nothing is stored. A reply's `to` is the original's `from`.
- A step whose status is `succeeded`, `failed`, `stale` or `skipped`, or running with every
  run it has already submitted (its agents' sessions are over; the runs are only finishing),
  takes no messages: an `ask` or `say` to it, and a reply whose `to` it is (to a question it
  asked that is already answered, say), are refused (`conflict`, saying why) and nothing is
  stored or queued. A reply that answers or closes such a question while it is open is still
  taken: the question is answered (setting its `input`) or closed, and the step's retry takes
  it up. A retry's message reaches the step after the retry has reopened it.
- The thread: a reply keeps the original's; a message from a step's run lives on its own
  step's thread `step-<step>`; one to a step on `step-<step>`; between the orchestrator and the
  owner on the fixed thread `owner`.
- A reply to an open question answers it (an **answering reply**) atomically: with or without
  `answer` `{action, params?, values?}`; `answer.action == "close"` closes it without setting
  anything. A reply to a question already answered or closed is just a message, but one with
  an `answer` is refused (`conflict`, "question is no longer open"); `answer` on a reply to a
  message that is not a question is `invalid`. `body` may be empty only with an `answer`; an
  `ask` or `say` body may not be blank (an `ask` with `input` may have an empty body: it shows
  the input's doc).
- `input` names a declared plan input. The answering reply sets it: the value is
  `answer.values.value`, else `answer.params.value`, else the body; it goes through
  `plan_set_input`'s path with reason `message <id>: <title>`. A value that does not fit
  refuses the reply (`invalid`) and the question stays open.
- An open question to `owner` reserves a notification attempt and writes a `project.notify`
  record (`outcome: reserved`). Notes, replies and questions to anyone else never notify.

Each verb returns its receipt `{id, to, thread, delivery, run?}`. `delivery` is `delivered`
when a live run of the step that listens has started and is handed the message on its live
feed (`run` names it), and for `orchestrator` and `owner` (their inbox). A run listens when
its fn takes a `listen` input (every agent fn, and a pack fn that runs one and passes
`listen` on) and its inputs do not set it to `false`; its reservation freezes this, so the
receipt reads stored rows, never which guardian protocol (a held watch or a poll) the run's
guardian speaks. A run reserved before reservations froze it listens if its step runs an
agent fn without binding `listen: false`, or once it has acknowledged a message. `delivery`
is `queued` when the step will run (pending and not paused, or its run reserved but not
started, which `run` names) and its next run is assigned the message; `no_live_run` when the
step is pending but paused, or its live run does not listen: the message is kept and given to
the step's next run if one is ever started.

A message is a row `{id, verb, from, to, thread, body, title?, ui?, input?, data?, run?, at,
to_message?, answer?}` plus a `message` record written in the same transaction; a field with
no value is left out. `verb` is `ask`, `say` or `reply`; `to_message` and `answer` are a
reply's. Read as rows, a question also carries `state` (`open`, `answered` with
`answered_by`, the answering message's id, or `closed`). The record carries the same fields
flattened into `{seq, at, project, kind: "message", ...}` with the message's own time as
`posted_at`, and never `state`. Rows and records stored before the verbs keep their stored
fields and read in this shape: a reply if they replied (`reply_to` becomes `to_message`), else
a question if they needed a reply, else a note. Rows are never trimmed with the log and are
deleted with their project.

The retired `message_post` is accepted only from a caller presenting a run identity (a run's
callback, or `run`), as runs started on older releases still post: translated to `reply` when
it names `reply_to`, else to `ask` unless `needs_reply` is false, else `say`, addressed to
`orchestrator` when it names nobody; its `thread`, `from` and `author` are ignored. It answers
`{id}` and logs a deprecation line. It is not an MCP tool, not listed by `sluice tool` and not
documented for agents.

**Notify.** With `notify: {command, timeout_s}` in config.json, the coordinator runs `command`
(argv, no shell; cwd the home; its environment plus the home's and the project's `.env`) once
for each reserved notification, whether or not anything holds the scheduler lease, outside any
transaction and in a task of its own. stdin is the message's JSON plus `project` (its name) and
`project_id`; stdout is ignored; the last 2 KiB of stderr are kept. The attempt is claimed
durably before the command starts, so it never runs twice: exit 0 is `dispatched`; a non-zero
exit or a failure to start is retried twice more (after 1 s and 2 s) and then `failed`; a
timeout (`timeout_s`, default 30) is `uncertain` and not retried; a claim a stopped coordinator
left is `uncertain` (`process_lost`). Each result is a `project.notify` record and stays on the
attempt (`notification_attempts`). A reservation whose question is answered or closed before it
goes out is `failed` (`cancelled`) without running anything. Without `notify` reservations wait.

**Nobody reading.** With `unread_alert_min` in config.json, every 30 s the coordinator asks the
owner (a question from `sluice`) about each `unit.settled` record at least that many minutes
old in a project whose orchestrator readers (`next`) have all been silent that long and none has
read past it: once per record, whatever the threshold later becomes.

Question state is derived: `open`, `answered` or `closed`. A question posted by a step's run
is `waiting` while that run is live; otherwise it reports why nobody waits: `asking run is
being cancelled`, `<step> is <status>`, `<step> is cancelled`, `running another run`, `not in
the plan`, `call <run> is <status>`.

`message.ask` with `wait: true` (§16) blocks until the first answering reply and returns it as
`reply`; a closed question fails it (`question closed`). A retried step asking with the same
title takes up its own latest earlier question: an open one nobody waits on gets the new run as
asker; an answered one whose answer nobody claimed returns that answer at once. A claimed
answer (`claimed_by`) is never reused.

`messages(project, view, thread?, since?, owner=false)` → `{project, messages, last_id}`, read
as the caller: the owner for the dashboard and with `owner`, else the orchestrator:

| view | shows |
|---|---|
| `inbox` | open questions to the reader, then the other messages to it after its read position in their thread |
| `questions` | every open question in the project |
| `history` | every thread with a message to or from the reader |
| `thread` | one thread in full (`thread` required) |

Read positions are kept per project, reader and thread and advance through `mark_read` (the
dashboard marks what it shows, as `owner`).

## 9. The log

Every record is `{seq, at, project, kind, …}`; `seq` is home-wide and increasing, so one log's
seqs have gaps. Kinds:

| kind | fields |
|---|---|
| `plan.edit` | `rev, author, reason, ops` |
| `plan.input` | `rev, author, reason, name, value` |
| `step.output` | `rev, author, reason, step, outputs, force` |
| `step.retry` | `rev, author, reason, step, work` |
| `step.cancel` | `step, author, reason` |
| `step.submit` | `step, run, outputs, author` |
| `step.settle` | `step, run, author, reason` |
| `step.status` | `step, from, to, error, run_ids, needs` |
| `step.lease` | `step, run, lease, resource, amount, state, reason` |
| `step.queued` | `step, needs, resources, reason` |
| `call` | `call, fn, status, inputs, outputs, error, direct, author` |
| `message` | the message's fields, with `posted_at` for its time |
| `project.pause` | `paused, reason, author` |
| `project.archive` | `archived, reason, author` |
| `project.update` | `fields, reason, author`; a board slot's change names `board_slot:<key>` in `fields`, with `reason` `"cleared"` for a clear |
| `project.board` | `rev, cleared, reason, author` (never the program) |
| `project.rename` | `old_name, new_name, author` |
| `project.delete` | `project_id, name, author` |
| `project.capacity` | `resource, fn, capacity, error` |
| `project.notify` | `message, outcome, error` |
| `run.adopt` | `run, step, call, outcome` (`watching`, `finished`, `lost`) |
| `run.orphan` | `run` |
| `run.completion_action.register` | `run, target, message, author` |
| `run.completion_action` | `run, outcome, author` |
| `unit.settled` | `unit, work, steps: [{id, status, held, outputs, omitted}]` |

`step_progress` (§6.4) writes no record: progress is state on the step, not history.

`kinds` filters take exact kinds or the groups `plan`, `step`, `project`, `run`, `unit`.
`threads` keeps only messages on those threads (alone it means messages only). `statuses`
(step statuses) keeps only the `step.status` records whose `to` is one of them; `recipients`
(step ids, `orchestrator`, `owner`) keeps only the `message` records whose `to` is one of them.
Each filter speaks of its own records and lets records of other kinds through: a record is
returned when its kind passes `kinds` (and, when `threads` is given without `kinds`, is
`message`) and it passes every other filter that applies to its kind. `threads` and
`recipients` both apply to a message. An empty list is no filter. So `kinds: ["step.status",
"message"], statuses: ["failed", "stale"], recipients: ["orchestrator"]` returns failures,
stale steps and messages to the orchestrator, and nothing else. `next` and `sluice watch`
take none of these filters. Calls made without a project go to the home log (`project` null).

Each log keeps at most 10,000 records; past that it is trimmed to 9,000 in the same
transaction. A `since_seq` older than a log's trim floor, or newer than any seq the home has
issued, is `cursor_expired`. `plan_edits` is never trimmed, so `plan_history` reaches rev 1.

## 10. Waiting: `log_wait`, `step_wait`, `next`, `watch`

Waits block on the log's commits rather than polling: a wait reads once, then again only after
a record is committed to the log it watches, so a caller that would sleep and re-check holds one
of these instead.

`log_wait(project?, since_seq?, kinds?, threads?, statuses?, recipients?, limit=200,
timeout=300, wake="any")` returns `{records, last_seq}` as soon as a matching record (§9) exists
after `since_seq`, or with no records after `timeout` seconds (capped at 3600). Records that do
not match never wake it. `wake: "questions"` holds notes until a record that is not a note
arrives or the timeout passes.

`step_wait(project, steps? | tags?, until, timeout=300)` waits until every selected step meets
`until`: `"succeeded"` (each succeeded), `"settled"` (each settled, §7.1) or `{"any_of":
[statuses]}` (each in one of them). Exactly one of `steps` and `tags` selects (a tag selects
every step carrying it, `unit:<name>` a unit). It reads the selected steps' statuses once, at
once, and again after each commit to the project's log (every status change, pause and plan
edit writes a record), until the condition holds or `timeout` seconds pass (capped at 3600; 0
reads once). The reply is `{met, steps: {id: status}, seq}`: whether the condition held, each
selected step's status in plan order, and the project log's last seq at that reading, so a
`log_read` or `log_wait` from `seq` sees what came after. An unknown step, a tag no step
carries, both or neither of `steps` and `tags`, and an `until` that is not one of the three
shapes (an unknown status, an empty `any_of`, another field) are `invalid`, `errors` naming
each (`steps: no step x`, `tags: no step is tagged t`, `until.any_of[1]: …`); a selected step
removed from the plan during the wait ends it with the same error.

`next(projects=[], since_seq=0, me="orchestrator", timeout=300, all=false, settle=20,
settle_max=120, settles="short")` waits across projects (all live projects when empty) for what
an orchestrator acts on and returns `{records, notes, last_seq, timed_out}`. It wakes on:

- a `unit.settled` record (written when a unit settles, once per unit and work generation);
- a `step.status` to `failed`, `stale` or `skipped`;
- a question to `me` (or, stored before the verbs, to nobody), not from `me`;
- a reply to a question, not from `me`;
- `project.pause` or `project.archive` not authored by `me`;
- with `all`, any record.

A step's progress (§6.4) writes no record, so it wakes none of these waits, `all` included.
Notes are held and returned in `notes`. After the first waking record it keeps collecting until
`settle` seconds pass with nothing new, or `settle_max` seconds after the first. Messages come
first in `records`. `settles` sets how much of a settled unit's step outputs are carried:
`short` (booleans, numbers, strings up to 80 characters and a `summary`'s first line; the rest
named in `omitted`), `full` or `none`. Every call records the reader's position and heartbeat in
`readers`.

`sluice next` is the same wait (§14), and `sluice watch` follows the log as JSON lines.

## 11. Verify

`verify(project?)` returns a list of problems `[{where, message}]` (empty when all is well) and
changes nothing. It covers the fn registry (shapes, names, icons, collisions), `.env` syntax,
project directories that belong to no live project (a warning), unfinished attempts, and for
each project its plan (full validation against its fns, missing `file` bindings) and its state
against the plan.

## 12. Tools

### 12.1 Transports

- **MCP**: streamable HTTP at `http://127.0.0.1:<port>/mcp`, served by `sluice serve`. The
  server's instructions are the `instructions` docs topic; every topic is also a resource
  `sluice://docs/<topic>`.
- **HTTP**: `POST /api/tools/<name>` with a JSON object body, the same flat arguments as MCP;
  the reply is the tool's JSON, errors mapped to 400 (`bad_request`, `invalid`), 404, 409
  (`conflict`, `cursor_expired`), 503 (`busy`), 408 (`cancelled`) and 500.
- **MCP over stdio**: `sluice mcp` serves the same tools, instructions and resources on its
  stdin and stdout, through the home's coordinator (started when nothing answers).
- **CLI**: `sluice tool <name> '<json>'`, or `--field value` flags (§14).
- **Wire**: every tool is a command `{"command": name, "args": {...}}` with a reply `{"reply":
  kind, "data": …}` on the coordinator socket (`sluice tool rpc '<request>'` sends a raw
  request). `docs/rust/schemas.json` (`CommandRequest`, `CommandReply`) is its schema.

The HTTP server binds loopback only. Every route refuses a `Host` that is not `127.0.0.1`,
`localhost` or `[::1]`, and refuses a present `Origin` that is not the same loopback origin.
Bodies are at most 1 MiB and at most 64 requests run at once. A tool call has a deadline of its
wait plus 30 s (`busy`, not retryable, "command deadline exceeded").

### 12.2 Arguments, results and errors

MCP and HTTP arguments are flat; an argument a tool does not take is refused (`bad_request`,
below). `project` is a current name or `id:<uuid>`; `steps`, `tags`, `projects` and `state`
also accept a single string. Waits (`wait`, `timeout`, `settle_max`) are capped at 3600 s.

Arguments are forgiving in the same way on every path that takes them (MCP, HTTP, a fn's
`ctx.tool` and `sluice tool`), and both rules read the command's generated schema:

- An unknown tool, or an argument a tool does not take, is `bad_request` naming it and the
  nearest valid names, at most three: those within edit distance 3 (at most half the given
  name's length, at least 1), and those containing it or contained in it (case, and `-` for
  `_`, ignored): `unknown tool step_contxt; did you mean step_context?`, `step_submit takes no
  argument 'output'; did you mean outputs?`. With no near name, the message lists the
  arguments the tool takes instead.
- Where an argument's schema takes an integer and no string (message ids such as
  `to_message` and `since`, seqs such as `since_seq`, revisions such as `rev` and
  `expected_rev`, limits and waits), at any depth, a string of decimal digits (surrounding
  whitespace ignored) is that integer: `"to_message": "24771"` is `24771`. Any other string
  there is `bad_request` naming the field (`to_message: expected an integer, not "abc"`). A
  field that takes strings keeps its digits as a string. A reply object is returned as is; a non-object reply is wrapped as
`{"result": …}` in MCP structured content; an acknowledgement is `{"ok": true}`.

Errors are `{"error": kind, "message", …}`:

| kind | extra fields | meaning |
|---|---|---|
| `bad_request` | | malformed or unknown argument |
| `not_found` | | unknown project, step, fn, call, unit |
| `conflict` | `current_rev?` | stale revision, state changed, question no longer open |
| `invalid` | `errors` | validation failed; each error has a path |
| `busy` | `retryable` | drain, maintenance, deadline, adopting runs |
| `storage` | | database or I/O failure |
| `cursor_expired` | | `since_seq` outside the log |
| `process_lost` | | a run's process vanished without a result |
| `cancelled` | | cancelled work |
| `fn_failure` | | a fn failed |
| `transient` | | a retryable failure that exhausted its retries |
| `rejected` | | a fn refused the work on purpose |
| `agent_failure` | `kind, session?` | an agent session failed |
| `exited_without_submit` | `session?` | a step's work ended without a valid `step_submit` |

### 12.3 Authors

Every write records an author: the tool's `author` argument when given; else `SLUICE_AUTHOR`;
else `step:<SLUICE_STEP>` when set; else the MCP client's name; else `mcp` (MCP) or `cli`
(`sluice tool`). The dashboard writes as `owner`, capacity calls as `capacity`.

### 12.4 Tool reference

Edit tools share `rev?`, `dry_run=false`, `reason=""` and `author?` (`plan_patch` requires
`rev` and `reason`) and return the edit result (§6.10) or, with `dry_run`, the preview. Selection tools take
`steps?` and/or `tags?` (a `unit:` tag selects the unit); naming neither is `bad_request`, an
unknown step `not_found`.

**Projects and fns**

| tool | arguments | result |
|---|---|---|
| `docs` | `topic?` | the topic's markdown, or the index |
| `projects_list` | | `[{project_id, name, description, rev, settings_rev, counts, paused, archived, board_rev, resources?, icon?}]` (live projects, by name); `counts` maps step status to the plan's steps in it, `resources` each declared resource to `{capacity}` or `{capacity_fn}`, `icon` is `{kind: "image", type}` or `{kind: "text", text}` |
| `project_create` | `name`, `description=""`, `icon?`, `resources={}`, `author?` | `{project_id, name}` |
| `project_update` | `project`, `new_name?`, `description?`, `icon?` (`""` removes), `resources?` (each key set, null removes), `paused?`, `archived?`, `expected_settings_rev?`, `reason?`, `author?` | `{project_id, name}`; changes write `project.rename`, `project.pause`, `project.archive`, `project.update` |
| `board_set` | `project`, `program` (null clears), `expected_rev?`, `reason?`, `author?` | `{rev}`; a stale `expected_rev` is `conflict`, a program that does not check `invalid` with each problem as `line N: …`; the same program again changes nothing; writes `project.board` |
| `board_slot_set` | `project`, `key`, `markdown` (`""` or null clears), `author?` | `{key, updated_at, cleared, changed}`; no revision; `key` is `[a-z0-9][a-z0-9_.-]{0,63}`, `markdown` at most 16 KiB, a project at most 64 slots and 256 KiB of them, else `invalid`; the same markdown again changes nothing; writes `project.update` with `fields` `["board_slot:<key>"]` |
| `board_get` | `project` | `{project, rev, program}` (`rev` 0 and `program` null before any board) |
| `project_delete` | `project`, `confirm_name`, `expected_settings_rev` (from `projects_list`), `author?` | `{project_id, name, deleted}`; the project must be archived, and nothing of it live |
| `fn_list` | `project?` | `[{name, doc, inputs, outputs, scope, submits?, icon?, open?}]` |
| `fn_get` | `name`, `project?` | the fn.json plus `scope` and `path` (null for a builtin) |
| `fn_save` | `fn`, `main_py`, `project?` | `{name, scope, path, generation}` |
| `fn_call` | `name`, `inputs={}`, `project?`, `wait?` (seconds, default 0), `direct=false`, `author?` | `{call, project_id, status, inputs, outputs, error, direct}`; `direct` runs it now through a guardian and ignores `wait` |
| `call_status` | `call`, `project?` | as `fn_call`, plus `finishing?` (§6.4) |
| `recipe_list` | `project` | `[{name, doc, params, scope}]`, a broken file as `{name, scope, error}` |

An `icon` is a text icon (at most 16 characters), an absolute or `~/` path to an SVG, PNG,
WebP, JPEG or GIF file of at most 256 KiB, which the coordinator reads once and stores, or
`{media_type, bytes_base64}`. A missing or unreadable path is `invalid`.

Each call's status changes are `call` records. `core.external` cannot be called.

A step's run may call `board_set`, `board_slot_set` (and `board_get`) for its own project, as
it may the plan edits; another project's board is outside its authority.

**Plan edits**

| tool | specific arguments |
|---|---|
| `plan_patch` | `project`, `rev`, `ops` (RFC 6902), `start=true` |
| `step_add` | `project`, `step`, `spec`, `start=true` |
| `step_update` | `project`, `step`, `changes` (each key replaces that field, null removes it) |
| `step_remove` | `project`, `steps?`, `tags?` |
| `step_pause` | `project`, `steps?`, `tags?`, `subtree=false`, `paused=true` (sets `"paused"` to the `reason`, or `true` without one, a step already paused keeping its own; `false` removes it) |
| `unit_add` | `project`, `recipe`, `unit`, `params={}`, `after={}`, `inputs={}`, `tags=[]`, `start=true` |
| `unit_tag` | `project`, `unit`, `add=[]`, `remove=[]` |
| `edge_add`, `edge_remove` | `project`, `step` (a step or `unit:<name>`: its entry steps), `after` (entries) |
| `step_set_input` | `project`, `steps?`, `tags?`, `inputs` |
| `plan_prune` | `project`, `units?`, `tags?`, `older_than=0` (seconds) |

`step_pause` with `subtree` also selects every step downstream of the selection (reading from
or gated on one, transitively). `unit_add` reports the steps it added in `steps`, `unit_tag`
the unit's steps and `step_pause` the steps selected.

`plan_prune` selects done units (all, or those named or tagged) whose last step finished at
least `older_than` seconds ago, keeps any unit that a surviving step or plan output references
(computed as a closure), and removes the rest in one edit; naming a unit that does not exist or
is not done is `invalid`. The reply is the edit result plus `units` (removed) and `kept`:
`[{unit, step}]` or `[{unit, output}]`, each kept unit with the step or plan output that holds
it; `steps` lists the removed steps.

**State and values**

| tool | arguments | result |
|---|---|---|
| `plan_get` | `project` | `{project, rev, plan}` |
| `plan_history` | `project`, `since_rev?` | records: every edit (`plan.edit`, from rev 1) and the log's `plan.input`, `step.output`, `step.retry` |
| `plan_set_input` | `project`, `name`, `value`, `rev?`, `dry_run`, `reason`, `author?` | `{ok: true}`, or the preview |
| `step_set_output` | `project`, `step`, `outputs`, `force=false`, `reason`, `author?` | `{ok: true}` |
| `step_retry` | `project`, `steps?`, `tags?`, `message?`, `reason`, `expected_rev?`, `author?` | `{project, steps, rearmed, stopped_at}` |
| `step_cancel` | `project`, `steps?`, `tags?`, `reason`, `expected_rev?`, `author?` | `{ok: true}` |
| `step_submit` | `project`, `step`, `run`, `outputs`, `author?` | `{ok: true}` |
| `step_progress` | `project`, `step`, `run`, `outputs` | `{project, step, run, progress, at}` (§6.4) |
| `step_settle` | `project`, `step`, `reason=""`, `author?` | `{project, step, run, outputs}` (§7.5) |
| `status` | `project`, `steps?`, `tags?`, `brief=false`, `all=false`, `view="steps"`, `state?` | below |
| `step_context` | `project`, `step` | below |
| `plan_view` | `project`, `format="mermaid"`, `all=false` | text |
| `verify` | `project?` | `[{where, message}]` |

`status`, steps view: `{project, rev, board_rev, paused, inputs, outputs, resources, steps: {id: {status,
outputs, error, run_ids, done, total, instances, manual, paused?, queued?, waiting?,
finishing?}}, done_units?}`; a finishing step (§6.4) stays `running` and carries `finishing`.
Without a selection, done units are left out and counted in `done_units: {units, steps}` unless
`all`. `brief` cuts strings over 200 characters (`… [n more characters]`). `resources` maps
each to `{capacity, held, queued, error}`. `paused` is `true` or the pause's reason. Every
pending step that is not about to start has `waiting`, the reasons in order: `paused` or
`paused: <reason>`, `project paused`, each handoff not ready (`step <id> is <status>`, `plan
input <name> has no value`), each gate not satisfied (`after <entry> (<why>)`), the resource
shortfall (`queued: needs lane 1 (4/4 held)`, with `queued` listing the resources), and for a
`core.external` step with nothing else, `external: set its outputs with step_set_output`. Units view (`view: "units"`, `brief` refused): `{project, rev, board_rev, paused, resources?,
units: [{unit, state, age, engine, steps, blocked, last, line, finishing?}], done_units?}`;
`finishing` lists the unit's finishing steps `[{step, since, submission_seq, release}]`, whose
mark in `steps` is `▷` and which `line` names (`finishing <step>`) when nothing blocks it; `state` one of
`running`, `failed`, `settled`, `blocked`, `queued`, `pending`, filterable with `state`
(`state` is refused in the steps view).

`step_context` (also `sluice me`): `{project, project_id, step, fn, doc, status, started,
finished, elapsed, run, inputs, upstream: [{step, fn, status, outputs, error}], messages (open
questions on its thread), submit: {outputs, command, note}, thread, ask, needs?, queued?,
leases?, finishing?, attempt?}` (`finishing` §6.4, `attempt` §7.4); `submit.command` and `ask` are ready-to-run `sluice tool` lines; `submit.note` is
"Submit only when you are finished: submitting ends your session."

`plan_view`: Mermaid `flowchart TD` with one subgraph per unit, nodes labelled `id / fn /
status [done/total] / doc`, a class per status, done units left out with a `%% n done units (m
steps) left out` comment unless `all`; or `format: "html"`, a standalone page with an SVG of the
same graph.

**Messages and the log**

| tool | arguments | result |
|---|---|---|
| `ask` | `project`, `to`, `body`, `title?`, `ui?`, `input?`, `data?`, `run?` | `{id, to, thread, delivery, run?}` (§8) |
| `say` | `project`, `to`, `body`, `data?`, `run?` | `{id, to, thread, delivery, run?}` (§8) |
| `reply` | `project`, `to_message`, `body=""`, `answer?`, `run?` | `{id, to, thread, delivery, run?}` (§8) |
| `messages` | `project`, `view`, `thread?`, `since?`, `owner=false` | `{project, messages, last_id}` |
| `log_read` | `project?`, `since_seq?`, `kinds?`, `threads?`, `statuses?`, `recipients?`, `limit=200` | `{records, last_seq}`; without `since_seq` the latest records; filters §9 |
| `log_wait` | as `log_read`, plus `timeout=300`, `wake="any"` | `{records, last_seq}` |
| `step_wait` | `project`, `steps?` or `tags?`, `until`, `timeout=300` | `{met, steps, seq}` (§10) |
| `next` | §10 | `{records, notes, last_seq, timed_out}` |
| `query` | `sql`, `params=[]`, `limit=200` | `{columns, rows, truncated}` |

`query` runs one read-only statement on a fresh connection: at most 100,000 bytes of SQL, `limit`
1 to 1000 rows, a 2 s deadline, a 1 MiB response; writes, `ATTACH`, extensions and unsafe
functions are refused. JSON columns come back as JSON text.

**Maintenance**

| tool | arguments | result |
|---|---|---|
| `drain` | `projects?`, `author?` | `{paused, status: {mode, owner, paused, blockers, pending_calls, open_questions, drained}}` |
| `release` | `author?` | `{released}` |

### 12.5 Commands outside MCP

The wire also carries `mark_read` (advance a reader's position on a thread), `backup`, `builtin`
(guardian-authenticated only), `submission`, `acquire_lease`, `release_lease` and
`register_completion_action` (run callbacks only).

## 13. Dashboard

`sluice serve` serves the dashboard on the same port. Pages (look: `DESIGN.md`):

| route | page |
|---|---|
| `/` | projects: each with its status glyph, progress and what stops it; archived ones folded. "Runner stopped" heads it while nothing holds the scheduler lease (no `loop`, no `serve` without `--no-runner`) |
| `/projects/<name>` | redirects (307) to `/projects/id/<uuid>` |
| `/projects/id/<p>` | the board; query `order=live\|plan` (units by attention, running, ready, held, done; or the plan's order), `show=all\|active\|attention\|done` (which units: every one, not done, with a failed or stale step, done), `q=` (a search: the steps whose id, doc or unit id contain every word of it, any case and order, at most 200 characters; units without one hide; the page says how many matched), `tag=`, `format=mermaid` (the `plan_view` Mermaid; `all=true` keeps the done units). They combine; its `…/stream` takes the same query and draws the board under it. A done unit (every step succeeded or skipped) is one line, its steps in the lane marks (`fork✓ work✓ rm–`), opening to its cards; two or more in a row (under `order=live` every one, after the live work) sit on one shelf, "n done units · m steps", closed unless `show=done` or a search matches in it, which draws the shelf and the matching units open |
| `/projects/id/<p>/units/<u>` | one unit's board |
| `/projects/id/<p>/steps/<s>` | one step: status, actions (Retry first and primary on a failed step), finishing, error, progress (while fresher than the outputs, §6.4), outputs (those not set yet named on one line), inputs, runs |
| `POST /projects/id/<p>/steps/<s>/actions` | `action=pause\|unpause\|retry\|cancel`, `revision`, `message` (retry feedback) |
| `/inbox`, `/questions`, `/history` | the message views across projects |
| `/projects/id/<p>/{inbox,questions,history,thread}` | the same for one project; `thread?thread=<name>` |
| `POST /projects/id/<p>/messages` | post an answer or reply as `owner` |
| `POST /projects/id/<p>/messages/read` | mark messages read |
| `/log`, `/projects/id/<p>/log` | the log, 50 records a page, filtered by kinds and threads |
| `/fns` | the functions a project (`?project=`) or the home sees |
| `/projects/id/<p>/settings` (GET, POST), `/preview`, `/icon`, `/delete` | project settings |
| `POST /projects/id/<p>/settings/board` | the Board section: `op=save\|clear`, `program`, `expected_rev`; the page with the outcome |
| `POST /projects/id/<p>/settings/board/preview` | the body (a draft program) drawn as the board would draw it, with the project's data |
| `POST /projects/id/<p>/board/action` | a board Button: `board_rev`, `button` (its number), `field-<n>`; JSON `{ok, message}` with `Accept: application/json`, else a redirect to the board |
| `/projects/id/<p>/icon` | the project's image icon |
| `POST /settings` | display preferences (theme, value types) |
| `/static/<name>` | assets |

A finishing step (§6.4) keeps its running glyph; its card's caption reads "finishing", and its
drawer and page add a "finishing" badge and a Finishing section: when it submitted, the
`step.submit` record's seq, its release, and that `step_settle` settles a run that lingers.

A step with progress fresher than its outputs (§6.4) has a Progress section in its drawer and
page, before Outputs: its fields as the outputs show theirs, under a "live" badge (the running
glyph) while it runs, else "progress", and when it was set: by its current run, or by its
last, which ended without making it outputs. The board's `Output` shows the same value.

Every page has a `…/stream` twin that patches the page live over Datastar SSE. Pages render
fully without JavaScript; every value is HTML-escaped and markdown bodies are rendered on the
server with unsafe link schemes refused. The only external assets are two font stylesheets from
cdn.jsdelivr.net; every script is served from `/static/` (Datastar 1.0.4, OpenUI lang-core
0.3.0, zod 4.6.5 and sluice's own). A page and each stream batch are drawn from one store
snapshot, with each running run's activity (its run files' modification times) read once;
nothing that changes meanwhile fails the page. The version a page carries (`ver`) is a
fingerprint of the HTML its stream patches, so a stream opened at it sends nothing until
something shown changes. A page that fails answers the shared JSON error with its status, its
message never empty. Assets linked with their fingerprint (`?v=`) are served immutable.

**Questions with a ui.** A question's `ui` is an OpenUI Lang program drawn by the inbox page:
one statement per line, the first drawn, components `Stack`, `Heading`, `Text`, `Callout`,
`Table`, `Separator`, `Form`, `Input`, `Textarea`, `Select`, `Radio`, `Checkbox`, `Button`
(signatures in `docs("inbox")`). Unparseable lines are dropped and counted; the text box always
remains. A Button answers `{action, params, values}`; the text box answers with action
`answer` and its text as the body; "Close" answers with action `close`.

**The project board.** A project with a board (`board_set`, `docs("board")`) draws it on its
plan page: from 1280 px wide a right-hand column beside the plan (the page's column widens so
nav, plan and board keep shared edges; the column scrolls on its own; with the step drawer open
the drawer takes the side instead); below that a section after the plan, shown in turn with the
plan by a Plan · Board switch that remembers its choice per project in `localStorage` (until one
is picked a phone shows the board, a wider window the plan; without script both show, the plan
first). Without a board the page is as before. The program is
checked again and drawn on the server: the question components draw as the inbox draws them,
and the data components are filled when the page renders and with every live patch:
`Units(state?)` (the units view's rows), `StepStatus(step)`, `Output(step, field)` (the
freshest value: the step's progress for that field while it is fresher than the outputs, §6.4,
marked "live" with the running glyph while the step runs and "progress" after, with when it
was set; otherwise the output), `Metric(label, query)`, `Query(query, caption?)`,
`Chart(kind, query, caption?)` (bar or
line, an inline SVG), `Slot(key, fallback?)` (a slot's markdown, set per event by
`board_slot_set`, with "Updated <time>" under it; unset, its fallback muted or "Not set
yet.") and `LatestMessage(from, chars?)` (the project's newest message whose `from` is
`from`, with its time linking to it in its thread, its body cut to `chars`, default 280, with
an ellipsis and a link to the whole; none, "No message from <from> yet."); `Markdown(text)`
draws its text as markdown. Slot, Markdown and LatestMessage use the dashboard's markdown
renderer (escaped, unsafe link schemes refused); `Text` stays plain. A query runs through the
`query` tool's path, views and limits, every `?` bound to the project's id, at most 16 per
board; a stream reruns them when the project's log, plan, board or step progress changes (a
slot change is both of the first) and at least every 30 s. A component that cannot be drawn (a bad
query, an unknown step, a non-numeric chart) is an inline error box naming it, its line and the
reason; the page still renders. A Button sends `say(to: "orchestrator")` as `owner` with body
`Board: <label>` and data `{board_rev, action, params, values}` (a primary button checks its
form's rules first, `invalid` otherwise); if the board's rev changed since the page was drawn
it is refused (`conflict`, shown under the board) and nothing is sent. The board never edits
the plan. Project settings has a Board section: the program, a live preview, Save (fenced by
`expected_rev`) and Clear, then the board's slots, read-only, by key: each with when it was
updated, by whom, and its text on one line.

## 14. CLI

`sluice <mode>`; errors print as JSON on stderr with a non-zero exit.

| mode | |
|---|---|
| `serve [--no-runner] [--port P] [--host H]` | dashboard, MCP and HTTP tools; takes the scheduler lease unless `--no-runner`; loopback only. Host and port default to config.json's `http`, else 127.0.0.1:3065 |
| `loop` | takes the scheduler lease and holds it until SIGINT/SIGTERM |
| `coordinator [--maintenance]` | runs the home's coordinator in the foreground |
| `install fence <reason> \| unfence \| select <release_dir> <home> \| status` | §2.2 |
| `tool [name [json\|-] [--field value]…]` | without a name, lists the tools; with one, runs it (its arguments as below) and prints its result as MCP returns it (`{"ok": true}` for an acknowledgement); `tool <name> --help` lists its fields |
| `tool rpc '<request>'` | sends a raw wire request |
| `next [-p P]… [--since-seq N \| --cursor FILE] [--me NAME] [--timeout 300] [--settle 20] [--settle-max 120] [--all] [--settles short\|full\|none] [--cut 600] [--json]` | the `next` wait; without a since it starts at the top of the selected logs; `--cursor` reads and writes the seq in a file |
| `watch [-p P] [--kinds K,…] [--threads T,…] [--since-seq N] [--wake any\|questions]` | follows the log, one JSON record per line, until killed |
| `drain [-p P]… [--no-wait] [--release]` | drains (waits until drained unless `--no-wait`) or releases |
| `me [--project P] [--step S] [--json]` | `step_context` for the current step (from `SLUICE_PROJECT_ID`/`SLUICE_PROJECT` and `SLUICE_STEP`) |
| `doctor [--json]` | host prerequisites, each engine (`codex`, `claude`, `devin`): its executable on PATH, `--version` and whether its profile supports it, and the selected release's manifest check. The engine probes run with HOME and the engines' config dirs in a private scratch directory, so no session starts and no credential is read; a missing or unsupported engine is a warning, not a failure |
| `query [SQL [PARAM…]] [--limit N] [--table [--width 60]]` | the `query` tool, read directly from the database; without SQL, every public table and view with its columns |
| `backup PATH [--force]` | an online copy of `sluice.db` |
| `docs [topic]` | the agent docs: the pages the `docs` tool serves |
| `mcp` | the MCP server over stdio (§12.1) |
| `agent hook --engine codex\|claude\|devin --event E [--run R]` | engine hook entry (internal) |
| `guardian`, `payload-exec` | internal |

`sluice next` prints one line per event: `MSG|NOTE <thread> <from> -> <to>: <body>`, `STEP <id>
<from> -> <to>: <error tail>`, `UNIT <u> settled: <outputs>` (long outputs named with a hint to
read them with `sluice query` or `--settles full`), `PROJECT <id> paused by <author>`, and last
`seq N` or `timeout seq N`; `--json` prints the reply.

`sluice tool` takes the wire argument names with these conveniences: `steps`/`tags` (and
`after`, `projects`, `state`) as plain values or lists, `expected`/`dry_run`/`reason`/`author`
flat on edit tools, MCP's public names (`rev` for `expected`, `wait` for `fn_call`, `timeout`
and `wake` for `log_wait`, `timeout` for `step_wait`, `timeout`, `settle` and `settle_max` for
`next`, `older_than` for `plan_prune`, `fn` for `fn_save`), `older_than_hours` for
`plan_prune`, `name` for `project_update` and `project_delete` (which also fills
`confirm_name` and the current `expected_settings_rev`), `params.unit` for `unit_add`, and
`step`/`input`/`value` for `step_set_input`. Any other name is refused with the nearest ones (§12.2).

A tool's arguments are one JSON object, flags, or both:

- **JSON**: the object as the argument after the name; `-` there reads it from stdin. With
  neither an argument nor a flag, stdin is read only when it is a redirected regular file
  (`< args.json`), never a pipe or a terminal, so such a call never waits on a stdin nobody
  writes to.
- **Flags**: `--<field> VALUE` or `--<field>=VALUE`, one per field, the flag being the field
  name with `-` or `_` (`--to-message`, `--to_message`). The value is the text as given for a
  field whose schema takes only strings (`--to owner`, `--body 42` and `--project demo` stay
  strings). For any other field it is parsed as JSON when it parses (`--to-message 24771`,
  `--outputs '{"ok": true}'`, `--steps '["a","b"]'`, `--until '{"any_of": ["failed"]}'`);
  else, for a list of strings that takes no single string, it is a one-item list of the text
  (`--kinds message`, `--statuses failed`, `--recipients orchestrator`); else it is the text
  (`--steps a`, `--until settled`, `--value hello`). A field that takes neither a string nor
  a number nor a boolean (an object such as `outputs`) refuses text that is not JSON. A boolean field's flag with no value is
  `true` (`--owner`, `--dry-run`). A value that starts with `--` needs the `=` form; a
  repeated flag keeps its last value.
- **Files**: `--<field>-file PATH` reads the value from a file by the same rule, taking the
  file's text exactly (no newline trimmed); `-` reads stdin, which one argument at most may do.
- **Both**: the flags apply over the JSON object, so a flag wins over the same field in it.
- **Run defaults**: in a run (`SLUICE_RUN_ID` set), a call that leaves out `project` gets
  `id:$SLUICE_PROJECT_ID` (when set), and one that leaves out `run` gets `$SLUICE_RUN_ID`, on
  every tool that takes them; `step_submit`, `step_progress` and `step_context` also get
  `step` from `SLUICE_STEP`. No other step is defaulted: a step that names a target is always given. A
  field the call gives is kept, even as null (`"run": null` speaks as the orchestrator,
  `"project": null` leaves an optional project out), and `project_update` or `project_delete`
  given `name` names its project that way. Only `sluice tool` defaults; MCP, HTTP and
  `ctx.tool` do not, except that `ctx.tool("step_progress", …)` defaults `step` and `run` to
  the run's own.
- **Help**: `sluice tool <name> --help` (or `-h`) prints the tool's description and each field
  with its type, whether it is required or its default, and what it is, from the tool's schema;
  `sluice tool --help` prints the usage.

```sh
sluice tool reply --project demo --to-message 24771 --body-file - <<'END'
It's on the "parser" branch.
END
sluice tool say --to orchestrator --body 'blocked on the schema'   # in a run
sluice tool step_submit --outputs-file outputs.json                # in a run
sluice tool status '{"project": "demo"}' --brief
```

## 15. Agent engines

`agent.claude`, `agent.codex`, `agent.devin`, `agent.review` and `agent.run` run a supervised
interactive session of the engine CLI in the run's private tmux, in `cwd`. The supervisor
writes the task (for a step's run, first the note on the step's previous attempt under `##
Previous attempt` (§7.4) and then `## Task`; the prompt or spec, the step's inputs under `## Inputs`, the outputs to submit
with the exact `step_submit` command under `## Outputs you must submit` and "Submit only when
you are finished: submitting ends your session." with the `--outputs-file` form that needs no
shell quoting, and, unless `listen: false`, how to `ask` the orchestrator, `say` to it and
`reply`, with the run's id, and their flag form, §14), watches the session through the
engine's hooks, nudges a stalled session, and ends it when the agent is done. The result carries
`session` (pass it back to resume) and `git` facts `{head_before, head_after, commits, dirty}`
of `cwd`. A failed session is an `agent_failure` error with its `kind` and `session`. Agent fns
retry a transient failure up to 3 times, 600 s apart, resuming the session; a rate limit whose
reset the engine reported waits until just after that reset instead.

Codex runs with a private `CODEX_HOME` under `<home>/codex-native-homes/`, whose `auth.json` is a
symlink to the owner's (`$CODEX_HOME/auth.json`, by default `~/.codex/auth.json`), never a copy,
with `cli_auth_credentials_store = "file"`. Codex rotates its refresh token at each refresh,
writes that file in place and reloads it before refreshing, so one refresh, in any run or in the
owner's own Codex, serves them all. Each launch or resume, under `codex-native-homes/.auth.lock`,
replaces any copy an earlier release left in a private home with the link; a copy for the owner's
account refreshed later than the owner's file (`last_refresh`) is written to the owner's file
first, and an older one never is.

A stalled coordinator or guardian never fails an agent run. The supervisor's reads, its
acknowledgements of live messages and its notes to the orchestrator retry a peer that answers
`busy` (retryable) or does not answer within its 5 s, with waits growing from 50 ms to 1 s,
until it answers, the run is cancelled, or (acknowledgements and notes) ten minutes have passed;
it keeps answering the engine's hooks meanwhile. Each retry of a note is the same request, so
the coordinator posts it once. A guardian refuses an acknowledgement of a message it has not
offered yet, and its watch may lag the supervisor's read, so a refusal is retried for a minute
before it counts. An acknowledgement still unanswered after ten minutes fails the run as
`Transient` (the helper's retry resumes the session); a definite refusal fails it as `Invalid`.
A note that never got through is logged and dropped. A `busy` from the socket client says what
happened and to whom: `could not connect to the coordinator (…); the request was not sent`,
`the run's guardian did not answer within 5s; acceptance may be unknown`, or `… ended the
exchange without an answer (…)`.

The agent is done when it submits: the supervisor stops the session as soon as the run's
valid submission is stored, busy or not, and returns the result (§6.4); `final` is the agent's
last message so far, which a busy agent may not have finished. An idle agent that has not submitted is nudged
after the settle (`SLUICE_AGENT_SETTLE_S`), or, before the first nudge of an engine that does
not report background work (Codex, Devin), after `SLUICE_AGENT_GRACE_MIN`; after the last nudge,
or when the engine exits first, the run fails with kind `ExitedWithoutSubmit` and the step with
`exited_without_submit`. A step with nothing to submit ends a settle after its agent goes idle,
or when its reported background work ends (at most `SLUICE_AGENT_WORK_MIN`).

A Devin turn ends at a `Stop` hook that no other hook follows for 10 s, once the pane no longer
shows Devin working (the `(esc twice to interrupt)` spinner or the `Guide Devin while it works`
placeholder). A tool or compaction hook after a `Stop` keeps the turn open, or reopens it: in
Fusion the sidekick's turn end fires `Stop` under the lead's prompt and the lead works on. Input
pasted into Devin is accepted by the `UserPromptSubmit` that carries it. Input pasted while
Devin works waits in its queue until the turn ends. It fails the run as `UnknownAcceptance` only
after 20 s with no hook, no working pane and no sight of it queued.

An engine's account problems end the run at once, typed: a hard usage cap with `agent_failure`
kind `QuotaExhausted`, an auth failure (logged out, a token expired or revoked, the account
barred) with kind `AuthFailed`. Neither is transient: the supervisor neither retries nor replays
input, and the failure keeps the session, so the step can be retried with its `session` bound
once the owner has acted (after the reset or with quota added; after signing in again on this
host), or run on another engine. A hard cap is a usage, plan, spend or credit limit whose reset
is unknown, or any limit whose reset is more than `SLUICE_AGENT_QUOTA_RESET_MIN` (15) away. A
short rate limit (its reset within the threshold, or a rate limit with no known reset) stays
transient; with a known reset the retry starts just after it (2 s) instead of 600 s later.

The message says what happened and what to do, then gives the engine's own text: `<engine>:
<what happened>[ (<window>)][; resets <YYYY-MM-DDTHH:MMZ> (in <relative>)] — <what to do>, then
step_retry. <Engine> said: <text>`, for example ``codex: not logged in (token revoked) — run
`codex login` on this host, then step_retry. Codex said: Your access token could not be
refreshed because your refresh token was revoked. …``, `claude: weekly limit reached
(seven_day); resets 2026-10-08T23:00Z (in 3d) — wait for the reset or buy usage credits at
https://claude.ai/settings/usage, then step_retry (or run the step on another engine). Claude
said: …` or `devin: weekly usage quota exhausted — buy usage or turn on auto-reload at
https://app.devin.ai/settings/usage, or wait for it to reset, then step_retry (…). Devin said:
…`. A short rate limit's message ends its head with `— retrying just after the reset` (or `after
the standard backoff`). The engine's text is cut to 1000 characters and anything token-like in it
is masked as `[redacted]`: a run with a credential prefix (`sk-`, `eyJ`, `ghp_`, `ya29.`, …), one
of 40 or more characters with letters and digits, or one with a digit after a credential word
(`Bearer`, `token=`, `api_key`, …); a 32-hex trace id, a `req_…` request id or a UUID stays. The
run's log (`stderr.log`) records the same text under the kind (`agent failed: AuthFailed: …`),
and each transient backoff with its wait. Each engine's signals, structured first:

- Codex: a turn's error (`turn/completed` or `turn/failed`, or an `error` notification Codex
  will not retry) with `codexErrorInfo` `unauthorized`, or an HTTP failure with
  `httpStatusCode` 401, is an auth failure; `usageLimitExceeded` is a usage limit and
  `rateLimitExceeded` a rate limit, their reset taken from the used-up window (`usedPercent` 100,
  `windowDurationMins`, `resetsAt`) of the latest `account/rateLimits/updated`, unless that
  limit's `credits` still carry on (`hasCredits` or `unlimited`). That update with
  `rateLimitReachedType` set and no credits fails the run at once, even with the turn still
  open: a `workspace_*_credits_depleted` or `workspace_*_usage_limit_reached` type is a usage
  limit, `rate_limit_reached` a rate limit. After either failure no further turn starts. At
  launch `account/read` with no account while the provider requires OpenAI auth fails as
  `AuthFailed` before any turn. Failing the structured fields, an error object's message in
  Codex's own words: `You've hit your usage limit. … try again at …`, `Quota exceeded. Check your
  plan and billing details.`, `Your workspace is out of credits.` and the like are a usage limit
  with no known reset; `Your access token could not be refreshed …`, `Your authentication session
  could not be refreshed …`, `unexpected status 401 Unauthorized: …` and the like are an auth
  failure.
- Claude: the API error's category, from the transcript's error entry (`isApiErrorMessage`,
  `error`) or `StopFailure`'s `error`: `authentication_failed` is an auth failure (except
  Claude's own `Authentication error · This may be a temporary network issue`, which stays
  transient), as are `oauth_org_not_allowed`, `account_on_hold`, `verification_required` and
  `cloud_credential_error` with their own fix; `billing_error` is a usage limit with no reset;
  `quotaLimits` `{status: "rejected", rateLimitType, resetsAt}` (`five_hour`, `seven_day`,
  `seven_day_opus`, …) is a usage limit with that reset; a `rate_limit` error whose text starts
  with Claude's limit wording (`You've hit your …`, `You've reached your …`, `You're out of usage
  credits`, `Claude AI usage limit reached|<epoch>`, …) is a usage limit. With no category (or
  `invalid_request`, `unknown`), text starting with Claude's login wording (`Not logged in`,
  `Login expired`, `OAuth token revoked`, `Invalid API key`, `Please run /login`, …) is an auth
  failure. The entry, when read, decides over the hook. Any other 429 stays transient. Logged
  out, Claude opens on its login screen (`Select login method:`, with no composer): the run
  fails as `AuthFailed` before any input is taken.
- Devin, out of quota or no longer authenticated, prints a notice after the prompt it took
  (`⚠︎ Quota exhausted` with `Your weekly usage quota has been exhausted. Visit
  https://app.devin.ai/settings/usage ... (trace ID: ...)`, `Usage limit reached`, `Usage
  paused`; `Authentication required` with `Your session is no longer authenticated. Run /login
  …`), sends no further hook and leaves its composer idle. While a turn is open or input is
  pasted, each observation reads the pane, and when such a notice is the last entry right above
  the idle composer (not tool output, an earlier entry or a quote) it is a usage limit with no
  reset or an auth failure, so the run fails at once instead of at the stall cap.

Only an engine's error channels are read (Codex's error objects, rate-limit updates and account,
Claude's API error entry, `StopFailure` and login screen, Devin's last pane entry and login
picker), and text only at its start, so the words in tool output or the agent's own prose never
match. Logged out, Devin opens on its login picker (`How would you like to log in?`, `Log in with
browser`): the run fails as `AuthFailed` before any input is taken, as Claude's login screen does.
`sluice doctor` probes each engine's version only, in a scratch home that holds no credential, so
it does not report auth.

A pane-driven engine (Claude, Devin) that stops before its first turn on an interactive screen
sluice does not answer can take no input, so the run fails at once with `agent_failure` kind
`BlockedScreen`; like the account kinds it is neither retried nor replayed and keeps the session.
The screens are read only while the engine's composer is not showing, before any turn of the
invocation:

- Claude 2.1.284: its first-run setup (the theme picker, `Let's get started.` / `Choose the text
  style that looks best with your terminal`; `Security notes:`; `Use Claude Code's terminal
  setup?`), setup's failed connectivity check (`Unable to connect to Anthropic services`), the
  API-key prompt (`Do you want to use this API key?`), Anthropic's updated terms (`Updates to
  Consumer Terms and Policies`), `Managed settings require approval`, a project's MCP servers
  (`New MCP server found in this project`), `Allow external CLAUDE.md file imports?`, and a
  required update (`… needs an update.`, `… older than the minimum version required by your
  organization`), which Claude prints as it exits;
- Devin 3000.11.3: its organization picker (`Select your Devin organization:`) and
  workspace-trust prompt (`Do you trust the authors of …`).

Sluice still answers Claude's folder-trust and bypass-permissions dialogs and closes Devin's menus
(`Select a menu item`, `Select model`, `Search sessions`). Any other non-blank screen that stands
unchanged before the first turn for `SLUICE_AGENT_SCREEN_S` (20 s) fails the same way: a normal
start draws its composer within seconds, and a slower one (a connectivity check, MCP servers
connecting, a long session loading) animates a spinner, which restarts the clock. The message says
which screen and what the owner must do, then quotes the screen's non-empty rows joined by ` | `
(cut to 1000 characters, anything token-like masked): `<engine>: blocked on <screen> — <what to
do>, then step_retry. <Engine> showed: <rows>`, for example ``claude: blocked on its first-run
setup (theme picker) — Claude Code's first-run setup is unfinished for the account it runs as:
run `claude` once on this host as that user and finish it (…), then step_retry. Claude showed:
Let's get started. | Choose the text style that looks best with your terminal | …``, or for an
unknown one ``<engine>: blocked on a screen sluice does not recognize, unchanged for 20s before
any turn — run `<engine>` once in the step's cwd on this host and answer it (or attach to the
run's private pane while it waits), then step_retry. …``.

When a pane-driven run fails before its engine completed a turn, or with a timeout or stall kind
(`TurnStartTimeout`, `ReadyTimeout`, `StallCap`, `WallCap`), the supervisor keeps the pane before
tearing it down: its screen and up to 50 rows above it, anything token-like masked, in
`pane-at-failure.txt` (mode 0600) in the invocation's directory (`runs/<run>/`), and the message
ends with `pane at failure (last rows; whole screen: <path>):` and the last 6 non-empty rows, each
trimmed, on lines of their own. A `BlockedScreen` message, which already quotes its screen, ends
with `pane at failure: <path>` instead. A cancellation keeps nothing, and a capture or write that
fails leaves the failure as it was. The private tmux keeps an exited engine's dead pane
(`remain-on-exit`) until the teardown, so what it printed as it exited is kept too. The run's log
records every failure but a cancellation as `agent failed: <kind>: <message>`.

Limits (minutes unless noted), overridable through environment variables: `SLUICE_AGENT_MAX_MIN`
(600, the wall cap), `SLUICE_AGENT_STALL_MIN` (30), `SLUICE_AGENT_SETTLE_S` (10),
`SLUICE_AGENT_GRACE_MIN` (10), `SLUICE_AGENT_POLL_S`, `SLUICE_AGENT_READY_S` (180),
`SLUICE_AGENT_TURN_START_S` (60), `SLUICE_AGENT_WAIT_MIN` (90), `SLUICE_AGENT_DIALOG_S` (60),
`SLUICE_AGENT_QUIET_MIN` (45), `SLUICE_AGENT_WORK_MIN` (10), `SLUICE_AGENT_NUDGES`,
`SLUICE_AGENT_QUOTA_RESET_MIN` (15; 0 makes every limit with a known reset a hard cap),
`SLUICE_AGENT_SCREEN_S` (20, seconds: how long an unrecognized screen may stand before the first
turn).

### 15.1 Choosing a model

The `model` input of `agent.claude`, `agent.codex`, `agent.devin` and `agent.run` is one of two
JSON objects; left out, the engine's default runs, which is the same object filled in: Devin
`{"type":"normal","model":"swe-2","effort":"high"}`, Codex
`{"type":"normal","model":"sol","effort":"high"}`, Claude
`{"type":"normal","model":"opus","effort":"high"}`.

| shape | composes |
|---|---|
| normal `{"type":"normal","model":M,"effort":E?,"fast":bool?}`, any engine | Devin `M[-E][-fast]`: `swe-2-high`, `claude-opus-5-5-high-fast`, `adaptive`. Codex: model `M` (`sol` and `astra` name `gpt-6.1-sol` and `gpt-6-astra`; any other name is a `codex debug models` slug) at reasoning effort `E`, recorded `gpt-6.1-sol@high` (no `E`: the model's own default, recorded `gpt-6.1-sol`). Claude: `--model M --effort E` (`M` is `opus` or `claude-opus-5-5`, `E` `low`…`max`; no `E`: Claude's settings), recorded `opus@high`. Codex and Claude cannot run fast: `"fast": true` is refused there |
| fusion `{"type":"fusion","main":{"model":M,"effort":E?,"fast":bool?},"sidekick":{"model":S,"effort":F?,"priority":bool?}}`, Devin only | `fusion-M[-E][-fast]-sidekick-S[-F][-priority]`: `fusion-claude-opus-5-5-high-sidekick-swe-2-high`, `fusion-claude-opus-5-5-high-fast-sidekick-gpt-5-6-luna-high-priority`, `fusion-claude-opus-5-5-high-sidekick-glm-5-2` |

The objects are strict: an unknown key anywhere, a string that is not a non-empty string, or a
`fast` or `priority` that is not a boolean is refused; `effort` is left out for a model that
has none. At launch, before the session starts, the composed id is checked against what the
engine lists: `devin models list --format json` (each variant's `model_uid`), `codex debug
models` (each slug, and `<slug>@<effort>` for each reasoning effort it supports), and for
Claude, which has no listing, the pinned models and efforts above. A listing is reused for 10
minutes per process. An id the engine does not list fails the run with `agent_failure` kind
`Invalid` naming the composed id and up to 5 nearest listed ids (those it begins first, then by
edit distance); nothing else runs in its place. A fusion on Codex or Claude, `fast` there, or a
malformed object is refused the same way. The plan types `model` as `Any?`, so a stored plan
still holding a retired form (a string such as `"sol"` or `"fusion"`, or a separate `effort`
input) keeps validating; such a step fails at launch with kind `Invalid` and a message giving
the object to use instead (`"sol"` with effort `xhigh` →
`{"type":"normal","model":"sol","effort":"xhigh"}`, `"fusion"` →
`{"type":"fusion","main":{"model":"claude-opus-5-5","effort":"high"},"sidekick":{"model":"swe-2","effort":"high"}}`).
The result's `model` is the id the run resolved and launched. `agent.review` and `decide.llm`
run Claude's default.

## 16. Built-in fns

| fn | inputs | outputs | notes |
|---|---|---|---|
| `core.echo` | `value: Any` | `value: Any` | inline |
| `core.collect` | `items: Any[]` | `items: Any[]` | the fan-in join; inline |
| `core.format` | `template: string`, `values: Any` | `text: string` | `{0}` from an array, `{name}` from a record, `{{`/`}}` literal; non-strings as JSON; inline |
| `core.external` | | | open; never runs (§6.5) |
| `inline.bash` | `code: string`, `cwd: string?`, `check: boolean?` | `stdout, stderr: string`, `code: int` | open; errexit and pipefail; extra inputs as environment variables (`-` → `_`); declared outputs from the JSON object written to `$OUT`; fails on a non-zero exit unless `check: false` |
| `inline.python` | `code: string`, `cwd: string?` | `value: Any?`, `stdout: string` | open; standard library; sees `inp` and each extra input; `out` is the result |
| `message.ask` | `to`, `body`, `title?`, `ui?`, `input?`, `data: Any?`, `wait: boolean?` | `id: int`, `receipt: Any`, `reply: Any?` | §8; `wait` blocks until answered |
| `message.say` | `to`, `body`, `data: Any?` | `id: int`, `receipt: Any` | §8 |
| `message.reply` | `to_message: int`, `body?`, `answer?` | `id: int`, `receipt: Any` | §8 |
| `message.wait` | `thread`, `since: int?`, `to?`, `timeout: int?` (300), `wake?` | `messages: Any[]`, `last_seq: int` | waits for messages on a thread after `since`; `wake: "questions"` holds the rest |
| `message.post` | `body`, `thread?`, `to?`, `needs_reply?`, `reply_to?`, `answer?`, `title?`, `ui?`, `input?`, `data?`, `from?`, `wait?` | `id: int`, `reply: Any?` | retired; plans that name it run through the `message_post` translation (§8), only in a run |
| `agent.claude` | `cwd`, `prompt`, `model: Any?` (Opus, default `{"type":"normal","model":"opus","effort":"high"}`), `session?`, `listen?` | `result`, `model`, `session`, `git` | open |
| `agent.codex` | `cwd`, `spec`, `model: Any?` (default `{"type":"normal","model":"sol","effort":"high"}`), `log?`, `session?`, `report_path?`, `listen?` | `log`, `final`, `model`, `report?`, `session`, `git` | open |
| `agent.devin` | `cwd`, `spec`, `model: Any?` (default `{"type":"normal","model":"swe-2","effort":"high"}`; or a fusion), `log?`, `session?`, `report_path?`, `listen?` | `log`, `final`, `model`, `report?`, `session`, `git` | open |
| `agent.review` | `cwd`, `base`, `standards`, `notes?`, `session?`, `listen?` | `summary`, `sha`, `commits: int`, `session`, `git` | open; reviews and fixes a branch diff with Claude |
| `agent.run` | `engine` (`devin`, `codex`, `claude`), `cwd`, `spec`, `model: Any?`, `session?`, `report_path?`, `listen?` | `final`, `model`, `report?`, `session`, `git` | open |
| `decide.llm` | `question`, `context: Any?`, `options: string[]`, `threshold: float?` | `choice`, `p: float`, `confident: boolean` | 2 retries, 30 s apart |
| `git.head` | `path` | `branch`, `sha` | |
| `git.merge` | `repo`, `source`, `target`, `message?`, `push: boolean?` | `merged: boolean`, `sha?`, `conflicts: string[]` | in a temporary worktree; conflicts are data |
| `git.push` | `path`, `branch`, `remote?`, `force_with_lease: boolean?` | `sha` | |
| `git.rebase` | `path`, `onto` | `ok: boolean`, `sha`, `conflicts: string[]` | conflicts abort and are data |
| `git.worktree` | `repo`, `base`, `branch`, `path?` | `path`, `branch`, `sha` | |
| `git.worktree_rm` | `repo`, `path`, `force: boolean?` | `removed: boolean` | |
| `gh.pr` | `path`, `base`, `head`, `title`, `body`, `draft: boolean?` | `number: int`, `url` | creates or updates the open PR |
| `gh.pr_wait` | `path`, `pr`, `until` (`checks`, `merged`), `interval: int?`, `timeout: int?` | `state` (`green`, `red`, `conflicting`, `merged`, `closed`, `timeout`), `sha`, `url`, `failed: string[]` | 3 retries, 30 s apart |
| `gh.run_cancel` | `path`, `run_id: int` | `cancelled: boolean` | |
| `gh.run_latest` | `path`, `branch?`, `workflow?` | `run_id: int`, `sha`, `status`, `conclusion?`, `url`, `workflow`, `failed_jobs: string[]` | |
| `jev.ask` | `state: Any`, `questions: Any`, `model?` | `answers: Any`, `model`, `usage: Any` | TypeSafe System One; needs `TYPESAFE_API_KEY`; 3 retries, 5 s apart (all `jev.*`) |
| `jev.choice` | `state`, `instructions`, `options`, `min_confidence: float?`, `model?` | `choice`, `probabilities`, `confidence: float`, `confident: boolean`, `model` | |
| `jev.score` | `state`, `instructions`, `levels: Any[]`, `model?` | `score: float`, `probabilities`, `confidence: float`, `legend`, `model` | |
| `jev.noul` | `state`, `instructions`, `yes?`, `no?`, `model?` | `noul: float`, `model` | |

Unmarked inputs and outputs are `string`; `?` marks optional ones. Unless noted, a builtin does
not retry. Builtin icons: a spark for the agent fns, a review mark, a fork for `decide.llm`, a
branch for `git.*`, a PR mark for `gh.*`, an arrow leaving a box for `core.external`, a bubble
for `message.ask`, `message.say`, `message.reply` and `message.post`, and an envelope for
`message.wait`.
