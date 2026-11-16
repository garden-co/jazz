//! `stage-plan`: StagePlan from the command line, as a **backend client** of
//! the app's TypeScript main backend (`createJazz`), proving who it is with
//! a machine credential (an OIDC token the main backend trusts with
//! `trustWorkload`).
//!
//! ```bash
//! export JAZZ_SERVER_URL=https://stage-plan.example/jazz   # the main backend's /jazz
//! export JAZZ_APP_ID=…                                     # the main backend's jazz.appId
//! export JAZZ_TOKEN_COMMAND='gh-oidc-token jazz:stage-plan' # or JAZZ_TOKEN / JAZZ_TOKEN_FILE
//! stage-plan show "Spring tour" --venue "The Roundhouse" --date 2026-05-01 --doors 19:00
//! stage-plan tasks <show-id>
//! stage-plan add <show-id> "Book the van" [--status todo]
//! stage-plan move <task-id> doing
//! stage-plan gate                       # write, then read back, one JSON line per step
//! ```
//!
//! `STAGE_PLAN_MODE` is `scoped` (the default): the main backend trusts the
//! token as one user (`trustWorkload({ as: (claims) => ({ userId }) })`),
//! and the client reads and writes as that user, under StagePlan's
//! permissions.

use std::process::ExitCode;

use std::time::Duration;

use ::jazz_app::{
    AppId, BackendClientKind, CLOSE_TIMEOUT, Config, DurabilityTier, Jazz, ObjectId, ReadTier,
    Storage, TokenReason,
};
use serde_json::{Value, json};
use stage_plan_rust_cli::{
    ShowDetails, add_task, ensure_show, jazz_app, move_task, parse_status, tasks_of, tasks_of_with,
};

const USAGE: &str = "usage:
  stage-plan show <name> [--venue V] [--date D] [--doors T]
  stage-plan tasks <show-id>
  stage-plan add <show-id> <title> [--status todo|doing|done|blocked]
  stage-plan move <task-id> <todo|doing|done|blocked>
  stage-plan gate

env:
  JAZZ_SERVER_URL   the main backend's Jazz endpoint, e.g. http://localhost:3000/jazz
  JAZZ_APP_ID       the main backend's app id (createJazz's jazz.appId)
  JAZZ_TOKEN        the machine credential (an OIDC token), or
  JAZZ_TOKEN_FILE   a file holding it (read again whenever a token is needed), or
  JAZZ_TOKEN_COMMAND  a shell command printing it (run whenever a token is needed)
  STAGE_PLAN_MODE   scoped (default) or trusted
  STAGE_PLAN_DATA_DIR  keep the client's store here (default: in memory)";

type Error = Box<dyn std::error::Error + Send + Sync>;

fn env(name: &str) -> Result<String, Error> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(format!("set {name}").into()),
    }
}

/// `--name value` from `args`, removing both.
fn option(args: &mut Vec<String>, name: &str) -> Option<String> {
    let at = args.iter().position(|arg| arg == name)?;
    args.remove(at);
    (at < args.len()).then(|| args.remove(at))
}

fn id(text: &str) -> Result<ObjectId, Error> {
    uuid::Uuid::parse_str(text)
        .map(ObjectId::from_uuid)
        .map_err(|_| format!("`{text}` is not an id").into())
}

fn status(text: &str) -> Result<jazz_app::TaskStatus, Error> {
    parse_status(text)
        .ok_or_else(|| format!("`{text}` is not a stage: todo, doing, done or blocked").into())
}

/// Where the machine credential comes from. It is looked up again for every
/// token the client asks for, so a short-lived token that a file or command
/// renews keeps working.
#[derive(Clone, Debug)]
enum TokenSource {
    Literal(String),
    File(std::path::PathBuf),
    Command(String),
}

impl TokenSource {
    fn from_env() -> Result<Self, Error> {
        if let Ok(token) = env("JAZZ_TOKEN") {
            return Ok(Self::Literal(token.trim().to_owned()));
        }
        if let Ok(path) = env("JAZZ_TOKEN_FILE") {
            return Ok(Self::File(path.into()));
        }
        if let Ok(command) = env("JAZZ_TOKEN_COMMAND") {
            return Ok(Self::Command(command));
        }
        Err(
            "set JAZZ_TOKEN, JAZZ_TOKEN_FILE or JAZZ_TOKEN_COMMAND to the machine credential"
                .into(),
        )
    }

    async fn token(&self) -> Result<String, String> {
        let token = match self {
            Self::Literal(token) => token.clone(),
            Self::File(path) => tokio::fs::read_to_string(path)
                .await
                .map_err(|error| format!("reading JAZZ_TOKEN_FILE {}: {error}", path.display()))?,
            Self::Command(command) => {
                let output = tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(command)
                    .stderr(std::process::Stdio::inherit())
                    .output()
                    .await
                    .map_err(|error| format!("running JAZZ_TOKEN_COMMAND: {error}"))?;
                if !output.status.success() {
                    return Err(format!("JAZZ_TOKEN_COMMAND failed: {}", output.status));
                }
                String::from_utf8(output.stdout)
                    .map_err(|_| "JAZZ_TOKEN_COMMAND printed no text".to_owned())?
            }
        };
        let token = token.trim().to_owned();
        if token.is_empty() {
            return Err("the machine credential is empty".into());
        }
        Ok(token)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Scoped,
    Trusted,
}

impl Mode {
    fn from_env() -> Result<Self, Error> {
        match std::env::var("STAGE_PLAN_MODE").as_deref() {
            Err(_) | Ok("") | Ok("scoped") => Ok(Self::Scoped),
            Ok("trusted") => Ok(Self::Trusted),
            Ok(other) => Err(format!("STAGE_PLAN_MODE is `{other}`: scoped or trusted").into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Scoped => "scoped",
            Self::Trusted => "trusted",
        }
    }
}

/// What every client of this run opens with.
#[derive(Clone, Debug)]
struct Target {
    server: String,
    app_id: AppId,
    credential: TokenSource,
    mode: Mode,
    data_dir: Option<std::path::PathBuf>,
}

impl Target {
    fn from_env() -> Result<Self, Error> {
        let app_id = env("JAZZ_APP_ID")?;
        Ok(Self {
            server: env("JAZZ_SERVER_URL")?,
            app_id: AppId::from_string(&app_id).map_err(|_| {
                format!("JAZZ_APP_ID `{app_id}` is not an app id (createJazz's jazz.appId)")
            })?,
            credential: TokenSource::from_env()?,
            mode: Mode::from_env()?,
            data_dir: std::env::var_os("STAGE_PLAN_DATA_DIR").map(Into::into),
        })
    }

    /// Opens a client against the main backend, with the machine
    /// credential. The main backend decides what it is trusted as; this
    /// checks that it is what `STAGE_PLAN_MODE` expects.
    async fn open(&self, storage: Storage) -> Result<Jazz, Error> {
        let credential = self.credential.clone();
        let provider = move |_reason: TokenReason| {
            let credential = credential.clone();
            async move {
                credential
                    .token()
                    .await
                    .map(Some)
                    .map_err(::jazz_app::Error::Config)
            }
        };
        let mut config = Config::new(self.app_id)
            .server(self.server.clone())
            .storage(storage)
            .credential(provider);
        // A test main backend on another host of a private network may serve
        // plain http: say so explicitly. localhost needs nothing.
        if std::env::var("JAZZ_ALLOW_INSECURE_HTTP").as_deref() == Ok("1") {
            config = config.allow_insecure_http();
        }
        // A backend client checks the users it acts for by the app's
        // permissions, so it opens with them (as TS createJazzClient does).
        let files = ::jazz_app::AppFiles::new(jazz_app::APP, None)
            .with_permissions(include_bytes!("../jazz.permissions.json").as_slice());
        let jazz = Jazz::open(files, config).await?;
        let expected = match self.mode {
            Mode::Scoped => BackendClientKind::Scoped,
            Mode::Trusted => BackendClientKind::Trusted,
        };
        if jazz.backend_client() != Some(expected) {
            let found = jazz.backend_client();
            let _ = jazz.close(Duration::ZERO).await;
            return Err(format!(
                "STAGE_PLAN_MODE={} but the main backend trusts this credential as {found:?}",
                self.mode.as_str()
            )
            .into());
        }
        Ok(jazz)
    }

    fn storage(&self) -> Storage {
        match &self.data_dir {
            Some(dir) => Storage::Dir(dir.clone()),
            None => Storage::Memory,
        }
    }
}

/// The user the session signed in as: the `userId` the main backend's
/// `trustWorkload` mapped the token to.
fn user_id(jazz: &Jazz) -> Option<String> {
    jazz.session()
        .and_then(|session| session.state().user().map(|user| user.subject.clone()))
}

/// One step of `gate`, printed as one JSON line.
fn emit(step: &str, mode: Mode, ok: bool, mut fields: Value) {
    let line = fields.as_object_mut().expect("fields are an object");
    line.insert("step".into(), json!(step));
    line.insert("mode".into(), json!(mode.as_str()));
    line.insert("ok".into(), json!(ok));
    let mut ordered = serde_json::Map::new();
    for key in ["step", "mode", "ok"] {
        ordered.insert(key.into(), line.remove(key).unwrap());
    }
    ordered.append(line);
    println!("{}", Value::Object(ordered));
}

/// `stage-plan gate`: the backend-client check, as machine-readable steps.
///
/// Scoped (`STAGE_PLAN_MODE=scoped`):
/// - `open`: the client signed in through the main backend as its user.
/// - `write`: the user's show and a task in it, accepted by the main
///   backend under StagePlan's permissions (the writes wait for the server).
/// - `read`: a second, fresh client with the same credential reads the
///   show's tasks from the server and finds the task.
/// - with `STAGE_PLAN_FOREIGN_SHOW`, `foreign_read` and `foreign_write`:
///   the user can't see that show or add to it.
///
/// Trusted (`STAGE_PLAN_MODE=trusted`):
/// - `open`, then `write` and `read` as the system (a show no user owns).
/// - `for_user_write` and `for_user_read` as `STAGE_PLAN_USER_A`: A's
///   private show and a task, read back.
/// - `for_user_read` and `for_user_write` as `STAGE_PLAN_USER_B`: B can't
///   see A's show, nor add a task to it.
///
/// Each step prints `{"step": .., "mode": .., "ok": true|false, ..}`; the
/// first failure ends the run with exit code 1.
async fn gate(target: &Target) -> Result<(), Error> {
    let mode = target.mode;
    let fail = |step: &str, error: &dyn std::fmt::Display| {
        emit(step, mode, false, json!({ "error": error.to_string() }));
        format!("{step} failed: {error}")
    };
    let run = uuid::Uuid::new_v4();
    let details = |name: String| ShowDetails {
        name,
        venue: "The Roundhouse".into(),
        date: "2026-05-01".into(),
        doors: "19:00".into(),
    };

    let writer = target
        .open(Storage::Memory)
        .await
        .map_err(|error| fail("open", &error))?;
    emit(
        "open",
        mode,
        true,
        json!({
            "kind": mode.as_str(),
            "user": user_id(&writer),
            "account": writer.account().map(|account| account.0.to_string()),
        }),
    );

    // Write, then read back from a fresh client.
    let global = writer.durable(DurabilityTier::GlobalServer);
    let written = async {
        let show = match mode {
            // The user's own show: they are its chief.
            Mode::Scoped => ensure_show(&global, details(format!("Gate show {run}"))).await?,
            // The system's: a show whose chief is no one's account.
            Mode::Trusted => {
                let d = details(format!("Gate system show {run}"));
                let nobody = ObjectId::from_uuid(uuid::Uuid::new_v4());
                global
                    .insert(jazz_app::NewShow::new(
                        d.name, d.venue, d.date, d.doors, nobody,
                    ))
                    .await?
            }
        };
        let task = add_task(
            &global,
            show.id,
            format!("Gate task {run}"),
            jazz_app::TaskStatus::Todo,
        )
        .await?;
        Ok::<_, stage_plan_rust_cli::Error>((show, task))
    }
    .await;
    let (show, task) = written.map_err(|error| fail("write", &error))?;
    emit(
        "write",
        mode,
        true,
        json!({
            "user": user_id(&writer),
            "show": show.id.uuid().to_string(),
            "task": task.id.uuid().to_string(),
            "title": task.title,
        }),
    );
    writer
        .close(CLOSE_TIMEOUT)
        .await
        .map_err(|error| fail("write", &error))?;

    let reader = target
        .open(Storage::Memory)
        .await
        .map_err(|error| fail("read", &error))?;
    read_back(&reader, mode, "read", user_id(&reader), show.id, task.id).await?;

    match mode {
        Mode::Scoped => {
            // Someone else's show: the user may neither see it nor add to it.
            if let Ok(foreign) = env("STAGE_PLAN_FOREIGN_SHOW") {
                let foreign = id(&foreign).map_err(|error| fail("foreign_read", &error))?;
                kept_out(&reader, mode, foreign, user_id(&reader)).await?;
            }
        }
        Mode::Trusted => {
            let user_a =
                env("STAGE_PLAN_USER_A").map_err(|error| fail("for_user_write", &error))?;
            let user_b = env("STAGE_PLAN_USER_B").map_err(|error| fail("for_user_read", &error))?;
            let fail_for = |step: &str, user: &str, error: &dyn std::fmt::Display| {
                emit(
                    step,
                    mode,
                    false,
                    json!({ "user": user, "error": error.to_string() }),
                );
                format!("{step} as {user} failed: {error}")
            };

            // A's private show, written and read back as A.
            let as_a = reader
                .for_user(&user_a, Value::Null)
                .await
                .map_err(|error| fail_for("for_user_write", &user_a, &error))?;
            let global_a = as_a.durable(DurabilityTier::GlobalServer);
            let written = async {
                let show =
                    ensure_show(&global_a, details(format!("{user_a}'s private show {run}")))
                        .await?;
                let task = add_task(
                    &global_a,
                    show.id,
                    "Secret setlist",
                    jazz_app::TaskStatus::Todo,
                )
                .await?;
                Ok::<_, stage_plan_rust_cli::Error>((show, task))
            }
            .await;
            let (a_show, a_task) =
                written.map_err(|error| fail_for("for_user_write", &user_a, &error))?;
            emit(
                "for_user_write",
                mode,
                true,
                json!({
                    "user": user_a,
                    "account": a_show.chief_account.uuid().to_string(),
                    "show": a_show.id.uuid().to_string(),
                    "task": a_task.id.uuid().to_string(),
                }),
            );
            read_back(
                &as_a,
                mode,
                "for_user_read",
                Some(user_a.clone()),
                a_show.id,
                a_task.id,
            )
            .await?;

            // B can neither see A's show nor add to it.
            let as_b = reader
                .for_user(&user_b, Value::Null)
                .await
                .map_err(|error| fail_for("for_user_read", &user_b, &error))?;
            kept_out(&as_b, mode, a_show.id, Some(user_b)).await?;
        }
    }
    reader
        .close(CLOSE_TIMEOUT)
        .await
        .map_err(|error| fail("read", &error))?;
    Ok(())
}

/// `step`: `jazz` reads `show`'s tasks from the server and finds `task`.
async fn read_back(
    jazz: &Jazz,
    mode: Mode,
    step: &str,
    user: Option<String>,
    show: ObjectId,
    task: ObjectId,
) -> Result<(), Error> {
    let tasks = match tasks_of_with(jazz, show, ReadTier::Remote).await {
        Ok(tasks) => tasks,
        Err(error) => {
            emit(
                step,
                mode,
                false,
                json!({ "user": user, "error": error.to_string() }),
            );
            return Err(format!("{step} failed: {error}").into());
        }
    };
    let found = tasks.iter().any(|t| t.id == task);
    let mut fields = json!({
        "user": user,
        "show": show.uuid().to_string(),
        "task": task.uuid().to_string(),
        "tasks": tasks.iter().map(|t| t.id.uuid().to_string()).collect::<Vec<_>>(),
        "visible": found,
    });
    if !found {
        // What the client has locally, to tell a server-side refusal from a
        // write that never left.
        let local = tasks_of_with(jazz, show, ReadTier::LocalOnly)
            .await
            .map(|tasks| {
                tasks
                    .iter()
                    .map(|t| t.id.uuid().to_string())
                    .collect::<Vec<_>>()
            })
            .map_err(|error| error.to_string());
        fields["local_tasks"] = json!(local);
    }
    emit(step, mode, found, fields);
    if !found {
        return Err(format!(
            "{step} failed: task {} not among show {}'s tasks",
            task.uuid(),
            show.uuid()
        )
        .into());
    }
    Ok(())
}

/// `foreign_read` and `foreign_write` (or `for_user_read` and
/// `for_user_write`, for a trusted client acting for a user): `jazz` must
/// not see `show` or its tasks, and the server must refuse a task added to
/// it. `ok` is true when the user was kept out.
async fn kept_out(
    jazz: &Jazz,
    mode: Mode,
    show: ObjectId,
    user: Option<String>,
) -> Result<(), Error> {
    let (read_step, write_step) = match mode {
        Mode::Scoped => ("foreign_read", "foreign_write"),
        Mode::Trusted => ("for_user_read", "for_user_write"),
    };
    let fail = |step: &str, error: &dyn std::fmt::Display| {
        emit(
            step,
            mode,
            false,
            json!({ "user": user, "error": error.to_string() }),
        );
        format!("{step} failed: {error}")
    };
    let seen = jazz
        .all_with(jazz_app::shows::query().id(show), ReadTier::Remote)
        .await
        .map_err(|error| fail(read_step, &error))?;
    let tasks = tasks_of_with(jazz, show, ReadTier::Remote)
        .await
        .map_err(|error| fail(read_step, &error))?;
    let visible = !seen.is_empty() || !tasks.is_empty();
    emit(
        read_step,
        mode,
        !visible,
        json!({
            "user": user,
            "show": show.uuid().to_string(),
            "tasks": tasks.iter().map(|t| t.id.uuid().to_string()).collect::<Vec<_>>(),
            "visible": visible,
        }),
    );
    if visible {
        return Err(format!("{read_step} failed: {} is visible", show.uuid()).into());
    }

    let global = jazz.durable(DurabilityTier::GlobalServer);
    match add_task(&global, show, "Not yours", jazz_app::TaskStatus::Todo).await {
        Ok(task) => {
            emit(
                write_step,
                mode,
                false,
                json!({
                    "user": user,
                    "show": show.uuid().to_string(),
                    "refused": false,
                    "task": task.id.uuid().to_string(),
                }),
            );
            Err(format!(
                "{write_step} failed: the server accepted a task in {}",
                show.uuid()
            )
            .into())
        }
        Err(error) => {
            emit(
                write_step,
                mode,
                true,
                json!({
                    "user": user,
                    "show": show.uuid().to_string(),
                    "refused": true,
                    "error": error.to_string(),
                }),
            );
            Ok(())
        }
    }
}

async fn run(mut args: Vec<String>) -> Result<(), Error> {
    let command = if args.is_empty() {
        String::new()
    } else {
        args.remove(0)
    };
    if !matches!(command.as_str(), "show" | "tasks" | "add" | "move" | "gate") {
        return Err(USAGE.into());
    }
    let target = Target::from_env()?;
    if command == "gate" {
        return gate(&target).await;
    }
    let jazz = target.open(target.storage()).await?;
    let global = jazz.durable(DurabilityTier::GlobalServer);
    let done: Result<(), Error> = async {
        match command.as_str() {
            "show" => {
                let venue = option(&mut args, "--venue").unwrap_or_default();
                let date = option(&mut args, "--date").unwrap_or_default();
                let doors = option(&mut args, "--doors").unwrap_or_default();
                let [name] = args.as_slice() else {
                    return Err(USAGE.into());
                };
                let show = ensure_show(
                    &global,
                    ShowDetails {
                        name: name.clone(),
                        venue,
                        date,
                        doors,
                    },
                )
                .await?;
                println!("{}\t{}", show.id.uuid(), show.name);
            }
            "tasks" => {
                let [show] = args.as_slice() else {
                    return Err(USAGE.into());
                };
                for task in tasks_of(&jazz, id(show)?).await? {
                    println!(
                        "{}\t{}\t{}\t{}",
                        task.id.uuid(),
                        task.status.as_str(),
                        task.rank,
                        task.title
                    );
                }
            }
            "add" => {
                let stage = option(&mut args, "--status")
                    .map_or(Ok(jazz_app::TaskStatus::Todo), |s| status(&s))?;
                let [show, title] = args.as_slice() else {
                    return Err(USAGE.into());
                };
                let task = add_task(&global, id(show)?, title.clone(), stage).await?;
                println!(
                    "{}\t{}\t{}",
                    task.id.uuid(),
                    task.status.as_str(),
                    task.rank
                );
            }
            "move" => {
                let [task, stage] = args.as_slice() else {
                    return Err(USAGE.into());
                };
                let task = move_task(&global, id(task)?, status(stage)?).await?;
                println!(
                    "{}\t{}\t{}",
                    task.id.uuid(),
                    task.status.as_str(),
                    task.rank
                );
            }
            _ => unreachable!(),
        }
        Ok(())
    }
    .await;
    // Let the writes made reach the main backend, and the store close.
    jazz.close(CLOSE_TIMEOUT).await?;
    done
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(std::env::args().skip(1).collect()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
