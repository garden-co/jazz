//! `stage-plan`: StagePlan from the command line, as the app's backend
//! acting as one signed-in user.
//!
//! ```bash
//! export JAZZ_APP_ID=… JAZZ_SERVER_URL=http://localhost:1625
//! export JAZZ_ADMIN_SECRET=… JAZZ_BACKEND_SECRET=…
//! export STAGE_PLAN_USER=alice          # the user it acts as
//! stage-plan show "Spring tour" --venue "The Roundhouse" --date 2026-05-01 --doors 19:00
//! stage-plan tasks <show-id>
//! stage-plan add <show-id> "Book the van" [--status todo]
//! stage-plan move <task-id> doing
//! ```

use ::jazz_app::{AppId, Backend, Config, Jazz, ObjectId, Storage};
use stage_plan_rust_cli::{
    ShowDetails, add_task, ensure_show, jazz_app, move_task, parse_status, tasks_of,
};

const USAGE: &str = "usage:
  stage-plan show <name> [--venue V] [--date D] [--doors T]
  stage-plan tasks <show-id>
  stage-plan add <show-id> <title> [--status todo|doing|done|blocked]
  stage-plan move <task-id> <todo|doing|done|blocked>";

fn env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("set {name}"))
}

/// `--name value` from `args`, removing both.
fn option(args: &mut Vec<String>, name: &str) -> Option<String> {
    let at = args.iter().position(|arg| arg == name)?;
    args.remove(at);
    (at < args.len()).then(|| args.remove(at))
}

fn id(text: &str) -> Result<ObjectId, String> {
    uuid::Uuid::parse_str(text)
        .map(ObjectId::from_uuid)
        .map_err(|_| format!("`{text}` is not an id"))
}

fn status(text: &str) -> Result<jazz_app::TaskStatus, String> {
    parse_status(text)
        .ok_or_else(|| format!("`{text}` is not a stage: todo, doing, done or blocked"))
}

async fn open() -> Result<Jazz, Box<dyn std::error::Error>> {
    let app_id = env("JAZZ_APP_ID")?;
    let backend = Backend::new(
        env("JAZZ_BACKEND_SECRET")?,
        env("JAZZ_ADMIN_SECRET")?,
        std::env::var("STAGE_PLAN_USER").unwrap_or_else(|_| "stage-plan-cli".into()),
    )
    .permissions(include_bytes!("../jazz.permissions.json").as_slice());
    let data_dir =
        std::env::var("STAGE_PLAN_DATA_DIR").unwrap_or_else(|_| "./stage-plan-data".into());
    let config =
        Config::new(AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id)))
            .server(
                std::env::var("JAZZ_SERVER_URL").unwrap_or_else(|_| "http://localhost:1625".into()),
            )
            .storage(Storage::Dir(data_dir.into()))
            .backend(backend);
    Ok(jazz_app::open(config).await?)
}

async fn run(mut args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let command = if args.is_empty() {
        String::new()
    } else {
        args.remove(0)
    };
    let jazz = open().await?;
    let done = match command.as_str() {
        "show" => {
            let venue = option(&mut args, "--venue").unwrap_or_default();
            let date = option(&mut args, "--date").unwrap_or_default();
            let doors = option(&mut args, "--doors").unwrap_or_default();
            let [name] = args.as_slice() else {
                return Err(USAGE.into());
            };
            let show = ensure_show(
                &jazz,
                ShowDetails {
                    name: name.clone(),
                    venue,
                    date,
                    doors,
                },
            )
            .await?;
            println!("{}\t{}", show.id.uuid(), show.name);
            Ok(())
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
            Ok(())
        }
        "add" => {
            let stage = option(&mut args, "--status")
                .map_or(Ok(jazz_app::TaskStatus::Todo), |s| status(&s))?;
            let [show, title] = args.as_slice() else {
                return Err(USAGE.into());
            };
            let task = add_task(&jazz, id(show)?, title.clone(), stage).await?;
            println!(
                "{}\t{}\t{}",
                task.id.uuid(),
                task.status.as_str(),
                task.rank
            );
            Ok(())
        }
        "move" => {
            let [task, stage] = args.as_slice() else {
                return Err(USAGE.into());
            };
            let task = move_task(&jazz, id(task)?, status(stage)?).await?;
            println!(
                "{}\t{}\t{}",
                task.id.uuid(),
                task.status.as_str(),
                task.rank
            );
            Ok(())
        }
        _ => Err(USAGE.into()),
    };
    // A persistent store: let the writes made finish and the store close.
    jazz.shutdown().await?;
    done
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(std::env::args().skip(1).collect()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
