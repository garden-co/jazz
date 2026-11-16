//! StagePlan from Rust: a slice of the app over the bindings `jazz-tools
//! generate` writes from `jazz.app.json` (`src/jazz_app.rs`).
//!
//! The CLI is a backend client of the app's TypeScript main backend: it
//! signs in with a machine credential the main backend trusts as one user
//! (the `session.user` of `jazz.permissions.json`), creates that user's
//! show, lists the show's tasks by rank, adds tasks and moves them between
//! stages.

// The generated bindings, committed. `jazz-tools generate --check` (and the
// `bindings_are_current` test) refuse a stale copy.
pub mod jazz_app;

use ::jazz_app_support::{Jazz, ObjectId, Order, ReadTier};

use crate::jazz_app::{NewShow, NewTask, Show, Task, TaskStatus, TaskUpdate, shows, tasks};

/// Errors of the slice: the client's, or a request it can't carry out.
#[derive(Debug)]
pub enum Error {
    Jazz(::jazz_app_support::Error),
    /// The client acts as no user (a trusted client outside `for_user`).
    NoUser,
    NotFound(&'static str, ObjectId),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Jazz(error) => write!(f, "{error}"),
            Error::NoUser => f.write_str("the client acts as no user"),
            Error::NotFound(what, id) => write!(f, "no {what} {}", id.uuid()),
        }
    }
}

impl std::error::Error for Error {}

impl From<::jazz_app_support::Error> for Error {
    fn from(error: ::jazz_app_support::Error) -> Self {
        Error::Jazz(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// The signed-in user: `session.user` in `jazz.permissions.json`.
pub fn me(jazz: &Jazz) -> Result<ObjectId> {
    jazz.account()
        .map(|account| ObjectId::from_uuid(account.0))
        .ok_or(Error::NoUser)
}

/// What a new show says about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowDetails {
    pub name: String,
    pub venue: String,
    pub date: String,
    pub doors: String,
}

/// The user's show of this name, created with them as its crew chief
/// (`chiefAccount = session.user`) if they have none yet.
pub async fn ensure_show(jazz: &Jazz, details: ShowDetails) -> Result<Show> {
    let me = me(jazz)?;
    let mine = shows::query()
        .filter(shows::chief_account().eq(me))
        .filter(shows::name().eq(details.name.as_str()));
    if let Some(show) = jazz.one(mine).await? {
        return Ok(show);
    }
    let ShowDetails {
        name,
        venue,
        date,
        doors,
    } = details;
    Ok(jazz
        .insert(NewShow::new(name, venue, date, doors, me))
        .await?)
}

/// A show's tasks, ordered by rank.
pub async fn tasks_of(jazz: &Jazz, show: ObjectId) -> Result<Vec<Task>> {
    tasks_of_with(jazz, show, ReadTier::LocalFirst).await
}

/// A show's tasks, ordered by rank, read at `tier` (`ReadTier::Remote`: as
/// the server has them).
pub async fn tasks_of_with(jazz: &Jazz, show: ObjectId, tier: ReadTier) -> Result<Vec<Task>> {
    Ok(jazz
        .all_with(
            shows::tasks()
                .of(show)
                .order_by(tasks::rank(), Order::Asc)
                .order_by(tasks::id(), Order::Asc),
            tier,
        )
        .await?)
}

/// The rank after every task of a stage (`rankAfter` in the TS app).
async fn rank_after(jazz: &Jazz, show: ObjectId, status: TaskStatus) -> Result<f64> {
    let last = jazz
        .one(
            shows::tasks()
                .of(show)
                .filter(tasks::status().eq(status))
                .order_by(tasks::rank(), Order::Desc),
        )
        .await?;
    Ok(last.map_or(0.0, |task| task.rank) + 1.0)
}

/// Adds a task at the end of its stage.
pub async fn add_task(
    jazz: &Jazz,
    show: ObjectId,
    title: impl Into<String>,
    status: TaskStatus,
) -> Result<Task> {
    let rank = rank_after(jazz, show, status).await?;
    Ok(jazz.insert(NewTask::new(show, title, status, rank)).await?)
}

/// Moves a task to the end of another stage.
pub async fn move_task(jazz: &Jazz, task: ObjectId, status: TaskStatus) -> Result<Task> {
    let current = jazz
        .get::<Task>(task)
        .await?
        .ok_or(Error::NotFound("task", task))?;
    if current.status == status {
        return Ok(current);
    }
    let rank = rank_after(jazz, current.show_id, status).await?;
    jazz.update(
        task,
        TaskUpdate {
            status: Some(status),
            rank: Some(rank),
            ..Default::default()
        },
    )
    .await?;
    jazz.get::<Task>(task)
        .await?
        .ok_or(Error::NotFound("task", task))
}

/// A stage by the name `jazz.app.json` gives it.
pub fn parse_status(name: &str) -> Option<TaskStatus> {
    [
        TaskStatus::Todo,
        TaskStatus::Doing,
        TaskStatus::Done,
        TaskStatus::Blocked,
    ]
    .into_iter()
    .find(|status| status.as_str() == name)
}
