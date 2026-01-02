mod branch;
mod commit;
mod merge;
mod node;
mod object;

pub use branch::Branch;
pub use commit::{Commit, CommitId};
pub use merge::{LastWriterWins, MergeStrategy};
pub use node::{generate_object_id, LocalNode};
pub use object::Object;
