mod db;
mod error;
mod file;
mod page;
mod superblock;
#[cfg(target_arch = "wasm32")]
pub mod wasm_bench;

pub use db::{BTreeOptions, OpfsBTree, OpfsBTreeFiles};
pub use error::BTreeError;
#[cfg(target_arch = "wasm32")]
pub use file::OpfsFile;
#[cfg(not(target_arch = "wasm32"))]
pub use file::StdFile;
pub use file::{MemoryFile, SyncFile};
