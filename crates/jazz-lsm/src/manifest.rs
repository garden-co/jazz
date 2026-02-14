use serde::{Deserialize, Serialize};

pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 1;
pub(crate) const MANIFEST_CHECKPOINT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) format_version: u32,
    pub(crate) next_file_id: u64,
    pub(crate) next_seq: u64,
    pub(crate) levels: Vec<Vec<SstMeta>>,
    pub(crate) required_merge_ops: Vec<u32>,
}

impl Manifest {
    pub(crate) fn new(num_levels: usize) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            next_file_id: 1,
            next_seq: 1,
            levels: vec![Vec::new(); num_levels],
            required_merge_ops: Vec::new(),
        }
    }

    pub(crate) fn apply_edit(&mut self, edit: &ManifestEdit) {
        self.next_file_id = edit.next_file_id;
        self.next_seq = edit.next_seq;
        self.required_merge_ops = edit.required_merge_ops.clone();

        for removal in &edit.removals {
            if removal.level >= self.levels.len() {
                continue;
            }
            self.levels[removal.level].retain(|meta| meta.id != removal.id);
        }

        for addition in &edit.additions {
            if addition.level >= self.levels.len() {
                self.levels.resize_with(addition.level + 1, Vec::new);
            }
            self.levels[addition.level].push(addition.clone());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ManifestCheckpoint {
    pub(crate) format_version: u32,
    pub(crate) last_edit_id: u64,
    pub(crate) manifest: Manifest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ManifestEdit {
    pub(crate) id: u64,
    pub(crate) next_file_id: u64,
    pub(crate) next_seq: u64,
    pub(crate) required_merge_ops: Vec<u32>,
    pub(crate) additions: Vec<SstMeta>,
    pub(crate) removals: Vec<ManifestRemoval>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ManifestRemoval {
    pub(crate) level: usize,
    pub(crate) id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SstMeta {
    pub(crate) id: u64,
    pub(crate) level: usize,
    pub(crate) path: String,
    pub(crate) min_key: Vec<u8>,
    pub(crate) max_key: Vec<u8>,
    pub(crate) bytes: u64,
    pub(crate) records: u64,
}
