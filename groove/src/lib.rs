use std::collections::{BTreeMap, HashMap};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

struct CommitID(u128); // hash of commit

struct Commit {
    parents: Vec<CommitID>,
    content: Box<[u8]>,
    meta: Option<BTreeMap<String, String>>,
}

struct Branch {
    name: Option<String>,
    commits: HashMap<CommitID, Commit>,
    commit_children: HashMap<CommitID, Vec<CommitID>>,
    frontier_indices: Vec<CommitID>
}

struct Object {
    id: u128, // uuidv7
    prefix: String,
    branches: Vec<Branch>,
    meta: Option<BTreeMap<String, String>>,
}


struct LocalNode {
    objects: BTreeMap<u128, Object>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
