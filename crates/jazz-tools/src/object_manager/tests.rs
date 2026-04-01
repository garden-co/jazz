use super::*;
use crate::commit::CommitAckState;
use crate::storage::MemoryStorage;

#[test]
fn create_object_without_metadata() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let id = manager.create(&mut io, None);

    let object = manager.get(id).expect("object should exist");
    assert_eq!(object.id, id);
    assert!(object.metadata.is_empty());
    assert!(object.branches.is_empty());
}

#[test]
fn create_object_with_metadata() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let mut metadata = HashMap::new();
    metadata.insert("name".to_string(), "test".to_string());

    let id = manager.create(&mut io, Some(metadata));

    let object = manager.get(id).expect("object should exist");
    assert_eq!(object.metadata.get("name"), Some(&"test".to_string()));
}

#[test]
fn get_nonexistent_object_returns_none() {
    let manager = ObjectManager::new();
    let fake_id = ObjectId::new();

    assert!(manager.get(fake_id).is_none());
}

// --- add_commit tests ---

#[test]
fn add_commit_rejects_unknown_object() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let fake_object_id = ObjectId::new();
    let author = ObjectId::new();

    let result = manager.add_commit(
        &mut io,
        fake_object_id,
        "main",
        vec![],
        b"content".to_vec(),
        author,
        None,
    );

    assert_eq!(result, Err(Error::ObjectNotFound(fake_object_id)));
}

#[test]
fn add_commit_creates_branch_for_parentless_commit() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let commit_id = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .expect("should succeed");

    let object = manager.get(object_id).unwrap();
    assert!(object.branches.contains_key(&BranchName::new("main")));

    let branch = &object.branches[&BranchName::new("main")];
    assert!(branch.commits.contains_key(&commit_id));
}

#[test]
fn add_commit_rejects_unknown_branch_with_parents() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();
    let fake_parent = CommitId([0u8; 32]);

    let result = manager.add_commit(
        &mut io,
        object_id,
        "nonexistent",
        vec![fake_parent],
        b"content".to_vec(),
        author,
        None,
    );

    assert_eq!(
        result,
        Err(Error::BranchNotFound(BranchName::new("nonexistent")))
    );
}

#[test]
fn add_commit_rejects_unknown_parent() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    // Create branch with initial commit
    manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let fake_parent = CommitId([0u8; 32]);
    let result = manager.add_commit(
        &mut io,
        object_id,
        "main",
        vec![fake_parent],
        b"child".to_vec(),
        author,
        None,
    );

    assert_eq!(result, Err(Error::ParentNotFound(fake_parent)));
}

#[test]
fn add_commit_with_valid_parent_succeeds() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let parent_id = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let child_id = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![parent_id],
            b"child".to_vec(),
            author,
            None,
        )
        .expect("should succeed");

    let commits = manager.get_commits(object_id, "main").unwrap();
    assert!(commits.contains_key(&child_id));
    assert_eq!(commits[&child_id].parents.as_slice(), &[parent_id]);
}

// --- tips management tests ---

#[test]
fn parentless_commit_becomes_tip() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let commit_id = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tip_ids.len(), 1);
    assert!(tip_ids.contains(&commit_id));
}

#[test]
fn child_commit_replaces_parent_in_tips() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let parent_id = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let child_id = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![parent_id],
            b"child".to_vec(),
            author,
            None,
        )
        .unwrap();

    let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tip_ids.len(), 1);
    assert!(!tip_ids.contains(&parent_id));
    assert!(tip_ids.contains(&child_id));
}

#[test]
fn diverging_twigs_create_multiple_tips() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            author,
            None,
        )
        .unwrap();

    let twig_a = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"twig_a".to_vec(),
            author,
            None,
        )
        .unwrap();

    let twig_b = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"twig_b".to_vec(),
            author,
            None,
        )
        .unwrap();

    let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tip_ids.len(), 2);
    assert!(tip_ids.contains(&twig_a));
    assert!(tip_ids.contains(&twig_b));
}

#[test]
fn merge_commit_consolidates_tips() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            author,
            None,
        )
        .unwrap();

    let twig_a = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"twig_a".to_vec(),
            author,
            None,
        )
        .unwrap();

    let twig_b = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"twig_b".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Merge both twigs
    let merge = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![twig_a, twig_b],
            b"merge".to_vec(),
            author,
            None,
        )
        .unwrap();

    let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tip_ids.len(), 1);
    assert!(tip_ids.contains(&merge));
}

#[test]
fn multiple_roots_create_multiple_tips() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let root1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root1".to_vec(),
            author,
            None,
        )
        .unwrap();

    let root2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root2".to_vec(),
            author,
            None,
        )
        .unwrap();

    let tip_ids = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tip_ids.len(), 2);
    assert!(tip_ids.contains(&root1));
    assert!(tip_ids.contains(&root2));
}

// --- getter tests ---

#[test]
fn get_tip_ids_rejects_unknown_object() {
    let manager = ObjectManager::new();
    let fake_id = ObjectId::new();

    let result = manager.get_tip_ids(fake_id, "main");
    assert_eq!(result, Err(Error::ObjectNotFound(fake_id)));
}

#[test]
fn get_tip_ids_rejects_unknown_branch() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);

    let result = manager.get_tip_ids(object_id, "nonexistent");
    assert_eq!(
        result,
        Err(Error::BranchNotFound(BranchName::new("nonexistent")))
    );
}

#[test]
fn get_tips_returns_commit_structs() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let commit_id = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let tips = manager.get_tips(object_id, "main").unwrap();
    assert_eq!(tips.len(), 1);
    assert!(tips.contains_key(&commit_id));
    assert_eq!(tips[&commit_id].content, b"initial".to_vec());
}

#[test]
fn get_commits_returns_all_commits() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let c1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"first".to_vec(),
            author,
            None,
        )
        .unwrap();

    let c2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c1],
            b"second".to_vec(),
            author,
            None,
        )
        .unwrap();

    let c3 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c2],
            b"third".to_vec(),
            author,
            None,
        )
        .unwrap();

    let commits = manager.get_commits(object_id, "main").unwrap();
    assert_eq!(commits.len(), 3);
    assert!(commits.contains_key(&c1));
    assert!(commits.contains_key(&c2));
    assert!(commits.contains_key(&c3));
}

#[test]
fn get_commits_rejects_unknown_object() {
    let manager = ObjectManager::new();
    let fake_id = ObjectId::new();

    let result = manager.get_commits(fake_id, "main");
    assert!(matches!(result, Err(Error::ObjectNotFound(id)) if id == fake_id));
}

#[test]
fn get_commits_rejects_unknown_branch() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);

    let result = manager.get_commits(object_id, "nonexistent");
    assert!(
        matches!(result, Err(Error::BranchNotFound(ref name)) if name.as_str() == "nonexistent")
    );
}

// --- subscription tests ---

#[test]
fn subscribe_to_loaded_branch_gets_immediate_update_with_frontier() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let c1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"first".to_vec(),
            author,
            None,
        )
        .unwrap();
    let c2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c1],
            b"second".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Clear any updates from add_commit (no subscribers yet)
    manager.take_subscription_updates();

    let sub_id = manager.subscribe(object_id, "main");

    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].object_id, object_id);
    assert_eq!(updates[0].branch_name, BranchName::new("main"));
    // Only the current frontier (tip), not all commits
    assert_eq!(updates[0].commit_ids, vec![c2]);
}

// NOTE: subscribe_to_unloaded_branch_triggers_load_request and subscriber_gets_update_on_load_response
// tests are deleted - with sync storage, loading is immediate, no request/response pattern

#[test]
fn add_commit_notifies_subscriber() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let c1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Subscribe after initial commit
    let sub_id = manager.subscribe(object_id, "main");
    manager.take_subscription_updates(); // Clear initial update

    // Add another commit
    let c2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c1],
            b"second".to_vec(),
            author,
            None,
        )
        .unwrap();

    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].commit_ids, vec![c2]);
}

#[test]
fn multiple_subscribers_each_get_updates() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let c1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let sub1 = manager.subscribe(object_id, "main");
    let sub2 = manager.subscribe(object_id, "main");
    manager.take_subscription_updates(); // Clear initial updates

    let c2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c1],
            b"second".to_vec(),
            author,
            None,
        )
        .unwrap();

    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 2);

    let sub_ids: HashSet<_> = updates.iter().map(|u| u.subscription_id).collect();
    assert!(sub_ids.contains(&sub1));
    assert!(sub_ids.contains(&sub2));

    for update in &updates {
        assert_eq!(update.commit_ids, vec![c2]);
    }
}

#[test]
fn unsubscribe_stops_updates() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let c1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let sub_id = manager.subscribe(object_id, "main");
    manager.take_subscription_updates();

    manager.unsubscribe(sub_id);

    // Add a commit after unsubscribing
    manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c1],
            b"second".to_vec(),
            author,
            None,
        )
        .unwrap();

    let updates = manager.take_subscription_updates();
    assert!(updates.is_empty());
}

#[test]
fn unsubscribe_clears_pending_updates() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    let sub_id = manager.subscribe(object_id, "main");
    // Don't take updates yet - they're pending

    manager.unsubscribe(sub_id);

    let updates = manager.take_subscription_updates();
    assert!(updates.is_empty());
}

#[test]
fn subscribe_tips_only_gets_only_tips() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let c1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"first".to_vec(),
            author,
            None,
        )
        .unwrap();
    let c2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c1],
            b"second".to_vec(),
            author,
            None,
        )
        .unwrap();

    let _sub_id = manager.subscribe(object_id, "main");

    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 1);
    // Only the tip commit, not all commits
    assert_eq!(updates[0].commit_ids.len(), 1);
    assert!(updates[0].commit_ids.contains(&c2));
    assert!(!updates[0].commit_ids.contains(&c1));
}

#[test]
fn frontier_evolves_through_diamond_graph() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            author,
            None,
        )
        .unwrap();

    let _sub_id = manager.subscribe(object_id, "main");

    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].commit_ids, vec![root]);

    let a = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"a".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].commit_ids, vec![a]);

    let b = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"b".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].commit_ids.len(), 2);
    assert_eq!(updates[0].commit_ids[0], a);
    assert_eq!(updates[0].commit_ids[1], b);

    let merge = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![a, b],
            b"merge".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].commit_ids, vec![merge]);
}

#[test]
fn subscription_ids_are_unique() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);

    let sub1 = manager.subscribe(object_id, "main");
    let sub2 = manager.subscribe(object_id, "main");
    let sub3 = manager.subscribe(object_id, "other");

    assert_ne!(sub1, sub2);
    assert_ne!(sub2, sub3);
    assert_ne!(sub1, sub3);
}

#[test]
fn frontier_with_extended_divergence() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            author,
            None,
        )
        .unwrap();

    let _sub_id = manager.subscribe(object_id, "main");
    manager.take_subscription_updates();

    let a1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"a1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids, vec![a1]);

    let b1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"b1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 2);
    assert_eq!(updates[0].commit_ids[0], a1);
    assert_eq!(updates[0].commit_ids[1], b1);

    let a2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![a1],
            b"a2".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 2);
    assert_eq!(updates[0].commit_ids[0], b1);
    assert_eq!(updates[0].commit_ids[1], a2);

    let b2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![b1],
            b"b2".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 2);
    assert_eq!(updates[0].commit_ids[0], a2);
    assert_eq!(updates[0].commit_ids[1], b2);

    let a3 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![a2],
            b"a3".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 2);
    assert_eq!(updates[0].commit_ids[0], b2);
    assert_eq!(updates[0].commit_ids[1], a3);

    let merge = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![a3, b2],
            b"merge".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids, vec![merge]);
}

#[test]
fn get_or_load_computes_correct_tips_for_multi_commit_branch() {
    // Simulates cold-start: session 1 creates A→B, session 2 loads from
    // storage and must compute tips correctly (only B, not both A and B).
    //
    //   session 1: A → B       (2 commits, B is only tip)
    //   session 2: get_or_load → add_commit C with parent B
    let mut io = MemoryStorage::new();
    let author = ObjectId::new();

    // --- Session 1: create object with two commits ---
    let object_id = {
        let mut mgr = ObjectManager::new();
        let oid = mgr.create(&mut io, None);
        let a = mgr
            .add_commit(&mut io, oid, "main", vec![], b"A".to_vec(), author, None)
            .unwrap();
        mgr.add_commit(&mut io, oid, "main", vec![a], b"B".to_vec(), author, None)
            .unwrap();
        oid
    };

    // --- Session 2: new ObjectManager, load from storage ---
    let mut mgr2 = ObjectManager::new();
    let loaded = mgr2.get_or_load(object_id, &io, &["main".to_string()]);
    assert!(loaded.is_some(), "object should load from storage");

    // Verify only one tip (B), not two
    let tips = mgr2.get_tip_ids(object_id, "main".to_string()).unwrap();
    assert_eq!(tips.len(), 1, "should have exactly 1 tip after loading A→B");

    // Adding a new commit should succeed — this failed before the fix
    // because both A and B were tips, and add_commit rejected A as
    // ParentNotFound during the tails check.
    let tip = *tips.iter().next().unwrap();
    mgr2.add_commit(
        &mut io,
        object_id,
        "main",
        vec![tip],
        b"C".to_vec(),
        author,
        None,
    )
    .expect("add_commit after cold-start load should succeed");
}

#[test]
fn get_or_load_hydrates_missing_requested_branches_for_cached_objects() {
    let mut io = MemoryStorage::new();
    let author = ObjectId::new();

    let object_id = {
        let mut mgr = ObjectManager::new();
        let oid = mgr.create(&mut io, None);
        mgr.add_commit(&mut io, oid, "main", vec![], b"main".to_vec(), author, None)
            .unwrap();
        mgr.add_commit(
            &mut io,
            oid,
            "draft",
            vec![],
            b"draft".to_vec(),
            author,
            None,
        )
        .unwrap();
        oid
    };

    let mut mgr = ObjectManager::new();
    mgr.get_or_load(object_id, &io, &["main".to_string()]);

    let object = mgr.get(object_id).expect("object should be cached");
    assert!(object.branches.contains_key(&BranchName::new("main")));
    assert!(
        !object.branches.contains_key(&BranchName::new("draft")),
        "setup should start with only the first requested branch cached"
    );

    mgr.get_or_load(object_id, &io, &["draft".to_string()]);

    let object = mgr.get(object_id).expect("object should stay cached");
    assert!(object.branches.contains_key(&BranchName::new("main")));
    assert!(
        object.branches.contains_key(&BranchName::new("draft")),
        "subsequent get_or_load calls should hydrate newly requested branches"
    );
}

#[test]
fn frontier_with_three_way_divergence() {
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            author,
            None,
        )
        .unwrap();

    let _sub_id = manager.subscribe(object_id, "main");
    manager.take_subscription_updates();

    let a1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"a1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids, vec![a1]);

    let b1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"b1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 2);

    let c1 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![root],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 3);
    assert!(updates[0].commit_ids.contains(&a1));
    assert!(updates[0].commit_ids.contains(&b1));
    assert!(updates[0].commit_ids.contains(&c1));
    assert_eq!(updates[0].commit_ids[0], a1);
    assert_eq!(updates[0].commit_ids[1], b1);
    assert_eq!(updates[0].commit_ids[2], c1);

    let a2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![a1],
            b"a2".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 3);
    assert!(updates[0].commit_ids.contains(&b1));
    assert!(updates[0].commit_ids.contains(&c1));
    assert!(updates[0].commit_ids.contains(&a2));

    let c2 = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![c1],
            b"c2".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 3);
    assert!(updates[0].commit_ids.contains(&b1));
    assert!(updates[0].commit_ids.contains(&a2));
    assert!(updates[0].commit_ids.contains(&c2));

    let merge_ab = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![a2, b1],
            b"merge_ab".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids.len(), 2);
    assert!(updates[0].commit_ids.contains(&c2));
    assert!(updates[0].commit_ids.contains(&merge_ab));

    let merge_all = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![merge_ab, c2],
            b"merge_all".to_vec(),
            author,
            None,
        )
        .unwrap();
    let updates = manager.take_subscription_updates();
    assert_eq!(updates[0].commit_ids, vec![merge_all]);
}

// --- history & conflict management tests ---

#[test]
fn lww_selects_highest_timestamp_tip() {
    // Two concurrent edits diverge from the same root. The one with the
    // higher timestamp should be the LWW winner (last in tips_by_timestamp).
    //
    //   root (ts=100)
    //    ├── alice_edit (ts=200)
    //    └── bob_edit   (ts=300)   ← LWW winner
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    // Root commit via add_commit (auto-timestamps)
    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            alice,
            None,
        )
        .unwrap();

    // Inject two diverging commits with controlled timestamps via receive_commit
    let alice_edit = Commit {
        parents: smallvec![root],
        content: b"alice-edit".to_vec(),
        timestamp: 200,
        author: alice.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let bob_edit = Commit {
        parents: smallvec![root],
        content: b"bob-edit".to_vec(),
        timestamp: 300,
        author: bob.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };

    let alice_id = manager
        .receive_commit(&mut io, object_id, "main", alice_edit)
        .unwrap();
    let bob_id = manager
        .receive_commit(&mut io, object_id, "main", bob_edit)
        .unwrap();

    // Both should be tips (diverged frontier)
    let tips = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips.len(), 2);
    assert!(tips.contains(&alice_id));
    assert!(tips.contains(&bob_id));

    // tips_by_timestamp sorts oldest-first → last element is the LWW winner
    let object = manager.get(object_id).unwrap();
    let branch = &object.branches[&BranchName::new("main")];
    let sorted = ObjectManager::tips_by_timestamp(&branch.commits, &branch.tips);
    assert_eq!(
        *sorted.last().unwrap(),
        bob_id,
        "LWW winner should be bob (ts=300 > ts=200)"
    );
}

#[test]
fn lww_deterministic_on_equal_timestamps() {
    // Two commits with identical timestamps. The tie-breaking order is
    // process-deterministic (Rust's stable sort over SmolSet iteration order)
    // but not canonically defined by CommitId. We verify determinism: repeated
    // calls always return the same winner.
    //
    //   root
    //    ├── edit_x (ts=500)
    //    └── edit_y (ts=500)
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let author = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            author,
            None,
        )
        .unwrap();

    let edit_x = Commit {
        parents: smallvec![root],
        content: b"edit-x".to_vec(),
        timestamp: 500,
        author: author.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let edit_y = Commit {
        parents: smallvec![root],
        content: b"edit-y".to_vec(),
        timestamp: 500,
        author: author.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };

    manager
        .receive_commit(&mut io, object_id, "main", edit_x)
        .unwrap();
    manager
        .receive_commit(&mut io, object_id, "main", edit_y)
        .unwrap();

    let tips = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips.len(), 2);

    // Call tips_by_timestamp multiple times — must always return same order
    let object = manager.get(object_id).unwrap();
    let branch = &object.branches[&BranchName::new("main")];
    let first_result = ObjectManager::tips_by_timestamp(&branch.commits, &branch.tips);
    for _ in 0..10 {
        let result = ObjectManager::tips_by_timestamp(&branch.commits, &branch.tips);
        assert_eq!(
            result, first_result,
            "tips_by_timestamp should be deterministic on equal timestamps"
        );
    }
}

#[test]
fn receive_commit_idempotent_during_conflict() {
    // Replaying a commit that already exists should not alter the frontier
    // or emit spurious subscription notifications.
    //
    //   root → alice_edit (tip)
    //   root → bob_edit   (tip)     ← 2 tips = conflict
    //
    //   receive_commit(bob_edit) again → no change, still 2 tips
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            alice,
            None,
        )
        .unwrap();

    let alice_edit = Commit {
        parents: smallvec![root],
        content: b"alice-edit".to_vec(),
        timestamp: 100,
        author: alice.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let bob_edit = Commit {
        parents: smallvec![root],
        content: b"bob-edit".to_vec(),
        timestamp: 200,
        author: bob.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };

    let alice_id = manager
        .receive_commit(&mut io, object_id, "main", alice_edit)
        .unwrap();
    let bob_id = manager
        .receive_commit(&mut io, object_id, "main", bob_edit.clone())
        .unwrap();

    // Subscribe and drain initial updates
    let _sub_id = manager.subscribe(object_id, "main");
    manager.take_subscription_updates();

    // Replay bob's commit — should be a no-op
    let replayed_id = manager
        .receive_commit(&mut io, object_id, "main", bob_edit)
        .unwrap();
    assert_eq!(replayed_id, bob_id, "idempotent: same CommitId on replay");

    // Tips unchanged
    let tips = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips.len(), 2);
    assert!(tips.contains(&alice_id));
    assert!(tips.contains(&bob_id));

    // No spurious subscription notifications
    let updates = manager.take_subscription_updates();
    assert!(
        updates.is_empty(),
        "replaying an existing commit should not notify subscribers"
    );
}

#[test]
fn truncate_with_diverged_tips() {
    // Truncation should work correctly when the branch has multiple tips.
    //
    //   root → a1 → a2 (tip)
    //   root → b1 → b2 (tip)
    //
    //   truncate(tails={a1, b1}) → root deleted, a1/b1 become tails, a2/b2 still tips
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            alice,
            None,
        )
        .unwrap();

    // Alice's chain: root → a1 → a2
    let a1_commit = Commit {
        parents: smallvec![root],
        content: b"a1".to_vec(),
        timestamp: 100,
        author: alice.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let a1 = manager
        .receive_commit(&mut io, object_id, "main", a1_commit)
        .unwrap();

    let a2_commit = Commit {
        parents: smallvec![a1],
        content: b"a2".to_vec(),
        timestamp: 200,
        author: alice.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let a2 = manager
        .receive_commit(&mut io, object_id, "main", a2_commit)
        .unwrap();

    // Bob's chain: root → b1 → b2
    let b1_commit = Commit {
        parents: smallvec![root],
        content: b"b1".to_vec(),
        timestamp: 150,
        author: bob.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let b1 = manager
        .receive_commit(&mut io, object_id, "main", b1_commit)
        .unwrap();

    let b2_commit = Commit {
        parents: smallvec![b1],
        content: b"b2".to_vec(),
        timestamp: 250,
        author: bob.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let b2 = manager
        .receive_commit(&mut io, object_id, "main", b2_commit)
        .unwrap();

    // Verify pre-truncation state: 5 commits, 2 tips
    let commits_before = manager.get_commits(object_id, "main").unwrap();
    assert_eq!(commits_before.len(), 5); // root, a1, a2, b1, b2

    let tips_before = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips_before.len(), 2);
    assert!(tips_before.contains(&a2));
    assert!(tips_before.contains(&b2));

    // Truncate: tails = {a1, b1}
    let mut tail_ids = HashSet::new();
    tail_ids.insert(a1);
    tail_ids.insert(b1);

    let result = manager.truncate_branch(&mut io, object_id, "main", tail_ids);
    assert_eq!(
        result,
        TruncateResult::Success { deleted_commits: 1 },
        "should delete root commit only"
    );

    // Post-truncation: 4 commits remain (a1, a2, b1, b2), root gone
    let commits_after = manager.get_commits(object_id, "main").unwrap();
    assert_eq!(commits_after.len(), 4);
    assert!(!commits_after.contains_key(&root), "root should be deleted");
    assert!(commits_after.contains_key(&a1));
    assert!(commits_after.contains_key(&a2));
    assert!(commits_after.contains_key(&b1));
    assert!(commits_after.contains_key(&b2));

    // Tips unchanged
    let tips_after = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips_after.len(), 2);
    assert!(tips_after.contains(&a2));
    assert!(tips_after.contains(&b2));

    // Tails set correctly
    let object = manager.get(object_id).unwrap();
    let branch = &object.branches[&BranchName::new("main")];
    let tails = branch.tails.as_ref().expect("tails should be set");
    assert!(tails.contains(&a1));
    assert!(tails.contains(&b1));
}

#[test]
fn truncate_rejects_when_tip_not_descendant_of_tail() {
    // Safety invariant: all tips must be descendants of some tail.
    //
    //   root → a (tip)
    //   root → b (tip)
    //
    //   truncate(tails={a}) → error: b is not a descendant of a
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            alice,
            None,
        )
        .unwrap();

    let alice_edit = Commit {
        parents: smallvec![root],
        content: b"alice-edit".to_vec(),
        timestamp: 100,
        author: alice.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let bob_edit = Commit {
        parents: smallvec![root],
        content: b"bob-edit".to_vec(),
        timestamp: 200,
        author: bob.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };

    let alice_id = manager
        .receive_commit(&mut io, object_id, "main", alice_edit)
        .unwrap();
    let bob_id = manager
        .receive_commit(&mut io, object_id, "main", bob_edit)
        .unwrap();

    // Verify 2 tips
    let tips = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips.len(), 2);

    // Truncate with only alice as tail — bob is not a descendant of alice
    let mut tail_ids = HashSet::new();
    tail_ids.insert(alice_id);

    let result = manager.truncate_branch(&mut io, object_id, "main", tail_ids);

    // Should fail: bob_id is a tip but is not a descendant of alice_id
    assert_eq!(
        result,
        TruncateResult::PermanentError(TruncateError::TipBeforeTail(bob_id)),
        "should reject: bob is not a descendant of alice"
    );

    // State unchanged — commits still intact
    let commits = manager.get_commits(object_id, "main").unwrap();
    assert_eq!(commits.len(), 3); // root, alice, bob
}

#[test]
fn lww_offline_edit_wins_when_later() {
    // Real offline scenario: Bob syncs up to alice-v1, goes offline,
    // Alice continues updating (v2..v4). Bob makes 1 edit offline.
    // Bob's edit happens AFTER alice's last update (higher timestamp),
    // so bob wins even though alice has more commits.
    //
    //   root → a1(200) → a2(300) → a3(400) → a4(500)    ← alice online
    //   root → a1(200) → bob-offline(700)                 ← bob offline, edits later
    //
    //   LWW winner: bob-offline (ts=700 > ts=500)
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    // Root
    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"root".to_vec(),
            alice,
            None,
        )
        .unwrap();

    // Alice's chain: root → a1 → a2 → a3 → a4
    let mut parent = root;
    let mut alice_ids = Vec::new();
    for (i, ts) in [(1, 200u64), (2, 300), (3, 400), (4, 500)] {
        let commit = Commit {
            parents: smallvec![parent],
            content: format!("alice-v{i}").into_bytes(),
            timestamp: ts,
            author: alice.to_string(),
            metadata: None,
            stored_state: StoredState::default(),
            ack_state: CommitAckState::default(),
        };
        let id = manager
            .receive_commit(&mut io, object_id, "main", commit)
            .unwrap();
        alice_ids.push(id);
        parent = id;
    }
    let a1 = alice_ids[0];
    let a4 = *alice_ids.last().unwrap();

    // Bob was offline since a1. He edits from a1 at ts=700 (after alice finished).
    let bob_commit = Commit {
        parents: smallvec![a1],
        content: b"bob-offline-edit".to_vec(),
        timestamp: 700,
        author: bob.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let b1 = manager
        .receive_commit(&mut io, object_id, "main", bob_commit)
        .unwrap();

    // Two tips: a4 and b1 (diverged from a1)
    let tips = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips.len(), 2);
    assert!(tips.contains(&a4));
    assert!(tips.contains(&b1));

    // LWW: bob wins because ts=700 > ts=500
    let object = manager.get(object_id).unwrap();
    let branch = &object.branches[&BranchName::new("main")];
    let sorted = ObjectManager::tips_by_timestamp(&branch.commits, &branch.tips);
    assert_eq!(
        *sorted.last().unwrap(),
        b1,
        "bob (ts=700) should be LWW winner over alice-v4 (ts=500)"
    );
    assert_eq!(
        branch.commits[&b1].content, b"bob-offline-edit",
        "winner content should be bob's offline edit"
    );
}

#[test]
fn lww_different_fields_same_object_whole_commit_wins() {
    // Two concurrent edits on different "fields" of the same object.
    // With whole-object LWW, the winner's entire content replaces the loser's.
    // This means one side's field change is lost.
    //
    //   root (content: title="task", completed=false)
    //    ├── alice_edit (ts=200, content: title="alice-title", completed=false)
    //    └── bob_edit   (ts=300, content: title="task", completed=true)
    //
    //   LWW winner: bob_edit (ts=300) → title reverts to "task", completed=true
    //   Alice's title change is lost.
    let mut io = MemoryStorage::new();
    let mut manager = ObjectManager::new();
    let object_id = manager.create(&mut io, None);
    let alice = ObjectId::new();
    let bob = ObjectId::new();

    let root = manager
        .add_commit(
            &mut io,
            object_id,
            "main",
            vec![],
            b"title=task,completed=false".to_vec(),
            alice,
            None,
        )
        .unwrap();

    // Alice edits title only (her snapshot has completed=false)
    let alice_edit = Commit {
        parents: smallvec![root],
        content: b"title=alice-title,completed=false".to_vec(),
        timestamp: 200,
        author: alice.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let _alice_id = manager
        .receive_commit(&mut io, object_id, "main", alice_edit)
        .unwrap();

    // Bob edits completed only (his snapshot has title=task)
    let bob_edit = Commit {
        parents: smallvec![root],
        content: b"title=task,completed=true".to_vec(),
        timestamp: 300,
        author: bob.to_string(),
        metadata: None,
        stored_state: StoredState::default(),
        ack_state: CommitAckState::default(),
    };
    let bob_id = manager
        .receive_commit(&mut io, object_id, "main", bob_edit)
        .unwrap();

    // Two tips
    let tips = manager.get_tip_ids(object_id, "main").unwrap();
    assert_eq!(tips.len(), 2);

    // LWW: bob wins (ts=300 > ts=200)
    let object = manager.get(object_id).unwrap();
    let branch = &object.branches[&BranchName::new("main")];
    let sorted = ObjectManager::tips_by_timestamp(&branch.commits, &branch.tips);
    let winner = *sorted.last().unwrap();
    assert_eq!(winner, bob_id, "bob (ts=300) should be LWW winner");

    // Bob's content wins — alice's title change is lost
    assert_eq!(
        branch.commits[&winner].content, b"title=task,completed=true",
        "whole-object LWW: bob's full snapshot wins, alice's title change lost"
    );
}
