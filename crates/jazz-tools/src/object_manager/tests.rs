use super::*;
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
