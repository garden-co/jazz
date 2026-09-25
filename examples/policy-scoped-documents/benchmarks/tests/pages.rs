use jazz_example_policy_scoped_documents_benchmark::{
    Fixture, OWNERS, OWNERS_PER_ORG, Page, Policy, document_row, user,
};

#[test]
fn subscription_hydration_has_the_exact_independently_expected_page() {
    for policy in [Policy::Unrestricted, Policy::Owner, Policy::OwnerOrOrg] {
        let fixture = Fixture::new(1_000, policy);
        for (page, expected) in [
            (
                Page::Owner(2),
                (20..30).rev().map(document_row).collect::<Vec<_>>(),
            ),
            (
                Page::Org(0),
                match policy {
                    Policy::Owner => (20..30).rev().map(document_row).collect(),
                    _ => (0..40).rev().map(document_row).collect(),
                },
            ),
        ] {
            let mut session = fixture.session(page, 50, user(2));
            let (mut stream, event) = session.subscribe();
            let jazz::db::SubscriptionEvent::Delta {
                reset,
                settled,
                added,
                updated,
                removed,
                ..
            } = event
            else {
                panic!("initial page must be a delta");
            };
            assert!(reset && settled);
            assert!(updated.is_empty() && removed.is_empty());
            assert_eq!(
                added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
                expected
            );
            jazz::db::block_on(stream.close()).expect("close subscription");
        }
    }
}

#[test]
fn timed_organization_page_cannot_be_satisfied_by_direct_ownership() {
    let fixture = Fixture::new(10_000, Policy::OwnerOrOrg);
    let rows = fixture.session(Page::Org(0), 50, user(2)).read();
    // All 50 newest rows belong to owner 3, not member 2. A planner that
    // incorrectly narrows the OR policy to owner=2 must fail this assertion.
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        (350..400).rev().map(document_row).collect::<Vec<_>>()
    );
}

#[test]
fn ordered_page_preserves_id_ties_and_refills_after_deletions() {
    let tied = Fixture::with_order_values(1_000, Policy::OwnerOrOrg, |index| (index / 5) as u64);
    let rows = tied.session(Page::Org(0), 3, user(2)).read();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        (35..38).map(document_row).collect::<Vec<_>>()
    );

    let deleted = Fixture::new(1_000, Policy::Unrestricted);
    deleted.delete_documents(&[29, 28, 27]);
    let rows = deleted.session(Page::Owner(2), 2, user(2)).read();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [document_row(26), document_row(25)]
    );
}

#[test]
fn unrelated_delete_and_restore_registers_do_not_expand_ordered_page_reads() {
    let fixture = Fixture::new(1_000, Policy::OwnerOrOrg);
    let unrelated = (700..1_000).collect::<Vec<_>>();
    fixture.delete_documents(&unrelated);
    for restored in [false, true] {
        if restored {
            fixture.restore_documents(&unrelated, |index| index as u64);
        }
        let mut session = fixture.session(Page::Org(0), 10, user(2));
        let rows = session.read();
        assert_eq!(
            rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
            (30..40).rev().map(document_row).collect::<Vec<_>>()
        );
        let register_reads = session.take_metrics().register_global_current_rows.reads;
        assert!(
            register_reads <= 32,
            "unrelated deletion and restore records must not be read for this page: {register_reads}"
        );
    }
}

#[test]
fn sparse_ordered_pages_do_not_scan_unrelated_deletion_registers() {
    let fixture = Fixture::new(1_000, Policy::OwnerOrOrg);
    let unrelated = (700..1_000).collect::<Vec<_>>();
    fixture.delete_documents(&unrelated);

    // The owner bucket has only ten rows, and the organization does not exist.
    // Both are complete pages even without an extra row past the public limit.
    for (page, limit, expected) in [
        (
            Page::Owner(2),
            50,
            (20..30).rev().map(document_row).collect::<Vec<_>>(),
        ),
        (Page::Org(25), 10, Vec::new()),
    ] {
        let mut session = fixture.session(page, limit, user(2));
        let rows = session.read();
        assert_eq!(
            rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
            expected
        );
        let register_reads = session.take_metrics().register_global_current_rows.reads;
        assert!(
            register_reads <= 100,
            "{page:?} read {register_reads} unrelated deletion registers"
        );
    }

    // The first bounded batch contains only deleted rows. Later indexed
    // candidates must fill the page without opening unrelated tombstones.
    fixture.delete_documents(&(20..40).collect::<Vec<_>>());
    let mut session = fixture.session(Page::Org(0), 10, user(2));
    let rows = session.read();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        (10..20).rev().map(document_row).collect::<Vec<_>>()
    );
    let register_reads = session.take_metrics().register_global_current_rows.reads;
    assert!(
        register_reads <= 100,
        "deleted page prefix read {register_reads} unrelated deletion registers"
    );
}

// Public Db integration: an independently computed oracle checks both policy
// branches, non-members, exact descending order, empty and oversized pages.
#[test]
fn pages_match_exact_authorized_membership_and_order() {
    for policy in [Policy::Unrestricted, Policy::Owner, Policy::OwnerOrOrg] {
        let fixture = Fixture::new(1_000, policy);
        for identity in [1, 2] {
            for page in [
                Page::Owner(0),
                Page::Owner(1),
                Page::Owner(2),
                Page::Owner(4),
                Page::Org(0),
                Page::Org(1),
            ] {
                for limit in [0, 1, 10, 50] {
                    let per_owner = fixture.table_rows / OWNERS;
                    let expected = (0..fixture.table_rows)
                        .rev()
                        .filter(|index| {
                            let owner = index / per_owner;
                            let org = owner / OWNERS_PER_ORG;
                            let predicate = match page {
                                Page::Owner(requested) => owner == requested,
                                Page::Org(requested) => org == requested,
                            };
                            let allowed = match policy {
                                Policy::Unrestricted => true,
                                Policy::Owner => owner == identity,
                                Policy::OwnerOrOrg => {
                                    owner == identity || org * OWNERS_PER_ORG + 2 == identity
                                }
                            };
                            predicate && allowed
                        })
                        .take(limit)
                        .map(document_row)
                        .collect::<Vec<_>>();
                    let rows = fixture.session(page, limit, user(identity)).read();
                    assert_eq!(
                        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
                        expected,
                        "policy={policy:?} identity={identity} page={page:?} limit={limit}"
                    );
                }
            }
        }
    }
}

/// Ordered pages under a restrictive policy, read as ordinary users rather
/// than the system identity, equal the same query over a schema without
/// composite indexes, truncated to the limit. Order values tie in groups of
/// three, and deletes land on tie groups and at bucket edges.
#[test]
fn policy_scoped_pages_equal_the_unindexed_query_for_each_user() {
    fn ids(rows: Vec<jazz::node::CurrentRow>) -> Vec<jazz::ids::RowUuid> {
        rows.iter().map(|row| row.row_uuid()).collect()
    }
    let order = |index: usize| (index / 3) as u64;
    let deleted = [39, 38, 36, 33, 25, 24, 22, 21, 20, 17, 5, 1];
    for policy in [Policy::Unrestricted, Policy::Owner, Policy::OwnerOrOrg] {
        let indexed = Fixture::with_order_values_and_indexes(1_000, policy, true, order);
        let unindexed = Fixture::with_order_values_and_indexes(1_000, policy, false, order);
        indexed.delete_documents(&deleted);
        unindexed.delete_documents(&deleted);
        for identity in [2, 3, 7] {
            for page in [Page::Owner(2), Page::Owner(3), Page::Org(0), Page::Org(1)] {
                let control = ids(unindexed.session(page, 1_000, user(identity)).read());
                for limit in [1, 2, 3, 4, 5, 7, 9, 10, 11, 29, 50] {
                    let expected: Vec<_> = control.iter().copied().take(limit).collect();
                    assert_eq!(
                        ids(indexed.session(page, limit, user(identity)).read()),
                        expected,
                        "{policy:?} user {identity} {page:?} limit {limit}"
                    );
                }
            }
        }
    }
}
