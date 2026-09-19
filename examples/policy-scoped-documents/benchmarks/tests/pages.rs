use jazz_example_policy_scoped_documents_benchmark::{
    Fixture, OWNERS, OWNERS_PER_ORG, Page, Policy, document_row, user,
};

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

// Public Db integration: an independently computed oracle checks both policy
// branches, non-members, exact descending order, empty and oversized pages.
#[test]
fn pages_match_exact_authorized_membership_and_order() {
    for policy in [Policy::None, Policy::Owner, Policy::OwnerOrOrg] {
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
                                Policy::None => true,
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
