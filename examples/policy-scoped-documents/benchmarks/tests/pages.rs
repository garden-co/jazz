use jazz_example_policy_scoped_documents_benchmark::{
    Fixture, OWNERS, OWNERS_PER_ORG, Page, Policy, document_row, user,
};

// Public Db integration: an independently computed oracle checks both policy
// branches, non-members, exact descending order, empty and oversized pages.
#[test]
fn pages_match_exact_authorized_membership_and_order() {
    for policy in [Policy::None, Policy::Owner, Policy::OwnerOrOrg] {
        let fixture = Fixture::new(1_000, policy);
        for identity in [2, 3] {
            for page in [
                Page::Owner(0),
                Page::Owner(2),
                Page::Owner(3),
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
                                    owner == identity || org * OWNERS_PER_ORG + 3 == identity
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
