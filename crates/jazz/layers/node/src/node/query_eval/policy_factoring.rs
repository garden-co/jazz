//! Factoring of disjunctive policy branches back into a conjunction of
//! disjunctions.
//!
//! Schema conversion expands a policy into disjunctive normal form: an AND of
//! `k` ORs of two alternatives becomes `2^k` branches of `k` atoms each.
//! Lowering every branch separately makes compile work exponential in `k`,
//! although most branches share the same atoms. When the branch set is a
//! cartesian product of independent atom groups, the policy authorizes a row
//! exactly when, for every group, one of the group's alternatives does. That
//! lowers as one union per group, intersected on the row id: `2k` atom chains
//! instead of `k * 2^k`.

use std::collections::BTreeSet;

use crate::query::{InheritsVia, JoinVia, PolicyBranch, Predicate, ReachableVia};

/// One conjunct of a policy branch.
#[derive(Clone)]
enum Atom {
    Filter(Predicate),
    Join(JoinVia),
    Reachable(ReachableVia),
    Inherits(InheritsVia),
}

/// Split `branches` into independent factors, each a list of alternatives.
///
/// Returns `None` unless the branches are exactly the cartesian product of at
/// least two factors, one of which has more than one alternative. Each
/// alternative keeps its atoms in the order the original branches list them.
pub(super) fn factor_policy_branches(branches: &[PolicyBranch]) -> Option<Vec<Vec<PolicyBranch>>> {
    if branches.len() < 4 {
        return None;
    }
    // Atoms are identified by their encoding; equal atoms in different
    // branches are the same conjunct.
    let mut atoms = Vec::<(Vec<u8>, Atom)>::new();
    let mut rows = Vec::<BTreeSet<usize>>::with_capacity(branches.len());
    for branch in branches {
        let mut row = BTreeSet::new();
        for atom in branch_atoms(branch) {
            let key = atom_key(&atom)?;
            let index = match atoms.iter().position(|(existing, _)| *existing == key) {
                Some(index) => index,
                None => {
                    atoms.push((key, atom));
                    atoms.len() - 1
                }
            };
            row.insert(index);
        }
        rows.push(row);
    }
    let distinct_rows = rows.iter().collect::<BTreeSet<_>>();
    if distinct_rows.len() != rows.len() {
        return None;
    }

    // Two atoms belong to the same factor when their joint presence pattern
    // is not the product of their individual ones.
    let mut parent = (0..atoms.len()).collect::<Vec<_>>();
    for left in 0..atoms.len() {
        for right in left + 1..atoms.len() {
            let joint = rows
                .iter()
                .map(|row| (row.contains(&left), row.contains(&right)))
                .collect::<BTreeSet<_>>();
            let left_patterns = joint.iter().map(|(l, _)| *l).collect::<BTreeSet<_>>();
            let right_patterns = joint.iter().map(|(_, r)| *r).collect::<BTreeSet<_>>();
            if joint.len() != left_patterns.len() * right_patterns.len() {
                let (a, b) = (find(&mut parent, left), find(&mut parent, right));
                parent[a] = b;
            }
        }
    }
    let mut groups = Vec::<(usize, BTreeSet<usize>)>::new();
    for atom in 0..atoms.len() {
        let root = find(&mut parent, atom);
        match groups
            .iter_mut()
            .find(|(group_root, _)| *group_root == root)
        {
            Some((_, members)) => {
                members.insert(atom);
            }
            None => groups.push((root, BTreeSet::from([atom]))),
        }
    }
    let factors = groups
        .iter()
        .map(|(_, members)| {
            rows.iter()
                .map(|row| row.intersection(members).copied().collect::<BTreeSet<_>>())
                .collect::<BTreeSet<_>>()
        })
        .collect::<Vec<_>>();
    if factors.len() < 2 || factors.iter().all(|alternatives| alternatives.len() < 2) {
        return None;
    }
    // Pairwise independence does not imply the whole set is a product; only
    // factor when it is exactly one. Every row is some combination of the
    // factors' alternatives (they are its projections), rows are distinct and
    // the groups partition the atoms, so equal counts mean every combination
    // is a row.
    let product_size = factors.iter().try_fold(1usize, |size, alternatives| {
        size.checked_mul(alternatives.len())
    })?;
    if product_size != rows.len() {
        return None;
    }

    Some(
        factors
            .into_iter()
            .map(|alternatives| {
                alternatives
                    .into_iter()
                    .map(|members| branch_from_atoms(&atoms, &members))
                    .collect()
            })
            .collect(),
    )
}

fn find(parent: &mut [usize], mut index: usize) -> usize {
    while parent[index] != index {
        parent[index] = parent[parent[index]];
        index = parent[index];
    }
    index
}

fn branch_atoms(branch: &PolicyBranch) -> impl Iterator<Item = Atom> + '_ {
    branch
        .filters
        .iter()
        .cloned()
        .map(Atom::Filter)
        .chain(branch.joins.iter().cloned().map(Atom::Join))
        .chain(branch.reachable.iter().cloned().map(Atom::Reachable))
        .chain(branch.inherits.iter().cloned().map(Atom::Inherits))
}

fn atom_key(atom: &Atom) -> Option<Vec<u8>> {
    let encoded = match atom {
        Atom::Filter(filter) => postcard::to_allocvec(&(0u8, filter)),
        Atom::Join(join) => postcard::to_allocvec(&(1u8, join)),
        Atom::Reachable(reachable) => postcard::to_allocvec(&(2u8, reachable)),
        Atom::Inherits(inherits) => postcard::to_allocvec(&(3u8, inherits)),
    };
    encoded.ok()
}

/// Rebuilds a partial branch from atom indices, which follow first
/// appearance and so keep each kind's original relative order.
fn branch_from_atoms(atoms: &[(Vec<u8>, Atom)], members: &BTreeSet<usize>) -> PolicyBranch {
    let mut branch = PolicyBranch {
        filters: Vec::new(),
        joins: Vec::new(),
        reachable: Vec::new(),
        inherits: Vec::new(),
    };
    for index in members {
        match &atoms[*index].1 {
            Atom::Filter(filter) => branch.filters.push(filter.clone()),
            Atom::Join(join) => branch.joins.push(join.clone()),
            Atom::Reachable(reachable) => branch.reachable.push(reachable.clone()),
            Atom::Inherits(inherits) => branch.inherits.push(inherits.clone()),
        }
    }
    branch
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{col, eq, param};

    fn atom(name: &str) -> Predicate {
        eq(col(name), param(name))
    }

    fn branch(names: &[&str]) -> PolicyBranch {
        PolicyBranch {
            filters: names.iter().map(|name| atom(name)).collect(),
            joins: Vec::new(),
            reachable: Vec::new(),
            inherits: Vec::new(),
        }
    }

    /// Every combination of one alternative per factor, in DNF order.
    fn expand(factors: &[&[&[&str]]]) -> Vec<PolicyBranch> {
        let mut branches = vec![Vec::<&str>::new()];
        for alternatives in factors {
            branches = branches
                .iter()
                .flat_map(|prefix| {
                    alternatives.iter().map(move |alternative| {
                        prefix.iter().chain(alternative.iter()).copied().collect()
                    })
                })
                .collect();
        }
        branches.iter().map(|names| branch(names)).collect()
    }

    fn names(factors: Vec<Vec<PolicyBranch>>) -> Vec<Vec<Vec<Predicate>>> {
        factors
            .into_iter()
            .map(|alternatives| alternatives.into_iter().map(|b| b.filters).collect())
            .collect()
    }

    #[test]
    fn and_of_ors_factors_into_one_group_per_or() {
        let branches = expand(&[&[&["a"], &["b"]], &[&["c"], &["d"]], &[&["e"], &["f"]]]);
        assert_eq!(branches.len(), 8);
        let factors = factor_policy_branches(&branches).expect("a product of three ORs");
        assert_eq!(
            names(factors),
            vec![
                vec![vec![atom("a")], vec![atom("b")]],
                vec![vec![atom("c")], vec![atom("d")]],
                vec![vec![atom("e")], vec![atom("f")]],
            ]
        );
    }

    #[test]
    fn shared_atoms_and_multi_atom_alternatives_stay_together() {
        // shared AND (a OR (b AND c)) AND (d OR e)
        let branches = expand(&[&[&["shared"]], &[&["a"], &["b", "c"]], &[&["d"], &["e"]]]);
        let factors = factor_policy_branches(&branches).expect("a product with a shared atom");
        assert_eq!(
            names(factors),
            vec![
                vec![vec![atom("shared")]],
                vec![vec![atom("a")], vec![atom("b"), atom("c")]],
                vec![vec![atom("d")], vec![atom("e")]],
            ]
        );
    }

    #[test]
    fn branch_sets_that_are_not_a_product_are_left_alone() {
        // (a AND c) OR (a AND d) OR (b AND c): the (b AND d) combination is
        // missing, so no factoring is equivalent.
        let mut branches = expand(&[&[&["a"], &["b"]], &[&["c"], &["d"]]]);
        branches.pop();
        branches.push(branch(&["e", "f"]));
        assert!(factor_policy_branches(&branches).is_none());

        // Pairwise independent atoms whose joint pattern is still no product:
        // the even-parity rows of three bits.
        let parity = [
            branch(&[]),
            branch(&["a", "b"]),
            branch(&["a", "c"]),
            branch(&["b", "c"]),
        ];
        assert!(factor_policy_branches(&parity).is_none());
    }

    #[test]
    fn plain_disjunctions_and_small_sets_are_left_alone() {
        let or = [
            branch(&["a"]),
            branch(&["b"]),
            branch(&["c"]),
            branch(&["d"]),
        ];
        assert!(factor_policy_branches(&or).is_none());
        let small = expand(&[&[&["a"]], &[&["b"], &["c"]]]);
        assert!(factor_policy_branches(&small).is_none());
    }
}
