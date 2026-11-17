//! Equality-filtered queries (`E2EE_PROTOCOL.md` §4.5; migrations §3.3):
//! one token per held version, cold and offline queries, completeness, the
//! shapes that are refused, and queries on a converting column.

mod common;
mod content_support;

use common::space::*;
use content_support::*;
use jazz_e2ee::Error;
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::convert::{
    MAX_REVERSE_VALUES, Side, SourceIndex, encrypted_source_present, encrypted_target_present,
    plan_conversion_equality, value_side,
};
use jazz_e2ee::protocol::content::{
    CLEARED_MARKER, ContentError, CreationFate, HeldKeys, QueryPlan, QueryTokens, RowTarget,
    TableTarget, UnsupportedQuery, plan_equality,
};
use jazz_e2ee::protocol::domain::SharedDomain;
use jazz_e2ee::protocol::records::space::SpaceAddress;
use jazz_e2ee::records::EqualityValue;

fn world(seed: u64) -> World {
    let mut world = World::new(seed);
    world.account(ALICE);
    world.account(BOB);
    world.add_member(ALICE);
    world.add_member(BOB);
    world.create(ALICE);
    world
}

fn table<'a>(domain: &'a SharedDomain, address: &'a SpaceAddress) -> TableTarget<'a> {
    TableTarget {
        domain,
        space: address,
        table_id: TABLE,
    }
}

fn row<'a>(domain: &'a SharedDomain, address: &'a SpaceAddress, row_id: &'a str) -> RowTarget<'a> {
    RowTarget {
        domain,
        space: address,
        table_id: TABLE,
        row_id,
    }
}

fn plan(world: &World, held: &HeldKeys, target: &TableTarget<'_>, text: &str) -> QueryPlan {
    let tagger = world
        .shared
        .registry
        .select(&MechanismConfig::default())
        .unwrap()
        .equality;
    plan_equality(
        tagger,
        held,
        target,
        &title(),
        &EqualityValue::Text(text),
        true,
    )
    .unwrap()
}

fn tokens(plan: QueryPlan) -> QueryTokens {
    match plan {
        QueryPlan::Tokens(tokens) => tokens,
        other => panic!("expected tokens, got {other:?}"),
    }
}

#[test]
fn a_query_has_one_token_per_held_version() {
    let mut world = world(1);
    let domain = shared(&world);
    let address = space(ROW);
    let before = held_of(&world, ALICE, ROW);
    let first = seal_texts(
        &world,
        &write_key(&before, &address),
        &row(&domain, &address, ROW_1),
        &[(title(), "x")],
    );
    world.rotate(ALICE);
    let after = held_of(&world, ALICE, ROW);
    let second = seal_texts(
        &world,
        &write_key(&after, &address),
        &row(&domain, &address, ROW_2),
        &[(title(), "x")],
    );
    let query = tokens(plan(&world, &after, &table(&domain, &address), "x"));
    let mut expected = vec![
        first[0].equality_token.clone().unwrap(),
        second[0].equality_token.clone().unwrap(),
    ];
    expected.sort();
    assert_eq!(query.tokens, expected);
    assert_eq!(query.epochs.len(), 2);
    assert!(!query.incomplete);
    // Another value matches neither.
    let other = tokens(plan(&world, &after, &table(&domain, &address), "y"));
    assert!(other.tokens.iter().all(|token| !expected.contains(token)));
    // The token does not bind the row.
    assert_eq!(query.tokens.len(), 2);
}

#[test]
fn a_query_covers_cells_this_device_wrote_under_a_refused_key() {
    let world = world(2);
    let domain = shared(&world);
    let address = space(ROW);
    let mut held = HeldKeys::new();
    let lost = own_creation(&world, ALICE, ROW, "tx");
    let root = lost.root_id().to_owned();
    held.hold(lost).unwrap();
    let sealed = seal_texts(
        &world,
        &write_key(&held, &address),
        &row(&domain, &address, ROW_1),
        &[(title(), "x")],
    );
    held.space_mut(&root)
        .unwrap()
        .settle(CreationFate::NotAccepted, None);
    held.hold(acquire_held(&world, ALICE, ROW)).unwrap();
    let query = tokens(plan(&world, &held, &table(&domain, &address), "x"));
    assert_eq!(query.tokens.len(), 2);
    assert!(
        query
            .tokens
            .contains(sealed[0].equality_token.as_ref().unwrap())
    );
}

#[test]
fn cold_offline_and_refused_queries() {
    let world = world(3);
    let domain = shared(&world);
    let address = space(ROW);
    let target = table(&domain, &address);
    let tagger = world
        .shared
        .registry
        .select(&MechanismConfig::default())
        .unwrap()
        .equality;
    let held = held_of(&world, ALICE, ROW);
    let value = EqualityValue::Text("x");
    // Offline: no tokens, every candidate is decrypted and compared.
    assert_eq!(
        plan_equality(tagger, &held, &target, &title(), &value, false),
        Ok(QueryPlan::Offline)
    );
    // Online with no key of the space: acquire first, never `IN ()`.
    assert_eq!(
        plan_equality(tagger, &HeldKeys::new(), &target, &title(), &value, true),
        Ok(QueryPlan::NeedsKey)
    );
    // A column without an equality index is refused, never scanned.
    assert_eq!(
        plan_equality(tagger, &held, &target, &notes(), &value, true),
        Err(ContentError::UnsupportedQuery(
            UnsupportedQuery::UnindexedEquality
        ))
    );
    assert_eq!(
        plan_equality(
            tagger,
            &held,
            &target,
            &title(),
            &EqualityValue::Int(1),
            true
        ),
        Err(ContentError::Protocol(ProtocolError::Crypto(
            Error::InvalidInput
        )))
    );
    for shape in [
        UnsupportedQuery::OrderByEncrypted,
        UnsupportedQuery::Traversal,
        UnsupportedQuery::Union,
        UnsupportedQuery::PartialSelect,
        UnsupportedQuery::IncludeEncryptedChild,
        UnsupportedQuery::Aggregate,
        UnsupportedQuery::NonEqualityPredicate,
        UnsupportedQuery::UnindexedEquality,
        UnsupportedQuery::NoSpaceConstraint,
    ] {
        let message = ContentError::UnsupportedQuery(shape).to_string();
        assert!(message.contains(shape.name()), "{message}");
    }
}

#[test]
fn a_query_knows_when_the_chain_has_versions_it_does_not_hold() {
    let mut world = world(4);
    let domain = shared(&world);
    let address = space(ROW);
    world.deliver(ALICE, BOB, &world.current());
    let mut bob = held_of(&world, BOB, ROW);
    bob.observe_chain(&chain_of(&world, ROW));
    assert!(!tokens(plan(&world, &bob, &table(&domain, &address), "x")).incomplete);
    // Alice rotates; Bob reads the new chain before its delivery opens.
    world.rotate(ALICE);
    bob.observe_chain(&chain_of(&world, ROW));
    let query = tokens(plan(&world, &bob, &table(&domain, &address), "x"));
    assert!(query.incomplete);
    assert_eq!(query.tokens.len(), 1);
    bob.hold(acquire_held(&world, BOB, ROW)).unwrap();
    let query = tokens(plan(&world, &bob, &table(&domain, &address), "x"));
    assert!(!query.incomplete);
    assert_eq!(query.tokens.len(), 2);
}

#[test]
fn a_query_on_a_converting_column() {
    let mut world = world(5);
    world.add_member_of(OTHER_ROW, ALICE);
    world.create_of(OTHER_ROW, ALICE);
    let domain = shared(&world);
    let old = space(ROW);
    let new = space(OTHER_ROW);
    let mut held = held_of(&world, ALICE, ROW);
    held.hold(acquire_held(&world, ALICE, OTHER_ROW)).unwrap();
    let tagger = world
        .shared
        .registry
        .select(&MechanismConfig::default())
        .unwrap()
        .equality;
    let value = EqualityValue::Text("x");
    let target = table(&domain, &new);
    let plan_with = |source: Option<&SourceIndex<'_>>, pending| {
        plan_conversion_equality(
            tagger,
            &held,
            &target,
            &title(),
            &value,
            source,
            pending,
            true,
        )
    };
    // Nothing pending: the target predicate alone.
    let plan = plan_with(None, 0).unwrap();
    assert_eq!(plan.source, None);
    assert_eq!(
        plan.target,
        plan_equality(tagger, &held, &target, &title(), &value, true).unwrap()
    );
    // Pending rows no index reaches: refused, never a result without them.
    assert_eq!(
        plan_with(None, 3),
        Err(ContentError::ConversionPending { pending: 3 })
    );
    let untagged = SourceIndex {
        table: table(&domain, &old),
        column: notes(),
        values: &[EqualityValue::Text("x")],
    };
    assert_eq!(
        plan_with(Some(&untagged), 3),
        Err(ContentError::ConversionPending { pending: 3 })
    );
    // An identity conversion (a re-home): the searched value's tokens under
    // the source's own context, the old space.
    let identity = SourceIndex {
        table: table(&domain, &old),
        column: title(),
        values: &[EqualityValue::Text("x")],
    };
    let plan = plan_with(Some(&identity), 3).unwrap();
    assert_eq!(
        plan.source,
        Some(plan_equality(tagger, &held, &table(&domain, &old), &title(), &value, true).unwrap())
    );
    // A declared reverse mapping: one token per source value, at most 16.
    let preimages = [EqualityValue::Text("1"), EqualityValue::Text("01")];
    let reverse = SourceIndex {
        values: &preimages,
        ..identity
    };
    let QueryPlan::Tokens(source) = plan_with(Some(&reverse), 3).unwrap().source.unwrap() else {
        panic!("tokens");
    };
    assert_eq!(source.tokens.len(), 2);
    let too_many = vec![EqualityValue::Text("1"); MAX_REVERSE_VALUES + 1];
    let excessive = SourceIndex {
        values: &too_many,
        ..identity
    };
    assert_eq!(
        plan_with(Some(&excessive), 3),
        Err(ContentError::Protocol(ProtocolError::Crypto(
            Error::InvalidInput
        )))
    );
    let none = SourceIndex {
        values: &[],
        ..identity
    };
    let QueryPlan::Tokens(source) = plan_with(Some(&none), 3).unwrap().source.unwrap() else {
        panic!("tokens");
    };
    assert!(source.tokens.is_empty());
}

#[test]
fn the_source_decides_whenever_it_is_present() {
    // A present source wins over any target, so a stale target tag never
    // selects the row's value.
    assert_eq!(value_side(true, true), Side::Source);
    assert_eq!(value_side(true, false), Side::Source);
    assert_eq!(value_side(false, true), Side::Target);
    assert_eq!(value_side(false, false), Side::Neither);
    assert!(!encrypted_source_present(&CLEARED_MARKER));
    assert!(encrypted_source_present(&[0; 78]));
    assert!(!encrypted_target_present(&CLEARED_MARKER));
    assert!(!encrypted_target_present(b""));
    assert!(encrypted_target_present(&[0; 78]));
}
