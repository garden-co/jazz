//! Equality queries across held versions, including cold and offline queries (E2EE protocol §4.5).

mod common;
mod content_support;

use common::space::*;
use content_support::*;
use jazz_e2ee::Error;
use jazz_e2ee::agility::MechanismConfig;
use jazz_e2ee::protocol::ProtocolError;
use jazz_e2ee::protocol::content::{
    ContentError, CreationFate, HeldKeys, QueryPlan, QueryTokens, RowTarget, TableTarget,
    UnsupportedQuery, plan_equality,
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
        &EqualityValue::Text(text.to_owned()),
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
    let mut lost = own_creation(&world, ALICE, ROW, "tx");
    held.hold(lost.clone()).unwrap();
    let sealed = seal_texts(
        &world,
        &write_key(&held, &address),
        &row(&domain, &address, ROW_1),
        &[(title(), "x")],
    );
    lost.settle(CreationFate::NotAccepted, None);
    held.hold(lost).unwrap();
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
    let value = EqualityValue::Text("x".into());
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
    let shape = UnsupportedQuery::UnindexedEquality;
    let message = ContentError::UnsupportedQuery(shape).to_string();
    assert!(message.contains(shape.name()), "{message}");
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
