//! Timings of the encrypted paths at scale. Run on demand:
//! `cargo test -p jazz-e2ee-client --release --test scale -- --ignored --nocapture`.
//! `SCALE_MEMBERS` and `SCALE_EPOCHS` set the size of the shared space.

mod common;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use common::*;
use jazz::db::{Db, block_on};
use jazz::e2ee::EncryptionSettings;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee_client::{AccountStatus, E2eeClient, MemoryStore};

struct Device {
    who: AuthorSubject,
    client: E2eeClient,
    db: Db,
    _link: jazz::db::Upstream,
    live: RefCell<Vec<jazz::db::SubscriptionStream>>,
}

fn subject(n: u32) -> AuthorSubject {
    let mut bytes = [0xc0; 16];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn device(server: &Db, n: u32) -> Device {
    let who = subject(n);
    let settings = EncryptionSettings::default();
    let client = open_account(Rc::new(MemoryStore::new()), who, settings.clone(), None);
    let mut node = [0xd0; 16];
    node[..4].copy_from_slice(&n.to_be_bytes());
    let (db, link) = session_on(
        &client,
        server,
        who,
        &settings,
        NodeUuid::from_bytes(node),
        None,
    );
    let device = Device {
        who,
        client,
        db,
        _link: link,
        live: Default::default(),
    };
    let status = drive(
        server,
        &[&device.db],
        device.client.ensure_account(&device.db),
    );
    assert_eq!(status.unwrap(), AccountStatus::Active);
    device
}

impl Device {
    fn id(&self) -> String {
        account_id(self.who)
    }

    fn follow_notes(&self, server: &Db) {
        let prepared = self.db.prepare_query(&Query::from("notes")).unwrap();
        let stream = block_on(self.db.subscribe(&prepared, Default::default())).unwrap();
        self.live.borrow_mut().push(stream);
        pump(server, &self.db);
    }

    fn write_note(&self, server: &Db, space: uuid::Uuid, body: &str) {
        drive(
            server,
            &[&self.db],
            self.client.write(&self.db, async || {
                self.db
                    .insert("notes", note(space, body), Default::default())
                    .await
            }),
        )
        .unwrap();
    }

    fn read_notes(&self, server: &Db, space: uuid::Uuid) -> usize {
        let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
        drive(
            server,
            &[&self.db],
            self.client.read(&self.db, async || {
                let prepared = self.db.prepare_query(&query)?;
                self.db.all(&prepared, local()).await
            }),
        )
        .unwrap()
        .len()
    }

    fn address(&self, space: uuid::Uuid) -> jazz::e2ee::SpaceAddress {
        let scope = self.db.table_identity("projects").unwrap().unwrap().0;
        jazz::e2ee::SpaceAddress {
            scope_id: scope.to_string(),
            identifier: space.to_string(),
        }
    }
}

fn add_member(_server: &Db, by: &Device, space: uuid::Uuid, account: &str) {
    let scope = by.db.table_identity("projects").unwrap().unwrap().0;
    let cells = BTreeMap::from([
        ("scopeId".to_owned(), Value::Uuid(scope)),
        ("identifier".to_owned(), Value::Uuid(space)),
        (
            "accountId".to_owned(),
            Value::Uuid(uuid::Uuid::parse_str(account).unwrap()),
        ),
    ]);
    block_on(by.db.insert("__e2ee_members", cells, Default::default())).unwrap();
}

fn size(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn per_op(label: &str, ops: usize, started: Instant) -> f64 {
    let each = started.elapsed().as_secs_f64() * 1000.0 / ops as f64;
    eprintln!("SCALE {label}: {each:.3} ms per op over {ops} ops");
    each
}

/// Warm encrypted writes and reads against the same operations on a table
/// that is not encrypted.
#[test]
#[ignore = "timing run"]
fn warm_encrypted_and_plain_operations() {
    let server = secure_authority_with(&EncryptionSettings::default(), None);
    let alice = device(&server, 1);
    let space = project(&alice.db, "plans");
    pump(&server, &alice.db);
    add_member(&server, &alice, space, &alice.id());
    pump(&server, &alice.db);
    alice.follow_notes(&server);
    alice.write_note(&server, space, "first");
    pump(&server, &alice.db);
    let ops = size("SCALE_OPS", 200);

    for _ in 0..10 {
        alice.write_note(&server, space, "warm");
        alice.read_notes(&server, space);
    }
    let started = Instant::now();
    for _ in 0..ops {
        alice.write_note(&server, space, "body");
    }
    per_op("encrypted write", ops, started);
    let started = Instant::now();
    for _ in 0..ops {
        alice.read_notes(&server, space);
    }
    per_op("encrypted read", ops, started);

    let plain_read = Query::from("projects");
    for _ in 0..10 {
        project(&alice.db, "warm");
    }
    let started = Instant::now();
    for _ in 0..ops {
        project(&alice.db, "plain");
    }
    per_op("plain write", ops, started);
    let started = Instant::now();
    for _ in 0..ops {
        let prepared = alice.db.prepare_query(&plain_read).unwrap();
        block_on(alice.db.all(&prepared, local())).unwrap();
    }
    per_op("plain read", ops, started);
}

/// A space of `SCALE_MEMBERS` real members rotated `SCALE_EPOCHS` times:
/// the holder's delivery pass, and a member's first read on a new device.
#[test]
#[ignore = "timing run"]
fn space_of_many_members_and_epochs() {
    let members = size("SCALE_MEMBERS", 100);
    let epochs = size("SCALE_EPOCHS", 100);
    let server = secure_authority_with(&EncryptionSettings::default(), None);
    let alice = device(&server, 1);
    let space = project(&alice.db, "plans");
    pump(&server, &alice.db);
    add_member(&server, &alice, space, &alice.id());
    pump(&server, &alice.db);
    alice.follow_notes(&server);
    alice.write_note(&server, space, "first");
    pump(&server, &alice.db);
    drive(&server, &[&alice.db], alice.client.reconcile(&alice.db)).unwrap();

    let started = Instant::now();
    let mut last = None;
    for n in 0..members {
        let member = device(&server, n as u32 + 2);
        add_member(&server, &alice, space, &member.id());
        pump(&server, &alice.db);
        last = Some(member);
    }
    eprintln!("SCALE setup of {members} members: {:?}", started.elapsed());
    let reader = last.unwrap();
    reader.follow_notes(&server);

    let address = alice.address(space);
    let started = Instant::now();
    for _ in 0..epochs {
        drive(
            &server,
            &[&alice.db],
            alice.client.rotate(&alice.db, &address),
        )
        .unwrap();
    }
    eprintln!("SCALE setup of {epochs} epochs: {:?}", started.elapsed());

    let started = Instant::now();
    let delivered = drive(
        &server,
        &[&alice.db, &reader.db],
        alice.client.deliver_space(&alice.db, &address),
    )
    .unwrap();
    eprintln!(
        "SCALE delivery pass over {members} members, {epochs} epochs ({delivered} delivered): {:?}",
        started.elapsed()
    );
    let started = Instant::now();
    let again = drive(
        &server,
        &[&alice.db, &reader.db],
        alice.client.deliver_space(&alice.db, &address),
    )
    .unwrap();
    eprintln!(
        "SCALE second delivery pass ({again} delivered): {:?}",
        started.elapsed()
    );

    pump(&server, &reader.db);
    let started = Instant::now();
    let rows = reader.read_notes(&server, space);
    eprintln!(
        "SCALE first read by a member of {members} members, {epochs} epochs ({rows} rows): {:?}",
        started.elapsed()
    );
}
