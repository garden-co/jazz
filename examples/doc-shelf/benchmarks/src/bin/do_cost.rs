//! What one DocShelf visit costs an authority on Cloudflare Durable Objects:
//! sync messages each way per phase, and (with `JAZZ_SQL_TRACE` and
//! `JAZZ_SQL_TRACE_EXPANDED=1` under jazz's `sql-trace` feature) a trace of
//! the authority's statements, marked by phase, for
//! `dev/durable-objects/rows-written.mjs` to replay on DO SQLite and count
//! rows written.
//!
//! A visit: a new device connects and syncs the shelf, opens a long
//! document, types 20 words, renames a document, creates a short note, then
//! stays connected and idle.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::rc::Rc;

use jazz::db::{
    Db, InsertOptions, ReadOpts, ReadTier, SubscriptionStream, TransportError, UpdateOptions,
    WireTransport, WireTransportAdapter, block_on,
};
use jazz::ids::AuthorSubject;
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz_example_doc_shelf_benchmark::{DOCS, OPEN, config, doc_id, seed};

#[derive(Default, Clone, Copy)]
struct Traffic {
    frames: usize,
    bytes: usize,
}

struct Counted {
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    sent: Rc<RefCell<Traffic>>,
}

impl WireTransport for Counted {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let mut sent = self.sent.borrow_mut();
        sent.frames += 1;
        sent.bytes += frame.len();
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn mark(phase: &str) {
    if let Some(path) = std::env::var_os("JAZZ_SQL_TRACE") {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .expect("open trace");
        writeln!(file, "{}", serde_json::json!({ "mark": phase })).expect("mark trace");
    }
}

fn main() {
    let server = block_on(Db::open(config(0x52))).expect("open authority");
    mark("seed");
    seed(&server);

    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let up_sent = Rc::new(RefCell::new(Traffic::default()));
    let down_sent = Rc::new(RefCell::new(Traffic::default()));
    let client = Counted {
        outbound: Rc::clone(&up),
        inbound: Rc::clone(&down),
        sent: Rc::clone(&up_sent),
    };
    let authority = Counted {
        outbound: down,
        inbound: up,
        sent: Rc::clone(&down_sent),
    };

    let mut report = Vec::new();
    let mut phase_start = (Traffic::default(), Traffic::default());
    let mut end_phase = |name: &str, rounds: usize| {
        let (u, d) = (*up_sent.borrow(), *down_sent.borrow());
        report.push(serde_json::json!({
            "phase": name,
            "rounds": rounds,
            "up_frames": u.frames - phase_start.0.frames,
            "up_bytes": u.bytes - phase_start.0.bytes,
            "down_frames": d.frames - phase_start.1.frames,
            "down_bytes": d.bytes - phase_start.1.bytes,
        }));
        phase_start = (u, d);
    };

    mark("connect");
    let device = block_on(Db::open(config(0x81))).expect("open device");
    let _upstream =
        block_on(device.connect_upstream(Box::new(WireTransportAdapter::current(client))));
    let _subscriber = server
        .accept_subscriber(
            Box::new(WireTransportAdapter::current(authority)),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    let round = || {
        block_on(device.tick()).expect("device tick");
        block_on(server.tick()).expect("authority tick");
        block_on(device.tick()).expect("device tick");
    };
    let remote = ReadOpts {
        tier: ReadTier::Remote,
        ..ReadOpts::default()
    };
    let shelf = device
        .prepare_query(&Query::from("docs"))
        .expect("prepare shelf");
    let mut stream: SubscriptionStream =
        block_on(device.subscribe(&shelf, remote.clone())).expect("subscribe shelf");
    let mut rows = 0;
    let mut rounds = 0;
    while rows < DOCS + 1 && rounds < 4096 {
        rounds += 1;
        round();
        // Bodies still missing chunks read as "not observed yet".
        if let Ok(read) = block_on(device.all(&shelf, ReadOpts::default())) {
            rows = read.len();
        }
        while stream.try_next_event().is_some() {}
    }
    // Let the last body chunks land.
    let settle = |rounds: &mut usize| {
        let mut quiet = 0;
        while quiet < 2 && *rounds < 4096 {
            let before = up_sent.borrow().frames + down_sent.borrow().frames;
            round();
            *rounds += 1;
            let after = up_sent.borrow().frames + down_sent.borrow().frames;
            quiet = if after == before { quiet + 1 } else { 0 };
        }
    };
    settle(&mut rounds);
    end_phase("connect and sync the shelf", rounds);

    mark("open");
    let editor = device
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(doc_id(OPEN).0))))
        .expect("prepare editor");
    let _editor = block_on(device.subscribe(&editor, remote)).expect("subscribe editor");
    let mut rounds = 0;
    settle(&mut rounds);
    end_phase("open a long document", rounds);

    mark("type");
    let mut rounds = 0;
    for word in 0..20 {
        block_on(device.append_value(
            "docs",
            doc_id(OPEN),
            "body",
            format!(" word{word}").into_bytes(),
        ))
        .expect("type a word");
        settle(&mut rounds);
    }
    end_phase("type 20 words (one sync each)", rounds);

    mark("rename");
    let mut rounds = 0;
    block_on(device.update(
        "docs",
        doc_id(3),
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("Doc 003 (renamed)".to_owned()),
        )]),
        UpdateOptions::default(),
    ))
    .expect("rename");
    settle(&mut rounds);
    end_phase("rename a document", rounds);

    mark("create");
    let mut rounds = 0;
    block_on(device.insert(
        "docs",
        BTreeMap::from([
            ("title".to_owned(), Value::String("New note".to_owned())),
            ("folder".to_owned(), Value::String("Tour 2026".to_owned())),
            (
                "body".to_owned(),
                Value::String("Call the venue about load-in.".repeat(40)),
            ),
            ("updated_at".to_owned(), Value::I32(1)),
        ]),
        InsertOptions {
            row_id: Some(doc_id(20_000)),
            ..Default::default()
        },
    ))
    .expect("create a note");
    settle(&mut rounds);
    end_phase("create a short note", rounds);

    mark("idle");
    for _ in 0..30 {
        round();
    }
    end_phase("stay connected, idle (30 rounds)", 30);
    mark("end");
    println!("{}", serde_json::to_string_pretty(&report).expect("report"));
}
