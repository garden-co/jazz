use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use groove::jazz_transport::ServerEvent;
use groove::object::{BranchName, ObjectId};
use groove::sync_manager::SyncPayload;
use std::io::Cursor;

#[path = "generated/server_event_generated.rs"]
mod flatbuffers_generated;
use flatbuffers_generated::jazz::bench as fb;

mod server_event_capnp {
    include!(concat!(env!("OUT_DIR"), "/server_event_capnp.rs"));
}

#[derive(Clone)]
struct BenchEvent {
    type_: String,
    object_id: String,
    branch_name: String,
    metadata: String,
    commits: Vec<String>,
}

fn sample_events() -> (ServerEvent, BenchEvent) {
    let object_id = ObjectId::new();
    let event = ServerEvent::SyncUpdate {
        payload: Box::new(SyncPayload::ObjectUpdated {
            object_id,
            metadata: None,
            branch_name: BranchName::new("main"),
            commits: Vec::new(),
        }),
    };
    let bench_event = bench_event_from_server_event(&event);
    (event, bench_event)
}

fn bench_event_from_server_event(event: &ServerEvent) -> BenchEvent {
    match event {
        ServerEvent::SyncUpdate { payload } => match payload.as_ref() {
            SyncPayload::ObjectUpdated {
                object_id,
                metadata,
                branch_name,
                commits,
                ..
            } => BenchEvent {
                type_: "SyncUpdate".to_string(),
                object_id: object_id.to_string(),
                branch_name: branch_name.to_string(),
                metadata: metadata
                    .as_ref()
                    .and_then(|value| serde_json::to_string(value).ok())
                    .unwrap_or_default(),
                commits: commits.iter().map(|commit| format!("{commit:?}")).collect(),
            },
            _ => panic!("benchmark expects ObjectUpdated payload"),
        },
        _ => panic!("benchmark expects SyncUpdate event"),
    }
}

fn encode_flatbuffers(event: &BenchEvent) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let type_ = builder.create_string(&event.type_);
    let object_id = builder.create_string(&event.object_id);
    let branch_name = builder.create_string(&event.branch_name);
    let metadata = builder.create_string(&event.metadata);
    let commit_offsets: Vec<_> = event
        .commits
        .iter()
        .map(|commit| builder.create_string(commit))
        .collect();
    let commits = builder.create_vector(&commit_offsets);
    let object_updated = fb::ObjectUpdatedPayload::create(
        &mut builder,
        &fb::ObjectUpdatedPayloadArgs {
            object_id: Some(object_id),
            metadata: Some(metadata),
            commits: Some(commits),
            branch_name: Some(branch_name),
        },
    );
    let payload = fb::SyncPayload::create(
        &mut builder,
        &fb::SyncPayloadArgs {
            object_updated: Some(object_updated),
        },
    );
    let server_event = fb::ServerEvent::create(
        &mut builder,
        &fb::ServerEventArgs {
            type_: Some(type_),
            payload: Some(payload),
        },
    );
    fb::finish_server_event_buffer(&mut builder, server_event);
    builder.finished_data().to_vec()
}

fn decode_flatbuffers(bytes: &[u8]) {
    let event = fb::root_as_server_event(bytes).expect("flatbuffers decode");
    black_box(event.type_());
    if let Some(payload) = event.payload() {
        if let Some(object_updated) = payload.object_updated() {
            black_box(object_updated.object_id());
            black_box(object_updated.branch_name());
            black_box(object_updated.metadata());
            if let Some(commits) = object_updated.commits() {
                for index in 0..commits.len() {
                    black_box(commits.get(index));
                }
            }
        }
    }
}

fn encode_capnp(event: &BenchEvent) -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut root = message.init_root::<server_event_capnp::server_event::Builder<'_>>();
        root.set_type(&event.type_);
        let mut payload = root.reborrow().init_payload();
        let mut object_updated = payload.reborrow().init_object_updated();
        object_updated.set_object_id(&event.object_id);
        object_updated.set_branch_name(&event.branch_name);
        object_updated.set_metadata(&event.metadata);
        let mut commits = object_updated
            .reborrow()
            .init_commits(event.commits.len() as u32);
        for (index, commit) in event.commits.iter().enumerate() {
            commits.set(index as u32, commit);
        }
    }

    let mut bytes = Vec::new();
    capnp::serialize::write_message(&mut bytes, &message).expect("capnp encode");
    bytes
}

fn decode_capnp(bytes: &[u8]) {
    let mut cursor = Cursor::new(bytes);
    let reader = capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
        .expect("capnp decode");
    let event = reader
        .get_root::<server_event_capnp::server_event::Reader<'_>>()
        .expect("capnp root");
    black_box(event.get_type().expect("capnp type"));
    let payload = event.get_payload().expect("capnp payload");
    let object_updated = payload.get_object_updated().expect("capnp object_updated");
    black_box(object_updated.get_object_id().expect("capnp object_id"));
    black_box(object_updated.get_branch_name().expect("capnp branch_name"));
    black_box(object_updated.get_metadata().expect("capnp metadata"));
    let commits = object_updated.get_commits().expect("capnp commits");
    for index in 0..commits.len() {
        black_box(commits.get(index).expect("capnp commit"));
    }
}

fn bench_encode(c: &mut Criterion) {
    let (event, bench_event) = sample_events();
    let json_size = serde_json::to_vec(&event).expect("json encode").len() as u64;
    let flex_size = flexbuffers::to_vec(&event).expect("flex encode").len() as u64;
    let msgpack_size = rmp_serde::to_vec_named(&event)
        .expect("msgpack encode")
        .len() as u64;
    let cbor_size = serde_cbor::to_vec(&event).expect("cbor encode").len() as u64;
    let flatbuffers_size = encode_flatbuffers(&bench_event).len() as u64;
    let capnp_size = encode_capnp(&bench_event).len() as u64;

    let mut group = c.benchmark_group("server_event/encode");
    group.throughput(Throughput::Bytes(json_size));
    group.bench_with_input(BenchmarkId::new("json", json_size), &event, |b, event| {
        b.iter(|| {
            let bytes = serde_json::to_vec(black_box(event)).expect("json encode");
            black_box(bytes);
        });
    });

    group.throughput(Throughput::Bytes(flex_size));
    group.bench_with_input(
        BenchmarkId::new("flexbuffers", flex_size),
        &event,
        |b, event| {
            b.iter(|| {
                let bytes = flexbuffers::to_vec(black_box(event)).expect("flex encode");
                black_box(bytes);
            });
        },
    );

    group.throughput(Throughput::Bytes(msgpack_size));
    group.bench_with_input(
        BenchmarkId::new("messagepack", msgpack_size),
        &event,
        |b, event| {
            b.iter(|| {
                let bytes = rmp_serde::to_vec_named(black_box(event)).expect("msgpack encode");
                black_box(bytes);
            });
        },
    );

    group.throughput(Throughput::Bytes(cbor_size));
    group.bench_with_input(BenchmarkId::new("cbor", cbor_size), &event, |b, event| {
        b.iter(|| {
            let bytes = serde_cbor::to_vec(black_box(event)).expect("cbor encode");
            black_box(bytes);
        });
    });

    group.throughput(Throughput::Bytes(flatbuffers_size));
    group.bench_with_input(
        BenchmarkId::new("flatbuffers", flatbuffers_size),
        &bench_event,
        |b, event| {
            b.iter(|| {
                let bytes = encode_flatbuffers(black_box(event));
                black_box(bytes);
            });
        },
    );

    group.throughput(Throughput::Bytes(capnp_size));
    group.bench_with_input(
        BenchmarkId::new("capnp", capnp_size),
        &bench_event,
        |b, event| {
            b.iter(|| {
                let bytes = encode_capnp(black_box(event));
                black_box(bytes);
            });
        },
    );
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let (event, bench_event) = sample_events();
    let json_bytes = serde_json::to_vec(&event).expect("json encode");
    let flex_bytes = flexbuffers::to_vec(&event).expect("flex encode");
    let msgpack_bytes = rmp_serde::to_vec_named(&event).expect("msgpack encode");
    let cbor_bytes = serde_cbor::to_vec(&event).expect("cbor encode");
    let flatbuffers_bytes = encode_flatbuffers(&bench_event);
    let capnp_bytes = encode_capnp(&bench_event);

    let mut group = c.benchmark_group("server_event/decode");
    group.throughput(Throughput::Bytes(json_bytes.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("json", json_bytes.len()),
        &json_bytes,
        |b, bytes| {
            b.iter(|| {
                let decoded: ServerEvent =
                    serde_json::from_slice(black_box(bytes)).expect("json decode");
                black_box(decoded);
            });
        },
    );

    group.throughput(Throughput::Bytes(flex_bytes.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("flexbuffers", flex_bytes.len()),
        &flex_bytes,
        |b, bytes| {
            b.iter(|| {
                let decoded: ServerEvent =
                    flexbuffers::from_slice(black_box(bytes)).expect("flex decode");
                black_box(decoded);
            });
        },
    );

    group.throughput(Throughput::Bytes(msgpack_bytes.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("messagepack", msgpack_bytes.len()),
        &msgpack_bytes,
        |b, bytes| {
            b.iter(|| {
                let decoded: ServerEvent =
                    rmp_serde::from_slice(black_box(bytes)).expect("msgpack decode");
                black_box(decoded);
            });
        },
    );

    group.throughput(Throughput::Bytes(cbor_bytes.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("cbor", cbor_bytes.len()),
        &cbor_bytes,
        |b, bytes| {
            b.iter(|| {
                let decoded: ServerEvent =
                    serde_cbor::from_slice(black_box(bytes)).expect("cbor decode");
                black_box(decoded);
            });
        },
    );

    group.throughput(Throughput::Bytes(flatbuffers_bytes.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("flatbuffers", flatbuffers_bytes.len()),
        &flatbuffers_bytes,
        |b, bytes| {
            b.iter(|| {
                decode_flatbuffers(black_box(bytes));
            });
        },
    );

    group.throughput(Throughput::Bytes(capnp_bytes.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("capnp", capnp_bytes.len()),
        &capnp_bytes,
        |b, bytes| {
            b.iter(|| {
                decode_capnp(black_box(bytes));
            });
        },
    );
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
