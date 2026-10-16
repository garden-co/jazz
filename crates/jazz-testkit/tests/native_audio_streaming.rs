//! Qualification of the enrolled native route for an audio editor (#3947).
//! Metadata lives separately from audio; this does not qualify typed-query
//! projection of large cells (#2090). No device playback is exercised here.

use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use jazz::groove::large_values::full_materializations_for_test;
use jazz::query::Query;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{
    ClientStorage, ColumnType, DurabilityTier, JazzClient, PolicyExpr, ReadTier, SchemaBuilder,
    Session, TablePolicies, TableSchema, Value,
};
use jazz_server::JazzServer;
use jazz_testkit::{enroll_test_context, wait_for_global_txs, wait_for_subscription_update};
use tempfile::TempDir;

fn schema() -> jazz::tools::Schema {
    let owner = PolicyExpr::eq_session(
        "owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    );
    let policies = TablePolicies::new()
        .with_insert(owner.clone())
        .with_select(owner);
    SchemaBuilder::new()
        .table(
            TableSchema::builder("audio")
                .column("owner", ColumnType::Text)
                .column("contents", ColumnType::Bytea)
                .policies(policies.clone()),
        )
        .table(
            TableSchema::builder("recordings")
                .column("owner", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(policies),
        )
        .build()
}

// A valid two-second 48 kHz mono, signed 16-bit PCM WAV spanning large-value
// chunks. The fixture's explicit RIFF bytes are audio input, not a Jazz encoding.
fn pcm_wav() -> Vec<u8> {
    let pcm: Vec<u8> = (0..96_000_i32)
        .flat_map(|frame| ((frame % 1024 - 512) as i16).to_le_bytes())
        .collect();
    let length = pcm.len() as u32;
    let mut wav = Vec::new();
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + length).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&48_000_u32.to_le_bytes());
    wav.extend_from_slice(&96_000_u32.to_le_bytes());
    wav.extend_from_slice(&2_u16.to_le_bytes());
    wav.extend_from_slice(&16_u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&length.to_le_bytes());
    wav.extend_from_slice(&pcm);
    wav
}

struct ObservedReader {
    file: File,
    largest_request: Arc<AtomicUsize>,
}

impl Read for ObservedReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.largest_request
            .fetch_max(buffer.len(), Ordering::SeqCst);
        self.file.read(buffer)
    }
}

/// Alice imports a WAV using an enrolled persistent native client, observes
/// its metadata, settles it, closes, and reopens without a server. Bob cannot
/// read Alice's audio. A retained clone cannot restart the closed context.
///
/// alice ──stream WAV──► local storage ──settle──► server
/// alice ◄──metadata delta── server             bob ──range read──✗
/// alice ──shutdown──► reopen offline ──middle range──► exact bytes
#[tokio::test]
async fn enrolled_native_audio_survives_offline_reopen() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = schema();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let directory = TempDir::new().unwrap();
            let wav = pcm_wav();
            let path = directory.path().join("interview.wav");
            std::fs::write(&path, &wav).unwrap();
            let mut context = server.make_client_context_for_user(schema.clone(), "alice");
            context.admin_secret = None;
            context.backend_secret = None;
            context.storage = ClientStorage::Persistent;
            context.storage_factory = Some(Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory));
            context.data_dir = directory.path().join("project");
            enroll_test_context(&mut context).await.unwrap();
            let connector = Arc::new(jazz_native_transport::NativeWebSocketConnector);
            let alice =
                JazzClient::connect_with_native_transport(context.clone(), connector.clone())
                    .await
                    .unwrap();
            let mut metadata = alice.subscribe(Query::from("recordings")).await.unwrap();
            let largest_request = Arc::new(AtomicUsize::new(0));
            let (audio_id, imported) = tokio::time::timeout(
                Duration::from_secs(30),
                alice.insert_streaming_value(
                    "audio",
                    jazz::row_input!("owner" => "alice"),
                    "contents",
                    ObservedReader {
                        file: File::open(path).unwrap(),
                        largest_request: largest_request.clone(),
                    },
                ),
            )
            .await
            .expect("native owner import must not spin")
            .unwrap();
            assert!(largest_request.load(Ordering::SeqCst) <= 64 * 1024);
            alice
                .wait_for_transaction(imported, DurabilityTier::Local)
                .await
                .unwrap();
            let (_, _, announced) = alice
                .insert(
                    "recordings",
                    jazz::row_input!("owner" => "alice", "name" => "interview.wav"),
                )
                .unwrap();
            wait_for_global_txs(&alice, &[imported, announced.unwrap()]).await;
            let mut deltas = Vec::new();
            wait_for_subscription_update(
                &mut metadata,
                &mut deltas,
                Duration::from_secs(30),
                "imported recording metadata",
                |log| {
                    log.iter().any(|delta| {
                        delta.added.iter().any(|row| {
                            row.row.get("name") == Some(&Value::Text("interview.wav".into()))
                        })
                    })
                },
            )
            .await;
            let middle = 44 + 48_000;
            // The one internal hook is needed because public returned bytes
            // cannot distinguish a range read from rebuilding the entire WAV.
            let before = full_materializations_for_test();
            assert_eq!(
                alice
                    .read_value_range("audio", audio_id, "contents", middle..middle + 128)
                    .await
                    .unwrap(),
                wav[middle as usize..middle as usize + 128]
            );
            assert_eq!(full_materializations_for_test() - before, 0);

            let mut bob_context = server.make_client_context_for_user(schema, "bob");
            bob_context.admin_secret = None;
            bob_context.backend_secret = None;
            enroll_test_context(&mut bob_context).await.unwrap();
            let bob = JazzClient::connect_with_native_transport(bob_context, connector.clone())
                .await
                .unwrap();
            assert!(
                bob.read_value_range("audio", audio_id, "contents", middle..middle + 128)
                    .await
                    .is_err()
            );
            let (_, forged) = bob
                .insert_streaming_value(
                    "audio",
                    jazz::row_input!("owner" => "alice"),
                    "contents",
                    io::Cursor::new(wav.clone()),
                )
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(30),
                    bob.wait_for_transaction(forged, DurabilityTier::GlobalServer),
                )
                .await
                .expect("foreign-owner write must settle")
                .is_err()
            );
            bob.shutdown().await.unwrap();
            let retained = alice.clone();
            alice.shutdown().await.unwrap();
            assert!(
                retained
                    .read_value_range("audio", audio_id, "contents", 0..4)
                    .await
                    .is_err()
            );
            drop(metadata);
            server.shutdown().await;

            context.server_url.clear();
            let reopened = JazzClient::connect_with_native_transport(context, connector)
                .await
                .unwrap();
            assert!(!reopened.is_connected());
            assert_eq!(
                reopened
                    .query(Query::from("recordings"), ReadTier::LocalFirst)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                reopened
                    .read_value_range("audio", audio_id, "contents", middle..middle + 128)
                    .await
                    .unwrap(),
                wav[middle as usize..middle as usize + 128]
            );
            reopened.shutdown().await.unwrap();
        })
        .await;
}

struct BrokenReader(bool);

impl Read for BrokenReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.0 {
            return Err(io::Error::other("fixture read failure"));
        }
        self.0 = true;
        buffer.fill(0);
        Ok(buffer.len())
    }
}

/// Alice's failed native import publishes no row. Unsupported transaction and
/// backend-session contexts fail explicitly instead of bypassing their scopes.
#[tokio::test]
async fn failed_or_unsupported_audio_operations_do_not_publish_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("audio").column("contents", ColumnType::Bytea))
                .allow_all()
                .build();
            let alice = JazzClient::test_client(schema).await;
            assert!(
                alice
                    .insert_streaming_value(
                        "audio",
                        jazz::row_input!(),
                        "contents",
                        BrokenReader(false)
                    )
                    .await
                    .is_err()
            );
            assert!(
                alice
                    .query(Query::from("audio"), ReadTier::LocalFirst)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let (id, retried) = alice
                .insert_streaming_value(
                    "audio",
                    jazz::row_input!(),
                    "contents",
                    io::Cursor::new(b"audio".to_vec()),
                )
                .await
                .unwrap();
            alice
                .wait_for_transaction(retried, DurabilityTier::Local)
                .await
                .unwrap();
            let transaction = alice.begin_transaction().unwrap();
            assert!(
                transaction
                    .insert_streaming_value(
                        "audio",
                        jazz::row_input!(),
                        "contents",
                        io::Cursor::new(b"audio".to_vec())
                    )
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("transaction")
            );
            assert!(
                transaction
                    .read_value_range("audio", id, "contents", 0..4)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("transaction")
            );
            transaction.rollback().unwrap();
            assert!(
                alice
                    .for_session(Session::new("urn:jazz:test", "bob"))
                    .insert_streaming_value(
                        "audio",
                        jazz::row_input!(),
                        "contents",
                        io::Cursor::new(b"audio".to_vec())
                    )
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("session")
            );
            assert!(
                alice
                    .for_session(Session::new("urn:jazz:test", "bob"))
                    .read_value_range("audio", id, "contents", 0..4)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("session")
            );
            assert_eq!(
                alice
                    .query(Query::from("audio"), ReadTier::LocalFirst)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            alice.shutdown().await.unwrap();
        })
        .await;
}
