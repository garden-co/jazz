//! Physical channel progress must not depend on a suspended semantic query.
use jazz::query::Query;
use jazz::tools::native_transport_connector::{
    NativeTransportConnector, NativeTransportFuture, NativeTransportRequest,
};
use jazz::tools::test_support::{AllowAll, ordinary_rows};
use jazz::tools::{ColumnType, ReadTier, SchemaBuilder, TableSchema, Value};
use jazz::wire::channels::ChannelClass;
use jazz::wire::{TransportError, WireFrame, WireTransport};
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Public socket-adapter instrumentation is necessary to establish an upload
// has reached the server and remains in progress without wall-clock races.
// Model a slow receiver by retaining the first bulk consumption grant and all
// later grants (their sequence is connection-global). Query/control data still
// flows using its separate initial window; release replays grants unchanged.
type WireWake = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct BulkCreditGate {
    held: Mutex<VecDeque<Vec<u8>>>,
    observed: tokio::sync::Notify,
    bulk_charge: std::sync::atomic::AtomicUsize,
    released: std::sync::atomic::AtomicBool,
    wake: Mutex<Option<WireWake>>,
}
impl BulkCreditGate {
    async fn received_by_server(&self) {
        loop {
            let notified = self.observed.notified();
            if !self.held.lock().unwrap().is_empty()
                && self.bulk_charge.load(std::sync::atomic::Ordering::SeqCst)
                    > 4 * 1024 * 1024 - jazz::wire::channels::MAX_CHANNEL_FRAME_PAYLOAD - 256
            {
                return;
            }
            notified.await;
        }
    }
    fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.wake.lock().unwrap().as_ref().unwrap()();
    }
}
struct GatedConnector(Arc<BulkCreditGate>);
impl NativeTransportConnector for GatedConnector {
    fn connect(&self, request: NativeTransportRequest) -> NativeTransportFuture {
        let gate = self.0.clone();
        *gate.wake.lock().unwrap() = Some(request.wake.clone());
        Box::pin(async move {
            let mut connection = jazz_native_transport::NativeWebSocketConnector
                .connect(request)
                .await?;
            connection.transport = Box::new(GatedWire {
                inner: connection.transport,
                gate,
            });
            Ok(connection)
        })
    }
}
struct GatedWire {
    inner: Box<dyn WireTransport + Send>,
    gate: Arc<BulkCreditGate>,
}
impl WireTransport for GatedWire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let bulk_charge = match jazz::wire::decode_frame(&frame).unwrap() {
            WireFrame::Channel(channel)
                if matches!(
                    channel.extent.class,
                    ChannelClass::Writes | ChannelClass::LargeValue
                ) =>
            {
                jazz::wire::channel_credit::channel_frame_credit_cost(frame.len())
            }
            _ => 0,
        };
        self.inner.send_frame(frame)?;
        self.gate
            .bulk_charge
            .fetch_add(bulk_charge, std::sync::atomic::Ordering::SeqCst);
        self.gate.observed.notify_one();
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        if self.gate.released.load(std::sync::atomic::Ordering::SeqCst) {
            if let Some(frame) = self.gate.held.lock().unwrap().pop_front() {
                return Some(frame);
            }
            return self.inner.try_recv_frame();
        }
        while let Some(frame) = self.inner.try_recv_frame() {
            if matches!(jazz::wire::decode_frame(&frame).unwrap(), WireFrame::ChannelCredit(ref credit) if credit.class == ChannelClass::Writes || !self.gate.held.lock().unwrap().is_empty())
            {
                let mut held = self.gate.held.lock().unwrap();
                assert!(
                    held.len() < 512,
                    "credit gate remains bounded by initial per-class windows"
                );
                held.push_back(frame);
                self.gate.observed.notify_one();
            } else {
                return Some(frame);
            }
        }
        None
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_native_client_reads_indirect_text_and_json() {
    tokio::task::LocalSet::new().run_until(async {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("documents")
                .column("label", ColumnType::Text)
                .column("text", ColumnType::Text)
                .column("json", ColumnType::Json { schema: None }))
            .table(TableSchema::builder("ready").column("label", ColumnType::Text))
            .table(TableSchema::builder("probe").column("label", ColumnType::Text))
            .allow_all().build();
        let server = jazz_server::JazzServer::start_with_schema(schema.clone()).await.unwrap();
        let writer = connect_ready_client(&server, &schema, "pump-writer", "ready", Duration::from_secs(30)).await;
        for size in [65_536, 65_537, 4 * 1024 * 1024 + 65_537] {
            // A deterministic nonuniform payload also exercises transport compression.
            let mut random = 0x1234_5678_u32;
            let text: String = (0..size).map(|_| {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                char::from(b'a' + (random % 26) as u8)
            }).collect();
            let json = format!("{{\"text\":\"{}\"}}", &text[..size - 11]);
            let label = size.to_string();
            let (_, _, tx) = writer.insert("documents", jazz::row_input!("label" => label.clone(), "text" => text.clone(), "json" => json.clone())).unwrap();
            wait_for_edge_txs(&writer, &[tx.unwrap()]).await;
            // Ready against a separate empty table, so no document is prefetched.
            let reader = connect_ready_user(&server, &schema, &format!("pump-reader-{size}"), "ready", Duration::from_secs(30)).await;
            // Poll remote demand once, then cancel it before running another query.
            // This deterministically exercises cancellation without a timing race.
            {
                use std::future::Future;
                let mut cancelled = Box::pin(reader.query(Query::from("documents"), ReadTier::Remote));
                std::future::poll_fn(|cx| {
                    assert!(cancelled.as_mut().poll(cx).is_pending(), "fresh remote query must await admission");
                    std::task::Poll::Ready(())
                }).await;
            }
            let rows = tokio::time::timeout(Duration::from_secs(30), reader.query(Query::from("documents").select(["label"]), ReadTier::Remote)).await.expect("small projection progresses").unwrap();
            assert!(ordinary_rows(rows).iter().any(|(_, row)| row == &[Value::Text(label.clone())]));
            let rows = tokio::time::timeout(Duration::from_secs(30), reader.query(Query::from("documents").select(["label", "text", "json"]), ReadTier::Remote)).await.expect("remote indirect read progresses beyond channel credit window").unwrap();
            let rows = ordinary_rows(rows);
            let row = rows.iter().find(|(_, row)| row[0] == Value::Text(label.clone())).expect("inserted document");
            assert_eq!(row.1[1], Value::Text(text.clone()));
            assert_eq!(row.1[2], Value::Text(json.clone()));
            if size > 4 * 1024 * 1024 {
                let mut context = server.make_client_context_for_user(schema.clone(), "gated-uploader");
                context.backend_secret = None;
                context.admin_secret = None;
                jazz_testkit::enroll_test_context(&mut context).await.unwrap();
                let gate = Arc::new(BulkCreditGate::default());
                let uploader = jazz::tools::JazzClient::connect_with_native_transport(context, Arc::new(GatedConnector(gate.clone()))).await.unwrap();
                jazz_testkit::wait_for_edge_query_ready(&uploader, "ready", Duration::from_secs(30)).await;
                let (probe, _, tx) = writer.insert("probe", jazz::row_input!("label" => "fresh server row")).unwrap();
                wait_for_edge_txs(&writer, &[tx.unwrap()]).await;
                let (_, _, upload) = uploader.insert("documents", jazz::row_input!("label" => "upload", "text" => text, "json" => json)).unwrap();
                let upload = upload.unwrap();
                tokio::time::timeout(Duration::from_secs(30), gate.received_by_server()).await.expect("server consumed upload frames");
                let result = tokio::time::timeout(Duration::from_secs(30), uploader.query(Query::from("probe").select(["label"]), ReadTier::Remote)).await.expect("fresh remote query progresses while bulk credits withheld").unwrap();
                assert_eq!(ordinary_rows(result), vec![(probe, vec![Value::Text("fresh server row".into())])]);
                {
                    use std::future::Future;
                    let mut settlement = Box::pin(uploader.wait_for_transaction(upload, jazz::tools::DurabilityTier::GlobalServer));
                    std::future::poll_fn(|cx| {
                        assert!(settlement.as_mut().poll(cx).is_pending(), "upload must remain incomplete until bulk credits return");
                        std::task::Poll::Ready(())
                    }).await;
                }
                gate.release();
                wait_for_edge_txs(&uploader, &[upload]).await;
                uploader.shutdown().await.unwrap();
            }
            reader.shutdown().await.unwrap();
        }
        writer.shutdown().await.unwrap();
        server.shutdown().await;
    }).await;
}
