mod support;
use std::sync::Arc;
use support::Pages;

/// Alice's authenticated backend creates her account through the ordinary
/// server routes. After the host stops, the next host resolves the same account;
/// a caller without backend authority cannot create one.
/// alice -> backend credential -> Jazz HTTP -> owner thread -> Rivet VFS
#[tokio::test(flavor = "multi_thread")]
async fn server_accounts_survive_host_restart() -> anyhow::Result<()> {
    use jazz_server::{AuthConfig, EmbeddedServer, ServerBuilder, ShutdownPhase, StorageBackend};
    let pages = Arc::new(Pages::default());
    let actor_id = uuid::Uuid::new_v4().to_string();
    let app_id = jazz::tools::AppId::random();
    let secret = "test-backend-secret-at-least-32-characters";
    let mut account = None;
    for first in [true, false] {
        let storage = jazz_rivet::worker_store(
            pages.clone(),
            actor_id.clone(),
            1,
            tokio::runtime::Handle::current(),
        );
        let runtime = tokio::runtime::Handle::current();
        let built = tokio::task::spawn_blocking(move || {
            runtime.block_on(
                ServerBuilder::new(app_id)
                    .with_auth_config(AuthConfig {
                        admin_secret: Some(secret.into()),
                        backend_secret: Some(secret.into()),
                        ..Default::default()
                    })
                    .with_storage(StorageBackend::Worker(storage))
                    .build(),
            )
        })
        .await?
        .map_err(anyhow::Error::msg)?;
        let server = EmbeddedServer::new(built);
        let body = br#"{"identity":{"issuer":"test","subject":"alice"}}"#.to_vec();
        let headers = vec![("content-type".into(), "application/json".into())];
        let register = format!("/apps/{app_id}/admin/accounts/login-or-register");
        let (status, _, _) = server
            .handle_parts("POST", &register, &headers, body.clone())
            .await
            .map_err(anyhow::Error::msg)?;
        assert_eq!(status, 401);
        let headers = [
            headers,
            vec![
                ("x-jazz-backend-credential".into(), secret.into()),
                ("x-jazz-backend-access".into(), "admin".into()),
            ],
        ]
        .concat();
        let path = if first {
            register
        } else {
            format!("/apps/{app_id}/admin/accounts/resolve")
        };
        let (status, _, response) = server
            .handle_parts("POST", &path, &headers, body)
            .await
            .map_err(anyhow::Error::msg)?;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&response));
        let response: serde_json::Value = serde_json::from_slice(&response)?;
        if first {
            account = Some(response["account"].clone());
        }
        assert_eq!(account.as_ref(), Some(&response["account"]));
        assert_eq!(server.shutdown().await, ShutdownPhase::StorageClosed);
        drop(server);
    }
    Ok(())
}

const SECRET: &str = "test-backend-secret-at-least-32-characters";
async fn start(pages: Arc<Pages>, actor: &str) -> anyhow::Result<jazz_server::EmbeddedServer> {
    let app = jazz_rivet::app_id_for_actor(actor);
    let storage =
        jazz_rivet::worker_store(pages, actor.into(), 1, tokio::runtime::Handle::current());
    let runtime = tokio::runtime::Handle::current();
    let built = tokio::task::spawn_blocking(move || {
        runtime.block_on(
            jazz_server::ServerBuilder::new(app)
                .with_auth_config(jazz_server::AuthConfig {
                    admin_secret: Some(SECRET.into()),
                    backend_secret: Some(SECRET.into()),
                    ..Default::default()
                })
                .with_storage(jazz_server::StorageBackend::Worker(storage))
                .build(),
        )
    })
    .await?
    .map_err(anyhow::Error::msg)?;
    Ok(jazz_server::EmbeddedServer::new(built))
}
async fn admin(
    server: &jazz_server::EmbeddedServer,
    path: &str,
    body: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let app = server.state().app_id;
    let (status, _, bytes) = server
        .handle_parts(
            "POST",
            &format!("/apps/{app}{path}"),
            &[
                ("content-type".into(), "application/json".into()),
                ("x-jazz-backend-credential".into(), SECRET.into()),
                ("x-jazz-backend-access".into(), "admin".into()),
            ],
            serde_json::to_vec(&body)?,
        )
        .await
        .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        status == 200,
        "HTTP {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    Ok(serde_json::from_slice(&bytes)?)
}
fn schema() -> jazz::tools::Schema {
    use jazz::tools::public_schema::{
        ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    };
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_delete(PolicyExpr::False),
                ),
        )
        .build()
}
async fn deploy(server: &jazz_server::EmbeddedServer) -> anyhow::Result<String> {
    let source = schema();
    let permissions: std::collections::HashMap<_, _> = source
        .iter()
        .map(|(name, table)| (*name, table.policies.clone()))
        .collect();
    let mut structural = source;
    for table in structural.values_mut() {
        table.policies = Default::default();
    }
    let hash = jazz::tools::public_schema::SchemaHash::compute(&structural).to_string();
    let response = admin(
        server,
        "/admin/bundles",
        serde_json::json!({
            "targetSchemaHash": hash, "schemas": [{"hash": hash, "schema": structural}],
            "migrations": [], "permissions": permissions,
        }),
    )
    .await?;
    Ok(response["bundle"].as_str().expect("bundle id").into())
}
async fn receive(
    peer: &mut jazz_server::EmbeddedSocketPeer,
) -> anyhow::Result<Vec<jazz::wire::WireFrame>> {
    let message = tokio::time::timeout(std::time::Duration::from_secs(20), peer.recv()).await?;
    let Some(jazz_server::EmbeddedOutgoing::Binary(bytes)) = message else {
        anyhow::bail!("socket ended: {message:?}");
    };
    let batch: Vec<Vec<u8>> = postcard::from_bytes(&bytes)?;
    batch
        .iter()
        .map(|frame| jazz::wire::decode_postcard_frame(frame).map_err(anyhow::Error::msg))
        .collect()
}
async fn socket(
    server: &jazz_server::EmbeddedServer,
    bundle: &str,
) -> anyhow::Result<jazz_server::EmbeddedSocketPeer> {
    use jazz::wire::{WireFrame, WireHello, WirePeerRole};
    let account = admin(
        server,
        "/admin/accounts/login-or-register",
        serde_json::json!({"identity":{"issuer":"test","subject":"alice"}}),
    )
    .await?;
    let mut peer = server.open_socket_with(&[]).map_err(anyhow::Error::msg)?;
    let identity = jazz::ids::AuthorSubject::authenticated("test", "alice")?.with_account(
        jazz::account_registry::AccountId(uuid::Uuid::parse_str(
            account["account"].as_str().unwrap(),
        )?),
    );
    assert!(peer.sender().send(serde_json::to_vec(&serde_json::json!({
        "peer_identity":identity.canonical(), "bundle":bundle,
        "auth":{"backend_secret":SECRET,"backend_session":{
            "account_id":account["account"], "issuer":"test", "user_id":"alice",
            "claims":{}, "authMode":"external"
        }}
    }))?));
    let hello = jazz::wire::encode_frame(&WireFrame::Hello(WireHello::current(
        WirePeerRole::Client,
        jazz::wire::FEATURE_THIN_RESULTS,
    )))?;
    assert!(peer.sender().send(postcard::to_allocvec(&vec![hello])?));
    let greeting = receive(&mut peer).await?;
    assert!(
        matches!(greeting.as_slice(), [WireFrame::Hello(_)]),
        "{greeting:?}"
    );
    Ok(peer)
}
async fn request(
    peer: &mut jazz_server::EmbeddedSocketPeer,
    id: u64,
    op: jazz::thin::ThinOp,
) -> anyhow::Result<jazz::thin::ThinBody> {
    use jazz::wire::WireFrame;
    let encoded =
        jazz::wire::encode_frame(&WireFrame::ThinRequest(jazz::thin::ThinRequest { id, op }))?;
    for frame in jazz::thin::thin_wire_frames(encoded)
        .map_err(|size| anyhow::anyhow!("frame too large: {size}"))?
    {
        assert!(
            peer.sender()
                .send_with_room(postcard::to_allocvec(&vec![frame])?)
                .await
        );
    }
    let mut assembler = jazz::thin::ThinAssembler::default();
    loop {
        for frame in receive(peer).await? {
            let frame = match frame {
                WireFrame::ThinPart(part) => {
                    match assembler.push(part).map_err(anyhow::Error::msg)? {
                        Some(bytes) => jazz::wire::decode_postcard_frame(&bytes)?,
                        None => continue,
                    }
                }
                frame => frame,
            };
            match frame {
                WireFrame::ThinResponse(response) if response.id == id => return Ok(response.body),
                other => anyhow::bail!("unexpected response: {other:?}"),
            }
        }
    }
}

/// Alice deploys through HTTP, writes and reads over the ordinary authenticated
/// socket, and sees her row after restart. A delete forbidden by the deployment
/// is denied. All catalogue, account and core SQL uses the real Rivet worker.
/// alice -> HTTP deployment -> socket -> Jazz owner -> Rivet pages -> restart
#[tokio::test(flavor = "multi_thread")]
async fn deployed_database_serves_authenticated_socket_and_reopens() -> anyhow::Result<()> {
    use jazz::thin::{ThinBody, ThinOp};
    let pages = Arc::new(Pages::default());
    let actor = uuid::Uuid::new_v4().to_string();
    let server = start(pages.clone(), &actor).await?;
    let bundle = deploy(&server).await?;
    let mut peer = socket(&server, &bundle).await?;
    assert_eq!(
        request(
            &mut peer,
            0,
            ThinOp::Schema {
                schema: serde_json::to_vec(&schema())?
            }
        )
        .await?,
        ThinBody::Ok
    );
    let descriptor =
        jazz::records::RecordDescriptor::new([("body", jazz::records::ValueType::String)]);
    let record = jazz::records::OwnedRecord::new(
        descriptor.create(&[jazz::records::Value::String("hello from alice".into())])?,
        descriptor,
    );
    let written = request(
        &mut peer,
        1,
        ThinOp::Insert {
            table: "notes".into(),
            cells: jazz::binding_codec::encode_named_cells(&record).map_err(anyhow::Error::msg)?,
            options: vec![],
            transaction: None,
        },
    )
    .await?;
    let ThinBody::Written { row, .. } = written else {
        anyhow::bail!("insert: {written:?}");
    };
    let denied = request(
        &mut peer,
        2,
        ThinOp::Delete {
            table: "notes".into(),
            row,
            options: vec![],
            transaction: None,
        },
    )
    .await?;
    assert!(matches!(denied, ThinBody::Error { .. }), "{denied:?}");
    drop(peer);
    assert_eq!(
        server.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
    drop(server);
    let server = start(pages, &actor).await?;
    let mut peer = socket(&server, &bundle).await?;
    assert_eq!(
        request(
            &mut peer,
            0,
            ThinOp::Schema {
                schema: serde_json::to_vec(&schema())?
            }
        )
        .await?,
        ThinBody::Ok
    );
    let response = request(
        &mut peer,
        1,
        ThinOp::Query {
            query: postcard::to_allocvec(&jazz::query::Query::from("notes"))?,
            options: br#"{"tier":"remote"}"#.to_vec(),
            transaction: None,
        },
    )
    .await?;
    let ThinBody::Rows { rows, .. } = response else {
        anyhow::bail!("query: {response:?}");
    };
    let rows: Vec<_> = jazz::binding_codec::decode_rows(&rows)
        .map_err(anyhow::Error::msg)?
        .into_iter()
        .flat_map(|batch| batch.rows)
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cells.get("body"),
        Some(&jazz::records::Value::String("hello from alice".into()))
    );
    drop(peer);
    assert_eq!(
        server.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
    Ok(())
}

/// Alice disconnects after her account write reaches storage. Shutdown waits
/// for that admitted commit, and Bob's new host can resolve Alice's account.
/// alice -> admitted commit (paused) -> disconnect -> close waits -> commit -> reopen
#[tokio::test(flavor = "multi_thread")]
async fn disconnected_request_finishes_before_storage_closes() -> anyhow::Result<()> {
    let pages = Arc::new(Pages::default());
    let actor = uuid::Uuid::new_v4().to_string();
    let server = Arc::new(start(pages.clone(), &actor).await?);
    let pause = pages.pause_next_commit();
    let request = tokio::spawn({
        let server = server.clone();
        async move {
            admin(
                &server,
                "/admin/accounts/login-or-register",
                serde_json::json!({"identity":{"issuer":"test","subject":"alice"}}),
            )
            .await
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pages.commit_entered.notified(),
    )
    .await?;
    request.abort();
    let close = tokio::spawn({
        let server = server.clone();
        async move { server.shutdown().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!close.is_finished(), "shutdown must wait for admitted work");
    drop(pause);
    assert_eq!(close.await?, jazz_server::ShutdownPhase::StorageClosed);
    let _ = request.await;
    drop(server);
    let server = start(pages, &actor).await?;
    let resolved = admin(
        &server,
        "/admin/accounts/resolve",
        serde_json::json!({"identity":{"issuer":"test","subject":"alice"}}),
    )
    .await?;
    assert!(resolved["account"].is_string(), "{resolved}");
    assert_eq!(
        server.shutdown().await,
        jazz_server::ShutdownPhase::StorageClosed
    );
    Ok(())
}
