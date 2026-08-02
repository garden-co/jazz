mod sql;

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use bytes::BytesMut;
use futures::{Sink, SinkExt, stream};
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::cancel::DefaultCancelHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionManager, DEFAULT_NAME, ErrorHandler,
    PgWireConnectionState, PgWireServerHandlers, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Execute, Parse, ParseComplete, Sync as PgSync,
    TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
use pgwire::messages::{
    DecodeContext, PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
};
use pgwire::tokio::server::{negotiate_tls, process_error, process_message};
use pgwire::types::format::FormatOptions;
use pgwire::types::{FromSqlText, ToSqlText};
use postgres_types::{FromSql, IsNull, ToSql};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Semaphore;

use crate::groove::records::Value;
use crate::groove::schema::ColumnType;
use crate::ids::SchemaVersionId;
use crate::query::{
    OrderDirection, Predicate, Query, all_of, any_of, col, eq, gt, gte, in_list, is_null, lit, lt,
    lte, ne, not, param,
};
use crate::schema::{JazzSchema, LargeValueKind, TableSchema};
use crate::tools::server::core_server_shell::{PostgresQueryResult, ServerShellHandle};
use crate::tools::server::{ServerState, ServerTopology};

use self::sql::{
    Command, CompareOp, FilterExpr, PageValue, ParsedStatement, ProjectedExpr, SelectPlan,
    SelectSource, SessionFunction, SqlLiteral,
};

const POSTGRES_USER: &str = "jazz";
const MAX_PAGE_SIZE: usize = 10_000;
const MAX_OFFSET: usize = 10_000;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const DATA_ROW_WIRE_OVERHEAD: usize = 1 + 4 + 2;
const MAX_CONCURRENT_DATABASE_JOBS: usize = 1;
const MAX_BUFFERED_RESPONSES: usize = 4;
const MAX_CONNECTIONS: usize = 64;
const MAX_CANCEL_CONNECTIONS: usize = 4;
const MAX_SQL_BYTES: usize = 64 * 1024;
const MAX_PARAMETER_COUNT: usize = 1_024;
const MAX_PARAMETER_BYTES: usize = 4 * 1024 * 1024;
const MAX_EXPANDED_BINDING_BYTES: usize = 8 * 1024 * 1024;
const CANCEL_REQUEST_LENGTH: u32 = 16;
const CANCEL_REQUEST_CODE: u32 = 80_877_102;
const STARTUP_FRAME_BYTES: usize = 10_000;
const QUERY_FRAME_BYTES: usize = 128 * 1024;
const PARSE_FRAME_BYTES: usize = MAX_SQL_BYTES * 2 + 16 * 1024;
const BIND_FRAME_BYTES: usize = MAX_PARAMETER_BYTES + 1024 * 1024;
const OTHER_FRAME_BYTES: usize = 64 * 1024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_STORED_STATEMENTS: usize = 64;
const MAX_STORED_PORTALS: usize = 64;
const MAX_OBJECT_NAME_BYTES: usize = 256;
const MAX_STORED_STATEMENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_STORED_PORTAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RESULT_COLUMNS: usize = 1_664;
const MAX_SIMPLE_BATCH_STATEMENTS: usize = 64;
const INCOMPLETE_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct PostgresServerHandle {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl PostgresServerHandle {
    pub(crate) async fn start(
        state: Arc<ServerState>,
        addr: SocketAddr,
        postgres_secret: String,
    ) -> Result<Self, String> {
        if !addr.ip().is_loopback() {
            return Err("the PostgreSQL interface must bind to a loopback address".to_owned());
        }
        if state.topology != ServerTopology::Core {
            return Err(
                "the PostgreSQL interface is only supported on a core server; edge storage is partial"
                    .to_owned(),
            );
        }
        if postgres_secret.is_empty() {
            return Err("the PostgreSQL interface requires a non-empty database secret".to_owned());
        }
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|error| format!("failed to bind PostgreSQL interface on {addr}: {error}"))?;
        let addr = listener
            .local_addr()
            .map_err(|error| format!("failed to resolve PostgreSQL listen address: {error}"))?;
        let database = state.app_id.to_string();
        let backend = Arc::new(PostgresBackend {
            state: state.clone(),
            database: database.clone(),
            parser: Arc::new(ParsedQueryParser),
            database_job: Arc::new(Semaphore::new(MAX_CONCURRENT_DATABASE_JOBS)),
            buffered_responses: Arc::new(Semaphore::new(MAX_BUFFERED_RESPONSES)),
        });
        let connection_manager = Arc::new(ConnectionManager::new());
        let factory = Arc::new(PostgresHandlerFactory {
            backend,
            auth: Arc::new(DatabaseAuth {
                database,
                password: postgres_secret,
            }),
            connection_manager,
        });
        let connection_slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let cancel_slots = Arc::new(Semaphore::new(MAX_CANCEL_CONNECTIONS));
        let shutdown = state.shutdown.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = shutdown.wait_requested() => break,
                    accepted = listener.accept() => {
                        let Ok((socket, peer)) = accepted else {
                            if !shutdown.is_shutting_down() {
                                tracing::error!("PostgreSQL listener stopped accepting connections");
                            }
                            break;
                        };
                        let Ok(connection_slot) = connection_slots.clone().try_acquire_owned() else {
                            let Ok(cancel_slot) = cancel_slots.clone().try_acquire_owned() else {
                                tracing::warn!(%peer, max_connections = MAX_CONNECTIONS, "PostgreSQL connection limit reached");
                                drop(socket);
                                continue;
                            };
                            let Some(connection_guard) = shutdown.try_enter_postgres_connection() else {
                                break;
                            };
                            let factory = factory.clone();
                            connections.spawn(async move {
                                let _cancel_slot = cancel_slot;
                                let _connection_guard = connection_guard;
                                if !looks_like_cancel_request(&socket).await {
                                    tracing::warn!(%peer, max_connections = MAX_CONNECTIONS, "rejected PostgreSQL connection above the regular limit");
                                    return;
                                }
                                match tokio::time::timeout(
                                    Duration::from_secs(1),
                                    process_bounded_socket(socket, factory),
                                )
                                .await
                                {
                                    Ok(Err(error)) => tracing::debug!(%peer, %error, "PostgreSQL cancel connection closed with an error"),
                                    Err(_) => tracing::warn!(%peer, "PostgreSQL cancel connection timed out"),
                                    Ok(Ok(())) => {}
                                }
                            });
                            continue;
                        };
                        let Some(connection_guard) = shutdown.try_enter_postgres_connection() else {
                            break;
                        };
                        let factory = factory.clone();
                        connections.spawn(async move {
                            let _connection_slot = connection_slot;
                            let _connection_guard = connection_guard;
                            if let Err(error) = process_bounded_socket(socket, factory).await {
                                tracing::debug!(%peer, %error, "PostgreSQL connection closed with an error");
                            }
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => {
                        if let Err(error) = result
                            && !error.is_cancelled()
                        {
                            tracing::debug!(%error, "PostgreSQL connection task failed");
                        }
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(Self { addr, task })
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
}

async fn looks_like_cancel_request(socket: &tokio::net::TcpStream) -> bool {
    let mut prefix = [0_u8; 8];
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    loop {
        let read = match tokio::time::timeout_at(deadline, socket.peek(&mut prefix)).await {
            Ok(Ok(read)) => read,
            _ => return false,
        };
        if read == 0 {
            return false;
        }
        if read >= prefix.len() {
            return u32::from_be_bytes(prefix[..4].try_into().expect("four-byte length"))
                == CANCEL_REQUEST_LENGTH
                && u32::from_be_bytes(prefix[4..].try_into().expect("four-byte code"))
                    == CANCEL_REQUEST_CODE;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn process_bounded_socket(
    socket: tokio::net::TcpStream,
    handlers: Arc<PostgresHandlerFactory>,
) -> Result<(), io::Error> {
    let startup_deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
    let socket = match tokio::time::timeout_at(
        startup_deadline,
        negotiate_tls::<PreparedStatement>(socket, None),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => return Ok(()),
    };
    let Some(socket) = socket else {
        return Ok(());
    };

    // TLS negotiation may already have read part of the startup frame. Keep
    // those bytes, then bypass Framed's unbounded frontend decoder so the
    // declared frame length is checked before any large allocation.
    let mut parts = socket.into_parts();
    let mut inbound = std::mem::take(&mut parts.read_buf);
    let mut socket = tokio_util::codec::Framed::from_parts(parts);

    let startup_handler = handlers.startup_handler();
    let simple_query_handler = handlers.backend.clone();
    let extended_query_handler = handlers.backend.clone();
    let copy_handler = handlers.copy_handler();
    let cancel_handler = handlers.cancel_handler();
    let error_handler = handlers.error_handler();

    loop {
        let state = socket.state();
        let protocol_version = socket.protocol_version();
        let read =
            read_bounded_frontend_message(socket.get_mut(), &mut inbound, state, protocol_version);
        let message = if matches!(
            state,
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            match tokio::time::timeout_at(startup_deadline, read).await {
                Ok(result) => result,
                Err(_) => return Ok(()),
            }
        } else {
            read.await
        }
        .map_err(io::Error::from)?;
        let Some(message) = message else {
            break;
        };
        if matches!(message, PgWireFrontendMessage::Terminate(_)) {
            break;
        }
        let is_extended_query = match socket.state() {
            PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
            _ => message.is_extended_query(),
        };
        if let Err(mut error) = process_message(
            message,
            &mut socket,
            startup_handler.clone(),
            simple_query_handler.clone(),
            extended_query_handler.clone(),
            copy_handler.clone(),
            cancel_handler.clone(),
        )
        .await
        {
            error_handler.on_error(&socket, &mut error);
            process_error(&mut socket, error, is_extended_query).await?;
        }
    }
    Ok(())
}

async fn read_bounded_frontend_message<S>(
    io: &mut S,
    inbound: &mut BytesMut,
    state: PgWireConnectionState,
    protocol_version: ProtocolVersion,
) -> PgWireResult<Option<PgWireFrontendMessage>>
where
    S: AsyncRead + Unpin,
{
    let startup = matches!(state, PgWireConnectionState::AwaitingStartup);
    let header_bytes = if startup { 4 } else { 5 };
    // An entirely idle authenticated connection is allowed to remain open. Once
    // a client starts a frame, it must finish it promptly so partial frames
    // cannot pin all connection slots indefinitely.
    if inbound.is_empty() && !fill_frontend_bytes(io, inbound, 1).await? {
        return Ok(None);
    }
    let frame_deadline = tokio::time::Instant::now() + INCOMPLETE_FRAME_TIMEOUT;
    let header_complete = tokio::time::timeout_at(
        frame_deadline,
        fill_frontend_bytes(io, inbound, header_bytes),
    )
    .await
    .map_err(|_| user_error("57014", "timed out waiting for a complete PostgreSQL frame"))??;
    if !header_complete {
        return Err(user_error("08P01", "truncated PostgreSQL frontend header"));
    }

    let (frame_bytes, frame_limit) = if startup {
        let frame_bytes = u32::from_be_bytes(
            inbound[..4]
                .try_into()
                .expect("startup frame has a four-byte header"),
        ) as usize;
        (frame_bytes, STARTUP_FRAME_BYTES)
    } else {
        let message_type = inbound[0];
        if matches!(state, PgWireConnectionState::AuthenticationInProgress) && message_type != b'p'
        {
            return Err(user_error(
                "08P01",
                "only a password message is valid during PostgreSQL authentication",
            ));
        }
        let body_bytes = u32::from_be_bytes(
            inbound[1..5]
                .try_into()
                .expect("frontend frame has a four-byte length"),
        ) as usize;
        let frame_bytes = body_bytes
            .checked_add(1)
            .ok_or_else(|| user_error("54000", "PostgreSQL frame length overflow"))?;
        let frame_limit = if matches!(state, PgWireConnectionState::AuthenticationInProgress) {
            STARTUP_FRAME_BYTES
        } else {
            frontend_frame_limit(message_type)
        };
        (frame_bytes, frame_limit)
    };
    if frame_bytes < header_bytes {
        return Err(user_error("08P01", "invalid PostgreSQL frame length"));
    }
    if frame_bytes > frame_limit {
        return Err(user_error(
            "54000",
            format!("PostgreSQL frontend frame cannot exceed {frame_limit} bytes"),
        ));
    }
    let frame_complete = tokio::time::timeout_at(
        frame_deadline,
        fill_frontend_bytes(io, inbound, frame_bytes),
    )
    .await
    .map_err(|_| user_error("57014", "timed out waiting for a complete PostgreSQL frame"))??;
    if !frame_complete {
        return Err(user_error("08P01", "truncated PostgreSQL frontend frame"));
    }

    let mut frame = inbound.split_to(frame_bytes);
    if inbound.is_empty() {
        *inbound = BytesMut::new();
    }
    let mut context = DecodeContext::default();
    context.protocol_version = protocol_version;
    context.awaiting_frontend_ssl = false;
    context.awaiting_frontend_startup = startup;
    if startup {
        validate_startup_frame(&frame)?;
    } else if matches!(state, PgWireConnectionState::AuthenticationInProgress)
        && (frame.len() < 6 || frame.last() != Some(&0))
    {
        return Err(user_error(
            "08P01",
            "invalid PostgreSQL cleartext password frame",
        ));
    }
    let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        PgWireFrontendMessage::decode(&mut frame, &context)
    }))
    .map_err(|_| user_error("08P01", "malformed PostgreSQL frontend frame"))?;
    let message = decoded?.ok_or_else(|| {
        user_error(
            "08P01",
            "complete PostgreSQL frontend frame could not be decoded",
        )
    })?;
    if !frame.is_empty() {
        return Err(user_error(
            "08P01",
            "PostgreSQL frontend frame contains trailing bytes",
        ));
    }
    Ok(Some(message))
}

fn validate_startup_frame(frame: &[u8]) -> PgWireResult<()> {
    if frame.len() < 8 {
        return Err(user_error("08P01", "PostgreSQL startup frame is too short"));
    }
    let code = u32::from_be_bytes(frame[4..8].try_into().expect("startup code"));
    if code == CANCEL_REQUEST_CODE {
        if frame.len() == CANCEL_REQUEST_LENGTH as usize {
            return Ok(());
        }
        return Err(user_error("08P01", "invalid PostgreSQL cancel frame"));
    }
    let mut cursor = 8;
    loop {
        let Some(key_end) = frame[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| cursor + offset)
        else {
            return Err(user_error(
                "08P01",
                "unterminated PostgreSQL startup parameter",
            ));
        };
        if key_end == cursor {
            if key_end + 1 == frame.len() {
                return Ok(());
            }
            return Err(user_error(
                "08P01",
                "trailing bytes after PostgreSQL startup parameters",
            ));
        }
        cursor = key_end + 1;
        let Some(value_end) = frame[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| cursor + offset)
        else {
            return Err(user_error(
                "08P01",
                "unterminated PostgreSQL startup parameter value",
            ));
        };
        cursor = value_end + 1;
        if cursor >= frame.len() {
            return Err(user_error(
                "08P01",
                "PostgreSQL startup parameters are missing their terminator",
            ));
        }
    }
}

async fn fill_frontend_bytes<S>(
    io: &mut S,
    inbound: &mut BytesMut,
    required: usize,
) -> PgWireResult<bool>
where
    S: AsyncRead + Unpin,
{
    let mut chunk = [0_u8; 8 * 1024];
    while inbound.len() < required {
        let missing = required - inbound.len();
        let read_limit = missing.min(chunk.len());
        let read = io.read(&mut chunk[..read_limit]).await?;
        if read == 0 {
            return Ok(false);
        }
        inbound.extend_from_slice(&chunk[..read]);
    }
    Ok(true)
}

fn frontend_frame_limit(message_type: u8) -> usize {
    match message_type {
        b'Q' => QUERY_FRAME_BYTES,
        b'P' => PARSE_FRAME_BYTES,
        b'B' => BIND_FRAME_BYTES,
        _ => OTHER_FRAME_BYTES,
    }
}

impl Drop for PostgresServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct DatabaseAuth {
    database: String,
    password: String,
}

impl Debug for DatabaseAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatabaseAuth")
            .field("database", &self.database)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl AuthSource for DatabaseAuth {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if login.user() != Some(POSTGRES_USER) {
            return Err(fatal_error("28000", "invalid PostgreSQL user"));
        }
        if login.database() != Some(self.database.as_str()) {
            return Err(fatal_error(
                "3D000",
                "database does not exist on this Jazz server",
            ));
        }
        Ok(Password::new(None, self.password.as_bytes().to_vec()))
    }
}

struct PostgresHandlerFactory {
    backend: Arc<PostgresBackend>,
    auth: Arc<DatabaseAuth>,
    connection_manager: Arc<ConnectionManager>,
}

impl PgWireServerHandlers for PostgresHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.backend.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = "16.6".to_owned();
        parameters.default_transaction_read_only = true;
        parameters.search_path = "public".to_owned();
        Arc::new(
            CleartextPasswordAuthStartupHandler::new((*self.auth).clone(), parameters)
                .with_connection_manager(self.connection_manager.clone()),
        )
    }

    fn cancel_handler(&self) -> Arc<impl pgwire::api::cancel::CancelHandler> {
        Arc::new(DefaultCancelHandler::new(self.connection_manager.clone()))
    }
}

struct PostgresBackend {
    state: Arc<ServerState>,
    database: String,
    parser: Arc<ParsedQueryParser>,
    database_job: Arc<Semaphore>,
    buffered_responses: Arc<Semaphore>,
}

#[derive(Default)]
struct ConnectionResources {
    usage: Mutex<ConnectionResourceUsage>,
}

#[derive(Default)]
struct ConnectionResourceUsage {
    statements: HashMap<String, usize>,
    statement_bytes: usize,
    portals: HashMap<String, PortalResourceUsage>,
    portal_bytes: usize,
    response_permits: HashMap<String, Arc<tokio::sync::OwnedSemaphorePermit>>,
}

struct PortalResourceUsage {
    bytes: usize,
    statement_name: String,
}

impl ConnectionResources {
    fn reserve_statement(&self, name: &str, bytes: usize) -> PgWireResult<()> {
        let mut usage = self.usage.lock().expect("PostgreSQL resource lock");
        let existing = usage.statements.get(name).copied();
        if existing.is_none() && usage.statements.len() >= MAX_STORED_STATEMENTS {
            return Err(user_error(
                "54000",
                format!(
                    "a PostgreSQL connection cannot retain more than {MAX_STORED_STATEMENTS} prepared statements"
                ),
            ));
        }
        let without_existing = usage.statement_bytes.saturating_sub(existing.unwrap_or(0));
        let next_bytes = without_existing
            .checked_add(bytes)
            .ok_or_else(|| user_error("54000", "stored PostgreSQL statement size overflow"))?;
        if next_bytes > MAX_STORED_STATEMENT_BYTES {
            return Err(user_error(
                "54000",
                format!(
                    "stored PostgreSQL statements cannot exceed {} MiB per connection",
                    MAX_STORED_STATEMENT_BYTES / (1024 * 1024)
                ),
            ));
        }
        usage.statements.insert(name.to_owned(), bytes);
        usage.statement_bytes = next_bytes;
        Ok(())
    }

    fn release_statement(&self, name: &str) -> Vec<String> {
        let mut usage = self.usage.lock().expect("PostgreSQL resource lock");
        if let Some(bytes) = usage.statements.remove(name) {
            usage.statement_bytes = usage.statement_bytes.saturating_sub(bytes);
        }
        let dependent_portals = usage
            .portals
            .iter()
            .filter(|(_, portal)| portal.statement_name == name)
            .map(|(portal_name, _)| portal_name.clone())
            .collect::<Vec<_>>();
        for portal_name in &dependent_portals {
            if let Some(portal) = usage.portals.remove(portal_name) {
                usage.portal_bytes = usage.portal_bytes.saturating_sub(portal.bytes);
            }
            usage.response_permits.remove(portal_name);
        }
        dependent_portals
    }

    fn reserve_portal(&self, name: &str, statement_name: &str, bytes: usize) -> PgWireResult<()> {
        let mut usage = self.usage.lock().expect("PostgreSQL resource lock");
        let existing = usage.portals.get(name).map(|portal| portal.bytes);
        if existing.is_none() && usage.portals.len() >= MAX_STORED_PORTALS {
            return Err(user_error(
                "54000",
                format!(
                    "a PostgreSQL connection cannot retain more than {MAX_STORED_PORTALS} portals"
                ),
            ));
        }
        let without_existing = usage.portal_bytes.saturating_sub(existing.unwrap_or(0));
        let next_bytes = without_existing
            .checked_add(bytes)
            .ok_or_else(|| user_error("54000", "stored PostgreSQL portal size overflow"))?;
        if next_bytes > MAX_STORED_PORTAL_BYTES {
            return Err(user_error(
                "54000",
                format!(
                    "stored PostgreSQL portal parameters cannot exceed {} MiB per connection",
                    MAX_STORED_PORTAL_BYTES / (1024 * 1024)
                ),
            ));
        }
        usage.portals.insert(
            name.to_owned(),
            PortalResourceUsage {
                bytes,
                statement_name: statement_name.to_owned(),
            },
        );
        usage.portal_bytes = next_bytes;
        Ok(())
    }

    fn release_portal(&self, name: &str) {
        let mut usage = self.usage.lock().expect("PostgreSQL resource lock");
        if let Some(portal) = usage.portals.remove(name) {
            usage.portal_bytes = usage.portal_bytes.saturating_sub(portal.bytes);
        }
        usage.response_permits.remove(name);
    }

    fn clear_portals(&self) {
        let mut usage = self.usage.lock().expect("PostgreSQL resource lock");
        usage.portals.clear();
        usage.portal_bytes = 0;
        usage.response_permits.clear();
    }

    fn retain_response(&self, portal_name: &str, permit: Arc<tokio::sync::OwnedSemaphorePermit>) {
        self.usage
            .lock()
            .expect("PostgreSQL resource lock")
            .response_permits
            .insert(portal_name.to_owned(), permit);
    }

    fn release_response(&self, portal_name: &str) {
        self.usage
            .lock()
            .expect("PostgreSQL resource lock")
            .response_permits
            .remove(portal_name);
    }
}

fn connection_resources<C: ClientInfo>(client: &C) -> Arc<ConnectionResources> {
    client
        .session_extensions()
        .get_or_insert_with(ConnectionResources::default)
}

fn remove_statement_and_dependent_portals<C>(client: &mut C, statement_name: &str)
where
    C: ClientInfo + ClientPortalStore,
    C::PortalStore: PortalStore,
{
    let dependent_portals = connection_resources(client).release_statement(statement_name);
    for portal_name in dependent_portals {
        client.portal_store().rm_portal(&portal_name);
    }
    client.portal_store().rm_statement(statement_name);
}

fn clear_connection_portals<C>(client: &mut C)
where
    C: ClientInfo + ClientPortalStore,
    C::PortalStore: PortalStore,
{
    connection_resources(client).clear_portals();
    client.portal_store().clear_portals();
}

fn ensure_object_name(name: &str, kind: &str) -> PgWireResult<()> {
    if name.len() > MAX_OBJECT_NAME_BYTES {
        return Err(user_error(
            "42622",
            format!("PostgreSQL {kind} names cannot exceed {MAX_OBJECT_NAME_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn validate_format_codes(codes: &[i16], expected_columns: usize, kind: &str) -> PgWireResult<()> {
    if !(codes.is_empty() || codes.len() == 1 || codes.len() == expected_columns) {
        return Err(user_error(
            "08P01",
            format!(
                "Bind supplied {} {kind} format codes for {expected_columns} values; expected 0, 1, or {expected_columns}",
                codes.len()
            ),
        ));
    }
    if codes.iter().any(|code| !matches!(code, 0 | 1)) {
        return Err(user_error(
            "08P01",
            format!("Bind {kind} format codes must be 0 (text) or 1 (binary)"),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct PreparedStatement {
    parsed: ParsedStatement,
    parameter_kinds: Vec<ColumnKind>,
    result_columns: Vec<OutputColumn>,
    schema_version: Option<SchemaVersionId>,
}

#[derive(Debug)]
struct ParsedQueryParser;

#[async_trait]
impl QueryParser for ParsedQueryParser {
    type Statement = PreparedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        ensure_sql_size(sql)?;
        Ok(PreparedStatement {
            parsed: sql::parse_sql(sql).map_err(sql_error)?,
            parameter_kinds: Vec::new(),
            result_columns: Vec::new(),
            schema_version: None,
        })
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(Vec::new())
    }

    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl SimpleQueryHandler for PostgresBackend {
    async fn do_query<C>(&self, client: &mut C, sql: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        ensure_sql_size(sql)?;
        // PostgreSQL's simple Query message replaces the unnamed statement and
        // every portal derived from it, even when named extended-protocol
        // objects remain on the connection.
        remove_statement_and_dependent_portals(client, DEFAULT_NAME);
        let statements = sql::parse_sql_batch(sql).map_err(sql_error)?;
        if statements.len() > MAX_SIMPLE_BATCH_STATEMENTS {
            return Err(user_error(
                "54000",
                format!(
                    "a simple-query batch cannot contain more than {MAX_SIMPLE_BATCH_STATEMENTS} statements"
                ),
            ));
        }
        if statements.len() > 1
            && statements.iter().any(|statement| {
                matches!(
                    statement,
                    ParsedStatement::Command(Command::Begin | Command::Commit | Command::Rollback)
                )
            })
        {
            return Err(user_error(
                "0A000",
                "transaction control commands must be sent as individual PostgreSQL statements",
            ));
        }
        if client.transaction_status() == TransactionStatus::Error {
            if statements.len() == 1
                && matches!(
                    statements.first(),
                    Some(ParsedStatement::Command(
                        Command::Rollback | Command::Commit
                    ))
                )
            {
                clear_connection_portals(client);
                return Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]);
            }
            return Err(failed_transaction_error());
        }
        let application_table_reads = statements
            .iter()
            .filter(|statement| {
                matches!(
                    statement,
                    ParsedStatement::Select(SelectPlan {
                        source: SelectSource::Table(_),
                        ..
                    })
                )
            })
            .count();
        if application_table_reads > 1 {
            return Err(user_error(
                "0A000",
                "simple-query batches may contain at most one application-table SELECT; send additional table reads as separate queries",
            ));
        }
        let row_responses = statements
            .iter()
            .filter(|statement| {
                matches!(
                    statement,
                    ParsedStatement::Select(_) | ParsedStatement::Command(Command::Show(_))
                )
            })
            .count();
        if row_responses > MAX_BUFFERED_RESPONSES {
            return Err(user_error(
                "54000",
                format!(
                    "simple-query batches may contain at most {MAX_BUFFERED_RESPONSES} row-producing statements"
                ),
            ));
        }
        let mut responses = Vec::with_capacity(statements.len());
        let ends_transaction = statements.iter().any(|statement| {
            matches!(
                statement,
                ParsedStatement::Command(Command::Commit | Command::Rollback)
            )
        });
        for statement in statements {
            responses.push(
                self.execute(statement, &[], &Format::UnifiedText, None)
                    .await?
                    .response,
            );
        }
        if ends_transaction {
            clear_connection_portals(client);
        }
        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresBackend {
    type Statement = PreparedStatement;
    type QueryParser = ParsedQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        ensure_sql_size(&message.query)?;
        let statement_name = message
            .name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());
        ensure_object_name(&statement_name, "prepared statement")?;
        if statement_name != DEFAULT_NAME
            && client
                .portal_store()
                .get_statement(&statement_name)
                .is_some()
        {
            return Err(user_error(
                "42P05",
                format!("prepared statement {statement_name:?} already exists"),
            ));
        }
        let parsed = sql::parse_sql(&message.query).map_err(sql_error)?;
        let parameter_kinds = self.parameter_kinds(&parsed).await?;
        validate_declared_parameter_types(&message.type_oids, &parameter_kinds)?;
        let result_columns = self.describe(&parsed, &Format::UnifiedBinary).await?;
        let schema_version = self.prepared_schema_version(&parsed).await?;
        let retained_statement_bytes = message
            .query
            .len()
            .checked_add(statement_name.len())
            .and_then(|bytes| bytes.checked_add(parameter_kinds.len() * 8))
            .and_then(|bytes| {
                result_columns.iter().try_fold(bytes, |total, column| {
                    total.checked_add(std::mem::size_of::<OutputColumn>() + column.name.len())
                })
            })
            .ok_or_else(|| user_error("54000", "stored PostgreSQL statement size overflow"))?;
        let statement = StoredStatement::new(
            statement_name.clone(),
            PreparedStatement {
                parsed,
                parameter_kinds: parameter_kinds.clone(),
                result_columns,
                schema_version,
            },
            parameter_kinds
                .into_iter()
                .map(|kind| Some(kind.pg_type()))
                .collect(),
        );
        if statement_name == DEFAULT_NAME {
            remove_statement_and_dependent_portals(client, DEFAULT_NAME);
        }
        connection_resources(client)
            .reserve_statement(&statement_name, retained_statement_bytes)?;
        client.portal_store().put_statement(Arc::new(statement));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let portal_name = message
            .portal_name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());
        ensure_object_name(statement_name, "prepared statement")?;
        ensure_object_name(&portal_name, "portal")?;
        if message.parameters.len() > MAX_PARAMETER_COUNT
            || message.parameter_format_codes.len() > MAX_PARAMETER_COUNT
            || message.result_column_format_codes.len() > MAX_PARAMETER_COUNT
        {
            return Err(user_error(
                "54000",
                format!(
                    "Bind cannot contain more than {MAX_PARAMETER_COUNT} parameters or format codes"
                ),
            ));
        }
        let parameter_bytes = message
            .parameters
            .iter()
            .try_fold(0_usize, |total, value| {
                total
                    .checked_add(value.as_ref().map_or(0, bytes::Bytes::len))
                    .ok_or_else(|| user_error("54000", "PostgreSQL parameter size overflow"))
            })?;
        if parameter_bytes > MAX_PARAMETER_BYTES {
            return Err(user_error(
                "54000",
                format!(
                    "encoded PostgreSQL parameters cannot exceed {} MiB",
                    MAX_PARAMETER_BYTES / (1024 * 1024)
                ),
            ));
        }
        let retained_portal_bytes = parameter_bytes
            .checked_add(statement_name.len())
            .and_then(|bytes| bytes.checked_add(portal_name.len()))
            .and_then(|bytes| {
                bytes.checked_add(
                    message.parameters.len() * std::mem::size_of::<Option<bytes::Bytes>>(),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    (message.parameter_format_codes.len()
                        + message.result_column_format_codes.len())
                        * 2,
                )
            })
            .ok_or_else(|| user_error("54000", "stored PostgreSQL portal size overflow"))?;
        let Some(statement) = client.portal_store().get_statement(statement_name) else {
            return Err(PgWireError::StatementNotFound(statement_name.to_owned()));
        };
        if portal_name != DEFAULT_NAME && client.portal_store().get_portal(&portal_name).is_some() {
            return Err(user_error(
                "42P03",
                format!("portal {portal_name:?} already exists"),
            ));
        }
        if message.parameters.len() != statement.statement.parameter_kinds.len() {
            return Err(user_error(
                "08P01",
                format!(
                    "prepared statement expects {} parameters, received {}",
                    statement.statement.parameter_kinds.len(),
                    message.parameters.len()
                ),
            ));
        }
        validate_format_codes(
            &message.parameter_format_codes,
            statement.statement.parameter_kinds.len(),
            "parameter",
        )?;
        validate_format_codes(
            &message.result_column_format_codes,
            statement.statement.result_columns.len(),
            "result-column",
        )?;
        let portal = Portal::try_new(&message, statement)?;
        if portal_name == DEFAULT_NAME {
            connection_resources(client).release_portal(DEFAULT_NAME);
            client.portal_store().rm_portal(DEFAULT_NAME);
        }
        connection_resources(client).reserve_portal(
            &portal_name,
            statement_name,
            retained_portal_bytes,
        )?;
        client.portal_store().put_portal(Arc::new(portal));
        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Sync closes the protocol's implicit transaction. Portals are
        // transaction-scoped in PostgreSQL, so only an explicit BEGIN keeps
        // them alive across this boundary; prepared statements remain.
        if client.transaction_status() == TransactionStatus::Idle {
            clear_connection_portals(client);
        }
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                client.transaction_status(),
            )))
            .await?;
        client.flush().await?;
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME).to_owned();
        let ends_transaction =
            client
                .portal_store()
                .get_portal(&portal_name)
                .is_some_and(|portal| {
                    matches!(
                        portal.statement.statement.parsed,
                        ParsedStatement::Command(Command::Commit | Command::Rollback)
                    )
                });
        let result = self._on_execute(client, message).await;
        connection_resources(client).release_response(&portal_name);
        if result.is_ok() && ends_transaction {
            clear_connection_portals(client);
        }
        result
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        ensure_object_name(name, "object")?;
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                remove_statement_and_dependent_portals(client, name);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                connection_resources(client).release_portal(name);
                client.portal_store().rm_portal(name);
            }
            target => return Err(PgWireError::InvalidTargetType(target)),
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await?;
        Ok(())
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if max_rows != 0 {
            return Err(user_error(
                "0A000",
                "incremental portal fetch is not supported; use LIMIT/OFFSET or keyset pagination",
            ));
        }
        let statement = &portal.statement.statement;
        if client.transaction_status() == TransactionStatus::Error {
            return match statement.parsed {
                ParsedStatement::Command(Command::Rollback | Command::Commit) => {
                    Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
                }
                _ => Err(failed_transaction_error()),
            };
        }
        let current_parameter_kinds = self.parameter_kinds(&statement.parsed).await?;
        let current_result_columns = self
            .describe(&statement.parsed, &portal.result_column_format)
            .await?;
        if current_parameter_kinds != statement.parameter_kinds
            || !same_output_schema(&current_result_columns, &statement.result_columns)
        {
            return Err(user_error(
                "0A000",
                "cached PostgreSQL plan changed after a Jazz schema update; prepare it again",
            ));
        }
        let parameters = decode_parameters(portal, &statement.parameter_kinds)?;
        let executed = self
            .execute(
                statement.parsed.clone(),
                &parameters,
                &portal.result_column_format,
                statement.schema_version,
            )
            .await?;
        if let Some(response_permit) = executed.response_permit {
            connection_resources(client).retain_response(&portal.name, response_permit);
        }
        Ok(executed.response)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let prepared = &statement.statement;
        let parameter_types = prepared
            .parameter_kinds
            .iter()
            .copied()
            .map(|kind| kind.pg_type())
            .collect();
        let fields = prepared
            .result_columns
            .iter()
            .cloned()
            .enumerate()
            .map(|(idx, column)| column.field_info(FieldFormat::Binary, idx))
            .collect();
        Ok(DescribeStatementResponse::new(parameter_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = portal
            .statement
            .statement
            .result_columns
            .iter()
            .cloned()
            .enumerate()
            .map(|(idx, column)| {
                let format = portal.result_column_format.format_for(idx);
                column.field_info(format, idx)
            })
            .collect();
        Ok(DescribePortalResponse::new(fields))
    }
}

impl PostgresBackend {
    fn shell(&self) -> PgWireResult<ServerShellHandle> {
        self.state.core_server_shell().ok_or_else(|| {
            user_error(
                "55000",
                "no runtime schema has been published for this Jazz app",
            )
        })
    }

    async fn schema(&self) -> PgWireResult<JazzSchema> {
        self.schema_optional().await?.ok_or_else(|| {
            user_error(
                "55000",
                "no runtime schema has been published for this Jazz app",
            )
        })
    }

    async fn schema_optional(&self) -> PgWireResult<Option<JazzSchema>> {
        let Some(shell) = self.state.core_server_shell() else {
            return Ok(None);
        };
        shell
            .postgres_schema()
            .await
            .map(Some)
            .map_err(internal_database_error)
    }

    async fn prepared_schema_version(
        &self,
        statement: &ParsedStatement,
    ) -> PgWireResult<Option<SchemaVersionId>> {
        match statement {
            ParsedStatement::Select(SelectPlan {
                source: SelectSource::Table(_),
                ..
            }) => Ok(Some(self.schema().await?.version_id())),
            _ => Ok(None),
        }
    }

    async fn execute(
        &self,
        statement: ParsedStatement,
        parameters: &[ParameterValue],
        format: &Format,
        expected_schema_version: Option<SchemaVersionId>,
    ) -> PgWireResult<ExecutedResponse> {
        match statement {
            ParsedStatement::Select(plan) => {
                let output = self
                    .execute_select(plan, parameters, expected_schema_version)
                    .await?;
                output.into_response(format)
            }
            ParsedStatement::Command(command) => self.execute_command(command, format),
        }
    }

    fn execute_command(&self, command: Command, format: &Format) -> PgWireResult<ExecutedResponse> {
        match command {
            Command::Show(setting) => {
                let value = match setting.to_ascii_lowercase().as_str() {
                    "server_version" => "16.6",
                    "search_path" => "public",
                    "transaction_read_only" | "default_transaction_read_only" => "on",
                    "standard_conforming_strings" => "on",
                    "client_encoding" | "server_encoding" => "UTF8",
                    "timezone" | "time.zone" => "Etc/UTC",
                    other => {
                        return Err(user_error(
                            "42704",
                            format!("unrecognized configuration parameter {other}"),
                        ));
                    }
                };
                QueryOutput {
                    columns: vec![OutputColumn::new(setting, ColumnKind::Text)],
                    rows: vec![vec![Cell::Text(value.to_owned())]],
                    response_permit: None,
                }
                .into_response(format)
            }
            Command::Begin => Ok(ExecutedResponse::without_permit(
                Response::TransactionStart(Tag::new("BEGIN")),
            )),
            Command::Commit => Ok(ExecutedResponse::without_permit(Response::TransactionEnd(
                Tag::new("COMMIT"),
            ))),
            Command::Rollback => Ok(ExecutedResponse::without_permit(Response::TransactionEnd(
                Tag::new("ROLLBACK"),
            ))),
        }
    }

    async fn execute_select(
        &self,
        plan: SelectPlan,
        parameters: &[ParameterValue],
        expected_schema_version: Option<SchemaVersionId>,
    ) -> PgWireResult<QueryOutput> {
        let limit = plan
            .limit
            .as_ref()
            .map(|value| resolve_page_value(value, parameters))
            .transpose()?;
        let offset = resolve_page_value(&plan.offset, parameters)?;
        if limit.is_some_and(|limit| limit > MAX_PAGE_SIZE) {
            return Err(user_error(
                "54000",
                format!("LIMIT cannot exceed {MAX_PAGE_SIZE} rows"),
            ));
        }
        if offset > MAX_OFFSET {
            return Err(user_error(
                "54000",
                format!("OFFSET cannot exceed {MAX_OFFSET}; use keyset pagination"),
            ));
        }
        let response_permit = self
            .buffered_responses
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                user_error(
                    "53000",
                    "too many PostgreSQL responses are still being consumed; finish or close an existing result before retrying",
                )
            })?;
        let source = plan.source.clone();
        match source {
            SelectSource::Table(table) => {
                if limit.is_none() {
                    return Err(user_error(
                        "54000",
                        format!("application-table reads require LIMIT (maximum {MAX_PAGE_SIZE})"),
                    ));
                }
                self.execute_table(
                    plan,
                    table,
                    parameters,
                    limit.expect("checked above"),
                    offset,
                    expected_schema_version,
                    response_permit,
                )
                .await
            }
            SelectSource::Databases | SelectSource::Tables | SelectSource::Columns => {
                let mut output = self
                    .execute_catalogue(plan, parameters, limit, offset)
                    .await?;
                output.response_permit = Some(Arc::new(response_permit));
                Ok(output)
            }
            SelectSource::Session => {
                let mut output = self.execute_session(plan, parameters, limit, offset)?;
                output.response_permit = Some(Arc::new(response_permit));
                Ok(output)
            }
        }
    }

    async fn execute_table(
        &self,
        plan: SelectPlan,
        table_name: String,
        parameters: &[ParameterValue],
        limit: usize,
        offset: usize,
        expected_schema_version: Option<SchemaVersionId>,
        response_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> PgWireResult<QueryOutput> {
        let query_permit = self
            .database_job
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| internal_error("PostgreSQL database query gate is closed"))?;
        let query_guard = self
            .state
            .shutdown
            .try_enter_postgres_query()
            .ok_or_else(|| user_error("57P01", "Jazz server is shutting down"))?;
        let schema = self.schema().await?;
        let schema_version = schema.version_id();
        if expected_schema_version.is_some_and(|expected| expected != schema_version) {
            return Err(user_error(
                "0A000",
                "cached PostgreSQL plan changed after a Jazz schema update; prepare it again",
            ));
        }
        let table = table_by_name(&schema, &table_name)?;
        let columns = projected_table_columns(&plan, table)?;
        let mut query = Query::from(table_name);
        if let Some(filter) = &plan.filter {
            let (predicate, bindings) = lower_table_filter(filter, table, parameters)?;
            query = query.filter(predicate);
            query = query.select(columns.iter().map(|column| column.source_name.clone()));
            for order in &plan.order_by {
                let column = ensure_table_column(table, &order.column)?;
                ensure_orderable_column(column, &order.column)?;
                query = query.order_by(
                    order.column.clone(),
                    if order.ascending {
                        OrderDirection::Asc
                    } else {
                        OrderDirection::Desc
                    },
                );
            }
            query = query.limit(limit);
            query = query.offset(offset);
            let result = self
                .shell()?
                .postgres_query(
                    query,
                    schema_version,
                    columns
                        .iter()
                        .map(|column| column.source_name.clone())
                        .collect(),
                    bindings,
                    query_permit,
                    response_permit,
                    query_guard,
                    MAX_RESPONSE_BYTES,
                )
                .await
                .map_err(postgres_database_query_error)?;
            return table_query_output(result, columns);
        }

        query = query.select(columns.iter().map(|column| column.source_name.clone()));
        for order in &plan.order_by {
            let column = ensure_table_column(table, &order.column)?;
            ensure_orderable_column(column, &order.column)?;
            query = query.order_by(
                order.column.clone(),
                if order.ascending {
                    OrderDirection::Asc
                } else {
                    OrderDirection::Desc
                },
            );
        }
        query = query.limit(limit);
        query = query.offset(offset);
        let result = self
            .shell()?
            .postgres_query(
                query,
                schema_version,
                columns
                    .iter()
                    .map(|column| column.source_name.clone())
                    .collect(),
                BTreeMap::new(),
                query_permit,
                response_permit,
                query_guard,
                MAX_RESPONSE_BYTES,
            )
            .await
            .map_err(postgres_database_query_error)?;
        table_query_output(result, columns)
    }

    async fn execute_catalogue(
        &self,
        plan: SelectPlan,
        parameters: &[ParameterValue],
        limit: Option<usize>,
        offset: usize,
    ) -> PgWireResult<QueryOutput> {
        let schema = if matches!(&plan.source, SelectSource::Databases) {
            None
        } else {
            self.schema_optional().await?
        };
        let relation = catalogue_relation(&plan.source, &self.database, schema.as_ref());
        relation.apply(plan, parameters, limit, offset)
    }

    fn execute_session(
        &self,
        plan: SelectPlan,
        parameters: &[ParameterValue],
        limit: Option<usize>,
        offset: usize,
    ) -> PgWireResult<QueryOutput> {
        if plan.filter.is_some() || !plan.order_by.is_empty() || offset != 0 {
            return Err(user_error(
                "0A000",
                "WHERE, ORDER BY, and OFFSET are not supported without FROM",
            ));
        }
        if !parameters.is_empty() {
            return Err(user_error(
                "0A000",
                "parameters are not supported in session SELECT expressions",
            ));
        }
        let mut columns = Vec::with_capacity(plan.projection.len());
        let mut row = Vec::with_capacity(plan.projection.len());
        for projection in &plan.projection {
            let (default_name, kind, cell) = match &projection.expr {
                ProjectedExpr::SessionFunction(SessionFunction::Version) => (
                    "version",
                    ColumnKind::Text,
                    Cell::Text("PostgreSQL 16.6 compatible Jazz read-only interface".to_owned()),
                ),
                ProjectedExpr::SessionFunction(SessionFunction::CurrentDatabase) => (
                    "current_database",
                    ColumnKind::Text,
                    Cell::Text(self.database.clone()),
                ),
                ProjectedExpr::SessionFunction(SessionFunction::CurrentSchema) => (
                    "current_schema",
                    ColumnKind::Text,
                    Cell::Text("public".to_owned()),
                ),
                ProjectedExpr::SessionFunction(SessionFunction::CurrentUser) => (
                    "current_user",
                    ColumnKind::Text,
                    Cell::Text(POSTGRES_USER.to_owned()),
                ),
                ProjectedExpr::Literal(literal) => {
                    let (kind, cell) = literal_cell(literal, parameters)?;
                    ("?column?", kind, cell)
                }
                _ => {
                    return Err(user_error(
                        "42703",
                        "a source-less SELECT may only project session functions or literals",
                    ));
                }
            };
            columns.push(OutputColumn::new(
                projection
                    .alias
                    .clone()
                    .unwrap_or_else(|| default_name.to_owned()),
                kind,
            ));
            row.push(cell);
        }
        let rows = if limit == Some(0) {
            Vec::new()
        } else {
            vec![row]
        };
        Ok(QueryOutput {
            columns,
            rows,
            response_permit: None,
        })
    }

    async fn describe(
        &self,
        statement: &ParsedStatement,
        _format: &Format,
    ) -> PgWireResult<Vec<OutputColumn>> {
        match statement {
            ParsedStatement::Command(Command::Show(setting)) => {
                Ok(vec![OutputColumn::new(setting.clone(), ColumnKind::Text)])
            }
            ParsedStatement::Command(_) => Ok(Vec::new()),
            ParsedStatement::Select(plan) => match &plan.source {
                SelectSource::Table(table_name) => {
                    let schema = self.schema().await?;
                    let table = table_by_name(&schema, table_name)?;
                    projected_table_columns(plan, table).map(|columns| {
                        columns
                            .into_iter()
                            .map(|column| OutputColumn::new(column.output_name, column.kind))
                            .collect()
                    })
                }
                SelectSource::Databases | SelectSource::Tables | SelectSource::Columns => {
                    let schema = if matches!(&plan.source, SelectSource::Databases) {
                        None
                    } else {
                        self.schema_optional().await?
                    };
                    let relation =
                        catalogue_relation(&plan.source, &self.database, schema.as_ref());
                    relation.projected_columns(plan)
                }
                SelectSource::Session => describe_session(plan),
            },
        }
    }

    async fn parameter_kinds(&self, statement: &ParsedStatement) -> PgWireResult<Vec<ColumnKind>> {
        let ParsedStatement::Select(plan) = statement else {
            return Ok(Vec::new());
        };
        let column_kinds = match &plan.source {
            SelectSource::Table(table_name) => {
                let schema = self.schema().await?;
                table_column_kinds(table_by_name(&schema, table_name)?)
            }
            SelectSource::Databases | SelectSource::Tables | SelectSource::Columns => {
                catalogue_column_kinds(&plan.source)
            }
            SelectSource::Session => BTreeMap::new(),
        };
        infer_parameter_kinds(plan, &column_kinds)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Bool,
    I32,
    I64,
    F64,
    Text,
    Bytea,
    Uuid,
}

impl ColumnKind {
    fn pg_type(self) -> Type {
        match self {
            Self::Bool => Type::BOOL,
            Self::I32 => Type::INT4,
            Self::I64 => Type::INT8,
            Self::F64 => Type::FLOAT8,
            Self::Text => Type::TEXT,
            Self::Bytea => Type::BYTEA,
            Self::Uuid => Type::UUID,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OutputColumn {
    name: String,
    kind: ColumnKind,
}

impl OutputColumn {
    fn new(name: impl Into<String>, kind: ColumnKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }

    fn field_info(&self, format: FieldFormat, _index: usize) -> FieldInfo {
        FieldInfo::new(self.name.clone(), None, None, self.kind.pg_type(), format)
    }
}

fn same_output_schema(left: &[OutputColumn], right: &[OutputColumn]) -> bool {
    left == right
}

#[derive(Clone, Debug)]
enum Cell {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Text(String),
    Bytea(Vec<u8>),
    Uuid(uuid::Uuid),
}

#[derive(Clone, Copy, Debug)]
struct PgUuid(uuid::Uuid);

impl ToSql for PgUuid {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if *ty != Type::UUID {
            return Err(format!("cannot encode UUID as {ty}").into());
        }
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for PgUuid {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if *ty != Type::UUID {
            return Err(format!("cannot decode {ty} as UUID").into());
        }
        Ok(Self(uuid::Uuid::from_slice(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }
}

impl ToSqlText for PgUuid {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if *ty != Type::UUID {
            return Err(format!("cannot encode UUID as {ty}").into());
        }
        out.extend_from_slice(self.0.to_string().as_bytes());
        Ok(IsNull::No)
    }
}

impl<'a> FromSqlText<'a> for PgUuid {
    fn from_sql_text(
        ty: &Type,
        input: &'a [u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if *ty != Type::UUID {
            return Err(format!("cannot decode {ty} as UUID").into());
        }
        Ok(Self(uuid::Uuid::parse_str(std::str::from_utf8(input)?)?))
    }
}

struct QueryOutput {
    columns: Vec<OutputColumn>,
    rows: Vec<Vec<Cell>>,
    response_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

struct ExecutedResponse {
    response: Response,
    response_permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

impl ExecutedResponse {
    fn without_permit(response: Response) -> Self {
        Self {
            response,
            response_permit: None,
        }
    }
}

impl QueryOutput {
    fn into_response(self, format: &Format) -> PgWireResult<ExecutedResponse> {
        let response_permit = self.response_permit;
        let fields = Arc::new(
            self.columns
                .iter()
                .enumerate()
                .map(|(idx, column)| column.field_info(format.format_for(idx), idx))
                .collect::<Vec<_>>(),
        );
        let mut encoded_rows = Vec::with_capacity(self.rows.len());
        let mut encoded_response_bytes = 0_usize;
        for row in self.rows {
            if row.len() != self.columns.len() {
                return Err(internal_error("row shape does not match result schema"));
            }
            let mut encoder = DataRowEncoder::new(fields.clone());
            for ((column, cell), _field) in self.columns.iter().zip(row.iter()).zip(fields.iter()) {
                encode_cell(&mut encoder, column.kind, cell)?;
            }
            let encoded_row = encoder.take_row();
            let encoded_row_bytes = encoded_row
                .data
                .len()
                .checked_add(DATA_ROW_WIRE_OVERHEAD)
                .ok_or_else(|| user_error("54000", "PostgreSQL response size overflow"))?;
            encoded_response_bytes = encoded_response_bytes
                .checked_add(encoded_row_bytes)
                .ok_or_else(|| user_error("54000", "PostgreSQL response size overflow"))?;
            if encoded_response_bytes > MAX_RESPONSE_BYTES {
                return Err(user_error(
                    "54000",
                    format!(
                        "encoded PostgreSQL data rows exceed the {} MiB response limit",
                        MAX_RESPONSE_BYTES / (1024 * 1024)
                    ),
                ));
            }
            encoded_rows.push(Ok(encoded_row));
        }
        let rows = stream::unfold(
            (encoded_rows.into_iter(), response_permit.clone()),
            |(mut rows, response_permit)| async move {
                rows.next().map(|row| (row, (rows, response_permit)))
            },
        );
        Ok(ExecutedResponse {
            response: Response::Query(QueryResponse::new(fields, rows)),
            response_permit,
        })
    }
}

fn encode_cell(encoder: &mut DataRowEncoder, kind: ColumnKind, cell: &Cell) -> PgWireResult<()> {
    match (kind, cell) {
        (ColumnKind::Bool, Cell::Bool(value)) => encoder.encode_field(value),
        (ColumnKind::I32, Cell::I32(value)) => encoder.encode_field(value),
        (ColumnKind::I64, Cell::I64(value)) => encoder.encode_field(value),
        (ColumnKind::F64, Cell::F64(value)) => encoder.encode_field(value),
        (ColumnKind::Text, Cell::Text(value)) => encoder.encode_field(value),
        (ColumnKind::Bytea, Cell::Bytea(value)) => encoder.encode_field(value),
        (ColumnKind::Uuid, Cell::Uuid(value)) => encoder.encode_field(&PgUuid(*value)),
        (ColumnKind::Bool, Cell::Null) => encoder.encode_field(&Option::<bool>::None),
        (ColumnKind::I32, Cell::Null) => encoder.encode_field(&Option::<i32>::None),
        (ColumnKind::I64, Cell::Null) => encoder.encode_field(&Option::<i64>::None),
        (ColumnKind::F64, Cell::Null) => encoder.encode_field(&Option::<f64>::None),
        (ColumnKind::Text, Cell::Null) => encoder.encode_field(&Option::<String>::None),
        (ColumnKind::Bytea, Cell::Null) => encoder.encode_field(&Option::<Vec<u8>>::None),
        (ColumnKind::Uuid, Cell::Null) => encoder.encode_field(&Option::<PgUuid>::None),
        _ => Err(internal_error("cell type does not match result schema")),
    }
}

#[derive(Clone, Debug)]
struct ProjectedTableColumn {
    source_name: String,
    output_name: String,
    kind: ColumnKind,
}

fn projected_table_columns(
    plan: &SelectPlan,
    table: &TableSchema,
) -> PgWireResult<Vec<ProjectedTableColumn>> {
    let mut output = Vec::new();
    for projection in &plan.projection {
        match &projection.expr {
            ProjectedExpr::Wildcard => {
                if projection.alias.is_some() {
                    return Err(user_error("42601", "a wildcard cannot have an alias"));
                }
                output.push(ProjectedTableColumn {
                    source_name: "id".to_owned(),
                    output_name: "id".to_owned(),
                    kind: ColumnKind::Uuid,
                });
                output.extend(
                    table
                        .columns
                        .iter()
                        .filter(|column| column.name != "id")
                        .map(|column| ProjectedTableColumn {
                            source_name: column.name.clone(),
                            output_name: column.name.clone(),
                            kind: jazz_column_kind(column),
                        }),
                );
            }
            ProjectedExpr::Column(name) => {
                let column = ensure_table_column(table, name)?;
                output.push(ProjectedTableColumn {
                    source_name: name.clone(),
                    output_name: projection.alias.clone().unwrap_or_else(|| name.clone()),
                    kind: column.map(jazz_column_kind).unwrap_or(ColumnKind::Uuid),
                });
            }
            _ => {
                return Err(user_error(
                    "0A000",
                    "table SELECT projections support columns and * only",
                ));
            }
        }
        ensure_result_column_count(output.len())?;
    }
    Ok(output)
}

fn ensure_result_column_count(count: usize) -> PgWireResult<()> {
    if count > MAX_RESULT_COLUMNS {
        return Err(user_error(
            "54000",
            format!("a result cannot contain more than {MAX_RESULT_COLUMNS} columns"),
        ));
    }
    Ok(())
}

fn table_query_output(
    result: PostgresQueryResult,
    columns: Vec<ProjectedTableColumn>,
) -> PgWireResult<QueryOutput> {
    let output_columns = columns
        .iter()
        .map(|column| OutputColumn::new(column.output_name.clone(), column.kind))
        .collect();
    let mut converted_bytes = result
        .rows
        .len()
        .saturating_mul(std::mem::size_of::<Vec<Cell>>());
    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            converted_bytes = converted_bytes
                .saturating_add(columns.len().saturating_mul(std::mem::size_of::<Cell>()));
            if converted_bytes > MAX_RESPONSE_BYTES {
                return Err(user_error(
                    "54000",
                    "converted PostgreSQL result exceeds the response limit",
                ));
            }
            columns
                .iter()
                .zip(row.values)
                .map(|(projected, value)| {
                    if projected.source_name == "id" {
                        return match value {
                            Some(Value::Uuid(value)) => Ok(Cell::Uuid(value)),
                            _ => Err(internal_error("row id is missing or invalid")),
                        };
                    }
                    let schema_column = result
                        .table
                        .columns
                        .iter()
                        .find(|column| column.name == projected.source_name)
                        .ok_or_else(|| internal_error("projected column disappeared"))?;
                    converted_bytes = converted_bytes
                        .saturating_add(jazz_cell_dynamic_size(value.as_ref(), schema_column)?);
                    if converted_bytes > MAX_RESPONSE_BYTES {
                        return Err(user_error(
                            "54000",
                            "converted PostgreSQL result exceeds the response limit",
                        ));
                    }
                    jazz_value_cell(value, schema_column)
                })
                .collect::<PgWireResult<Vec<_>>>()
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    Ok(QueryOutput {
        columns: output_columns,
        rows,
        response_permit: Some(Arc::new(result.response_permit)),
    })
}

fn jazz_cell_dynamic_size(
    value: Option<&Value>,
    column: &crate::schema::ColumnSchema,
) -> PgWireResult<usize> {
    let Some(value) = value else {
        return Ok(0);
    };
    Ok(match value {
        Value::String(value) => value.len(),
        Value::Bytes(value) => value.len(),
        Value::Enum(discriminant) => enum_variant(&column.column_type, *discriminant)?.len(),
        Value::Tuple(values) | Value::Array(values) => json_array_size_bound(values),
        Value::Nullable(Some(value)) => jazz_cell_dynamic_size(Some(value), column)?,
        Value::Nullable(None)
        | Value::U8(_)
        | Value::U16(_)
        | Value::U32(_)
        | Value::U64(_)
        | Value::I32(_)
        | Value::I64(_)
        | Value::F64(_)
        | Value::Bool(_)
        | Value::Uuid(_) => 0,
    })
}

fn json_array_size_bound(values: &[Value]) -> usize {
    values
        .iter()
        .fold(2_usize, |total, value| {
            total.saturating_add(json_value_size_bound(value))
        })
        .saturating_add(values.len().saturating_sub(1))
}

fn json_value_size_bound(value: &Value) -> usize {
    match value {
        Value::U8(_) => 3,
        Value::U16(_) => 5,
        Value::U32(_) => 10,
        Value::U64(_) => 20,
        Value::I32(_) => 11,
        Value::I64(_) => 20,
        Value::F64(_) => 32,
        Value::Bool(_) => 5,
        Value::String(value) => value.len().saturating_mul(6).saturating_add(2),
        Value::Bytes(value) => (value.len().saturating_add(2) / 3)
            .saturating_mul(4)
            .saturating_add(2),
        Value::Uuid(_) => 38,
        Value::Enum(_) => 3,
        Value::Tuple(values) | Value::Array(values) => json_array_size_bound(values),
        Value::Nullable(Some(value)) => json_value_size_bound(value),
        Value::Nullable(None) => 4,
    }
}

fn jazz_value_cell(
    value: Option<Value>,
    column: &crate::schema::ColumnSchema,
) -> PgWireResult<Cell> {
    let Some(value) = value else {
        return Ok(Cell::Null);
    };
    match value {
        Value::Bool(value) => Ok(Cell::Bool(value)),
        Value::I32(value) => Ok(Cell::I32(value)),
        Value::I64(value) => Ok(Cell::I64(value)),
        Value::U8(value) => Ok(Cell::I32(i32::from(value))),
        Value::U16(value) => Ok(Cell::I32(i32::from(value))),
        Value::U32(value) => Ok(Cell::I64(i64::from(value))),
        Value::U64(value) => Ok(Cell::Text(value.to_string())),
        Value::F64(value) => Ok(Cell::F64(value)),
        Value::String(value) => Ok(Cell::Text(value)),
        Value::Bytes(value) => match column.large_value {
            Some(LargeValueKind::Text) => String::from_utf8(value)
                .map(Cell::Text)
                .map_err(|_| user_error("22021", "text value is not valid UTF-8")),
            _ => Ok(Cell::Bytea(value)),
        },
        Value::Uuid(value) => Ok(Cell::Uuid(value)),
        Value::Enum(discriminant) => enum_variant(&column.column_type, discriminant)
            .map(|variant| Cell::Text(variant.to_owned())),
        Value::Tuple(values) | Value::Array(values) => {
            format_compound_value(&values).map(Cell::Text)
        }
        Value::Nullable(value) => match value {
            Some(value) => jazz_value_cell(Some(*value), column),
            None => Ok(Cell::Null),
        },
    }
}

fn format_compound_value(values: &[Value]) -> PgWireResult<String> {
    serde_json::to_string(
        &values
            .iter()
            .map(groove_value_json)
            .collect::<PgWireResult<Vec<_>>>()?,
    )
    .map_err(|error| internal_error(format!("failed to encode compound value as JSON: {error}")))
}

fn groove_value_json(value: &Value) -> PgWireResult<serde_json::Value> {
    Ok(match value {
        Value::U8(value) => (*value).into(),
        Value::U16(value) => (*value).into(),
        Value::U32(value) => (*value).into(),
        Value::U64(value) => (*value).into(),
        Value::I32(value) => (*value).into(),
        Value::I64(value) => (*value).into(),
        Value::F64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| user_error("22003", "non-finite float cannot be encoded as JSON"))?,
        Value::Bool(value) => (*value).into(),
        Value::String(value) => value.clone().into(),
        Value::Bytes(value) => base64::engine::general_purpose::STANDARD
            .encode(value)
            .into(),
        Value::Uuid(value) => value.to_string().into(),
        Value::Enum(value) => (*value).into(),
        Value::Tuple(values) | Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(groove_value_json)
                .collect::<PgWireResult<Vec<_>>>()?,
        ),
        Value::Nullable(Some(value)) => groove_value_json(value)?,
        Value::Nullable(None) => serde_json::Value::Null,
    })
}

fn enum_variant(column_type: &ColumnType, discriminant: u8) -> PgWireResult<&str> {
    match unwrap_nullable(column_type) {
        ColumnType::Enum(schema) => schema
            .variant(discriminant)
            .map_err(|error| user_error("22000", error.to_string())),
        _ => Err(internal_error("enum value has a non-enum schema")),
    }
}

fn jazz_column_kind(column: &crate::schema::ColumnSchema) -> ColumnKind {
    if column.large_value == Some(LargeValueKind::Text) {
        return ColumnKind::Text;
    }
    match unwrap_nullable(&column.column_type) {
        ColumnType::Bool => ColumnKind::Bool,
        ColumnType::I32 | ColumnType::U8 | ColumnType::U16 => ColumnKind::I32,
        ColumnType::I64 | ColumnType::U32 => ColumnKind::I64,
        ColumnType::U64 => ColumnKind::Text,
        ColumnType::F64 => ColumnKind::F64,
        ColumnType::String | ColumnType::Enum(_) | ColumnType::Tuple(_) | ColumnType::Array(_) => {
            ColumnKind::Text
        }
        ColumnType::Bytes => ColumnKind::Bytea,
        ColumnType::Uuid => ColumnKind::Uuid,
        ColumnType::Nullable(_) => unreachable!("nullable type was unwrapped"),
    }
}

fn unwrap_nullable(column_type: &ColumnType) -> &ColumnType {
    match column_type {
        ColumnType::Nullable(inner) => unwrap_nullable(inner),
        other => other,
    }
}

#[derive(Clone, Debug)]
enum ParameterValue {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Text(String),
    Bytea(Vec<u8>),
    Uuid(uuid::Uuid),
}

fn decode_parameters(
    portal: &Portal<PreparedStatement>,
    kinds: &[ColumnKind],
) -> PgWireResult<Vec<ParameterValue>> {
    if kinds.len() > MAX_PARAMETER_COUNT {
        return Err(user_error(
            "54000",
            format!("a query cannot have more than {MAX_PARAMETER_COUNT} parameters"),
        ));
    }
    if portal.parameter_len() != kinds.len() {
        return Err(user_error(
            "08P01",
            format!(
                "expected {} query parameters, received {}",
                kinds.len(),
                portal.parameter_len()
            ),
        ));
    }
    let mut parameters = Vec::with_capacity(kinds.len());
    let mut total_bytes = 0_usize;
    for (idx, kind) in kinds.iter().enumerate() {
        let parameter = match kind {
            ColumnKind::Bool => portal
                .parameter::<bool>(idx, &Type::BOOL)?
                .map(ParameterValue::Bool)
                .unwrap_or(ParameterValue::Null),
            ColumnKind::I32 => portal
                .parameter::<i32>(idx, &Type::INT4)?
                .map(ParameterValue::I32)
                .unwrap_or(ParameterValue::Null),
            ColumnKind::I64 => portal
                .parameter::<i64>(idx, &Type::INT8)?
                .map(ParameterValue::I64)
                .unwrap_or(ParameterValue::Null),
            ColumnKind::F64 => portal
                .parameter::<f64>(idx, &Type::FLOAT8)?
                .map(ParameterValue::F64)
                .unwrap_or(ParameterValue::Null),
            ColumnKind::Text => portal
                .parameter::<String>(idx, &Type::TEXT)?
                .map(ParameterValue::Text)
                .unwrap_or(ParameterValue::Null),
            ColumnKind::Bytea => portal
                .parameter::<Vec<u8>>(idx, &Type::BYTEA)?
                .map(ParameterValue::Bytea)
                .unwrap_or(ParameterValue::Null),
            ColumnKind::Uuid => portal
                .parameter::<PgUuid>(idx, &Type::UUID)?
                .map(|value| ParameterValue::Uuid(value.0))
                .unwrap_or(ParameterValue::Null),
        };
        total_bytes = total_bytes
            .checked_add(parameter_value_size(&parameter))
            .ok_or_else(|| user_error("54000", "PostgreSQL parameter size overflow"))?;
        if total_bytes > MAX_PARAMETER_BYTES {
            return Err(user_error(
                "54000",
                format!(
                    "decoded PostgreSQL parameters cannot exceed {} MiB",
                    MAX_PARAMETER_BYTES / (1024 * 1024)
                ),
            ));
        }
        parameters.push(parameter);
    }
    Ok(parameters)
}

fn parameter_value_size(parameter: &ParameterValue) -> usize {
    match parameter {
        ParameterValue::Null => 0,
        ParameterValue::Bool(_) => 1,
        ParameterValue::I32(_) => 4,
        ParameterValue::I64(_) | ParameterValue::F64(_) => 8,
        ParameterValue::Text(value) => value.len(),
        ParameterValue::Bytea(value) => value.len(),
        ParameterValue::Uuid(_) => 16,
    }
}

fn validate_declared_parameter_types(
    declared_oids: &[u32],
    inferred: &[ColumnKind],
) -> PgWireResult<()> {
    if declared_oids.len() > inferred.len() {
        return Err(user_error(
            "08P01",
            format!(
                "prepared statement declares {} parameter types but SQL contains only {} parameters",
                declared_oids.len(),
                inferred.len()
            ),
        ));
    }
    for (idx, (oid, kind)) in declared_oids.iter().zip(inferred).enumerate() {
        if *oid != 0 && Some(kind.pg_type()) != Type::from_oid(*oid) {
            return Err(user_error(
                "42804",
                format!(
                    "declared type for parameter ${} does not match inferred type {}",
                    idx + 1,
                    kind.pg_type()
                ),
            ));
        }
    }
    Ok(())
}

fn resolve_page_value(value: &PageValue, parameters: &[ParameterValue]) -> PgWireResult<usize> {
    match value {
        PageValue::Literal(value) => Ok(*value),
        PageValue::Placeholder(position) => {
            let parameter = parameters
                .get(position - 1)
                .ok_or_else(|| user_error("08P01", format!("missing parameter ${position}")))?;
            match parameter {
                ParameterValue::I64(value) => usize::try_from(*value).map_err(|_| {
                    user_error("22003", "LIMIT and OFFSET must be non-negative integers")
                }),
                ParameterValue::Null => Err(user_error(
                    "22004",
                    "LIMIT and OFFSET parameters cannot be NULL",
                )),
                _ => Err(user_error(
                    "42804",
                    "LIMIT and OFFSET parameters must have bigint type",
                )),
            }
        }
    }
}

fn lower_table_filter<'a>(
    filter: &FilterExpr,
    table: &'a TableSchema,
    parameters: &[ParameterValue],
) -> PgWireResult<(Predicate, BTreeMap<String, Value>)> {
    let mut bindings = BTreeMap::new();
    let mut binding_names = HashMap::<(usize, Option<&'a ColumnType>), String>::new();
    let mut expanded_binding_bytes = 0_usize;
    let predicate = lower_filter_node(
        filter,
        table,
        parameters,
        &mut bindings,
        &mut binding_names,
        &mut expanded_binding_bytes,
    )?;
    Ok((predicate, bindings))
}

fn lower_filter_node<'a>(
    filter: &FilterExpr,
    table: &'a TableSchema,
    parameters: &[ParameterValue],
    bindings: &mut BTreeMap<String, Value>,
    binding_names: &mut HashMap<(usize, Option<&'a ColumnType>), String>,
    expanded_binding_bytes: &mut usize,
) -> PgWireResult<Predicate> {
    match filter {
        FilterExpr::Compare {
            column,
            op,
            literal,
        } => {
            let column_schema = ensure_table_column(table, column)?;
            ensure_filterable_column(column_schema, column)?;
            let right = lower_literal_operand(
                literal,
                column_schema,
                parameters,
                bindings,
                binding_names,
                expanded_binding_bytes,
            )?;
            let left = col(column.clone());
            Ok(match op {
                CompareOp::Eq => eq(left, right),
                CompareOp::NotEq => ne(left, right),
                CompareOp::Lt => lt(left, right),
                CompareOp::LtEq => lte(left, right),
                CompareOp::Gt => gt(left, right),
                CompareOp::GtEq => gte(left, right),
            })
        }
        FilterExpr::IsNull { column, negated } => {
            let column_schema = ensure_table_column(table, column)?;
            if column_schema
                .is_none_or(|column| !matches!(column.column_type, ColumnType::Nullable(_)))
            {
                let predicate = eq(lit(Value::Bool(true)), lit(Value::Bool(false)));
                return Ok(if *negated { not(predicate) } else { predicate });
            }
            let predicate = is_null(col(column.clone()));
            Ok(if *negated { not(predicate) } else { predicate })
        }
        FilterExpr::In {
            column,
            values,
            negated,
        } => {
            let column_schema = ensure_table_column(table, column)?;
            ensure_filterable_column(column_schema, column)?;
            let values = values
                .iter()
                .map(|literal| {
                    lower_literal_operand(
                        literal,
                        column_schema,
                        parameters,
                        bindings,
                        binding_names,
                        expanded_binding_bytes,
                    )
                })
                .collect::<PgWireResult<Vec<_>>>()?;
            let predicate = in_list(col(column.clone()), values);
            Ok(if *negated { not(predicate) } else { predicate })
        }
        FilterExpr::And(left, right) => Ok(all_of([
            lower_filter_node(
                left,
                table,
                parameters,
                bindings,
                binding_names,
                expanded_binding_bytes,
            )?,
            lower_filter_node(
                right,
                table,
                parameters,
                bindings,
                binding_names,
                expanded_binding_bytes,
            )?,
        ])),
        FilterExpr::Or(left, right) => Ok(any_of([
            lower_filter_node(
                left,
                table,
                parameters,
                bindings,
                binding_names,
                expanded_binding_bytes,
            )?,
            lower_filter_node(
                right,
                table,
                parameters,
                bindings,
                binding_names,
                expanded_binding_bytes,
            )?,
        ])),
        FilterExpr::Not(inner) => Ok(not(lower_filter_node(
            inner,
            table,
            parameters,
            bindings,
            binding_names,
            expanded_binding_bytes,
        )?)),
    }
}

fn lower_literal_operand<'a>(
    literal_value: &SqlLiteral,
    column: Option<&'a crate::schema::ColumnSchema>,
    parameters: &[ParameterValue],
    bindings: &mut BTreeMap<String, Value>,
    binding_names: &mut HashMap<(usize, Option<&'a ColumnType>), String>,
    expanded_binding_bytes: &mut usize,
) -> PgWireResult<crate::query::Operand> {
    match literal_value {
        SqlLiteral::Placeholder(position) => {
            let parameter = parameters
                .get(position - 1)
                .ok_or_else(|| user_error("08P01", format!("missing parameter ${position}")))?;
            if matches!(parameter, ParameterValue::Null) {
                return Err(user_error(
                    "22004",
                    "NULL filter parameters are not supported; use IS NULL or IS NOT NULL",
                ));
            }
            let key = (*position, column.map(|column| &column.column_type));
            if let Some(name) = binding_names.get(&key) {
                return Ok(param(name.clone()));
            }
            let value = parameter_to_jazz_value(parameter, column)?;
            *expanded_binding_bytes = expanded_binding_bytes
                .checked_add(parameter_value_size(parameter))
                .ok_or_else(|| user_error("54000", "PostgreSQL binding size overflow"))?;
            if *expanded_binding_bytes > MAX_EXPANDED_BINDING_BYTES {
                return Err(user_error(
                    "54000",
                    format!(
                        "expanded PostgreSQL bindings cannot exceed {} MiB",
                        MAX_EXPANDED_BINDING_BYTES / (1024 * 1024)
                    ),
                ));
            }
            // Reuse one converted Jazz value for each PostgreSQL parameter and
            // exact target type. Different exact Jazz types can share a wire
            // OID, so those targets intentionally receive separate bindings.
            let name = format!("pg_{position}_{}", binding_names.len());
            bindings.insert(name.clone(), value);
            binding_names.insert(key, name.clone());
            Ok(param(name))
        }
        SqlLiteral::Null => Err(user_error(
            "42804",
            "use IS NULL or IS NOT NULL instead of comparing to NULL",
        )),
        other => Ok(lit(sql_literal_to_jazz_value(other, column)?)),
    }
}

fn sql_literal_to_jazz_value(
    literal: &SqlLiteral,
    column: Option<&crate::schema::ColumnSchema>,
) -> PgWireResult<Value> {
    let Some(column) = column else {
        return match literal {
            SqlLiteral::String(value) => uuid::Uuid::parse_str(value)
                .map(Value::Uuid)
                .map_err(|_| user_error("22P02", format!("invalid UUID literal {value}"))),
            _ => Err(user_error(
                "42804",
                "row id comparisons require a UUID literal",
            )),
        };
    };
    let column_type = unwrap_nullable(&column.column_type);
    match (literal, column_type) {
        (SqlLiteral::String(value), ColumnType::String) => Ok(Value::String(value.clone())),
        (SqlLiteral::String(value), ColumnType::Uuid) => uuid::Uuid::parse_str(value)
            .map(Value::Uuid)
            .map_err(|_| user_error("22P02", format!("invalid UUID literal {value}"))),
        (SqlLiteral::String(value), ColumnType::Enum(schema)) => schema
            .discriminant(value)
            .map(Value::Enum)
            .map_err(|error| user_error("22P02", error.to_string())),
        (SqlLiteral::Boolean(value), ColumnType::Bool) => Ok(Value::Bool(*value)),
        (SqlLiteral::Number(value), ColumnType::I32) => value
            .parse::<i32>()
            .map(Value::I32)
            .map_err(|_| user_error("22P02", format!("invalid integer literal {value}"))),
        (SqlLiteral::Number(value), ColumnType::I64) => value
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|_| user_error("22P02", format!("invalid bigint literal {value}"))),
        (SqlLiteral::Number(value), ColumnType::U8) => value
            .parse::<u8>()
            .map(Value::U8)
            .map_err(|_| user_error("22P02", format!("invalid unsigned literal {value}"))),
        (SqlLiteral::Number(value), ColumnType::U16) => value
            .parse::<u16>()
            .map(Value::U16)
            .map_err(|_| user_error("22P02", format!("invalid unsigned literal {value}"))),
        (SqlLiteral::Number(value), ColumnType::U32) => value
            .parse::<u32>()
            .map(Value::U32)
            .map_err(|_| user_error("22P02", format!("invalid unsigned literal {value}"))),
        (SqlLiteral::Number(value), ColumnType::U64) => value
            .parse::<u64>()
            .map(Value::U64)
            .map_err(|_| user_error("22P02", format!("invalid unsigned literal {value}"))),
        (SqlLiteral::Number(value), ColumnType::F64) => value
            .parse::<f64>()
            .map(Value::F64)
            .map_err(|_| user_error("22P02", format!("invalid float literal {value}"))),
        (SqlLiteral::Placeholder(_), _) => unreachable!("placeholder handled by caller"),
        _ => Err(user_error(
            "42804",
            format!("literal is incompatible with column {}", column.name),
        )),
    }
}

fn parameter_to_jazz_value(
    parameter: &ParameterValue,
    column: Option<&crate::schema::ColumnSchema>,
) -> PgWireResult<Value> {
    if column.is_none() {
        return match parameter {
            ParameterValue::Uuid(value) => Ok(Value::Uuid(*value)),
            ParameterValue::Null => Ok(Value::Nullable(None)),
            _ => Err(user_error("42804", "row id parameters must have UUID type")),
        };
    }
    let column = column.expect("checked above");
    if matches!(parameter, ParameterValue::Null) {
        return Ok(Value::Nullable(None));
    }
    let value = match parameter {
        ParameterValue::Null => unreachable!("handled above"),
        ParameterValue::Bool(value) => Ok(Value::Bool(*value)),
        ParameterValue::I32(value) => match unwrap_nullable(&column.column_type) {
            ColumnType::I32 => Ok(Value::I32(*value)),
            ColumnType::U8 => u8::try_from(*value)
                .map(Value::U8)
                .map_err(|_| user_error("22003", "parameter is outside u8 range")),
            ColumnType::U16 => u16::try_from(*value)
                .map(Value::U16)
                .map_err(|_| user_error("22003", "parameter is outside u16 range")),
            _ => Ok(Value::I32(*value)),
        },
        ParameterValue::I64(value) => match unwrap_nullable(&column.column_type) {
            ColumnType::U32 => u32::try_from(*value)
                .map(Value::U32)
                .map_err(|_| user_error("22003", "parameter is outside u32 range")),
            ColumnType::U64 => u64::try_from(*value)
                .map(Value::U64)
                .map_err(|_| user_error("22003", "parameter is outside u64 range")),
            _ => Ok(Value::I64(*value)),
        },
        ParameterValue::F64(value) => Ok(Value::F64(*value)),
        ParameterValue::Text(value) => match unwrap_nullable(&column.column_type) {
            ColumnType::Enum(schema) => schema
                .discriminant(value)
                .map(Value::Enum)
                .map_err(|error| user_error("22P02", error.to_string())),
            ColumnType::U64 => value
                .parse::<u64>()
                .map(Value::U64)
                .map_err(|_| user_error("22P02", "invalid unsigned bigint parameter")),
            _ => Ok(Value::String(value.clone())),
        },
        ParameterValue::Bytea(value) => Ok(Value::Bytes(value.clone())),
        ParameterValue::Uuid(value) => Ok(Value::Uuid(*value)),
    }?;
    Ok(wrap_non_null_for_column(value, &column.column_type))
}

fn wrap_non_null_for_column(value: Value, column_type: &ColumnType) -> Value {
    match column_type {
        ColumnType::Nullable(inner) => {
            Value::Nullable(Some(Box::new(wrap_non_null_for_column(value, inner))))
        }
        _ => value,
    }
}

fn table_by_name<'a>(schema: &'a JazzSchema, name: &str) -> PgWireResult<&'a TableSchema> {
    schema
        .tables
        .iter()
        .find(|table| table.name == name)
        .ok_or_else(|| user_error("42P01", format!("relation public.{name} does not exist")))
}

fn ensure_table_column<'a>(
    table: &'a TableSchema,
    name: &str,
) -> PgWireResult<Option<&'a crate::schema::ColumnSchema>> {
    if name == "id" {
        return Ok(None);
    }
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(Some)
        .ok_or_else(|| {
            user_error(
                "42703",
                format!("column {}.{name} does not exist", table.name),
            )
        })
}

fn ensure_filterable_column(
    column: Option<&crate::schema::ColumnSchema>,
    name: &str,
) -> PgWireResult<()> {
    let Some(column) = column else {
        return Ok(());
    };
    if column.large_value.is_some() {
        return Err(user_error(
            "0A000",
            format!("content comparisons are not supported for large-value column {name}"),
        ));
    }
    if is_postgres_text_surrogate(&column.column_type) {
        return Err(user_error(
            "0A000",
            format!(
                "WHERE comparisons are not supported for column {name} because its PostgreSQL text representation does not preserve Jazz comparison semantics"
            ),
        ));
    }
    Ok(())
}

fn ensure_orderable_column(
    column: Option<&crate::schema::ColumnSchema>,
    name: &str,
) -> PgWireResult<()> {
    let Some(column) = column else {
        return Ok(());
    };
    if column.large_value.is_some() {
        return Err(user_error(
            "0A000",
            format!("ORDER BY is not supported for large-value column {name}"),
        ));
    }
    if is_postgres_text_surrogate(&column.column_type) {
        return Err(user_error(
            "0A000",
            format!(
                "ORDER BY is not supported for column {name} because its PostgreSQL text representation does not preserve Jazz ordering semantics"
            ),
        ));
    }
    if matches!(column.column_type, ColumnType::Nullable(_)) {
        return Err(user_error(
            "0A000",
            format!(
                "ORDER BY nullable column {name} is not supported yet because PostgreSQL NULL ordering cannot be preserved"
            ),
        ));
    }
    Ok(())
}

fn is_postgres_text_surrogate(column_type: &ColumnType) -> bool {
    matches!(
        unwrap_nullable(column_type),
        ColumnType::U64 | ColumnType::Enum(_) | ColumnType::Tuple(_) | ColumnType::Array(_)
    )
}

fn table_column_kinds(table: &TableSchema) -> BTreeMap<String, ColumnKind> {
    let mut columns = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), jazz_column_kind(column)))
        .collect::<BTreeMap<_, _>>();
    columns.insert("id".to_owned(), ColumnKind::Uuid);
    columns
}

fn infer_parameter_kinds(
    plan: &SelectPlan,
    columns: &BTreeMap<String, ColumnKind>,
) -> PgWireResult<Vec<ColumnKind>> {
    let mut inferred = BTreeMap::<usize, ColumnKind>::new();
    if let Some(filter) = &plan.filter {
        infer_parameter_kinds_node(filter, columns, &mut inferred)?;
    }
    for page in plan.limit.iter().chain(std::iter::once(&plan.offset)) {
        if let PageValue::Placeholder(position) = page {
            insert_inferred_kind(&mut inferred, *position, ColumnKind::I64)?;
        }
    }
    if plan.projection.iter().any(|projection| {
        matches!(
            projection.expr,
            ProjectedExpr::Literal(SqlLiteral::Placeholder(_))
        )
    }) {
        return Err(user_error(
            "42P18",
            "source-less SELECT parameters are not supported",
        ));
    }
    let Some(max) = inferred.keys().next_back().copied() else {
        return Ok(Vec::new());
    };
    (1..=max)
        .map(|position| {
            inferred.get(&position).copied().ok_or_else(|| {
                user_error(
                    "42P18",
                    format!("could not determine data type of parameter ${position}"),
                )
            })
        })
        .collect()
}

fn infer_parameter_kinds_node(
    filter: &FilterExpr,
    columns: &BTreeMap<String, ColumnKind>,
    inferred: &mut BTreeMap<usize, ColumnKind>,
) -> PgWireResult<()> {
    match filter {
        FilterExpr::Compare {
            column, literal, ..
        } => infer_literal_kind(column, literal, columns, inferred),
        FilterExpr::In { column, values, .. } => {
            for literal in values {
                infer_literal_kind(column, literal, columns, inferred)?;
            }
            Ok(())
        }
        FilterExpr::And(left, right) | FilterExpr::Or(left, right) => {
            infer_parameter_kinds_node(left, columns, inferred)?;
            infer_parameter_kinds_node(right, columns, inferred)
        }
        FilterExpr::Not(inner) => infer_parameter_kinds_node(inner, columns, inferred),
        FilterExpr::IsNull { .. } => Ok(()),
    }
}

fn infer_literal_kind(
    column: &str,
    literal: &SqlLiteral,
    columns: &BTreeMap<String, ColumnKind>,
    inferred: &mut BTreeMap<usize, ColumnKind>,
) -> PgWireResult<()> {
    let SqlLiteral::Placeholder(position) = literal else {
        return Ok(());
    };
    let kind = columns
        .get(column)
        .copied()
        .ok_or_else(|| user_error("42703", format!("column {column} does not exist")))?;
    insert_inferred_kind(inferred, *position, kind)
}

fn insert_inferred_kind(
    inferred: &mut BTreeMap<usize, ColumnKind>,
    position: usize,
    kind: ColumnKind,
) -> PgWireResult<()> {
    if let Some(previous) = inferred.insert(position, kind)
        && previous != kind
    {
        return Err(user_error(
            "42P18",
            format!("parameter ${position} is used with incompatible column types"),
        ));
    }
    Ok(())
}

fn literal_cell(
    literal: &SqlLiteral,
    parameters: &[ParameterValue],
) -> PgWireResult<(ColumnKind, Cell)> {
    match literal {
        SqlLiteral::String(value) => Ok((ColumnKind::Text, Cell::Text(value.clone()))),
        SqlLiteral::Number(value) => {
            if let Ok(value) = value.parse::<i32>() {
                Ok((ColumnKind::I32, Cell::I32(value)))
            } else if let Ok(value) = value.parse::<i64>() {
                Ok((ColumnKind::I64, Cell::I64(value)))
            } else {
                value
                    .parse::<f64>()
                    .map(|value| (ColumnKind::F64, Cell::F64(value)))
                    .map_err(|_| user_error("22P02", format!("invalid number {value}")))
            }
        }
        SqlLiteral::Boolean(value) => Ok((ColumnKind::Bool, Cell::Bool(*value))),
        SqlLiteral::Null => Ok((ColumnKind::Text, Cell::Null)),
        SqlLiteral::Placeholder(position) => parameter_cell(
            parameters
                .get(position - 1)
                .ok_or_else(|| user_error("08P01", format!("missing parameter ${position}")))?,
        ),
    }
}

fn parameter_cell(parameter: &ParameterValue) -> PgWireResult<(ColumnKind, Cell)> {
    Ok(match parameter {
        ParameterValue::Null => (ColumnKind::Text, Cell::Null),
        ParameterValue::Bool(value) => (ColumnKind::Bool, Cell::Bool(*value)),
        ParameterValue::I32(value) => (ColumnKind::I32, Cell::I32(*value)),
        ParameterValue::I64(value) => (ColumnKind::I64, Cell::I64(*value)),
        ParameterValue::F64(value) => (ColumnKind::F64, Cell::F64(*value)),
        ParameterValue::Text(value) => (ColumnKind::Text, Cell::Text(value.clone())),
        ParameterValue::Bytea(value) => (ColumnKind::Bytea, Cell::Bytea(value.clone())),
        ParameterValue::Uuid(value) => (ColumnKind::Uuid, Cell::Uuid(*value)),
    })
}

fn describe_session(plan: &SelectPlan) -> PgWireResult<Vec<OutputColumn>> {
    plan.projection
        .iter()
        .map(|projection| {
            let (name, kind) = match &projection.expr {
                ProjectedExpr::SessionFunction(SessionFunction::Version) => {
                    ("version", ColumnKind::Text)
                }
                ProjectedExpr::SessionFunction(SessionFunction::CurrentDatabase) => {
                    ("current_database", ColumnKind::Text)
                }
                ProjectedExpr::SessionFunction(SessionFunction::CurrentSchema) => {
                    ("current_schema", ColumnKind::Text)
                }
                ProjectedExpr::SessionFunction(SessionFunction::CurrentUser) => {
                    ("current_user", ColumnKind::Text)
                }
                ProjectedExpr::Literal(SqlLiteral::Boolean(_)) => ("?column?", ColumnKind::Bool),
                ProjectedExpr::Literal(SqlLiteral::Number(value)) => {
                    if value.parse::<i32>().is_ok() {
                        ("?column?", ColumnKind::I32)
                    } else if value.parse::<i64>().is_ok() {
                        ("?column?", ColumnKind::I64)
                    } else {
                        ("?column?", ColumnKind::F64)
                    }
                }
                ProjectedExpr::Literal(_) => ("?column?", ColumnKind::Text),
                _ => {
                    return Err(user_error(
                        "42703",
                        "a source-less SELECT may only project session functions or literals",
                    ));
                }
            };
            Ok(OutputColumn::new(
                projection.alias.clone().unwrap_or_else(|| name.to_owned()),
                kind,
            ))
        })
        .collect()
}

struct VirtualRelation {
    columns: Vec<OutputColumn>,
    rows: Vec<Vec<Cell>>,
}

impl VirtualRelation {
    fn projected_columns(&self, plan: &SelectPlan) -> PgWireResult<Vec<OutputColumn>> {
        let mut projected = Vec::new();
        for projection in &plan.projection {
            match &projection.expr {
                ProjectedExpr::Wildcard => {
                    if projection.alias.is_some() {
                        return Err(user_error("42601", "a wildcard cannot have an alias"));
                    }
                    projected.extend(self.columns.clone());
                }
                ProjectedExpr::Column(name) => {
                    let column = self.column(name)?;
                    projected.push(OutputColumn::new(
                        projection.alias.clone().unwrap_or_else(|| name.clone()),
                        column.kind,
                    ));
                }
                _ => {
                    return Err(user_error(
                        "0A000",
                        "catalogue SELECT projections support columns and * only",
                    ));
                }
            }
            ensure_result_column_count(projected.len())?;
        }
        Ok(projected)
    }

    fn apply(
        mut self,
        plan: SelectPlan,
        parameters: &[ParameterValue],
        limit: Option<usize>,
        offset: usize,
    ) -> PgWireResult<QueryOutput> {
        if let Some(filter) = &plan.filter {
            let rows = std::mem::take(&mut self.rows);
            self.rows = rows
                .into_iter()
                .map(|row| {
                    evaluate_virtual_filter(filter, &self.columns, &row, parameters)
                        .map(|truth| (row, truth == SqlTruth::True))
                })
                .collect::<PgWireResult<Vec<_>>>()?
                .into_iter()
                .filter_map(|(row, matches)| matches.then_some(row))
                .collect();
        }
        for order in plan.order_by.iter().rev() {
            let idx = self.column_index(&order.column)?;
            self.rows.sort_by(|left, right| {
                compare_cells_for_order(&left[idx], &right[idx], order.ascending)
            });
        }

        let projection_indices = virtual_projection_indices(&self, &plan)?;
        let columns = self.projected_columns(&plan)?;
        let rows = self
            .rows
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|row| {
                projection_indices
                    .iter()
                    .map(|idx| row[*idx].clone())
                    .collect()
            })
            .collect();
        Ok(QueryOutput {
            columns,
            rows,
            response_permit: None,
        })
    }

    fn column(&self, name: &str) -> PgWireResult<&OutputColumn> {
        self.columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| user_error("42703", format!("catalogue column {name} does not exist")))
    }

    fn column_index(&self, name: &str) -> PgWireResult<usize> {
        self.columns
            .iter()
            .position(|column| column.name == name)
            .ok_or_else(|| user_error("42703", format!("catalogue column {name} does not exist")))
    }
}

fn virtual_projection_indices(
    relation: &VirtualRelation,
    plan: &SelectPlan,
) -> PgWireResult<Vec<usize>> {
    let mut indices = Vec::new();
    for projection in &plan.projection {
        match &projection.expr {
            ProjectedExpr::Wildcard => indices.extend(0..relation.columns.len()),
            ProjectedExpr::Column(name) => indices.push(relation.column_index(name)?),
            _ => {
                return Err(user_error(
                    "0A000",
                    "catalogue SELECT projections support columns and * only",
                ));
            }
        }
        ensure_result_column_count(indices.len())?;
    }
    Ok(indices)
}

fn catalogue_relation(
    source: &SelectSource,
    database: &str,
    schema: Option<&JazzSchema>,
) -> VirtualRelation {
    match source {
        SelectSource::Databases => VirtualRelation {
            columns: vec![OutputColumn::new("datname", ColumnKind::Text)],
            rows: vec![vec![Cell::Text(database.to_owned())]],
        },
        SelectSource::Tables => {
            let columns = vec![
                OutputColumn::new("table_catalog", ColumnKind::Text),
                OutputColumn::new("table_schema", ColumnKind::Text),
                OutputColumn::new("table_name", ColumnKind::Text),
                OutputColumn::new("table_type", ColumnKind::Text),
            ];
            let rows = schema
                .into_iter()
                .flat_map(|schema| &schema.tables)
                .map(|table| {
                    vec![
                        Cell::Text(database.to_owned()),
                        Cell::Text("public".to_owned()),
                        Cell::Text(table.name.clone()),
                        Cell::Text("BASE TABLE".to_owned()),
                    ]
                })
                .collect();
            VirtualRelation { columns, rows }
        }
        SelectSource::Columns => {
            let columns = vec![
                OutputColumn::new("table_catalog", ColumnKind::Text),
                OutputColumn::new("table_schema", ColumnKind::Text),
                OutputColumn::new("table_name", ColumnKind::Text),
                OutputColumn::new("column_name", ColumnKind::Text),
                OutputColumn::new("ordinal_position", ColumnKind::I32),
                OutputColumn::new("column_default", ColumnKind::Text),
                OutputColumn::new("is_nullable", ColumnKind::Text),
                OutputColumn::new("data_type", ColumnKind::Text),
                OutputColumn::new("udt_name", ColumnKind::Text),
            ];
            let mut rows = Vec::new();
            for table in schema.into_iter().flat_map(|schema| &schema.tables) {
                rows.push(vec![
                    Cell::Text(database.to_owned()),
                    Cell::Text("public".to_owned()),
                    Cell::Text(table.name.clone()),
                    Cell::Text("id".to_owned()),
                    Cell::I32(1),
                    Cell::Null,
                    Cell::Text("NO".to_owned()),
                    Cell::Text("uuid".to_owned()),
                    Cell::Text("uuid".to_owned()),
                ]);
                for (idx, column) in table
                    .columns
                    .iter()
                    .filter(|column| column.name != "id")
                    .enumerate()
                {
                    let kind = jazz_column_kind(column);
                    rows.push(vec![
                        Cell::Text(database.to_owned()),
                        Cell::Text("public".to_owned()),
                        Cell::Text(table.name.clone()),
                        Cell::Text(column.name.clone()),
                        Cell::I32(i32::try_from(idx + 2).unwrap_or(i32::MAX)),
                        Cell::Null,
                        Cell::Text(if matches!(column.column_type, ColumnType::Nullable(_)) {
                            "YES".to_owned()
                        } else {
                            "NO".to_owned()
                        }),
                        Cell::Text(information_schema_type(kind).to_owned()),
                        Cell::Text(postgres_udt_name(kind).to_owned()),
                    ]);
                }
            }
            VirtualRelation { columns, rows }
        }
        SelectSource::Table(_) | SelectSource::Session => unreachable!("not a virtual catalogue"),
    }
}

fn catalogue_column_kinds(source: &SelectSource) -> BTreeMap<String, ColumnKind> {
    let names = match source {
        SelectSource::Databases => vec![("datname", ColumnKind::Text)],
        SelectSource::Tables => vec![
            ("table_catalog", ColumnKind::Text),
            ("table_schema", ColumnKind::Text),
            ("table_name", ColumnKind::Text),
            ("table_type", ColumnKind::Text),
        ],
        SelectSource::Columns => vec![
            ("table_catalog", ColumnKind::Text),
            ("table_schema", ColumnKind::Text),
            ("table_name", ColumnKind::Text),
            ("column_name", ColumnKind::Text),
            ("ordinal_position", ColumnKind::I32),
            ("column_default", ColumnKind::Text),
            ("is_nullable", ColumnKind::Text),
            ("data_type", ColumnKind::Text),
            ("udt_name", ColumnKind::Text),
        ],
        SelectSource::Table(_) | SelectSource::Session => Vec::new(),
    };
    names
        .into_iter()
        .map(|(name, kind)| (name.to_owned(), kind))
        .collect()
}

fn evaluate_virtual_filter(
    filter: &FilterExpr,
    columns: &[OutputColumn],
    row: &[Cell],
    parameters: &[ParameterValue],
) -> PgWireResult<SqlTruth> {
    match filter {
        FilterExpr::Compare {
            column,
            op,
            literal,
        } => {
            let (idx, kind) = virtual_column(columns, column)?;
            let right = virtual_literal_cell(literal, kind, parameters)?;
            if matches!(row[idx], Cell::Null) || matches!(right, Cell::Null) {
                return Ok(SqlTruth::Unknown);
            }
            let ordering = compare_cells(&row[idx], &right);
            Ok(SqlTruth::from_bool(match op {
                CompareOp::Eq => ordering.is_eq(),
                CompareOp::NotEq => !ordering.is_eq(),
                CompareOp::Lt => ordering.is_lt(),
                CompareOp::LtEq => ordering.is_le(),
                CompareOp::Gt => ordering.is_gt(),
                CompareOp::GtEq => ordering.is_ge(),
            }))
        }
        FilterExpr::IsNull { column, negated } => {
            let (idx, _) = virtual_column(columns, column)?;
            Ok(SqlTruth::from_bool(
                matches!(row[idx], Cell::Null) != *negated,
            ))
        }
        FilterExpr::In {
            column,
            values,
            negated,
        } => {
            let (idx, kind) = virtual_column(columns, column)?;
            if matches!(row[idx], Cell::Null) {
                return Ok(SqlTruth::Unknown);
            }
            let mut truth = SqlTruth::False;
            for literal in values {
                let value = virtual_literal_cell(literal, kind, parameters)?;
                if matches!(value, Cell::Null) {
                    truth = SqlTruth::Unknown;
                } else if compare_cells(&row[idx], &value).is_eq() {
                    truth = SqlTruth::True;
                    break;
                }
            }
            Ok(if *negated { truth.not() } else { truth })
        }
        FilterExpr::And(left, right) => {
            Ok(evaluate_virtual_filter(left, columns, row, parameters)?
                .and(evaluate_virtual_filter(right, columns, row, parameters)?))
        }
        FilterExpr::Or(left, right) => Ok(evaluate_virtual_filter(left, columns, row, parameters)?
            .or(evaluate_virtual_filter(right, columns, row, parameters)?)),
        FilterExpr::Not(inner) => {
            Ok(evaluate_virtual_filter(inner, columns, row, parameters)?.not())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlTruth {
    False,
    True,
    Unknown,
}

impl SqlTruth {
    fn from_bool(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }

    fn not(self) -> Self {
        match self {
            Self::False => Self::True,
            Self::True => Self::False,
            Self::Unknown => Self::Unknown,
        }
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }
}

fn virtual_column(columns: &[OutputColumn], name: &str) -> PgWireResult<(usize, ColumnKind)> {
    columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.name == name)
        .map(|(idx, column)| (idx, column.kind))
        .ok_or_else(|| user_error("42703", format!("catalogue column {name} does not exist")))
}

fn virtual_literal_cell(
    literal: &SqlLiteral,
    kind: ColumnKind,
    parameters: &[ParameterValue],
) -> PgWireResult<Cell> {
    match literal {
        SqlLiteral::Placeholder(position) => {
            let parameter = parameters
                .get(position - 1)
                .ok_or_else(|| user_error("08P01", format!("missing parameter ${position}")))?;
            let (parameter_kind, cell) = parameter_cell(parameter)?;
            if parameter_kind != kind && !matches!(parameter, ParameterValue::Null) {
                return Err(user_error(
                    "42804",
                    "parameter type does not match catalogue column",
                ));
            }
            Ok(cell)
        }
        SqlLiteral::Null => Ok(Cell::Null),
        SqlLiteral::String(value) if kind == ColumnKind::Text => Ok(Cell::Text(value.clone())),
        SqlLiteral::Number(value) if kind == ColumnKind::I32 => value
            .parse::<i32>()
            .map(Cell::I32)
            .map_err(|_| user_error("22P02", format!("invalid integer {value}"))),
        SqlLiteral::Number(value) if kind == ColumnKind::I64 => value
            .parse::<i64>()
            .map(Cell::I64)
            .map_err(|_| user_error("22P02", format!("invalid bigint {value}"))),
        SqlLiteral::Boolean(value) if kind == ColumnKind::Bool => Ok(Cell::Bool(*value)),
        _ => Err(user_error(
            "42804",
            "literal type does not match catalogue column",
        )),
    }
}

fn compare_cells(left: &Cell, right: &Cell) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Cell::Null, Cell::Null) => Ordering::Equal,
        (Cell::Null, _) => Ordering::Less,
        (_, Cell::Null) => Ordering::Greater,
        (Cell::Bool(left), Cell::Bool(right)) => left.cmp(right),
        (Cell::I32(left), Cell::I32(right)) => left.cmp(right),
        (Cell::I64(left), Cell::I64(right)) => left.cmp(right),
        (Cell::F64(left), Cell::F64(right)) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        (Cell::Text(left), Cell::Text(right)) => left.cmp(right),
        (Cell::Bytea(left), Cell::Bytea(right)) => left.cmp(right),
        (Cell::Uuid(left), Cell::Uuid(right)) => left.cmp(right),
        _ => Ordering::Equal,
    }
}

fn compare_cells_for_order(left: &Cell, right: &Cell, ascending: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Cell::Null, Cell::Null) => Ordering::Equal,
        (Cell::Null, _) => {
            if ascending {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (_, Cell::Null) => {
            if ascending {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        _ => {
            let ordering = compare_cells(left, right);
            if ascending {
                ordering
            } else {
                ordering.reverse()
            }
        }
    }
}

fn information_schema_type(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Bool => "boolean",
        ColumnKind::I32 => "integer",
        ColumnKind::I64 => "bigint",
        ColumnKind::F64 => "double precision",
        ColumnKind::Text => "text",
        ColumnKind::Bytea => "bytea",
        ColumnKind::Uuid => "uuid",
    }
}

fn postgres_udt_name(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Bool => "bool",
        ColumnKind::I32 => "int4",
        ColumnKind::I64 => "int8",
        ColumnKind::F64 => "float8",
        ColumnKind::Text => "text",
        ColumnKind::Bytea => "bytea",
        ColumnKind::Uuid => "uuid",
    }
}

fn sql_error(error: sql::SqlError) -> PgWireError {
    user_error("0A000", error.to_string())
}

fn failed_transaction_error() -> PgWireError {
    user_error(
        "25P02",
        "current transaction is aborted; commands are ignored until ROLLBACK",
    )
}

fn ensure_sql_size(sql: &str) -> PgWireResult<()> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(user_error(
            "54000",
            format!("SQL text cannot exceed {MAX_SQL_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn internal_database_error(error: String) -> PgWireError {
    user_error("XX000", format!("Jazz database query failed: {error}"))
}

fn postgres_database_query_error(error: String) -> PgWireError {
    if error.starts_with("PostgreSQL response exceeds configured") {
        user_error("54000", error)
    } else if error.starts_with("PostgreSQL schema changed while planning") {
        user_error("40001", error)
    } else {
        internal_database_error(error)
    }
}

fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.into(),
    )))
}

fn fatal_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        code.to_owned(),
        message.into(),
    )))
}

fn internal_error(message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        message.into(),
    )))
}
