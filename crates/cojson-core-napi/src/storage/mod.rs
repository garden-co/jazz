#![allow(private_interfaces)]

use cojson_storage_fjall::FjallStorage;
use napi::bindgen_prelude::*;
use napi::Task;
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// NAPI result types
// ============================================================================

#[napi(object)]
#[derive(Clone, Debug)]
pub struct NapiCoValueResult {
  pub row_id: u32,
  pub header_json: String,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct NapiSessionResult {
  pub row_id: u32,
  pub co_value: u32,
  pub session_id: String,
  pub last_idx: u32,
  pub last_signature: String,
  pub bytes_since_last_signature: u32,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct NapiTransactionResult {
  pub ses: u32,
  pub idx: u32,
  pub tx: String,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct NapiSignatureResult {
  pub idx: u32,
  pub signature: String,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct NapiKnownStateResult {
  pub id: String,
  pub sessions: HashMap<String, u32>,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct NapiSyncUpdate {
  pub id: String,
  pub peer_id: String,
  pub synced: bool,
}

// ============================================================================
// Helper: convert FjallStorageError to napi::Error
// ============================================================================

fn to_napi_err(e: cojson_storage_fjall::FjallStorageError) -> napi::Error {
  napi::Error::from_reason(e.to_string())
}

// ============================================================================
// AsyncTask implementations
// ============================================================================

struct GetCoValueTask {
  storage: Arc<FjallStorage>,
  co_value_id: String,
}

impl Task for GetCoValueTask {
  type Output = Option<NapiCoValueResult>;
  type JsValue = Option<NapiCoValueResult>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_co_value(&self.co_value_id)
      .map(|opt| {
        opt.map(|r| NapiCoValueResult {
          row_id: r.row_id as u32,
          header_json: r.header_json,
        })
      })
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct UpsertCoValueTask {
  storage: Arc<FjallStorage>,
  id: String,
  header_json: Option<String>,
}

impl Task for UpsertCoValueTask {
  type Output = Option<u32>;
  type JsValue = Option<u32>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .upsert_co_value(&self.id, self.header_json.as_deref())
      .map(|opt| opt.map(|v| v as u32))
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct GetCoValueSessionsTask {
  storage: Arc<FjallStorage>,
  co_value_row_id: u64,
}

impl Task for GetCoValueSessionsTask {
  type Output = Vec<NapiSessionResult>;
  type JsValue = Vec<NapiSessionResult>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_co_value_sessions(self.co_value_row_id)
      .map(|sessions| {
        sessions
          .into_iter()
          .map(|s| NapiSessionResult {
            row_id: s.row_id as u32,
            co_value: s.co_value as u32,
            session_id: s.session_id,
            last_idx: s.last_idx,
            last_signature: s.last_signature,
            bytes_since_last_signature: s.bytes_since_last_signature,
          })
          .collect()
      })
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct GetSingleCoValueSessionTask {
  storage: Arc<FjallStorage>,
  co_value_row_id: u64,
  session_id: String,
}

impl Task for GetSingleCoValueSessionTask {
  type Output = Option<NapiSessionResult>;
  type JsValue = Option<NapiSessionResult>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_single_co_value_session(self.co_value_row_id, &self.session_id)
      .map(|opt| {
        opt.map(|s| NapiSessionResult {
          row_id: s.row_id as u32,
          co_value: s.co_value as u32,
          session_id: s.session_id,
          last_idx: s.last_idx,
          last_signature: s.last_signature,
          bytes_since_last_signature: s.bytes_since_last_signature,
        })
      })
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct AddSessionUpdateTask {
  storage: Arc<FjallStorage>,
  co_value_row_id: u64,
  session_id: String,
  last_idx: u32,
  last_signature: String,
  bytes_since_last_signature: u32,
}

impl Task for AddSessionUpdateTask {
  type Output = u32;
  type JsValue = u32;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .add_session_update(
        self.co_value_row_id,
        &self.session_id,
        self.last_idx,
        &self.last_signature,
        self.bytes_since_last_signature,
      )
      .map(|v| v as u32)
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct GetNewTransactionInSessionTask {
  storage: Arc<FjallStorage>,
  session_row_id: u64,
  from_idx: u32,
  to_idx: u32,
}

impl Task for GetNewTransactionInSessionTask {
  type Output = Vec<NapiTransactionResult>;
  type JsValue = Vec<NapiTransactionResult>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_new_transaction_in_session(self.session_row_id, self.from_idx, self.to_idx)
      .map(|txs| {
        txs
          .into_iter()
          .map(|t| NapiTransactionResult {
            ses: t.ses as u32,
            idx: t.idx,
            tx: t.tx,
          })
          .collect()
      })
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct AddTransactionTask {
  storage: Arc<FjallStorage>,
  session_row_id: u64,
  idx: u32,
  tx_json: String,
}

impl Task for AddTransactionTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .add_transaction(self.session_row_id, self.idx, &self.tx_json)
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(())
  }
}

struct GetSignaturesTask {
  storage: Arc<FjallStorage>,
  session_row_id: u64,
  first_new_tx_idx: u32,
}

impl Task for GetSignaturesTask {
  type Output = Vec<NapiSignatureResult>;
  type JsValue = Vec<NapiSignatureResult>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_signatures(self.session_row_id, self.first_new_tx_idx)
      .map(|sigs| {
        sigs
          .into_iter()
          .map(|s| NapiSignatureResult {
            idx: s.idx,
            signature: s.signature,
          })
          .collect()
      })
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct AddSignatureAfterTask {
  storage: Arc<FjallStorage>,
  session_row_id: u64,
  idx: u32,
  signature: String,
}

impl Task for AddSignatureAfterTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .add_signature_after(self.session_row_id, self.idx, &self.signature)
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(())
  }
}

struct MarkCoValueAsDeletedTask {
  storage: Arc<FjallStorage>,
  co_value_id: String,
}

impl Task for MarkCoValueAsDeletedTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .mark_co_value_as_deleted(&self.co_value_id)
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(())
  }
}

struct EraseCoValueButKeepTombstoneTask {
  storage: Arc<FjallStorage>,
  co_value_id: String,
}

impl Task for EraseCoValueButKeepTombstoneTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .erase_co_value_but_keep_tombstone(&self.co_value_id)
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(())
  }
}

struct GetAllCoValuesWaitingForDeleteTask {
  storage: Arc<FjallStorage>,
}

impl Task for GetAllCoValuesWaitingForDeleteTask {
  type Output = Vec<String>;
  type JsValue = Vec<String>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_all_co_values_waiting_for_delete()
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct TrackCoValuesSyncStateTask {
  storage: Arc<FjallStorage>,
  updates: Vec<(String, String, bool)>,
}

impl Task for TrackCoValuesSyncStateTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> napi::Result<Self::Output> {
    let refs: Vec<(&str, &str, bool)> = self
      .updates
      .iter()
      .map(|(id, peer, synced)| (id.as_str(), peer.as_str(), *synced))
      .collect();
    self
      .storage
      .track_co_values_sync_state(&refs)
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(())
  }
}

struct GetUnsyncedCoValueIDsTask {
  storage: Arc<FjallStorage>,
}

impl Task for GetUnsyncedCoValueIDsTask {
  type Output = Vec<String>;
  type JsValue = Vec<String>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_unsynced_co_value_ids()
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

struct StopTrackingSyncStateTask {
  storage: Arc<FjallStorage>,
  co_value_id: String,
}

impl Task for StopTrackingSyncStateTask {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .stop_tracking_sync_state(&self.co_value_id)
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(())
  }
}

struct GetCoValueKnownStateTask {
  storage: Arc<FjallStorage>,
  co_value_id: String,
}

impl Task for GetCoValueKnownStateTask {
  type Output = Option<NapiKnownStateResult>;
  type JsValue = Option<NapiKnownStateResult>;

  fn compute(&mut self) -> napi::Result<Self::Output> {
    self
      .storage
      .get_co_value_known_state(&self.co_value_id)
      .map(|opt| {
        opt.map(|ks| NapiKnownStateResult {
          id: ks.id,
          sessions: ks.sessions.into_iter().collect(),
        })
      })
      .map_err(to_napi_err)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
    Ok(output)
  }
}

// ============================================================================
// FjallStorageNapi — NAPI class
// ============================================================================

#[napi]
pub struct FjallStorageNapi {
  inner: Arc<FjallStorage>,
}

#[napi]
impl FjallStorageNapi {
  /// Open or create a fjall storage database at the given path.
  #[napi(constructor)]
  pub fn new(path: String) -> napi::Result<Self> {
    let inner = FjallStorage::open(&path).map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(Self {
      inner: Arc::new(inner),
    })
  }

  // === CoValue operations ===

  #[napi]
  pub fn get_co_value(&self, co_value_id: String) -> AsyncTask<GetCoValueTask> {
    AsyncTask::new(GetCoValueTask {
      storage: Arc::clone(&self.inner),
      co_value_id,
    })
  }

  #[napi]
  pub fn upsert_co_value(
    &self,
    id: String,
    header_json: Option<String>,
  ) -> AsyncTask<UpsertCoValueTask> {
    AsyncTask::new(UpsertCoValueTask {
      storage: Arc::clone(&self.inner),
      id,
      header_json,
    })
  }

  // === Session operations ===

  #[napi]
  pub fn get_co_value_sessions(&self, co_value_row_id: u32) -> AsyncTask<GetCoValueSessionsTask> {
    AsyncTask::new(GetCoValueSessionsTask {
      storage: Arc::clone(&self.inner),
      co_value_row_id: co_value_row_id as u64,
    })
  }

  #[napi]
  pub fn get_single_co_value_session(
    &self,
    co_value_row_id: u32,
    session_id: String,
  ) -> AsyncTask<GetSingleCoValueSessionTask> {
    AsyncTask::new(GetSingleCoValueSessionTask {
      storage: Arc::clone(&self.inner),
      co_value_row_id: co_value_row_id as u64,
      session_id,
    })
  }

  #[napi]
  pub fn add_session_update(
    &self,
    co_value_row_id: u32,
    session_id: String,
    last_idx: u32,
    last_signature: String,
    bytes_since_last_signature: u32,
  ) -> AsyncTask<AddSessionUpdateTask> {
    AsyncTask::new(AddSessionUpdateTask {
      storage: Arc::clone(&self.inner),
      co_value_row_id: co_value_row_id as u64,
      session_id,
      last_idx,
      last_signature,
      bytes_since_last_signature,
    })
  }

  // === Transaction operations ===

  #[napi]
  pub fn get_new_transaction_in_session(
    &self,
    session_row_id: u32,
    from_idx: u32,
    to_idx: u32,
  ) -> AsyncTask<GetNewTransactionInSessionTask> {
    AsyncTask::new(GetNewTransactionInSessionTask {
      storage: Arc::clone(&self.inner),
      session_row_id: session_row_id as u64,
      from_idx,
      to_idx,
    })
  }

  #[napi]
  pub fn add_transaction(
    &self,
    session_row_id: u32,
    idx: u32,
    tx_json: String,
  ) -> AsyncTask<AddTransactionTask> {
    AsyncTask::new(AddTransactionTask {
      storage: Arc::clone(&self.inner),
      session_row_id: session_row_id as u64,
      idx,
      tx_json,
    })
  }

  // === Signature operations ===

  #[napi]
  pub fn get_signatures(
    &self,
    session_row_id: u32,
    first_new_tx_idx: u32,
  ) -> AsyncTask<GetSignaturesTask> {
    AsyncTask::new(GetSignaturesTask {
      storage: Arc::clone(&self.inner),
      session_row_id: session_row_id as u64,
      first_new_tx_idx,
    })
  }

  #[napi]
  pub fn add_signature_after(
    &self,
    session_row_id: u32,
    idx: u32,
    signature: String,
  ) -> AsyncTask<AddSignatureAfterTask> {
    AsyncTask::new(AddSignatureAfterTask {
      storage: Arc::clone(&self.inner),
      session_row_id: session_row_id as u64,
      idx,
      signature,
    })
  }

  // === Deletion operations ===

  #[napi]
  pub fn mark_co_value_as_deleted(
    &self,
    co_value_id: String,
  ) -> AsyncTask<MarkCoValueAsDeletedTask> {
    AsyncTask::new(MarkCoValueAsDeletedTask {
      storage: Arc::clone(&self.inner),
      co_value_id,
    })
  }

  #[napi]
  pub fn erase_co_value_but_keep_tombstone(
    &self,
    co_value_id: String,
  ) -> AsyncTask<EraseCoValueButKeepTombstoneTask> {
    AsyncTask::new(EraseCoValueButKeepTombstoneTask {
      storage: Arc::clone(&self.inner),
      co_value_id,
    })
  }

  #[napi]
  pub fn get_all_co_values_waiting_for_delete(
    &self,
  ) -> AsyncTask<GetAllCoValuesWaitingForDeleteTask> {
    AsyncTask::new(GetAllCoValuesWaitingForDeleteTask {
      storage: Arc::clone(&self.inner),
    })
  }

  // === Sync tracking ===

  #[napi]
  pub fn track_co_values_sync_state(
    &self,
    updates: Vec<NapiSyncUpdate>,
  ) -> AsyncTask<TrackCoValuesSyncStateTask> {
    let owned: Vec<(String, String, bool)> = updates
      .into_iter()
      .map(|u| (u.id, u.peer_id, u.synced))
      .collect();
    AsyncTask::new(TrackCoValuesSyncStateTask {
      storage: Arc::clone(&self.inner),
      updates: owned,
    })
  }

  #[napi]
  pub fn get_unsynced_co_value_ids(&self) -> AsyncTask<GetUnsyncedCoValueIDsTask> {
    AsyncTask::new(GetUnsyncedCoValueIDsTask {
      storage: Arc::clone(&self.inner),
    })
  }

  #[napi]
  pub fn stop_tracking_sync_state(
    &self,
    co_value_id: String,
  ) -> AsyncTask<StopTrackingSyncStateTask> {
    AsyncTask::new(StopTrackingSyncStateTask {
      storage: Arc::clone(&self.inner),
      co_value_id,
    })
  }

  // === Known state ===

  #[napi]
  pub fn get_co_value_known_state(
    &self,
    co_value_id: String,
  ) -> AsyncTask<GetCoValueKnownStateTask> {
    AsyncTask::new(GetCoValueKnownStateTask {
      storage: Arc::clone(&self.inner),
      co_value_id,
    })
  }

  // === Lifecycle ===

  /// Close the database, flushing all pending writes to disk.
  #[napi]
  pub fn close(&self) -> napi::Result<()> {
    self
      .inner
      .close()
      .map_err(|e| napi::Error::from_reason(e.to_string()))
  }
}
