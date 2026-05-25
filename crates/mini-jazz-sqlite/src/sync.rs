use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bundle {
    pub txs: Vec<TxRecord>,
    pub projects: Vec<ProjectRecord>,
    pub todos: Vec<TodoRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxRecord {
    pub tx_id: String,
    pub node_id: String,
    pub local_epoch: i64,
    pub outcome: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub row_id: String,
    pub tx_id: String,
    pub title: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub created_by: String,
    pub updated_by: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TodoRecord {
    pub row_id: String,
    pub tx_id: String,
    pub title: String,
    pub done: bool,
    pub project_id: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub created_by: String,
    pub updated_by: String,
}
