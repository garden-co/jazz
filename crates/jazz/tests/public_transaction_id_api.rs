use jazz::db::{LocalTransactionRecord, TransactionFate, TransactionKind};
use jazz::tools::{ColumnType, OpenTransactionId, TransactionId, Value, WriteContext};
#[cfg(feature = "runtime")]
use jazz::tools::{DurabilityTier, JazzClient, JazzTransaction};

#[test]
fn public_api_uses_transaction_id_vocabulary() {
    let open_transaction_id = OpenTransactionId::new();
    let context = WriteContext::default().with_transaction_id(open_transaction_id);

    assert_eq!(context.transaction_id, Some(open_transaction_id));
    assert_eq!(context.transaction_id(), Some(open_transaction_id));
    let wire = serde_json::to_value(context).expect("serialise write context");
    assert_eq!(wire["transaction_id"], open_transaction_id.to_string());
    assert!(wire.get("batch_id").is_none());

    let transaction_id = "00000000000000000000000000000000"
        .parse::<TransactionId>()
        .expect("parse transaction id");
    assert_eq!(
        Value::TransactionId(*transaction_id.as_bytes()).column_type(),
        Some(ColumnType::TransactionId),
    );
}

#[test]
fn local_transaction_rejection_serializes_transaction_id_consistently() {
    let transaction_id = "00000000000000000000000000000000"
        .parse::<TransactionId>()
        .expect("parse transaction id");
    let record = LocalTransactionRecord {
        transaction_id,
        kind: TransactionKind::Mergeable,
        sealed: true,
        latest_settlement: TransactionFate::Rejected {
            transaction_id,
            code: "permission_denied".into(),
            reason: "write rejected by policy".into(),
        },
    };

    let wire = serde_json::to_value(record).expect("serialize local transaction record");
    assert_eq!(wire["transactionId"], transaction_id.to_string());
    assert_eq!(
        wire["latestSettlement"]["transactionId"],
        transaction_id.to_string()
    );
    assert!(wire.get("batchId").is_none());
    assert!(wire["latestSettlement"].get("batchId").is_none());
}

#[allow(dead_code)]
#[cfg(feature = "runtime")]
fn transaction_api_names(
    client: &JazzClient,
    transaction: &JazzTransaction,
    transaction_id: TransactionId,
) {
    let _: OpenTransactionId = transaction.transaction_id();
    std::mem::drop(client.wait_for_transaction(transaction_id, DurabilityTier::Local));
}
