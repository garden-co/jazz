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
    assert_eq!(wire["batch_id"], open_transaction_id.to_string());
    assert!(wire.get("transaction_id").is_none());

    let transaction_id = "00000000000000000000000000000000"
        .parse::<TransactionId>()
        .expect("parse transaction id");
    assert_eq!(
        Value::TransactionId(*transaction_id.as_bytes()).column_type(),
        Some(ColumnType::TransactionId),
    );
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
