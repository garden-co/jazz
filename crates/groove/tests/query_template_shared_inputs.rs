use groove::db::{Database, GraphBuilder};
use groove::ivm::{IvmRuntimeError, ProjectField, compile_template_graphs};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{ColumnType, DatabaseSchema};
use groove::storage::MemoryStorage;
use std::sync::Arc;

/// alice's bound projection keeps its input after bob edits the original graph;
/// mallory's false input contract still fails before any result is returned.
#[futures_test::test]
async fn shared_declarative_inputs_preserve_value_semantics_and_contract_checks() {
    let mut db = Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap();
    let output = RecordDescriptor::new([("id", ColumnType::U64), ("owner", ColumnType::String)]);
    let typed = compile_template_graphs(&[GraphBuilder::TemplateInput {
        slot: 0,
        output,
        input: None,
    }
    .project(["id", "owner"])])
    .unwrap();
    let GraphBuilder::TypedTemplate { program, .. } = &typed[0] else {
        panic!("expected a typed projection");
    };
    let source = |id, owner: &str| {
        GraphBuilder::values(
            RecordDescriptor::new([("id", ColumnType::U64)]),
            [vec![Value::U64(id)]],
        )
        .unwrap()
        .project_fields([
            ProjectField::named("id"),
            ProjectField::literal("owner", Value::String(owner.into())),
        ])
    };
    let mut input = Arc::new(source(11, "alice"));
    let bound = program
        .bind_declarative(std::slice::from_ref(&input), &[])
        .unwrap();
    *Arc::make_mut(&mut input) = source(22, "bob");
    assert_eq!(
        db.query_graph(input.as_ref().clone())
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22), Value::String("bob".into())], 1)],
    );
    drop(input);
    assert_eq!(
        db.query_graph(bound).await.unwrap().to_values().unwrap(),
        vec![(vec![Value::U64(11), Value::String("alice".into())], 1)],
    );

    let wrong = Arc::new(
        GraphBuilder::values(
            RecordDescriptor::new([("id", ColumnType::String)]),
            [vec![Value::String("mallory".into())]],
        )
        .unwrap(),
    );
    let declared = program.bind_declarative(&[wrong], &[]).unwrap();
    assert!(matches!(
        db.query_graph(declared).await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::GraphOutputMismatch
        )),
    ));
    let valid = program
        .bind_declarative(&[Arc::new(source(33, "alice"))], &[])
        .unwrap();
    assert_eq!(
        db.query_graph(valid).await.unwrap().to_values().unwrap(),
        vec![(vec![Value::U64(33), Value::String("alice".into())], 1)],
    );
}
