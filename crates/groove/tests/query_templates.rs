use groove::db::{Database, GraphBuilder};
use groove::ivm::{InputSourceReplacement, IvmRuntimeError, bind_template_graphs};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{ColumnType, DatabaseSchema};
use groove::storage::MemoryStorage;

async fn database() -> Database {
    Database::new(DatabaseSchema::new([]), MemoryStorage::new(&[]).unwrap())
        .await
        .unwrap()
}

fn descriptor() -> RecordDescriptor {
    RecordDescriptor::new([("id", ColumnType::U64)])
}

fn template() -> GraphBuilder {
    GraphBuilder::TemplateInput {
        slot: 0,
        output: descriptor(),
        input: None,
    }
    .project(["id"])
}

fn bind(db: &Database, graph: GraphBuilder) -> GraphBuilder {
    bind_template_graphs(&[template()], &[db.describe_template_input(graph).unwrap()])
        .unwrap()
        .remove(0)
}

#[futures_test::test]
async fn template_bindings_keep_private_inputs_and_lifetimes_separate() {
    let mut db = database().await;
    let first = db.allocate_input_source(descriptor());
    let second = db.allocate_input_source(descriptor());
    db.replace_input_sources([
        InputSourceReplacement {
            id: first,
            descriptor: descriptor(),
            records: vec![descriptor().create(&[Value::U64(11)]).unwrap()],
        },
        InputSourceReplacement {
            id: second,
            descriptor: descriptor(),
            records: vec![descriptor().create(&[Value::U64(22)]).unwrap()],
        },
    ])
    .await
    .unwrap();
    let first_graph = bind(&db, GraphBuilder::input_source(first, descriptor()));
    let second_graph = bind(&db, GraphBuilder::input_source(second, descriptor()));
    let first_subscription = db.subscribe_one_sink(first_graph.clone()).await.unwrap();
    let second_subscription = db.subscribe_one_sink(second_graph.clone()).await.unwrap();
    assert_eq!(
        db.next_subscription(&first_subscription)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(11)], 1)]
    );
    assert_eq!(
        db.next_subscription(&second_subscription)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22)], 1)]
    );
    db.replace_input_sources([InputSourceReplacement {
        id: first,
        descriptor: descriptor(),
        records: vec![descriptor().create(&[Value::U64(33)]).unwrap()],
    }])
    .await
    .unwrap();
    let changes = db
        .next_subscription(&first_subscription)
        .await
        .unwrap()
        .to_values()
        .unwrap();
    assert_eq!(changes.len(), 2);
    assert!(changes.contains(&(vec![Value::U64(11)], -1)));
    assert!(changes.contains(&(vec![Value::U64(33)], 1)));
    assert_eq!(
        db.query_graph(second_graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(22)], 1)]
    );
    db.unsubscribe(first_subscription.id());
    db.retire_input_sources([first]).await.unwrap();
    assert!(matches!(
        db.subscribe_one_sink(first_graph).await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::InputSourceRetired
        ))
    ));
}

#[futures_test::test]
async fn template_binding_rejects_missing_mismatched_and_foreign_inputs() {
    assert_eq!(
        bind_template_graphs(&[template()], &[]).unwrap_err(),
        groove::ivm::TemplateBindingError::MissingInput(0)
    );
    let db = database().await;
    assert!(db.graph_output_descriptor(&template()).is_err());
    let wrong = GraphBuilder::values(
        RecordDescriptor::new([("id", ColumnType::String)]),
        [vec![Value::String("wrong".into())]],
    )
    .unwrap();
    assert!(matches!(
        bind_template_graphs(&[template()], &[db.describe_template_input(wrong).unwrap()]),
        Err(groove::ivm::TemplateBindingError::DescriptorMismatch(0))
    ));
    let mut owner = database().await;
    let input = owner.allocate_input_source(descriptor());
    let mut foreign = database().await;
    assert!(matches!(
        foreign
            .subscribe_one_sink(bind(
                &owner,
                GraphBuilder::input_source(input, descriptor())
            ))
            .await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::ForeignInputSource
        ))
    ));
}

#[futures_test::test]
async fn declared_template_contract_is_rechecked_before_execution() {
    let mut db = database().await;
    let wrong = GraphBuilder::values(
        RecordDescriptor::new([("id", ColumnType::String)]),
        [vec![Value::String("not a number".into())]],
    )
    .unwrap();
    let declared = groove::ivm::TemplateGraphInput::with_output_contract(wrong, descriptor());
    let graph = bind_template_graphs(&[template()], &[declared])
        .unwrap()
        .remove(0);
    assert!(matches!(
        db.subscribe_one_sink(graph).await,
        Err(groove::db::Error::IvmRuntime(
            IvmRuntimeError::GraphOutputMismatch
        ))
    ));
    // A rejected declaration must not poison a later ordinary subscription.
    let valid = GraphBuilder::values(descriptor(), [vec![Value::U64(7)]]).unwrap();
    assert_eq!(
        db.query_graph(bind(&db, valid))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        vec![(vec![Value::U64(7)], 1)]
    );
}
