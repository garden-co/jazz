use groove::db::{Database, GraphBuilder, PredicateExpr};
use groove::ivm::ProjectField;
use groove::records::{RecordDescriptor, Value, ValueType};
use groove::schema::DatabaseSchema;
use groove::storage::MemoryStorage;

#[test]
fn query_graph_can_filter_project_and_join_inline_values() {
    let schema = DatabaseSchema::new([]);
    let storage = MemoryStorage::new(&[]);
    let mut database = Database::new(schema, storage).unwrap();

    let album_desc = RecordDescriptor::new([
        ("album_id", ValueType::U64),
        ("artist_id", ValueType::U64),
        ("title", ValueType::String),
    ]);
    let artist_desc =
        RecordDescriptor::new([("artist_id", ValueType::U64), ("name", ValueType::String)]);

    let albums = GraphBuilder::values(
        album_desc,
        [
            vec![
                Value::U64(10),
                Value::U64(1),
                Value::String("Speak No Evil".to_owned()),
            ],
            vec![
                Value::U64(11),
                Value::U64(2),
                Value::String("Expansions".to_owned()),
            ],
        ],
    )
    .unwrap()
    .filter(PredicateExpr::eq(
        "title",
        Value::String("Speak No Evil".to_owned()),
    ));

    let artists = GraphBuilder::values(
        artist_desc,
        [
            vec![Value::U64(1), Value::String("Wayne Shorter".to_owned())],
            vec![Value::U64(2), Value::String("McCoy Tyner".to_owned())],
        ],
    )
    .unwrap();

    let graph = GraphBuilder::join(albums, artists, ["artist_id"], ["artist_id"]).project_fields([
        ProjectField::renamed("left.title", "album"),
        ProjectField::renamed("right.name", "artist"),
    ]);

    let rows = database.query_graph(graph).unwrap().to_values().unwrap();

    assert_eq!(
        rows,
        vec![(
            vec![
                Value::String("Speak No Evil".to_owned()),
                Value::String("Wayne Shorter".to_owned()),
            ],
            1,
        )]
    );
}
