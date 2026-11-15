//! `vector` columns through the Rust bindings generated for
//! tests/fixtures/docs: `Vec<f32>` in, the same bits out, a column default,
//! and a wrong length refused at the write (SPEC 20.1).

#[path = "bindings/docs.rs"]
mod bindings;

use bindings::{Doc, DocUpdate, NewDoc};
use jazz_app::{AppId, Config};

/// alice writes an embedding and reads back the same `f32` bits; the
/// omitted `seed` takes its declared default; a vector of the wrong length
/// is refused and leaves the row as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vectors_round_trip_and_wrong_lengths_are_refused() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let embedding = vec![0.1f32, -0.0, 1e-45, f32::MAX];
    let doc = jazz
        .insert(NewDoc {
            title: "intro".into(),
            embedding: embedding.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let read: Doc = jazz.get(doc.id).await.unwrap().unwrap();
    assert_eq!(
        read.embedding
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>(),
        embedding.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
    );
    assert_eq!(read.seed, vec![0.5, -1.0], "the column's default");
    assert_eq!(read.draft_embedding, None);

    let refused = jazz
        .update(
            doc.id,
            DocUpdate {
                embedding: Some(vec![1.0, 2.0]),
                ..Default::default()
            },
        )
        .await;
    assert!(refused.is_err(), "two floats in a vector(4)");
    let refused = jazz
        .insert(NewDoc {
            title: "nan".into(),
            embedding: vec![f32::NAN, 0.0, 0.0, 0.0],
            ..Default::default()
        })
        .await;
    assert!(refused.is_err(), "NaN");
    let read: Doc = jazz.get(doc.id).await.unwrap().unwrap();
    assert_eq!(read.embedding.len(), 4);
}
