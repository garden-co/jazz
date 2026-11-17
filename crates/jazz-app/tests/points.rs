//! `point` columns through the Rust bindings generated for
//! tests/fixtures/places: `Point { lng, lat }` in, the same bits out, a
//! column default, and an out-of-range point refused at the write
//! (SPEC 20.1).

#[path = "bindings/places.rs"]
mod bindings;

use bindings::{NewPlace, Place, PlaceUpdate};
use jazz_app_support::{AppId, Config, Point};

/// alice writes a location and reads back the same `f64` bits; the omitted
/// `home` takes its declared default; a latitude out of range (here,
/// latitude and longitude swapped) is refused and leaves the row as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn points_round_trip_and_out_of_range_points_are_refused() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let location = Point {
        lng: -122.419_416,
        lat: 37.774_929,
    };
    let place = jazz
        .insert(NewPlace {
            name: "office".into(),
            location,
            pickup: None,
            home: None,
        })
        .await
        .unwrap();
    let read: Place = jazz.get(place.id).await.unwrap().unwrap();
    assert_eq!(read.location.lng.to_bits(), location.lng.to_bits());
    assert_eq!(read.location.lat.to_bits(), location.lat.to_bits());
    assert_eq!(
        read.home,
        Point {
            lng: 13.405,
            lat: 52.52
        },
        "the column's default"
    );
    assert_eq!(read.pickup, None);

    let swapped = Point {
        lng: location.lat,
        lat: location.lng,
    };
    let refused = jazz
        .update(
            place.id,
            PlaceUpdate {
                location: Some(swapped),
                ..Default::default()
            },
        )
        .await;
    assert!(refused.is_err(), "latitude -122 is out of range");
    let refused = jazz
        .insert(NewPlace {
            name: "nan".into(),
            location: Point {
                lng: f64::NAN,
                lat: 0.0,
            },
            pickup: None,
            home: None,
        })
        .await;
    assert!(refused.is_err(), "NaN");
    let read: Place = jazz.get(place.id).await.unwrap().unwrap();
    assert_eq!(read.location, location);
}
