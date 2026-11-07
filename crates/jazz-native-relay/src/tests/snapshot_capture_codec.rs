//! Application references use the established pollable byte-result response.
use super::*;

/// No actors: pin the additive capture command and the UTF-8 reference response
/// independently of the TypeScript implementation. Existing ordinals stay fixed.
#[test]
fn snapshot_capture_command_and_reference_response_have_pinned_bytes() {
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::CaptureSnapshot).unwrap(),
        vec![34]
    );
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../jazz/fixtures/snapshot_ref_v1.json")).unwrap();
    let reference = fixture["cases"][0]["reference"].as_str().unwrap();
    assert!(reference.len() < 128);
    let mut expected = vec![2, reference.len() as u8];
    expected.extend_from_slice(reference.as_bytes());
    let response = ForegroundDbCommandResponse::Rows {
        rows: reference.as_bytes().to_vec(),
    };
    assert_eq!(postcard::to_allocvec(&response).unwrap(), expected);
    assert_eq!(
        postcard::from_bytes::<ForegroundDbCommandResponse>(&expected).unwrap(),
        response
    );
}
