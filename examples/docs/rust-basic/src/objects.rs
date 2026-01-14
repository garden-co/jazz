//! Object and ObjectId examples

use groove::ObjectId;

//#region objectid-basics
/// Working with ObjectIds
pub fn objectid_basics() {
    // Generate a new ObjectId (UUIDv7 with Crockford Base32 encoding)
    let id = groove::generate_object_id();
    println!("Generated ObjectId: {}", id);

    // Parse from string
    let parsed: ObjectId = "01HXY2Z3456789ABCDEFGHJKLM".parse().unwrap();
    println!("Parsed ObjectId: {}", parsed);

    // ObjectIds are sortable by creation time (UUIDv7)
    let id1 = groove::generate_object_id();
    let id2 = groove::generate_object_id();
    // Later IDs sort after earlier IDs (usually, unless created in same millisecond)
    println!("id1 < id2: {}", id1 < id2);
}
//#endregion

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_objectid_basics() {
        objectid_basics();
    }
}
