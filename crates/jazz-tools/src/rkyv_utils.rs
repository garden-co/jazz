use std::{error::Error, fmt, hash::Hash};

use internment::Intern;
use rkyv::{
    Archive, Archived, Deserialize, Place, Resolver, Serialize,
    rancor::{Fallible, ResultExt as _, Source},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde_json::Value as JsonValue;

pub struct InternAsOwned;

impl<T> ArchiveWith<Intern<T>> for InternAsOwned
where
    T: Archive + Clone + Eq + Hash + Send + Sync + 'static,
{
    type Archived = Archived<T>;
    type Resolver = Resolver<T>;

    fn resolve_with(field: &Intern<T>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let value: &T = field.as_ref();
        value.resolve(resolver, out);
    }
}

impl<T, S> SerializeWith<Intern<T>, S> for InternAsOwned
where
    T: Archive + Clone + Eq + Hash + Send + Sync + Serialize<S> + 'static,
    S: Fallible + ?Sized,
{
    fn serialize_with(
        field: &Intern<T>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as Fallible>::Error> {
        let value: &T = field.as_ref();
        value.serialize(serializer)
    }
}

impl<T, D> DeserializeWith<Archived<T>, Intern<T>, D> for InternAsOwned
where
    T: Archive + Clone + Eq + Hash + Send + Sync + 'static,
    Archived<T>: Deserialize<T, D>,
    D: Fallible + ?Sized,
{
    fn deserialize_with(
        field: &Archived<T>,
        deserializer: &mut D,
    ) -> Result<Intern<T>, <D as Fallible>::Error> {
        Ok(Intern::new(field.deserialize(deserializer)?))
    }
}

#[derive(Debug)]
struct JsonSerializeError;

impl fmt::Display for JsonSerializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to serialize JSON value")
    }
}

impl Error for JsonSerializeError {}

#[derive(Debug)]
struct JsonDeserializeError;

impl fmt::Display for JsonDeserializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to deserialize JSON value")
    }
}

impl Error for JsonDeserializeError {}

pub struct JsonValueAsString;

impl ArchiveWith<JsonValue> for JsonValueAsString {
    type Archived = Archived<String>;
    type Resolver = Resolver<String>;

    fn resolve_with(field: &JsonValue, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let json = serde_json::to_string(field)
            .expect("JSON serialization succeeded during rkyv::Serialize");
        json.resolve(resolver, out);
    }
}

impl<S> SerializeWith<JsonValue, S> for JsonValueAsString
where
    S: Fallible + ?Sized,
    S::Error: Source,
    String: Serialize<S>,
{
    fn serialize_with(
        field: &JsonValue,
        serializer: &mut S,
    ) -> Result<Self::Resolver, <S as Fallible>::Error> {
        let json = serde_json::to_string(field).into_trace(JsonSerializeError)?;
        json.serialize(serializer)
    }
}

impl<D> DeserializeWith<Archived<String>, JsonValue, D> for JsonValueAsString
where
    D: Fallible + ?Sized,
    D::Error: Source,
    Archived<String>: Deserialize<String, D>,
{
    fn deserialize_with(
        field: &Archived<String>,
        deserializer: &mut D,
    ) -> Result<JsonValue, <D as Fallible>::Error> {
        let json = field.deserialize(deserializer)?;
        serde_json::from_str(&json).into_trace(JsonDeserializeError)
    }
}
