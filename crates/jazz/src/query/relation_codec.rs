// JRQ v1 is the explicit binary carrier for RelationQuery. It deliberately
// encodes the serde relation tree as a bounded typed value tree, never as a
// JSON string. The public RelationQuery representation and canonical identity
// remain owned by ast.rs/canonical_request.rs.
const MAGIC: &[u8] = b"JRQ\x01";
const MAX_BYTES: usize = 1 << 20;
const MAX_DEPTH: usize = 128;
const MAX_ITEMS: usize = 4096;
const MAX_STRING: usize = 1 << 16;

/// Failure while encoding or decoding the JRQ v1 relation-query carrier.
#[derive(Debug, thiserror::Error)]
pub enum RelationCodecError {
    /// The input violates the bounded JRQ v1 grammar.
    #[error("malformed relation query binary: {0}")]
    Malformed(&'static str),
    /// A serde-compatible relation field was structurally invalid.
    #[error("malformed relation query binary: {0}")]
    Detail(String),
}

/// Encode a relation query into its canonical JRQ v1 bytes.
pub fn encode_relation_query_v1(query: &RelationQuery) -> Result<Vec<u8>, RelationCodecError> {
    let value = serde_json::to_value(query).map_err(|_| RelationCodecError::Malformed("serialize"))?;
    let mut out = MAGIC.to_vec();
    encode_value(&value, &mut out)?;
    if out.len() > MAX_BYTES { return Err(RelationCodecError::Malformed("too large")); }
    Ok(out)
}

/// Decode exactly one canonical JRQ v1 value, rejecting trailing bytes.
pub fn decode_relation_query_v1_exact(bytes: &[u8]) -> Result<RelationQuery, RelationCodecError> {
    if bytes.len() > MAX_BYTES { return Err(RelationCodecError::Malformed("too large")); }
    if !bytes.starts_with(MAGIC) { return Err(RelationCodecError::Malformed("version")); }
    let mut input = &bytes[MAGIC.len()..];
    let value = decode_value(&mut input, 0)?;
    if !input.is_empty() { return Err(RelationCodecError::Malformed("trailing bytes")); }
    let query = serde_json::from_value(value).map_err(|error| RelationCodecError::Detail(error.to_string()))?;
    validate_relation_labels(&query)?;
    Ok(query)
}

fn relation_put_len(out: &mut Vec<u8>, mut value: usize) {
    loop { let mut byte = (value & 0x7f) as u8; value >>= 7; if value != 0 { byte |= 0x80; } out.push(byte); if value == 0 { break; } }
}
fn relation_get_len(input: &mut &[u8]) -> Result<usize, RelationCodecError> {
    let mut result = 0usize; let mut shift = 0;
    for index in 0..10 {
        let Some((&byte, rest)) = input.split_first() else { return Err(RelationCodecError::Malformed("truncated length")); }; *input = rest;
        let part = (byte & 0x7f) as usize;
        result |= part.checked_shl(shift).ok_or(RelationCodecError::Malformed("length overflow"))?;
        if byte & 0x80 == 0 { if index > 0 && part == 0 { return Err(RelationCodecError::Malformed("nonminimal length")); } return Ok(result); }
        shift += 7;
    }
    Err(RelationCodecError::Malformed("length overflow"))
}
fn relation_put_bytes(out: &mut Vec<u8>, bytes: &[u8]) { relation_put_len(out, bytes.len()); out.extend_from_slice(bytes); }
fn relation_get_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], RelationCodecError> { let len=relation_get_len(input)?; if len>MAX_STRING || input.len()<len { return Err(RelationCodecError::Malformed("invalid length")); } let (head,tail)=input.split_at(len); *input=tail; Ok(head) }
fn encode_value(value: &serde_json::Value, out: &mut Vec<u8>) -> Result<(), RelationCodecError> {
    match value {
        serde_json::Value::Null => out.push(0), serde_json::Value::Bool(false)=>out.push(1), serde_json::Value::Bool(true)=>out.push(2),
        serde_json::Value::Number(number)=>{out.push(3); relation_put_bytes(out, number.to_string().as_bytes());}
        serde_json::Value::String(string)=>{out.push(4); relation_put_bytes(out,string.as_bytes());}
        serde_json::Value::Array(values)=>{ if values.len()>MAX_ITEMS{return Err(RelationCodecError::Malformed("too many items"));} out.push(5);relation_put_len(out,values.len());for value in values{encode_value(value,out)?;}}
        serde_json::Value::Object(map)=>{if map.len()>MAX_ITEMS{return Err(RelationCodecError::Malformed("too many items"));} out.push(6);relation_put_len(out,map.len());for (key,value) in map {relation_put_bytes(out,key.as_bytes());encode_value(value,out)?;}}
    }; Ok(())
}
fn decode_value(input: &mut &[u8], depth: usize) -> Result<serde_json::Value, RelationCodecError> {
    if depth >= MAX_DEPTH{return Err(RelationCodecError::Malformed("too deep"));} let Some((&tag,rest))=input.split_first() else{return Err(RelationCodecError::Malformed("truncated value"));};*input=rest;
    Ok(match tag {0=>serde_json::Value::Null,1=>serde_json::Value::Bool(false),2=>serde_json::Value::Bool(true),3=>{let text=std::str::from_utf8(relation_get_bytes(input)?).map_err(|_|RelationCodecError::Malformed("invalid utf8"))?;let number: serde_json::Number=text.parse().map_err(|_|RelationCodecError::Malformed("invalid number"))?;if number.to_string()!=text{return Err(RelationCodecError::Malformed("noncanonical number"));}serde_json::Value::Number(number)},4=>serde_json::Value::String(std::str::from_utf8(relation_get_bytes(input)?).map_err(|_|RelationCodecError::Malformed("invalid utf8"))?.to_owned()),5=>{let count=relation_get_len(input)?;if count>MAX_ITEMS{return Err(RelationCodecError::Malformed("too many items"));}serde_json::Value::Array((0..count).map(|_|decode_value(input,depth+1)).collect::<Result<_,_>>()?)},6=>{let count=relation_get_len(input)?;if count>MAX_ITEMS{return Err(RelationCodecError::Malformed("too many items"));}let mut map=serde_json::Map::new();let mut previous=None;for _ in 0..count {let key=std::str::from_utf8(relation_get_bytes(input)?).map_err(|_|RelationCodecError::Malformed("invalid utf8"))?.to_owned();if previous.as_deref()>=Some(key.as_str()) {return Err(RelationCodecError::Malformed("noncanonical object keys"));}previous=Some(key.clone());if map.insert(key,decode_value(input,depth+1)?).is_some(){return Err(RelationCodecError::Malformed("duplicate key"));}}serde_json::Value::Object(map)},_=>return Err(RelationCodecError::Malformed("unknown tag"))})
}
fn validate_relation_labels(query: &RelationQuery) -> Result<(), RelationCodecError> {
    fn walk(rel:&RelationExpr)->Result<(),RelationCodecError>{match rel {RelationExpr::Union{inputs}=>{let mut labels=std::collections::BTreeSet::new();for arm in inputs {if arm.label.is_empty()||!labels.insert(&arm.label){return Err(RelationCodecError::Malformed("duplicate or empty union label"));}walk(&arm.input)?;}},RelationExpr::Filter{input,..}|RelationExpr::Project{input,..}|RelationExpr::Distinct{input,..}|RelationExpr::OrderBy{input,..}|RelationExpr::Offset{input,..}|RelationExpr::Limit{input,..}=>walk(input)?,RelationExpr::Join{left,right,..}=>{walk(left)?;walk(right)?},RelationExpr::Gather{seed,step,..}=>{walk(seed)?;walk(step)?},RelationExpr::TableScan{..}=>{}}Ok(())} walk(&query.rel)
}
