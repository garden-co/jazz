//! The large-value stream format's reader rules.

mod common;

use aead_stream::aead::Payload;
use aead_stream::{NewStream, StreamBE32, StreamPrimitive};
use chacha20poly1305::XChaCha20Poly1305;
use common::TestRandom;
use jazz_e2ee::Error;
use jazz_e2ee::context::frame_record;
use jazz_e2ee::mechanisms::stream::{
    self, NONCE_PREFIX_BYTES, RECORD_BYTES, StreamDecryptor, StreamEncryptor, TAG_BYTES,
};
use jazz_e2ee::primitives::hash;

const KEY: [u8; 32] = [9; 32];
const CONTEXT: &[u8] = b"context";

fn header_len() -> usize {
    stream::MECHANISM.header().unwrap().len() + NONCE_PREFIX_BYTES
}

fn decrypt_in_chunks(encoded: &[u8], chunk: usize) -> Result<Vec<u8>, Error> {
    let mut decryptor = StreamDecryptor::new(&KEY, CONTEXT)?;
    let mut out = Vec::new();
    for part in encoded.chunks(chunk.max(1)) {
        decryptor.update(part, &mut out)?;
    }
    decryptor.finish()?;
    Ok(out)
}

/// Builds an encoding from explicit records (plaintext, STREAM index, last
/// flag), bypassing the writer's rules.
fn custom(records: &[(&[u8], u32, bool)]) -> Vec<u8> {
    let header = stream::MECHANISM.header().unwrap();
    let aad = frame_record(&[&header, CONTEXT]).unwrap();
    let derived = hash::keyed_hash(&KEY, &aad);
    let prefix = [5u8; NONCE_PREFIX_BYTES];
    let stream = StreamBE32::<XChaCha20Poly1305>::new((&*derived).into(), (&prefix).into());
    let mut out = [header.as_slice(), &prefix].concat();
    for (message, index, last) in records {
        let record = stream
            .encrypt(
                *index,
                *last,
                Payload {
                    msg: message,
                    aad: &aad,
                },
            )
            .unwrap();
        out.extend_from_slice(&(record.len() as u32).to_be_bytes());
        out.extend_from_slice(&record);
    }
    out
}

/// Splits an encoding into its header and its framed records.
fn split(encoded: &[u8]) -> (Vec<u8>, Vec<Vec<u8>>) {
    let mut rest = &encoded[header_len()..];
    let mut records = Vec::new();
    while !rest.is_empty() {
        let length = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
        records.push(rest[..4 + length].to_vec());
        rest = &rest[4 + length..];
    }
    (encoded[..header_len()].to_vec(), records)
}

#[test]
fn round_trips_across_record_and_chunk_boundaries() {
    let mut rng = TestRandom(3);
    for size in [
        0,
        1,
        RECORD_BYTES - 1,
        RECORD_BYTES,
        RECORD_BYTES + 1,
        3 * RECORD_BYTES,
        1 << 20,
    ] {
        let plaintext = rng.bytes(size);
        let encoded = stream::encrypt(&KEY, CONTEXT, &plaintext, &mut rng).unwrap();
        let records = size.div_ceil(RECORD_BYTES) + 1;
        assert_eq!(
            encoded.len(),
            header_len() + size + records * (4 + TAG_BYTES),
            "size {size}"
        );
        for chunk in [1, 3, 4, 17, 4096, RECORD_BYTES + 20, usize::MAX] {
            if size > RECORD_BYTES && chunk < 17 {
                continue; // Byte-at-a-time over megabytes is slow and adds nothing.
            }
            assert_eq!(
                decrypt_in_chunks(&encoded, chunk).unwrap(),
                plaintext,
                "size {size}, chunk {chunk}"
            );
        }
    }
}

#[test]
fn writer_coalesces_input_into_full_records() {
    let mut rng = TestRandom(4);
    let (mut encryptor, mut out) = StreamEncryptor::new(&KEY, CONTEXT, &mut rng).unwrap();
    let prefix = out.len();
    for _ in 0..RECORD_BYTES / 1000 + 1 {
        encryptor.update(&[1; 1000], &mut out).unwrap();
    }
    assert_eq!(out.len(), prefix + 4 + RECORD_BYTES + TAG_BYTES);
    encryptor.finish(&mut out).unwrap();
    assert_eq!(
        decrypt_in_chunks(&out, usize::MAX).unwrap().len(),
        (RECORD_BYTES / 1000 + 1) * 1000
    );
}

#[test]
fn reader_rejects_what_the_format_forbids() {
    let valid = custom(&[(b"abc", 0, false), (b"def", 1, false), (b"", 2, true)]);
    assert_eq!(decrypt_in_chunks(&valid, usize::MAX).unwrap(), b"abcdef");
    let (header, records) = split(&valid);
    let join = |order: &[usize]| {
        let mut out = header.clone();
        for index in order {
            out.extend_from_slice(&records[*index]);
        }
        out
    };
    let rejected: Vec<(&str, Vec<u8>)> = vec![
        ("last record dropped", join(&[0, 1])),
        ("last data record dropped", join(&[0, 2])),
        ("first record dropped", join(&[1, 2])),
        ("reordered", join(&[1, 0, 2])),
        ("duplicated", join(&[0, 0, 1, 2])),
        ("record after last", join(&[0, 1, 2, 2])),
        ("trailing byte", [valid.as_slice(), &[0]].concat()),
        ("header only", header.clone()),
        ("truncated header", valid[..10].to_vec()),
        (
            "last flag on a data record",
            custom(&[(b"abc", 0, true), (b"", 1, true)]),
        ),
        (
            "empty last record without its flag",
            custom(&[(b"abc", 0, false), (b"", 1, false)]),
        ),
        ("non-empty last record", custom(&[(b"abc", 0, true)])),
        (
            "skipped index",
            custom(&[(b"abc", 0, false), (b"def", 2, false), (b"", 3, true)]),
        ),
        (
            "repeated index",
            custom(&[(b"abc", 0, false), (b"def", 0, false), (b"", 1, true)]),
        ),
        (
            "empty data record",
            custom(&[(b"", 0, false), (b"", 1, true)]),
        ),
    ];
    for (name, encoded) in rejected {
        for chunk in [1, 5, usize::MAX] {
            assert_eq!(
                decrypt_in_chunks(&encoded, chunk),
                Err(Error::InvalidCiphertext),
                "{name}, chunk {chunk}"
            );
        }
    }
    // Every bit of the encoding is authenticated or structural.
    for index in 0..valid.len() {
        for bit in 0..8 {
            let mut corrupted = valid.clone();
            corrupted[index] ^= 1 << bit;
            assert!(
                decrypt_in_chunks(&corrupted, usize::MAX).is_err(),
                "accepted a flipped bit at {index}:{bit}"
            );
        }
    }
    // Another value's records do not splice into this one.
    let other = stream::encrypt(&KEY, CONTEXT, b"abcdef", &mut TestRandom(8)).unwrap();
    let (_, other_records) = split(&other);
    let mut spliced = header.clone();
    spliced.extend_from_slice(&records[0]);
    spliced.extend_from_slice(&other_records[1]);
    assert_eq!(
        decrypt_in_chunks(&spliced, usize::MAX),
        Err(Error::InvalidCiphertext)
    );
    // Lengths other than 16 or 17..=65,552 are rejected before any record is read.
    for length in [0u32, 15, 65_553, u32::MAX] {
        let mut encoded = header.clone();
        encoded.extend_from_slice(&length.to_be_bytes());
        let mut decryptor = StreamDecryptor::new(&KEY, CONTEXT).unwrap();
        assert_eq!(
            decryptor.update(&encoded, &mut Vec::new()),
            Err(Error::InvalidCiphertext),
            "length {length}"
        );
        // A failed reader stays failed.
        assert_eq!(
            decryptor.update(&[], &mut Vec::new()),
            Err(Error::InvalidCiphertext)
        );
    }
    let max = custom(&[(&[0; RECORD_BYTES], 0, false), (b"", 1, true)]);
    assert_eq!(
        decrypt_in_chunks(&max, usize::MAX).unwrap().len(),
        RECORD_BYTES
    );
}

#[test]
fn plaintext_is_released_only_per_authenticated_record() {
    let mut rng = TestRandom(5);
    let plaintext = rng.bytes(RECORD_BYTES + 10);
    let encoded = stream::encrypt(&KEY, CONTEXT, &plaintext, &mut rng).unwrap();
    let first_record_end = header_len() + 4 + RECORD_BYTES + TAG_BYTES;
    let mut decryptor = StreamDecryptor::new(&KEY, CONTEXT).unwrap();
    let mut out = Vec::new();
    decryptor
        .update(&encoded[..first_record_end - 1], &mut out)
        .unwrap();
    assert!(out.is_empty());
    decryptor
        .update(&encoded[first_record_end - 1..first_record_end], &mut out)
        .unwrap();
    assert_eq!(out, plaintext[..RECORD_BYTES]);
    // A consumed prefix is not whole-value success.
    assert_eq!(decryptor.finish(), Err(Error::InvalidCiphertext));
}

#[test]
fn reader_holds_at_most_one_record() {
    let mut rng = TestRandom(6);
    let plaintext = rng.bytes(1 << 20);
    let encoded = stream::encrypt(&KEY, CONTEXT, &plaintext, &mut rng).unwrap();
    for chunk in [7, 4096, 100_000, encoded.len()] {
        let mut decryptor = StreamDecryptor::new(&KEY, CONTEXT).unwrap();
        let mut out = Vec::new();
        for part in encoded.chunks(chunk) {
            decryptor.update(part, &mut out).unwrap();
            assert!(decryptor.buffered() <= 4 + RECORD_BYTES + TAG_BYTES);
        }
        decryptor.finish().unwrap();
        assert_eq!(out, plaintext);
    }
}
