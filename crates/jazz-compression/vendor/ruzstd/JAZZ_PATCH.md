# Jazz bounded streaming patch

Pristine source: `ruzstd` 0.8.3, crates.io archive, upstream Git commit
`1c7aafb8e668f9ea2f44e6155bb7429e2442a3c1` (`ruzstd/` directory).
Source and license: <https://github.com/KillingSpark/zstd-rs/tree/1c7aafb8e668f9ea2f44e6155bb7429e2442a3c1/ruzstd>.
MIT license is preserved in LICENSE. Only library source, Cargo manifest,
Readme and license are copied; benchmark, corpus, and repository tooling are omitted.

`jazz.patch` records every change to copied upstream files. Reproduce by unpacking
the crates.io 0.8.3 archive and running `patch -p1 < jazz.patch` at its root.
The package has a distinct local name `jazz-ruzstd`; it is not a global Cargo patch.

## Why a local patch

Upstream 0.8.3 public read/collect methods retain a full window **without delivering
those bytes** until the frame ends. A small flushed channel message would never
reach the application while the channel remained open. The new opt-in
`FrameDecoder::read_streaming` copies only newly produced bytes with a delivery
cursor and drops delivered bytes only when they leave the retained window.
Do not mix it with upstream read/collect methods on one frame. This separate
API does not calculate checksums; the channel profile rejects checksum frames.
Existing bulk helpers keep using upstream read/collect behavior.

`set_max_block_output`, set after initialization, is opt-in. It checks encoded
block lengths, regenerated literal lengths and sequence counts before their
allocations. Both Huffman loops reject excess symbols before pushing beyond
the declared regenerated literal size, even when a malformed bitstream claims
fewer symbols than it actually encodes. The patch then checked-adds every match length to the literal count before
executing sequences or reserving regenerated output. The channel adapter checks
the frame window before initializing the decoder, and decodes one complete
bounded block at a time. Default bulk behavior has no new size limit.

As checked on 2026-09-19, upstream v0.9.0 adds configurable window limits but
retains both the withheld-window delivery behavior and unchecked match expansion:
<https://github.com/KillingSpark/zstd-rs/blob/v0.9.0/ruzstd/src/decoding/decode_buffer.rs>
and <https://github.com/KillingSpark/zstd-rs/blob/v0.9.0/ruzstd/src/decoding/sequence_execution.rs>.
An upstream release is therefore not yet a substitute for this patch.

## Validation

Jazz owns black-box tests in `../../tests/bounded_ruzstd.rs` and `stream.rs`.
Run `cargo test -p jazz-compression --no-default-features --features lz4,ruzstd`.
The tests cover immediate flushed delivery, repeated dictionary use beyond the
window, fragmented input/output, and rejection before literal/table/match output
allocation. Upstream corpus test/bench targets are intentionally not copied;
the library remains no_std and uses upstream decode primitives.

Pristine crates.io archive SHA-256: `a7c1c839d570d835527c9a5e4db7cb2198683a988cb9d7293fc8674e6bd58fc8`.
