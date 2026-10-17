//! Bounded, persistent channel codecs. This is a transport profile, not a
//! general-purpose decoder: one frame, a 64 KiB window, no dictionaries/checksums.
//! Flush preserves history; construct a new codec to reset a channel.
#[cfg(feature = "lz4")]
use std::io::Write;

pub const MAX_STREAM_CHUNK_BYTES: usize = 64 * 1024;
/// Largest staged encoded block including its header.
pub const MAX_STREAM_PENDING_BYTES: usize = MAX_STREAM_CHUNK_BYTES + 11;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Lz4,
    Zstd,
}

/// Exact progress. Remove `consumed` input bytes and retain all `written` output
/// bytes before calling again. Empty output applies backpressure (no progress).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub consumed: usize,
    pub written: usize,
    /// For flush: all pending output delivered. For finish/decode: frame ended.
    pub finished: bool,
}

enum Encoder {
    Unavailable,
    #[cfg(feature = "lz4")]
    Lz4(Box<lz4_flex::frame::FrameEncoder<Vec<u8>>>),
    #[cfg(feature = "zstd")]
    Zstd(zstd::stream::raw::Encoder<'static>),
}

pub struct StreamEncoder {
    inner: Encoder,
    #[cfg(feature = "lz4")]
    pending_pos: usize,
    ending: bool,
    ended: bool,
    failed: bool,
}

impl StreamEncoder {
    pub fn new(codec: Codec) -> Result<Self, String> {
        let inner = match codec {
            #[cfg(feature = "lz4")]
            Codec::Lz4 => Encoder::Lz4(Box::new(lz4_flex::frame::FrameEncoder::with_frame_info(
                lz4_flex::frame::FrameInfo::new()
                    .block_size(lz4_flex::frame::BlockSize::Max64KB)
                    .block_mode(lz4_flex::frame::BlockMode::Linked),
                Vec::with_capacity(MAX_STREAM_PENDING_BYTES),
            ))),
            #[cfg(feature = "zstd")]
            Codec::Zstd => {
                let mut encoder = zstd::stream::raw::Encoder::new(3).map_err(err)?;
                encoder
                    .set_parameter(zstd::stream::raw::CParameter::WindowLog(16))
                    .map_err(err)?;
                encoder
                    .set_parameter(zstd::stream::raw::CParameter::ChecksumFlag(false))
                    .map_err(err)?;
                // Even an empty stream must use the unknown-length channel
                // profile rather than zstd's automatic single-segment frame.
                encoder
                    .set_parameter(zstd::stream::raw::CParameter::ContentSizeFlag(false))
                    .map_err(err)?;
                Encoder::Zstd(encoder)
            }
            #[allow(unreachable_patterns)]
            _ => Encoder::Unavailable,
        };
        if matches!(inner, Encoder::Unavailable) {
            return Err("channel encoder feature is not compiled in".into());
        }
        Ok(Self {
            inner,
            #[cfg(feature = "lz4")]
            pending_pos: 0,
            ending: false,
            ended: false,
            failed: false,
        })
    }

    pub fn encode(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, String> {
        if self.failed || self.ending || self.ended {
            return Err("channel encoder is closed".into());
        }
        let result = self.encode_inner(input, output);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn encode_inner(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, String> {
        if output.is_empty() {
            return Ok(Progress::default());
        }
        #[cfg(any(feature = "lz4", feature = "zstd"))]
        let input = &input[..input.len().min(MAX_STREAM_CHUNK_BYTES)];
        #[cfg(not(any(feature = "lz4", feature = "zstd")))]
        let _ = input;
        match &mut self.inner {
            Encoder::Unavailable => Err("channel encoder unavailable".into()),
            #[cfg(feature = "lz4")]
            Encoder::Lz4(encoder) => {
                if self.pending_pos < encoder.get_ref().len() {
                    return Ok(drain(encoder.get_mut(), &mut self.pending_pos, output));
                }
                encoder.write_all(input).map_err(err)?;
                let mut progress = drain(encoder.get_mut(), &mut self.pending_pos, output);
                progress.consumed = input.len();
                Ok(progress)
            }
            #[cfg(feature = "zstd")]
            Encoder::Zstd(encoder) => {
                use zstd::stream::raw::Operation;
                let status = encoder.run_on_buffers(input, output).map_err(err)?;
                Ok(Progress {
                    consumed: status.bytes_read,
                    written: status.bytes_written,
                    finished: false,
                })
            }
        }
    }

    /// Repeatedly call with fresh output space until `finished`; do not encode
    /// another message until the flush is complete.
    pub fn flush(&mut self, output: &mut [u8]) -> Result<Progress, String> {
        self.flush_or_finish(output, false)
    }
    /// Repeatedly call until `finished`; subsequent encode/flush calls fail.
    pub fn finish(&mut self, output: &mut [u8]) -> Result<Progress, String> {
        self.flush_or_finish(output, true)
    }

    fn flush_or_finish(&mut self, output: &mut [u8], finish: bool) -> Result<Progress, String> {
        if self.failed || self.ended || (self.ending && !finish) {
            return Err("channel encoder is closed".into());
        }
        if output.is_empty() {
            return Ok(Progress::default());
        }
        let result = self.flush_inner(output, finish);
        if result.is_err() {
            self.failed = true;
        }
        if let Ok(progress) = result.as_ref()
            && finish
            && progress.finished
        {
            self.ended = true;
        }
        result
    }

    fn flush_inner(&mut self, output: &mut [u8], finish: bool) -> Result<Progress, String> {
        #[cfg(not(any(feature = "lz4", feature = "zstd")))]
        let _ = (output, finish);
        match &mut self.inner {
            Encoder::Unavailable => Err("channel encoder unavailable".into()),
            #[cfg(feature = "lz4")]
            Encoder::Lz4(encoder) => {
                // Drain before another write/flush, bounding the Vec to one block.
                if self.pending_pos < encoder.get_ref().len() {
                    let mut progress = drain(encoder.get_mut(), &mut self.pending_pos, output);
                    progress.finished = self.ending && encoder.get_ref().is_empty();
                    return Ok(progress);
                }
                if finish {
                    if !self.ending {
                        encoder.try_finish().map_err(err)?;
                        self.ending = true;
                    }
                } else {
                    encoder.flush().map_err(err)?;
                }
                let mut progress = drain(encoder.get_mut(), &mut self.pending_pos, output);
                progress.finished = encoder.get_ref().is_empty();
                Ok(progress)
            }
            #[cfg(feature = "zstd")]
            Encoder::Zstd(encoder) => {
                use zstd::stream::raw::{Operation, OutBuffer};
                let mut buffer = OutBuffer::around(output);
                let remaining = if finish {
                    self.ending = true;
                    encoder.finish(&mut buffer, true)
                } else {
                    encoder.flush(&mut buffer)
                }
                .map_err(err)?;
                Ok(Progress {
                    consumed: 0,
                    written: buffer.pos(),
                    finished: remaining == 0,
                })
            }
        }
    }
}

#[cfg(any(feature = "lz4", feature = "zstd", feature = "ruzstd"))]
fn err(error: impl std::fmt::Display) -> String {
    format!("channel codec: {error}")
}
#[cfg(feature = "lz4")]
fn drain(pending: &mut Vec<u8>, position: &mut usize, output: &mut [u8]) -> Progress {
    let written = output.len().min(pending.len() - *position);
    output[..written].copy_from_slice(&pending[*position..*position + written]);
    *position += written;
    if *position == pending.len() {
        pending.clear();
        *position = 0;
    }
    Progress {
        consumed: 0,
        written,
        finished: false,
    }
}

enum Decoder {
    Unavailable,
    #[cfg(feature = "lz4")]
    Lz4 {
        history: Vec<u8>,
        pending: Vec<u8>,
        position: usize,
    },
    #[cfg(feature = "zstd")]
    Zstd(zstd::stream::raw::Decoder<'static>),
    #[cfg(all(feature = "ruzstd", not(feature = "zstd")))]
    Ruzstd(Box<ruzstd::decoding::FrameDecoder>),
}

/// A decoder owns at most one encoded block and bounded codec history. The
/// caller owns message-size accounting and must never allocate from peer lengths.
/// Any decoding error poisons the instance; replace it when resetting a channel.
pub struct StreamDecoder {
    codec: Codec,
    inner: Decoder,
    packet: Vec<u8>,
    target: usize,
    packet_position: usize,
    block_limit: usize,
    header: bool,
    packet_ready: bool,
    last_block: bool,
    ended: bool,
    failed: bool,
}

impl StreamDecoder {
    pub fn new(codec: Codec) -> Result<Self, String> {
        let inner = match codec {
            #[cfg(feature = "lz4")]
            Codec::Lz4 => Decoder::Lz4 {
                history: Vec::new(),
                pending: Vec::new(),
                position: 0,
            },
            #[cfg(feature = "zstd")]
            Codec::Zstd => {
                let mut decoder = zstd::stream::raw::Decoder::new().map_err(err)?;
                decoder
                    .set_parameter(zstd::stream::raw::DParameter::WindowLogMax(16))
                    .map_err(err)?;
                Decoder::Zstd(decoder)
            }
            #[cfg(all(feature = "ruzstd", not(feature = "zstd")))]
            Codec::Zstd => Decoder::Ruzstd(Box::default()),
            #[allow(unreachable_patterns)]
            _ => Decoder::Unavailable,
        };
        if matches!(inner, Decoder::Unavailable) {
            return Err("channel decoder feature is not compiled in".into());
        }
        Ok(Self {
            codec,
            inner,
            packet: Vec::with_capacity(MAX_STREAM_PENDING_BYTES),
            target: match codec {
                Codec::Lz4 => 7,
                Codec::Zstd => 6,
            },
            packet_position: 0,
            block_limit: MAX_STREAM_CHUNK_BYTES,
            header: true,
            packet_ready: false,
            last_block: false,
            ended: false,
            failed: false,
        })
    }

    pub fn decode(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, String> {
        if self.failed {
            return Err("channel decoder is poisoned".into());
        }
        let result = self.decode_inner(input, output);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn decode_inner(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, String> {
        if self.ended {
            if !input.is_empty() {
                return Err("bytes after channel codec end".into());
            }
            return Ok(Progress {
                finished: true,
                ..Progress::default()
            });
        }
        if output.is_empty() {
            return Ok(Progress::default());
        }
        let mut progress = Progress::default();
        loop {
            if !self.packet_ready {
                let amount = (self.target - self.packet.len()).min(input.len() - progress.consumed);
                self.packet
                    .extend_from_slice(&input[progress.consumed..progress.consumed + amount]);
                progress.consumed += amount;
                if self.packet.len() < self.target {
                    return Ok(progress);
                }
                if self.header {
                    self.validate_header()?;
                    self.packet_ready = true;
                } else {
                    let header_len = self.block_header_len();
                    if self.target == header_len {
                        let (body_len, last) = self.block_size()?;
                        self.last_block = last;
                        self.target += body_len;
                        if body_len != 0 {
                            continue;
                        }
                    }
                    self.packet_ready = true;
                }
            }
            let (consumed, written, drained) =
                self.process_packet(&mut output[progress.written..])?;
            self.packet_position += consumed;
            progress.written += written;
            if drained {
                let last = !self.header && self.last_block;
                self.header = false;
                self.packet.clear();
                self.packet_position = 0;
                self.packet_ready = false;
                self.target = self.block_header_len();
                if last {
                    self.ended = true;
                    progress.finished = true;
                    return Ok(progress);
                }
            }
            if progress.written == output.len() {
                return Ok(progress);
            }
            if !drained && consumed == 0 && written == 0 {
                return Err("channel decoder stalled on a complete block".into());
            }
        }
    }

    /// Check an explicitly closed channel. A flush is not an end marker.
    pub fn finish(&self) -> Result<(), String> {
        if !self.failed && self.ended {
            Ok(())
        } else {
            Err("truncated or undrained channel codec stream".into())
        }
    }

    fn block_header_len(&self) -> usize {
        match self.codec {
            Codec::Lz4 => 4,
            Codec::Zstd => 3,
        }
    }
    fn validate_header(&mut self) -> Result<(), String> {
        let valid = match self.codec {
            // LZ4 v1 linked blocks, 64KiB, no optional fields; includes HC.
            Codec::Lz4 => self.packet == [0x04, 0x22, 0x4d, 0x18, 0x40, 0x40, 0xc0],
            // Zstd unknown content size, no checksum/dictionary, <=64KiB window.
            // Window descriptor 0x30 is exactly 64KiB (exponent 16, mantissa 0).
            Codec::Zstd => {
                self.packet[..5] == [0x28, 0xb5, 0x2f, 0xfd, 0] && self.packet[5] <= 0x30
            }
        };
        if valid {
            if self.codec == Codec::Zstd {
                let descriptor = self.packet[5];
                let base = 1usize << (10 + (descriptor >> 3));
                self.block_limit = base + (base / 8) * usize::from(descriptor & 7);
            }
            Ok(())
        } else {
            Err("unsupported channel codec header/window profile".into())
        }
    }
    fn block_size(&self) -> Result<(usize, bool), String> {
        let (size, stored, last) = match self.codec {
            Codec::Lz4 => {
                let value = u32::from_le_bytes(self.packet[..4].try_into().unwrap());
                let size = (value & 0x7fff_ffff) as usize;
                if value != 0 && size == 0 {
                    return Err("empty nonterminal lz4 block".into());
                }
                (size, size, value == 0)
            }
            Codec::Zstd => {
                let value = u32::from_le_bytes([self.packet[0], self.packet[1], self.packet[2], 0]);
                let size = (value >> 3) as usize;
                let kind = (value >> 1) & 3;
                if kind == 3 {
                    return Err("reserved zstd block type".into());
                }
                (size, if kind == 1 { 1 } else { size }, value & 1 != 0)
            }
        };
        if size > self.block_limit {
            return Err("channel codec block exceeds 64KiB/profile window".into());
        }
        Ok((stored, last))
    }

    fn process_packet(&mut self, output: &mut [u8]) -> Result<(usize, usize, bool), String> {
        let input = &self.packet[self.packet_position..];
        #[cfg(not(any(feature = "lz4", feature = "zstd", feature = "ruzstd")))]
        let _ = (input, &output);
        match &mut self.inner {
            Decoder::Unavailable => Err("channel decoder unavailable".into()),
            #[cfg(feature = "lz4")]
            Decoder::Lz4 {
                history,
                pending,
                position,
            } => {
                let consumed = input.len();
                if self.packet_position == 0 && !self.header && !self.last_block {
                    let raw = self.packet[3] & 0x80 != 0;
                    pending.resize(MAX_STREAM_CHUNK_BYTES, 0);
                    let len = if raw {
                        pending[..input.len() - 4].copy_from_slice(&input[4..]);
                        input.len() - 4
                    } else {
                        lz4_flex::block::decompress_into_with_dict(&input[4..], pending, history)
                            .map_err(err)?
                    };
                    pending.truncate(len);
                    let remove = (history.len() + len).saturating_sub(MAX_STREAM_CHUNK_BYTES);
                    history.drain(..remove);
                    history.extend_from_slice(pending);
                }
                let progress = drain(pending, position, output);
                Ok((consumed, progress.written, pending.is_empty()))
            }
            #[cfg(feature = "zstd")]
            Decoder::Zstd(decoder) => {
                use zstd::stream::raw::Operation;
                let status = decoder.run_on_buffers(input, output).map_err(err)?;
                let all_input = status.bytes_read == input.len();
                // If the output fills exactly, call again with empty input to
                // distinguish pending decoded output from a completed packet.
                let drained =
                    all_input && (status.bytes_written < output.len() || status.remaining == 0);
                Ok((status.bytes_read, status.bytes_written, drained))
            }
            #[cfg(all(feature = "ruzstd", not(feature = "zstd")))]
            Decoder::Ruzstd(decoder) => {
                if self.packet_position == 0 {
                    if self.header {
                        decoder.init(input).map_err(err)?;
                        decoder
                            .set_max_block_output(self.block_limit)
                            .map_err(err)?;
                    } else {
                        decoder
                            .decode_blocks(
                                input,
                                ruzstd::decoding::BlockDecodingStrategy::UptoBlocks(1),
                            )
                            .map_err(err)?;
                    }
                }
                let written = decoder.read_streaming(output);
                Ok((input.len(), written, decoder.streaming_available() == 0))
            }
        }
    }
}
