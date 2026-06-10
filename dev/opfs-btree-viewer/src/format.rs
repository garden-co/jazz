use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use opfs_btree::RawPageKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreviewMode {
    Utf8,
    Hex,
    Base64,
}

impl PreviewMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::Hex => "hex",
            Self::Base64 => "base64",
        }
    }

    pub fn all() -> [Self; 3] {
        [Self::Utf8, Self::Hex, Self::Base64]
    }
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    if bytes < 1024 * 1024 {
        return format!("{:.1} KiB", bytes as f64 / 1024.0);
    }
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

pub fn format_value(bytes: &[u8], mode: PreviewMode, limit: usize) -> String {
    match mode {
        PreviewMode::Utf8 => bytes_to_utf8(bytes, limit),
        PreviewMode::Hex => bytes_to_hex(bytes, limit),
        PreviewMode::Base64 => bytes_to_base64(bytes, limit),
    }
}

pub fn bytes_to_utf8(bytes: &[u8], limit: usize) -> String {
    let clipped = clip(bytes, limit);
    let mut text = String::from_utf8_lossy(clipped).into_owned();
    text = text
        .chars()
        .map(|ch| {
            if ch == '\n' || ch == '\t' || !ch.is_control() {
                ch
            } else {
                '.'
            }
        })
        .collect();
    if clipped.len() < bytes.len() {
        text.push_str("\n...");
    }
    text
}

pub fn bytes_to_hex(bytes: &[u8], limit: usize) -> String {
    let clipped = clip(bytes, limit);
    let mut out = clipped
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    if clipped.len() < bytes.len() {
        out.push_str(" ...");
    }
    out
}

fn bytes_to_base64(bytes: &[u8], limit: usize) -> String {
    let clipped = clip(bytes, limit);
    let mut out = STANDARD.encode(clipped);
    if clipped.len() < bytes.len() {
        out.push_str("...");
    }
    out
}

pub fn page_kind_label(kind: RawPageKind) -> &'static str {
    match kind {
        RawPageKind::SuperblockA => "superblock A",
        RawPageKind::SuperblockB => "superblock B",
        RawPageKind::Internal => "internal",
        RawPageKind::Leaf => "leaf",
        RawPageKind::Overflow => "overflow",
        RawPageKind::Freelist => "freelist",
        RawPageKind::Corrupt => "corrupt",
    }
}

fn clip(bytes: &[u8], limit: usize) -> &[u8] {
    if limit == usize::MAX {
        return bytes;
    }
    &bytes[..bytes.len().min(limit)]
}
