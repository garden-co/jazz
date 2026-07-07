#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum MagicColumnKind {
    CanRead,
    CreatedBy,
    CreatedAt,
    UpdatedBy,
    UpdatedAt,
}

pub(super) fn magic_column_kind(name: &str) -> Option<MagicColumnKind> {
    match name {
        "$canRead" => Some(MagicColumnKind::CanRead),
        "$createdBy" => Some(MagicColumnKind::CreatedBy),
        "$createdAt" => Some(MagicColumnKind::CreatedAt),
        "$updatedBy" => Some(MagicColumnKind::UpdatedBy),
        "$updatedAt" => Some(MagicColumnKind::UpdatedAt),
        _ => None,
    }
}

pub(super) fn is_magic_column_name(name: &str) -> bool {
    magic_column_kind(name).is_some()
}
