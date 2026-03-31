pub const RESERVED_MAGIC_COLUMN_PREFIX: char = '$';

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MagicColumnKind {
    CanRead,
    CanEdit,
    CanDelete,
    CreatedBy,
    CreatedAt,
    UpdatedBy,
    UpdatedAt,
}

impl MagicColumnKind {
    pub fn column_name(self) -> &'static str {
        match self {
            MagicColumnKind::CanRead => "$canRead",
            MagicColumnKind::CanEdit => "$canEdit",
            MagicColumnKind::CanDelete => "$canDelete",
            MagicColumnKind::CreatedBy => "$createdBy",
            MagicColumnKind::CreatedAt => "$createdAt",
            MagicColumnKind::UpdatedBy => "$updatedBy",
            MagicColumnKind::UpdatedAt => "$updatedAt",
        }
    }
}

pub fn magic_column_kind(name: &str) -> Option<MagicColumnKind> {
    match name {
        "$canRead" => Some(MagicColumnKind::CanRead),
        "$canEdit" => Some(MagicColumnKind::CanEdit),
        "$canDelete" => Some(MagicColumnKind::CanDelete),
        "$createdBy" => Some(MagicColumnKind::CreatedBy),
        "$createdAt" => Some(MagicColumnKind::CreatedAt),
        "$updatedBy" => Some(MagicColumnKind::UpdatedBy),
        "$updatedAt" => Some(MagicColumnKind::UpdatedAt),
        _ => None,
    }
}

pub fn is_magic_column_name(name: &str) -> bool {
    magic_column_kind(name).is_some()
}

pub fn is_reserved_magic_column_name(name: &str) -> bool {
    name.starts_with(RESERVED_MAGIC_COLUMN_PREFIX)
}
