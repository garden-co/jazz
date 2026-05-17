use crate::query_manager::types::{ColumnDescriptor, ColumnType};

pub const RESERVED_MAGIC_COLUMN_PREFIX: char = '$';
pub const CREATED_AT_COLUMN_NAME: &str = "$createdAt";
pub const UPDATED_AT_COLUMN_NAME: &str = "$updatedAt";

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
            MagicColumnKind::CreatedAt => CREATED_AT_COLUMN_NAME,
            MagicColumnKind::UpdatedBy => "$updatedBy",
            MagicColumnKind::UpdatedAt => UPDATED_AT_COLUMN_NAME,
        }
    }
}

pub fn magic_column_kind(name: &str) -> Option<MagicColumnKind> {
    match name {
        "$canRead" => Some(MagicColumnKind::CanRead),
        "$canEdit" => Some(MagicColumnKind::CanEdit),
        "$canDelete" => Some(MagicColumnKind::CanDelete),
        "$createdBy" => Some(MagicColumnKind::CreatedBy),
        CREATED_AT_COLUMN_NAME => Some(MagicColumnKind::CreatedAt),
        "$updatedBy" => Some(MagicColumnKind::UpdatedBy),
        UPDATED_AT_COLUMN_NAME => Some(MagicColumnKind::UpdatedAt),
        _ => None,
    }
}

pub(crate) fn magic_column_descriptor(kind: MagicColumnKind) -> ColumnDescriptor {
    let descriptor = ColumnDescriptor::new(
        kind.column_name(),
        match kind {
            MagicColumnKind::CanRead | MagicColumnKind::CanEdit | MagicColumnKind::CanDelete => {
                ColumnType::Boolean
            }
            MagicColumnKind::CreatedBy | MagicColumnKind::UpdatedBy => ColumnType::Text,
            MagicColumnKind::CreatedAt | MagicColumnKind::UpdatedAt => ColumnType::Timestamp,
        },
    );

    if matches!(
        kind,
        MagicColumnKind::CanRead | MagicColumnKind::CanEdit | MagicColumnKind::CanDelete
    ) {
        descriptor.nullable()
    } else {
        descriptor
    }
}

pub fn is_magic_column_name(name: &str) -> bool {
    magic_column_kind(name).is_some()
}

pub fn is_reserved_magic_column_name(name: &str) -> bool {
    name.starts_with(RESERVED_MAGIC_COLUMN_PREFIX)
}
