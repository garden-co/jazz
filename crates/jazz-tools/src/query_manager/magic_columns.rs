#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MagicColumnKind {
    CanRead,
    CanEdit,
    CanDelete,
}

impl MagicColumnKind {
    pub fn column_name(self) -> &'static str {
        match self {
            MagicColumnKind::CanRead => "_canRead",
            MagicColumnKind::CanEdit => "_canEdit",
            MagicColumnKind::CanDelete => "_canDelete",
        }
    }
}

pub fn magic_column_kind(name: &str) -> Option<MagicColumnKind> {
    match name {
        "_canRead" => Some(MagicColumnKind::CanRead),
        "_canEdit" => Some(MagicColumnKind::CanEdit),
        "_canDelete" => Some(MagicColumnKind::CanDelete),
        _ => None,
    }
}

pub fn is_magic_column_name(name: &str) -> bool {
    magic_column_kind(name).is_some()
}
