//! Lens types for schema migrations.
//!
//! Lenses define bidirectional row transformations between schema versions.
//! They are declarative (auto-invertible) and support row-level operations.

use uuid::Uuid;

use crate::model::public_api::types::{ColumnType, SchemaHash, TableSchema, Value};
use crate::object::ObjectId;

/// Direction for lens application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Apply lens forward: source schema → target schema
    Forward,
    /// Apply lens backward: target schema → source schema
    Backward,
}

impl Direction {
    pub fn reverse(&self) -> Self {
        match self {
            Direction::Forward => Direction::Backward,
            Direction::Backward => Direction::Forward,
        }
    }
}

/// A single lens operation representing a schema change.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LensOp {
    /// Rename a table.
    RenameTable { old_name: String, new_name: String },
    /// Add a column with a default value.
    AddColumn {
        table: String,
        column: String,
        column_type: ColumnType,
        default: Value,
    },
    /// Remove a column.
    RemoveColumn {
        table: String,
        column: String,
        /// Stored column type (needed for backward transform).
        column_type: ColumnType,
        /// Default value to use when reversing (adding column back).
        default: Value,
    },
    /// Rename a column.
    RenameColumn {
        table: String,
        old_name: String,
        new_name: String,
    },
    /// Add a new table.
    AddTable { table: String, schema: TableSchema },
    /// Remove a table.
    RemoveTable {
        table: String,
        /// Stored schema (needed for backward transform).
        schema: TableSchema,
    },
}

impl LensOp {
    /// Invert this operation (for computing backward transform).
    pub fn invert(&self) -> LensOp {
        match self {
            LensOp::RenameTable { old_name, new_name } => LensOp::RenameTable {
                old_name: new_name.clone(),
                new_name: old_name.clone(),
            },
            LensOp::AddColumn {
                table,
                column,
                column_type,
                default,
            } => LensOp::RemoveColumn {
                table: table.clone(),
                column: column.clone(),
                column_type: column_type.clone(),
                default: default.clone(),
            },
            LensOp::RemoveColumn {
                table,
                column,
                column_type,
                default,
            } => LensOp::AddColumn {
                table: table.clone(),
                column: column.clone(),
                column_type: column_type.clone(),
                default: default.clone(),
            },
            LensOp::RenameColumn {
                table,
                old_name,
                new_name,
            } => LensOp::RenameColumn {
                table: table.clone(),
                old_name: new_name.clone(),
                new_name: old_name.clone(),
            },
            LensOp::AddTable { table, schema } => LensOp::RemoveTable {
                table: table.clone(),
                schema: schema.clone(),
            },
            LensOp::RemoveTable { table, schema } => LensOp::AddTable {
                table: table.clone(),
                schema: schema.clone(),
            },
        }
    }
}

/// A lens transform containing a sequence of operations.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LensTransform {
    /// Ordered list of operations.
    pub ops: Vec<LensOp>,
    /// Indices of ops that are draft (auto-generated with uncertainty).
    /// These require manual review before use in production.
    pub draft_ops: Vec<usize>,
}

impl LensTransform {
    /// Create a new empty transform.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a transform with operations, all non-draft.
    pub fn with_ops(ops: Vec<LensOp>) -> Self {
        Self {
            ops,
            draft_ops: Vec::new(),
        }
    }

    /// Add an operation, optionally marking it as draft.
    pub fn push(&mut self, op: LensOp, is_draft: bool) {
        if is_draft {
            self.draft_ops.push(self.ops.len());
        }
        self.ops.push(op);
    }

    /// Check if any operations are draft.
    pub fn has_drafts(&self) -> bool {
        !self.draft_ops.is_empty()
    }

    /// Invert the entire transform (reverse all ops in reverse order).
    pub fn invert(&self) -> Self {
        let ops: Vec<LensOp> = self.ops.iter().rev().map(|op| op.invert()).collect();
        // Draft indices need to be remapped for reversed order
        let draft_ops: Vec<usize> = self
            .draft_ops
            .iter()
            .map(|&i| self.ops.len() - 1 - i)
            .collect();
        Self { ops, draft_ops }
    }
}

/// A complete lens mapping between two schema versions.
#[derive(Debug, Clone, PartialEq)]
pub struct Lens {
    /// Hash of the source schema.
    pub source_hash: SchemaHash,
    /// Hash of the target schema.
    pub target_hash: SchemaHash,
    /// Forward transformation: source → target.
    pub forward: LensTransform,
    /// Backward transformation: target → source.
    /// Auto-computed from forward by default.
    pub backward: LensTransform,
}

impl Lens {
    /// Create a new lens with forward operations.
    /// Backward transform is automatically computed.
    pub fn new(source_hash: SchemaHash, target_hash: SchemaHash, forward: LensTransform) -> Self {
        let backward = forward.invert();
        Self {
            source_hash,
            target_hash,
            forward,
            backward,
        }
    }

    /// Create a lens with explicit forward and backward transforms.
    pub fn with_backward(
        source_hash: SchemaHash,
        target_hash: SchemaHash,
        forward: LensTransform,
        backward: LensTransform,
    ) -> Self {
        Self {
            source_hash,
            target_hash,
            forward,
            backward,
        }
    }

    /// Compute an ObjectId for storage: UUIDv5(source_hash, target_hash).
    pub fn object_id(&self) -> ObjectId {
        // Use DNS namespace as a base, then hash our content
        let namespace = Uuid::NAMESPACE_DNS;
        let mut content = Vec::with_capacity(64);
        content.extend_from_slice(self.source_hash.as_bytes());
        content.extend_from_slice(self.target_hash.as_bytes());
        ObjectId::from_uuid(Uuid::new_v5(&namespace, &content))
    }

    /// Check if this lens has any draft operations.
    pub fn is_draft(&self) -> bool {
        self.forward.has_drafts() || self.backward.has_drafts()
    }

    /// Tables the forward transform adds and removes, in operation order.
    pub fn table_changes(&self) -> (Vec<String>, Vec<String>) {
        let (mut added, mut removed) = (Vec::new(), Vec::new());
        for op in &self.forward.ops {
            match op {
                LensOp::AddTable { table, .. } => added.push(table.clone()),
                LensOp::RemoveTable { table, .. } => removed.push(table.clone()),
                _ => {}
            }
        }
        (added, removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hash(n: u8) -> SchemaHash {
        SchemaHash::from_bytes([n; 32])
    }

    #[test]
    fn lens_op_invert_add_column() {
        let op = LensOp::AddColumn {
            table: "users".to_string(),
            column: "email".to_string(),
            column_type: ColumnType::Text,
            default: Value::Null,
        };

        let inverted = op.invert();
        assert!(matches!(inverted, LensOp::RemoveColumn { .. }));

        if let LensOp::RemoveColumn {
            table,
            column,
            column_type,
            ..
        } = inverted
        {
            assert_eq!(table, "users");
            assert_eq!(column, "email");
            assert_eq!(column_type, ColumnType::Text);
        }
    }

    #[test]
    fn lens_op_invert_rename() {
        let op = LensOp::RenameColumn {
            table: "users".to_string(),
            old_name: "email".to_string(),
            new_name: "email_address".to_string(),
        };

        let inverted = op.invert();
        if let LensOp::RenameColumn {
            old_name, new_name, ..
        } = inverted
        {
            assert_eq!(old_name, "email_address");
            assert_eq!(new_name, "email");
        }
    }

    #[test]
    fn lens_op_invert_rename_table() {
        let op = LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        };

        let inverted = op.invert();
        if let LensOp::RenameTable { old_name, new_name } = inverted {
            assert_eq!(old_name, "people");
            assert_eq!(new_name, "users");
        } else {
            panic!("expected RenameTable");
        }
    }

    #[test]
    fn lens_transform_invert() {
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "a".to_string(),
                column_type: ColumnType::Integer,
                default: Value::Integer(0),
            },
            false,
        );
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "b".to_string(),
                new_name: "c".to_string(),
            },
            false,
        );

        let inverted = transform.invert();
        assert_eq!(inverted.ops.len(), 2);

        // First op should be inverted rename (was second)
        assert!(matches!(inverted.ops[0], LensOp::RenameColumn { .. }));
        // Second op should be remove (was add first)
        assert!(matches!(inverted.ops[1], LensOp::RemoveColumn { .. }));
    }

    #[test]
    fn lens_object_id_deterministic() {
        let source = make_hash(1);
        let target = make_hash(2);

        let lens1 = Lens::new(source, target, LensTransform::new());
        let lens2 = Lens::new(source, target, LensTransform::new());

        assert_eq!(lens1.object_id(), lens2.object_id());
    }

    #[test]
    fn lens_object_id_different_for_different_hashes() {
        let source = make_hash(1);
        let target1 = make_hash(2);
        let target2 = make_hash(3);

        let lens1 = Lens::new(source, target1, LensTransform::new());
        let lens2 = Lens::new(source, target2, LensTransform::new());

        assert_ne!(lens1.object_id(), lens2.object_id());
    }

    #[test]
    fn lens_is_draft() {
        let source = make_hash(1);
        let target = make_hash(2);

        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".to_string(),
                column: "maybe".to_string(),
                column_type: ColumnType::Text,
                default: Value::Null,
            },
            true, // draft
        );

        let lens = Lens::new(source, target, transform);
        assert!(lens.is_draft());

        // Non-draft lens
        let lens2 = Lens::new(source, target, LensTransform::new());
        assert!(!lens2.is_draft());
    }
}
