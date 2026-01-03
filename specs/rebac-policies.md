# ReBAC Policy System

## Overview

Relationship-Based Access Control (ReBAC) for Jazz2, where permissions are derived from relationships in the data itself. Policies are expressed as SQL-like conditions and attached to tables, evaluated at query time and sync time.

## Goals

1. **Declarative policies** - Express who can SELECT/INSERT/UPDATE/DELETE as conditions
2. **Relationship inheritance** - Inherit permissions through foreign key references
3. **Query integration** - Combine policies with user queries into efficient incremental computation graphs
4. **Sync-aware** - Same policies determine what data syncs to each client

## Syntax

### SELECT Policy

Defines which rows a user can read:

```sql
CREATE POLICY ON documents FOR SELECT
  WHERE author_id = @viewer
     OR INHERITS SELECT FROM folder_id;
```

### INSERT Policy

Defines whether a user can create new rows and validates the new data:

```sql
CREATE POLICY ON documents FOR INSERT
  CHECK (
    INHERITS UPDATE FROM @new.folder_id
    AND @new.author_id = @viewer
  );
```

Note: INSERT uses only CHECK (not WHERE) since the row doesn't exist yet. All conditions reference `@new`.

### UPDATE Policy

Defines which rows a user can modify and validates the changes:

```sql
CREATE POLICY ON documents FOR UPDATE
  WHERE author_id = @viewer
     OR INHERITS UPDATE FROM folder_id
  CHECK (@new.author_id = @old.author_id);  -- can't change author
```

- WHERE clause: which existing rows can be modified
- CHECK clause: validates the new values against old values

### DELETE Policy

Defines which rows a user can delete:

```sql
CREATE POLICY ON documents FOR DELETE
  WHERE author_id = @viewer;
```

**Default**: If no DELETE policy is specified, falls back to the UPDATE policy.

## Special Variables

| Variable | Available In | Meaning |
|----------|--------------|---------|
| `@viewer` | All policies | The user ID evaluating the policy |
| `@new` | INSERT CHECK, UPDATE CHECK | The incoming/new row data |
| `@old` | UPDATE CHECK | The existing row data before update |

## INHERITS Clause

The `INHERITS` clause enables permission inheritance through foreign key references:

```sql
INHERITS <action> FROM <column>
INHERITS <action> FROM @new.<column>  -- in CHECK clauses
```

### Semantics

`INHERITS SELECT FROM folder_id` means:
1. Look up the row referenced by `folder_id`
2. Evaluate that table's SELECT policy for `@viewer`
3. If it passes, this clause evaluates to true
4. If `folder_id` is NULL, evaluates to false

### Cross-Action Inheritance

You can inherit a different action than you're defining:

```sql
-- "Anyone who can UPDATE the folder can INSERT documents into it"
CREATE POLICY ON documents FOR INSERT
  CHECK (INHERITS UPDATE FROM @new.folder_id);
```

Common patterns:

| Child Action | Inherits From Parent | Use Case |
|--------------|---------------------|----------|
| INSERT | UPDATE | Create children in containers you can edit |
| INSERT | SELECT | Create children in containers you can see (permissive) |
| SELECT | SELECT | Standard read inheritance |
| UPDATE | UPDATE | Standard write inheritance |
| DELETE | UPDATE | Default behavior |

### Recursive Inheritance

For self-referential tables (e.g., folders containing folders):

```sql
CREATE TABLE folders (
  parent_id TEXT REFERENCES folders(id),
  owner_id TEXT
);

CREATE POLICY ON folders FOR SELECT
  WHERE owner_id = @viewer
     OR INHERITS SELECT FROM parent_id;
```

The system walks up the tree until it finds a base case (`owner_id = @viewer`) or reaches a NULL reference.

**Cycle detection**: Track visited row IDs during evaluation. If we revisit an ID, return false and log a warning.

**Depth limit**: Configurable `max_policy_inheritance_depth` (default: 100). If exceeded, return false and log a warning.

## Combinators

### OR (Union)

Grant access if any condition matches:

```sql
CREATE POLICY ON comments FOR SELECT
  WHERE author_id = @viewer
     OR INHERITS SELECT FROM task_id;
```

### AND (Intersection)

Restrict inherited access:

```sql
-- Can read task if you can read the project AND task is not draft
CREATE POLICY ON tasks FOR SELECT
  WHERE INHERITS SELECT FROM project_id
    AND status != 'draft';
```

## Policy Rules

1. **One policy per action per table** - Multiple policies for the same action is an error
2. **Default allow with warning** - If no policy exists, allow access but log a warning once per table per app start
3. **DELETE defaults to UPDATE** - If no DELETE policy, use the UPDATE policy

## Query Integration

Policies are combined with user queries at graph construction time for efficient incremental evaluation:

```
User Query: SELECT * FROM tasks WHERE project_id = ?

Policy: WHERE assignee_id = @viewer OR INHERITS SELECT FROM project_id

Combined Graph:
  TableScan(tasks)
       │
       ▼
  Filter(project_id = ?)  ←── User's filter
       │
       ▼
  Filter(assignee_id = @viewer OR ...)  ←── Policy filter
       │
       ▼
    Output
```

The query planner should:
1. Merge policy predicates with user predicates
2. Order filters by selectivity (most restrictive first)
3. Use indexes where available (e.g., if filtering by a Ref column)

### Incremental Evaluation

When a row changes, the combined graph re-evaluates only affected nodes:

- If a row's `assignee_id` changes, re-evaluate policy filter
- If referenced folder's permissions change, propagate through INHERITS

## Sync Integration

At sync time, the same policies determine what commits to send:

1. For each row the client might need, evaluate SELECT policy
2. Only sync rows where policy passes for that client's `@viewer`
3. Policies evaluate against current state only (not historical states)

## Testing and Debugging

### EXPLAIN POLICY

```sql
EXPLAIN POLICY ON documents FOR SELECT
  WHERE id = '01ABC...'
  AS @viewer = '01XYZ...';

-- Output:
-- ALLOWED via: INHERITS SELECT FROM folder_id
--   folder '01DEF...' allowed via: owner_id = @viewer
```

### Programmatic Testing

```rust
// Check if a user can perform an action
db.check_policy(
    table: "documents",
    action: PolicyAction::Select,
    row_id: doc_id,
    viewer: user_id,
) -> PolicyResult;

pub enum PolicyResult {
    Allowed { via: String },  // Human-readable explanation
    Denied { reason: String },
}
```

### Property-Based Testing (Future)

Support for defining and testing policy invariants:

```sql
-- Invariant: UPDATE implies SELECT
CREATE POLICY INVARIANT ON documents
  ASSERT (CAN UPDATE IMPLIES CAN SELECT);
```

```rust
// Property test
proptest! {
    fn update_implies_select(user: ObjectId, doc: Document) {
        let can_update = db.check_policy(Update, &doc.id, user);
        let can_select = db.check_policy(Select, &doc.id, user);
        prop_assert!(can_update.implies(can_select));
    }
}
```

## Implementation

### AST Types

```rust
pub enum PolicyAction {
    Select,
    Insert,
    Update,
    Delete,
}

pub struct Policy {
    pub table: String,
    pub action: PolicyAction,
    pub where_clause: Option<PolicyExpr>,  // For SELECT, UPDATE, DELETE
    pub check_clause: Option<PolicyExpr>,  // For INSERT, UPDATE
}

pub enum PolicyExpr {
    // Comparisons
    Eq(PolicyValue, PolicyValue),
    Ne(PolicyValue, PolicyValue),
    // ...other comparisons...

    // Combinators
    And(Vec<PolicyExpr>),
    Or(Vec<PolicyExpr>),
    Not(Box<PolicyExpr>),

    // Inheritance
    Inherits {
        action: PolicyAction,
        column: PolicyColumnRef,  // Either "col" or "@new.col"
    },
}

pub enum PolicyValue {
    Column(String),           // Column on current row
    OldColumn(String),        // @old.column
    NewColumn(String),        // @new.column
    Viewer,                   // @viewer
    Literal(Value),           // Constant value
}

pub enum PolicyColumnRef {
    Current(String),          // column (for WHERE)
    New(String),              // @new.column (for CHECK)
}
```

### Storage

Policies are stored as table metadata alongside column definitions:

```rust
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub policies: HashMap<PolicyAction, Policy>,  // At most one per action
}
```

### Evaluation

```rust
pub struct PolicyEvaluator<'a> {
    db: &'a Database,
    viewer: ObjectId,
    visited: HashSet<(String, ObjectId)>,  // For cycle detection
    depth: usize,
}

impl PolicyEvaluator<'_> {
    pub fn check_select(&mut self, table: &str, row_id: ObjectId) -> bool;
    pub fn check_insert(&mut self, table: &str, new_row: &Row) -> bool;
    pub fn check_update(&mut self, table: &str, row_id: ObjectId, new_row: &Row) -> bool;
    pub fn check_delete(&mut self, table: &str, row_id: ObjectId) -> bool;

    fn eval_expr(&mut self, expr: &PolicyExpr, ctx: &EvalContext) -> bool;
    fn eval_inherits(&mut self, action: PolicyAction, ref_id: ObjectId, ref_table: &str) -> bool;
}
```

### Configuration

```rust
pub struct PolicyConfig {
    /// Maximum depth for recursive INHERITS evaluation
    pub max_inheritance_depth: usize,  // Default: 100

    /// Log warning when table has no policy (once per table per startup)
    pub warn_on_missing_policy: bool,  // Default: true
}
```

## SQL Parser Extensions

New grammar rules:

```
create_policy := CREATE POLICY ON table_name FOR action policy_clauses

action := SELECT | INSERT | UPDATE | DELETE

policy_clauses := where_clause? check_clause?

where_clause := WHERE policy_expr

check_clause := CHECK '(' policy_expr ')'

policy_expr := policy_term ((AND | OR) policy_term)*

policy_term := policy_comparison
             | INHERITS action FROM column_ref
             | NOT policy_term
             | '(' policy_expr ')'

policy_comparison := policy_value comparison_op policy_value

policy_value := '@viewer'
              | '@old' '.' IDENTIFIER
              | '@new' '.' IDENTIFIER
              | IDENTIFIER
              | literal

column_ref := IDENTIFIER
            | '@new' '.' IDENTIFIER
```

## Implementation Phases

### Phase 1: Core Types and Parser
- [ ] `PolicyAction`, `Policy`, `PolicyExpr` types
- [ ] SQL parser for CREATE POLICY
- [ ] Store policies in TableSchema

### Phase 2: Evaluation Engine
- [ ] `PolicyEvaluator` with cycle detection and depth limit
- [ ] Basic expression evaluation (comparisons, AND/OR/NOT)
- [ ] INHERITS evaluation with recursive lookup
- [ ] Integration with SELECT queries (filter results)

### Phase 3: Write Policies
- [ ] INSERT policy evaluation (CHECK on @new)
- [ ] UPDATE policy evaluation (WHERE on existing, CHECK on @old/@new)
- [ ] DELETE policy evaluation (WHERE, fallback to UPDATE)

### Phase 4: Query Integration
- [ ] Combine policy predicates with user query predicates
- [ ] Integrate with incremental query graph builder
- [ ] Optimize predicate ordering by selectivity

### Phase 5: Testing and Debugging
- [ ] EXPLAIN POLICY command
- [ ] `check_policy()` programmatic API
- [ ] Policy invariant assertions (future)
- [ ] Property-based testing helpers (future)

## Examples

### Basic Document Sharing

```sql
CREATE TABLE users (
  name TEXT
);

CREATE TABLE folders (
  name TEXT,
  owner_id TEXT REFERENCES users(id),
  parent_id TEXT REFERENCES folders(id)
);

CREATE TABLE documents (
  title TEXT,
  content TEXT,
  folder_id TEXT REFERENCES folders(id),
  author_id TEXT REFERENCES users(id)
);

-- Folder policies
CREATE POLICY ON folders FOR SELECT
  WHERE owner_id = @viewer
     OR INHERITS SELECT FROM parent_id;

CREATE POLICY ON folders FOR UPDATE
  WHERE owner_id = @viewer;

CREATE POLICY ON folders FOR INSERT
  CHECK (
    @new.owner_id = @viewer
    OR INHERITS UPDATE FROM @new.parent_id
  );

-- Document policies
CREATE POLICY ON documents FOR SELECT
  WHERE INHERITS SELECT FROM folder_id;

CREATE POLICY ON documents FOR INSERT
  CHECK (
    INHERITS UPDATE FROM @new.folder_id
    AND @new.author_id = @viewer
  );

CREATE POLICY ON documents FOR UPDATE
  WHERE author_id = @viewer
     OR INHERITS UPDATE FROM folder_id;
```

### Team-Based Access

```sql
CREATE TABLE teams (
  name TEXT
);

CREATE TABLE team_members (
  team_id TEXT REFERENCES teams(id),
  user_id TEXT REFERENCES users(id),
  role TEXT  -- 'admin', 'member', 'viewer'
);

CREATE TABLE projects (
  name TEXT,
  team_id TEXT REFERENCES teams(id)
);

-- Team membership check via subquery (future enhancement)
-- For now, use a join-based approach or denormalize

CREATE POLICY ON projects FOR SELECT
  WHERE EXISTS (
    SELECT 1 FROM team_members
    WHERE team_members.team_id = projects.team_id
      AND team_members.user_id = @viewer
  );
```

Note: The EXISTS subquery syntax is a future enhancement. Initial implementation may require denormalizing team membership or using application-level checks.
