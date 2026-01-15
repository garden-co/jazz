//! Integration tests for SQL parser.

use groove::ObjectId;
use groove::sql::{
    ColumnType, ConditionValue, PolicyAction, PolicyColumnRef, PolicyExpr, PolicyValue,
    PredicateValue, Projection, SelectExpr, Statement, parse,
};

#[test]
fn parse_create_table_simple() {
    let sql = "CREATE TABLE users (name STRING NOT NULL, age I64)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.name, "users");
            assert_eq!(ct.columns.len(), 2);
            assert_eq!(ct.columns[0].name, "name");
            assert_eq!(ct.columns[0].ty, ColumnType::String);
            assert!(!ct.columns[0].nullable);
            assert_eq!(ct.columns[1].name, "age");
            assert_eq!(ct.columns[1].ty, ColumnType::I64);
            assert!(ct.columns[1].nullable);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_create_table_with_ref() {
    let sql = "CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.name, "posts");
            assert_eq!(ct.columns[0].ty, ColumnType::Ref("users".into()));
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn parse_insert() {
    let sql = "INSERT INTO users (name, age, active) VALUES ('Alice', 30, true)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.table, "users");
            assert_eq!(ins.columns, vec!["name", "age", "active"]);
            assert_eq!(
                ins.values,
                vec![
                    PredicateValue::String("Alice".into()),
                    PredicateValue::I64(30),
                    PredicateValue::Bool(true),
                ]
            );
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_insert_with_null() {
    let sql = "INSERT INTO users (name, email) VALUES ('Bob', NULL)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(
                ins.values,
                vec![PredicateValue::String("Bob".into()), PredicateValue::Null]
            );
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_insert_with_object_id() {
    // ObjectIds are passed as string literals containing Base32.
    // The parser produces PredicateValue::String; the executor coerces to Ref
    // when inserting into a Ref column.
    let id = ObjectId::new(0x0192abcd12345678);
    let sql = format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Hello')",
        id
    );
    let stmt = parse(&sql).unwrap();

    match stmt {
        Statement::Insert(ins) => {
            // Parser produces String, not Ref (executor handles coercion)
            assert_eq!(ins.values[0], PredicateValue::String(id.to_string()));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_update() {
    // ObjectIds are passed as string literals containing Base32.
    // The parser produces PredicateValue::String; the executor coerces to Ref
    // when comparing against id or Ref columns.
    let id = ObjectId::new(0xabc123);
    let sql = format!(
        "UPDATE users SET email = 'new@example.com', age = 31 WHERE id = '{}'",
        id
    );
    let stmt = parse(&sql).unwrap();

    match stmt {
        Statement::Update(upd) => {
            assert_eq!(upd.table, "users");
            assert_eq!(upd.assignments.len(), 2);
            assert_eq!(
                upd.assignments[0],
                (
                    "email".into(),
                    PredicateValue::String("new@example.com".into())
                )
            );
            assert_eq!(upd.assignments[1], ("age".into(), PredicateValue::I64(31)));
            assert_eq!(upd.where_clause.len(), 1);
            assert_eq!(upd.where_clause[0].column.column, "id");
            // Parser produces String, not Ref (executor handles coercion)
            assert_eq!(
                upd.where_clause[0].right,
                ConditionValue::Literal(PredicateValue::String(id.to_string()))
            );
        }
        _ => panic!("expected Update"),
    }
}

#[test]
fn parse_delete() {
    let id = ObjectId::new(0xdef456);
    let sql = format!("DELETE FROM users WHERE id = '{}'", id);
    let stmt = parse(&sql).unwrap();

    match stmt {
        Statement::Delete(del) => {
            assert_eq!(del.table, "users");
            assert_eq!(del.where_clause.len(), 1);
            assert_eq!(del.where_clause[0].column.column, "id");
            assert_eq!(
                del.where_clause[0].right,
                ConditionValue::Literal(PredicateValue::String(id.to_string()))
            );
            assert!(!del.hard, "default delete should be soft");
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn parse_delete_all() {
    let sql = "DELETE FROM users";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Delete(del) => {
            assert_eq!(del.table, "users");
            assert!(del.where_clause.is_empty());
            assert!(!del.hard, "default delete should be soft");
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn parse_delete_hard() {
    let id = ObjectId::new(0xabc123);
    let sql = format!("DELETE FROM users WHERE id = '{}' HARD", id);
    let stmt = parse(&sql).unwrap();

    match stmt {
        Statement::Delete(del) => {
            assert_eq!(del.table, "users");
            assert_eq!(del.where_clause.len(), 1);
            assert!(del.hard, "DELETE ... HARD should set hard=true");
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn parse_delete_hard_all() {
    let sql = "DELETE FROM users HARD";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Delete(del) => {
            assert_eq!(del.table, "users");
            assert!(del.where_clause.is_empty());
            assert!(del.hard, "DELETE ... HARD should set hard=true");
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn parse_select_star() {
    let sql = "SELECT * FROM users";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            assert_eq!(sel.projection, Projection::All);
            assert_eq!(sel.from.table, "users");
            assert!(sel.from.joins.is_empty());
            assert!(sel.where_clause.is_empty());
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_columns() {
    let sql = "SELECT name, email FROM users";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => match sel.projection {
            Projection::Expressions(exprs) => {
                assert_eq!(exprs.len(), 2);
                match &exprs[0] {
                    SelectExpr::Column(qc) => assert_eq!(qc.column, "name"),
                    _ => panic!("expected Column"),
                }
                match &exprs[1] {
                    SelectExpr::Column(qc) => assert_eq!(qc.column, "email"),
                    _ => panic!("expected Column"),
                }
            }
            _ => panic!("expected Expressions"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_with_where() {
    let sql = "SELECT * FROM users WHERE active = true AND age = 30";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            assert_eq!(sel.where_clause.len(), 2);
            assert_eq!(sel.where_clause[0].column.column, "active");
            assert_eq!(
                sel.where_clause[0].right,
                ConditionValue::Literal(PredicateValue::Bool(true))
            );
            assert_eq!(sel.where_clause[1].column.column, "age");
            assert_eq!(
                sel.where_clause[1].right,
                ConditionValue::Literal(PredicateValue::I64(30))
            );
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_with_join() {
    let sql = "SELECT * FROM comments JOIN users ON comments.author = users.id";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            assert_eq!(sel.from.table, "comments");
            assert_eq!(sel.from.joins.len(), 1);
            assert_eq!(sel.from.joins[0].table, "users");
            assert_eq!(sel.from.joins[0].on.left.table, Some("comments".into()));
            assert_eq!(sel.from.joins[0].on.left.column, "author");
            assert_eq!(sel.from.joins[0].on.right.table, Some("users".into()));
            assert_eq!(sel.from.joins[0].on.right.column, "id");
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_table_star() {
    let sql = "SELECT users.* FROM comments JOIN users ON comments.author = users.id";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            assert_eq!(sel.projection, Projection::TableAll("users".into()));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_float() {
    let sql = "INSERT INTO data (value) VALUES (1.234)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.values[0], PredicateValue::F64(1.234));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_negative_number() {
    let sql = "INSERT INTO data (value) VALUES (-42)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.values[0], PredicateValue::I64(-42));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_with_comments() {
    let sql = "-- This is a comment\nSELECT * FROM users";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(_) => {}
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_with_semicolon() {
    let sql = "SELECT * FROM users;";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(_) => {}
        _ => panic!("expected Select"),
    }
}

#[test]
fn case_insensitive_keywords() {
    let sql = "select * from users where active = TRUE";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            assert_eq!(
                sel.where_clause[0].right,
                ConditionValue::Literal(PredicateValue::Bool(true))
            );
        }
        _ => panic!("expected Select"),
    }
}

// ========== ARRAY Subquery Tests ==========

#[test]
fn parse_array_subquery() {
    let sql = "SELECT f.id, ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id) AS notes FROM folders f";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            // Check FROM clause has alias
            assert_eq!(sel.from.table, "folders");
            assert_eq!(sel.from.alias, Some("f".to_string()));

            // Check projection has 2 expressions
            match sel.projection {
                Projection::Expressions(exprs) => {
                    assert_eq!(exprs.len(), 2);

                    // First: f.id
                    match &exprs[0] {
                        SelectExpr::Column(qc) => {
                            assert_eq!(qc.table, Some("f".to_string()));
                            assert_eq!(qc.column, "id");
                        }
                        _ => panic!("expected Column"),
                    }

                    // Second: ARRAY(...) AS notes
                    match &exprs[1] {
                        SelectExpr::Aliased { expr, alias } => {
                            assert_eq!(alias, "notes");
                            match expr.as_ref() {
                                SelectExpr::ArraySubquery(subquery) => {
                                    // Check subquery FROM
                                    assert_eq!(subquery.from.table, "notes");
                                    assert_eq!(subquery.from.alias, Some("n".to_string()));

                                    // Check subquery projection is just "n" (table alias)
                                    match &subquery.projection {
                                        Projection::Expressions(sub_exprs) => {
                                            assert_eq!(sub_exprs.len(), 1);
                                            match &sub_exprs[0] {
                                                SelectExpr::Column(qc) => {
                                                    assert_eq!(qc.table, None);
                                                    assert_eq!(qc.column, "n");
                                                }
                                                _ => panic!("expected Column for table alias"),
                                            }
                                        }
                                        _ => panic!("expected Expressions"),
                                    }

                                    // Check subquery WHERE
                                    assert_eq!(subquery.where_clause.len(), 1);
                                    assert_eq!(
                                        subquery.where_clause[0].column.table,
                                        Some("n".to_string())
                                    );
                                    assert_eq!(subquery.where_clause[0].column.column, "folder_id");
                                }
                                _ => panic!("expected ArraySubquery"),
                            }
                        }
                        _ => panic!("expected Aliased"),
                    }
                }
                _ => panic!("expected Expressions"),
            }
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_table_alias() {
    let sql = "SELECT u.name FROM users u WHERE u.active = true";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            assert_eq!(sel.from.table, "users");
            assert_eq!(sel.from.alias, Some("u".to_string()));

            match sel.projection {
                Projection::Expressions(exprs) => {
                    assert_eq!(exprs.len(), 1);
                    match &exprs[0] {
                        SelectExpr::Column(qc) => {
                            assert_eq!(qc.table, Some("u".to_string()));
                            assert_eq!(qc.column, "name");
                        }
                        _ => panic!("expected Column"),
                    }
                }
                _ => panic!("expected Expressions"),
            }
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_array_subquery_simple() {
    let sql = "SELECT ARRAY(SELECT title FROM notes) FROM folders";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => match sel.projection {
            Projection::Expressions(exprs) => {
                assert_eq!(exprs.len(), 1);
                match &exprs[0] {
                    SelectExpr::ArraySubquery(subquery) => {
                        assert_eq!(subquery.from.table, "notes");
                        match &subquery.projection {
                            Projection::Expressions(sub_exprs) => {
                                assert_eq!(sub_exprs.len(), 1);
                                match &sub_exprs[0] {
                                    SelectExpr::Column(qc) => {
                                        assert_eq!(qc.column, "title");
                                    }
                                    _ => panic!("expected Column"),
                                }
                            }
                            _ => panic!("expected Expressions in subquery"),
                        }
                    }
                    _ => panic!("expected ArraySubquery"),
                }
            }
            _ => panic!("expected Expressions"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_array_subquery_with_multiple_columns() {
    // This is the SQL generated for Issues with IssueLabels include
    let sql = "SELECT i.id, i.title, i.description, i.status, i.priority, i.project, i.createdAt, i.updatedAt, ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label FROM IssueLabels i_inner WHERE i_inner.issue = i.id) as IssueLabels FROM Issues i";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(sel) => {
            // Should be Expressions projection with 9 items (8 columns + 1 ARRAY)
            match &sel.projection {
                Projection::Expressions(exprs) => {
                    assert_eq!(exprs.len(), 9, "Expected 9 expressions in projection");

                    // Last expression should be ARRAY subquery with alias
                    match &exprs[8] {
                        SelectExpr::Aliased { expr, alias } => {
                            assert_eq!(alias, "IssueLabels");
                            match expr.as_ref() {
                                SelectExpr::ArraySubquery(subquery) => {
                                    assert_eq!(subquery.from.table, "IssueLabels");
                                    assert_eq!(subquery.where_clause.len(), 1);
                                }
                                _ => panic!("expected ArraySubquery, got {:?}", expr),
                            }
                        }
                        _ => panic!("expected Aliased, got {:?}", exprs[8]),
                    }
                }
                _ => panic!("expected Expressions, got {:?}", sel.projection),
            }
        }
        _ => panic!("expected Select"),
    }
}

// ========== Policy Parsing Tests ==========

#[test]
fn parse_policy_simple_select() {
    let sql = "CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.table, "documents");
            assert_eq!(policy.action, PolicyAction::Select);
            assert!(policy.check_clause.is_none());

            match policy.where_clause {
                Some(PolicyExpr::Eq(left, right)) => {
                    assert_eq!(left, PolicyValue::Column("owner_id".into()));
                    assert_eq!(right, PolicyValue::Viewer);
                }
                _ => panic!("expected Eq expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_with_inherits() {
    let sql = "CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Select);

            match policy.where_clause {
                Some(PolicyExpr::Inherits { action, column }) => {
                    assert_eq!(action, PolicyAction::Select);
                    assert_eq!(column, PolicyColumnRef::Current("folder_id".into()));
                }
                _ => panic!("expected Inherits expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_with_or() {
    let sql = "CREATE POLICY ON tasks FOR SELECT WHERE assignee_id = @viewer OR INHERITS SELECT FROM project_id";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => match policy.where_clause {
            Some(PolicyExpr::Or(exprs)) => {
                assert_eq!(exprs.len(), 2);

                match &exprs[0] {
                    PolicyExpr::Eq(left, right) => {
                        assert_eq!(*left, PolicyValue::Column("assignee_id".into()));
                        assert_eq!(*right, PolicyValue::Viewer);
                    }
                    _ => panic!("expected Eq"),
                }

                match &exprs[1] {
                    PolicyExpr::Inherits { action, column } => {
                        assert_eq!(*action, PolicyAction::Select);
                        assert_eq!(*column, PolicyColumnRef::Current("project_id".into()));
                    }
                    _ => panic!("expected Inherits"),
                }
            }
            _ => panic!("expected Or expression"),
        },
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_insert_with_check() {
    let sql = "CREATE POLICY ON documents FOR INSERT CHECK (@new.author_id = @viewer AND INHERITS UPDATE FROM @new.folder_id)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Insert);
            assert!(policy.where_clause.is_none());

            match policy.check_clause {
                Some(PolicyExpr::And(exprs)) => {
                    assert_eq!(exprs.len(), 2);

                    match &exprs[0] {
                        PolicyExpr::Eq(left, right) => {
                            assert_eq!(*left, PolicyValue::NewColumn("author_id".into()));
                            assert_eq!(*right, PolicyValue::Viewer);
                        }
                        _ => panic!("expected Eq"),
                    }

                    match &exprs[1] {
                        PolicyExpr::Inherits { action, column } => {
                            assert_eq!(*action, PolicyAction::Update);
                            assert_eq!(*column, PolicyColumnRef::New("folder_id".into()));
                        }
                        _ => panic!("expected Inherits"),
                    }
                }
                _ => panic!("expected And expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_update_with_where_and_check() {
    let sql = "CREATE POLICY ON documents FOR UPDATE WHERE author_id = @viewer CHECK (@new.author_id = @old.author_id)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Update);

            // WHERE clause
            match &policy.where_clause {
                Some(PolicyExpr::Eq(left, right)) => {
                    assert_eq!(*left, PolicyValue::Column("author_id".into()));
                    assert_eq!(*right, PolicyValue::Viewer);
                }
                _ => panic!("expected WHERE Eq"),
            }

            // CHECK clause
            match &policy.check_clause {
                Some(PolicyExpr::Eq(left, right)) => {
                    assert_eq!(*left, PolicyValue::NewColumn("author_id".into()));
                    assert_eq!(*right, PolicyValue::OldColumn("author_id".into()));
                }
                _ => panic!("expected CHECK Eq"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_with_literal() {
    let sql = "CREATE POLICY ON tasks FOR SELECT WHERE status != 'draft' AND priority > 5";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => match policy.where_clause {
            Some(PolicyExpr::And(exprs)) => {
                assert_eq!(exprs.len(), 2);

                match &exprs[0] {
                    PolicyExpr::Ne(left, right) => {
                        assert_eq!(*left, PolicyValue::Column("status".into()));
                        assert_eq!(
                            *right,
                            PolicyValue::Literal(PredicateValue::String("draft".into()))
                        );
                    }
                    _ => panic!("expected Ne"),
                }

                match &exprs[1] {
                    PolicyExpr::Gt(left, right) => {
                        assert_eq!(*left, PolicyValue::Column("priority".into()));
                        assert_eq!(*right, PolicyValue::Literal(PredicateValue::I64(5)));
                    }
                    _ => panic!("expected Gt"),
                }
            }
            _ => panic!("expected And expression"),
        },
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_with_not_and_parens() {
    let sql = "CREATE POLICY ON docs FOR SELECT WHERE NOT (status = 'deleted')";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => match policy.where_clause {
            Some(PolicyExpr::Not(inner)) => match *inner {
                PolicyExpr::Eq(left, right) => {
                    assert_eq!(left, PolicyValue::Column("status".into()));
                    assert_eq!(
                        right,
                        PolicyValue::Literal(PredicateValue::String("deleted".into()))
                    );
                }
                _ => panic!("expected Eq inside Not"),
            },
            _ => panic!("expected Not expression"),
        },
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_is_null() {
    let sql = "CREATE POLICY ON folders FOR SELECT WHERE parent_id IS NULL OR owner_id = @viewer";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => match policy.where_clause {
            Some(PolicyExpr::Or(exprs)) => {
                assert_eq!(exprs.len(), 2);

                match &exprs[0] {
                    PolicyExpr::IsNull(val) => {
                        assert_eq!(*val, PolicyValue::Column("parent_id".into()));
                    }
                    _ => panic!("expected IsNull"),
                }
            }
            _ => panic!("expected Or expression"),
        },
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_delete() {
    let sql = "CREATE POLICY ON documents FOR DELETE WHERE owner_id = @viewer";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Delete);
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_cross_action_inherits() {
    // "Anyone who can UPDATE the folder can INSERT documents into it"
    let sql = "CREATE POLICY ON documents FOR INSERT CHECK (INHERITS UPDATE FROM @new.folder_id)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Insert);

            match policy.check_clause {
                Some(PolicyExpr::Inherits { action, column }) => {
                    assert_eq!(action, PolicyAction::Update);
                    assert_eq!(column, PolicyColumnRef::New("folder_id".into()));
                }
                _ => panic!("expected Inherits expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_select_limit() {
    let sql = "SELECT * FROM users LIMIT 10";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.limit, Some(10));
            assert_eq!(s.offset, None);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_offset() {
    let sql = "SELECT * FROM users OFFSET 5";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.limit, None);
            assert_eq!(s.offset, Some(5));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_limit_offset() {
    let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.limit, Some(10));
            assert_eq!(s.offset, Some(5));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_limit_with_where() {
    let sql = "SELECT * FROM users WHERE active = true LIMIT 5";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.where_clause.len(), 1);
            assert_eq!(s.limit, Some(5));
            assert_eq!(s.offset, None);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_select_limit_offset_with_where() {
    let sql = "SELECT * FROM users WHERE active = true LIMIT 3 OFFSET 2";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.where_clause.len(), 1);
            assert_eq!(s.limit, Some(3));
            assert_eq!(s.offset, Some(2));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_create_table_with_blob() {
    let sql = "CREATE TABLE documents (title STRING NOT NULL, content BLOB, attachments BLOB[])";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.name, "documents");
            assert_eq!(ct.columns.len(), 3);
            assert_eq!(ct.columns[0].name, "title");
            assert_eq!(ct.columns[0].ty, ColumnType::String);
            assert!(!ct.columns[0].nullable);
            assert_eq!(ct.columns[1].name, "content");
            assert_eq!(ct.columns[1].ty, ColumnType::Blob);
            assert!(ct.columns[1].nullable);
            assert_eq!(ct.columns[2].name, "attachments");
            assert_eq!(ct.columns[2].ty, ColumnType::BlobArray);
            assert!(ct.columns[2].nullable);
        }
        _ => panic!("expected CreateTable"),
    }
}

// ========== Policy Parsing Tests: Claims and CONTAINS/IN ==========

#[test]
fn parse_policy_viewer_external_id() {
    let sql = "CREATE POLICY ON users FOR SELECT WHERE external_id = @viewer.external_id";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Select);

            match policy.where_clause {
                Some(PolicyExpr::Eq(left, right)) => {
                    assert_eq!(left, PolicyValue::Column("external_id".into()));
                    assert_eq!(right, PolicyValue::ViewerExternalId);
                }
                _ => panic!("expected Eq expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_viewer_claim() {
    let sql = "CREATE POLICY ON premium_features FOR SELECT WHERE @viewer.claims.subscriptionTier = 'pro'";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Select);

            match policy.where_clause {
                Some(PolicyExpr::Eq(left, right)) => {
                    assert_eq!(left, PolicyValue::ViewerClaim("subscriptionTier".into()));
                    assert_eq!(
                        right,
                        PolicyValue::Literal(PredicateValue::String("pro".into()))
                    );
                }
                _ => panic!("expected Eq expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_viewer_claim_org_id() {
    let sql = "CREATE POLICY ON org_documents FOR SELECT WHERE org_id = @viewer.claims.orgId";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => match policy.where_clause {
            Some(PolicyExpr::Eq(left, right)) => {
                assert_eq!(left, PolicyValue::Column("org_id".into()));
                assert_eq!(right, PolicyValue::ViewerClaim("orgId".into()));
            }
            _ => panic!("expected Eq expression"),
        },
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_contains() {
    let sql =
        "CREATE POLICY ON admin_settings FOR UPDATE WHERE @viewer.claims.roles CONTAINS 'admin'";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Update);

            match policy.where_clause {
                Some(PolicyExpr::Contains(left, right)) => {
                    assert_eq!(left, PolicyValue::ViewerClaim("roles".into()));
                    assert_eq!(
                        right,
                        PolicyValue::Literal(PredicateValue::String("admin".into()))
                    );
                }
                _ => panic!("expected Contains expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_in() {
    let sql = "CREATE POLICY ON team_docs FOR SELECT WHERE team_id IN @viewer.claims.groups";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Select);

            match policy.where_clause {
                Some(PolicyExpr::In(left, right)) => {
                    assert_eq!(left, PolicyValue::Column("team_id".into()));
                    assert_eq!(right, PolicyValue::ViewerClaim("groups".into()));
                }
                _ => panic!("expected In expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_combined_org_and_role() {
    let sql = "CREATE POLICY ON org_settings FOR UPDATE WHERE org_id = @viewer.claims.orgId AND @viewer.claims.roles CONTAINS 'org_admin'";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Update);

            match policy.where_clause {
                Some(PolicyExpr::And(exprs)) => {
                    assert_eq!(exprs.len(), 2);

                    // org_id = @viewer.claims.orgId
                    match &exprs[0] {
                        PolicyExpr::Eq(left, right) => {
                            assert_eq!(*left, PolicyValue::Column("org_id".into()));
                            assert_eq!(*right, PolicyValue::ViewerClaim("orgId".into()));
                        }
                        _ => panic!("expected Eq"),
                    }

                    // @viewer.claims.roles CONTAINS 'org_admin'
                    match &exprs[1] {
                        PolicyExpr::Contains(left, right) => {
                            assert_eq!(*left, PolicyValue::ViewerClaim("roles".into()));
                            assert_eq!(
                                *right,
                                PolicyValue::Literal(PredicateValue::String("org_admin".into()))
                            );
                        }
                        _ => panic!("expected Contains"),
                    }
                }
                _ => panic!("expected And expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_permissions_array() {
    // WorkOS-style permissions array check
    let sql = "CREATE POLICY ON documents FOR SELECT WHERE org_id = @viewer.claims.org_id AND @viewer.claims.permissions CONTAINS 'documents:read'";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => {
            assert_eq!(policy.action, PolicyAction::Select);

            match policy.where_clause {
                Some(PolicyExpr::And(exprs)) => {
                    assert_eq!(exprs.len(), 2);

                    // org_id = @viewer.claims.org_id
                    match &exprs[0] {
                        PolicyExpr::Eq(left, right) => {
                            assert_eq!(*left, PolicyValue::Column("org_id".into()));
                            assert_eq!(*right, PolicyValue::ViewerClaim("org_id".into()));
                        }
                        _ => panic!("expected Eq"),
                    }

                    // @viewer.claims.permissions CONTAINS 'documents:read'
                    match &exprs[1] {
                        PolicyExpr::Contains(left, right) => {
                            assert_eq!(*left, PolicyValue::ViewerClaim("permissions".into()));
                            assert_eq!(
                                *right,
                                PolicyValue::Literal(PredicateValue::String(
                                    "documents:read".into()
                                ))
                            );
                        }
                        _ => panic!("expected Contains"),
                    }
                }
                _ => panic!("expected And expression"),
            }
        }
        _ => panic!("expected CreatePolicy"),
    }
}

#[test]
fn parse_policy_multiple_claims_or() {
    // Multiple subscription tier checks
    let sql = "CREATE POLICY ON premium_features FOR SELECT WHERE @viewer.claims.subscriptionTier = 'pro' OR @viewer.claims.subscriptionTier = 'enterprise'";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::CreatePolicy(policy) => match policy.where_clause {
            Some(PolicyExpr::Or(exprs)) => {
                assert_eq!(exprs.len(), 2);

                match &exprs[0] {
                    PolicyExpr::Eq(left, right) => {
                        assert_eq!(*left, PolicyValue::ViewerClaim("subscriptionTier".into()));
                        assert_eq!(
                            *right,
                            PolicyValue::Literal(PredicateValue::String("pro".into()))
                        );
                    }
                    _ => panic!("expected Eq"),
                }

                match &exprs[1] {
                    PolicyExpr::Eq(left, right) => {
                        assert_eq!(*left, PolicyValue::ViewerClaim("subscriptionTier".into()));
                        assert_eq!(
                            *right,
                            PolicyValue::Literal(PredicateValue::String("enterprise".into()))
                        );
                    }
                    _ => panic!("expected Eq"),
                }
            }
            _ => panic!("expected Or expression"),
        },
        _ => panic!("expected CreatePolicy"),
    }
}
