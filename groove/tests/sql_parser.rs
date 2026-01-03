//! Integration tests for SQL parser.

use groove::sql::{parse, ColumnType, Projection, Statement, Value};

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
                    Value::String("Alice".into()),
                    Value::I64(30),
                    Value::Bool(true),
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
            assert_eq!(ins.values, vec![Value::String("Bob".into()), Value::Null,]);
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_insert_with_uuid() {
    let sql = "INSERT INTO posts (author, title) VALUES (x'0192abcd12345678', 'Hello')";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.values[0], Value::Ref(0x0192abcd12345678));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_update() {
    let sql = "UPDATE users SET email = 'new@example.com', age = 31 WHERE id = x'abc123'";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Update(upd) => {
            assert_eq!(upd.table, "users");
            assert_eq!(upd.assignments.len(), 2);
            assert_eq!(
                upd.assignments[0],
                ("email".into(), Value::String("new@example.com".into()))
            );
            assert_eq!(upd.assignments[1], ("age".into(), Value::I64(31)));
            assert_eq!(upd.where_clause.len(), 1);
            assert_eq!(upd.where_clause[0].column.column, "id");
        }
        _ => panic!("expected Update"),
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
            Projection::Columns(cols) => {
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0].column, "name");
                assert_eq!(cols[1].column, "email");
            }
            _ => panic!("expected Columns"),
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
            assert_eq!(sel.where_clause[0].value, Value::Bool(true));
            assert_eq!(sel.where_clause[1].column.column, "age");
            assert_eq!(sel.where_clause[1].value, Value::I64(30));
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
    let sql = "INSERT INTO data (value) VALUES (3.14)";
    let stmt = parse(sql).unwrap();

    match stmt {
        Statement::Insert(ins) => {
            assert_eq!(ins.values[0], Value::F64(3.14));
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
            assert_eq!(ins.values[0], Value::I64(-42));
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
            assert_eq!(sel.where_clause[0].value, Value::Bool(true));
        }
        _ => panic!("expected Select"),
    }
}
