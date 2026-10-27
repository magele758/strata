use std::path::PathBuf;

use strata::{Config, Database, Error, ExecuteResult, Session, Value};

fn dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("strata-interleave-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn cfg() -> Config {
    Config {
        buffer_pages: 64,
        columnar_flush_every: 10_000,
        password: None,
        lock_wait_ms: Some(2_000),
        max_connections: 8,
        group_commit_ms: 2,
    }
}

fn rows(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Rows { rows, .. } => rows,
        ExecuteResult::Exec { message, .. } => panic!("expected rows, got {message}"),
    }
}

fn ids(result: ExecuteResult) -> Vec<i64> {
    let mut out = Vec::new();
    for row in rows(result) {
        match row.as_slice() {
            [Value::Int(id)] => out.push(*id),
            other => panic!("expected one int column, got {other:?}"),
        }
    }
    out.sort_unstable();
    out
}

fn reopen_after_crash(db: Database, path: &PathBuf) -> Database {
    db.simulate_crash().unwrap();
    drop(db);
    Database::open(path, cfg()).unwrap()
}

#[test]
fn uncommitted_inserts_are_visible_only_to_their_session() {
    let path = dir("own-inserts");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let mut a = db.session();
    let mut b = db.session();
    db.execute_session(&mut a, "BEGIN").unwrap();
    db.execute_session(&mut b, "BEGIN").unwrap();
    db.execute_session(&mut a, "INSERT INTO t VALUES (1)").unwrap();
    db.execute_session(&mut b, "INSERT INTO t VALUES (2)").unwrap();

    assert_eq!(ids(db.execute_session(&mut a, "SELECT id FROM t").unwrap()), vec![1]);
    assert_eq!(ids(db.execute_session(&mut b, "SELECT id FROM t").unwrap()), vec![2]);
    assert!(ids(db.execute("SELECT id FROM t").unwrap()).is_empty());

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn commit_publishes_one_session_and_rollback_drops_the_other() {
    let path = dir("commit-rollback");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let mut a = db.session();
    let mut b = db.session();
    db.execute_session(&mut a, "BEGIN").unwrap();
    db.execute_session(&mut b, "BEGIN").unwrap();
    db.execute_session(&mut a, "INSERT INTO t VALUES (1)").unwrap();
    db.execute_session(&mut b, "INSERT INTO t VALUES (2)").unwrap();
    db.execute_session(&mut a, "COMMIT").unwrap();

    assert_eq!(ids(db.execute("SELECT id FROM t").unwrap()), vec![1]);

    db.execute_session(&mut b, "ROLLBACK").unwrap();
    assert_eq!(ids(db.execute("SELECT id FROM t").unwrap()), vec![1]);

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn same_row_update_conflicts_and_leaves_first_writer_unchanged() {
    let path = dir("same-row");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let mut a = db.session();
    db.execute_session(&mut a, "BEGIN").unwrap();
    db.execute_session(&mut a, "UPDATE t SET n = 11 WHERE id = 1").unwrap();

    let mut b = Session::new();
    db.execute_session(&mut b, "BEGIN").unwrap();
    let err = db
        .execute_session(&mut b, "UPDATE t SET n = 99 WHERE id = 1")
        .unwrap_err();
    assert!(matches!(err, Error::Conflict), "same-row update must conflict, got {err}");

    let seen = rows(db.execute_session(&mut a, "SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(seen, vec![vec![Value::Int(11)]]);

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn duplicate_primary_key_rejects_second_writer() {
    let path = dir("same-pk");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();

    let mut a = db.session();
    let mut b = db.session();
    db.execute_session(&mut a, "BEGIN").unwrap();
    db.execute_session(&mut b, "BEGIN").unwrap();
    db.execute_session(&mut a, "INSERT INTO t VALUES (1, 10)").unwrap();
    let err = db
        .execute_session(&mut b, "INSERT INTO t VALUES (1, 20)")
        .unwrap_err();
    assert!(
        matches!(err, Error::Exists(_) | Error::Conflict),
        "duplicate primary key must be exists or conflict, got {err}"
    );

    db.execute_session(&mut a, "COMMIT").unwrap();
    let got = rows(db.execute("SELECT id, n FROM t").unwrap());
    assert_eq!(got, vec![vec![Value::Int(1), Value::Int(10)]]);

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn crash_keeps_committed_insert_and_drops_open_insert() {
    let path = dir("crash-one-commit");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let mut a = db.session();
    db.execute_session(&mut a, "BEGIN").unwrap();
    db.execute_session(&mut a, "INSERT INTO t VALUES (1)").unwrap();
    db.execute_session(&mut a, "COMMIT").unwrap();

    let mut b = db.session();
    db.execute_session(&mut b, "BEGIN").unwrap();
    db.execute_session(&mut b, "INSERT INTO t VALUES (2)").unwrap();

    let db = reopen_after_crash(db, &path);
    assert_eq!(ids(db.execute("SELECT id FROM t").unwrap()), vec![1]);

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn crash_drops_both_uncommitted_inserts() {
    let path = dir("crash-neither");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let mut a = db.session();
    let mut b = db.session();
    db.execute_session(&mut a, "BEGIN").unwrap();
    db.execute_session(&mut b, "BEGIN").unwrap();
    db.execute_session(&mut a, "INSERT INTO t VALUES (1)").unwrap();
    db.execute_session(&mut b, "INSERT INTO t VALUES (2)").unwrap();

    let db = reopen_after_crash(db, &path);
    assert!(ids(db.execute("SELECT id FROM t").unwrap()).is_empty());

    let _ = std::fs::remove_dir_all(&path);
}
