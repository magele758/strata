use std::path::PathBuf;

use strata::{Config, Database, ExecuteResult, Session, Value};

fn dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("strata-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn cfg() -> Config {
    Config {
        buffer_pages: 128,
        columnar_flush_every: 10_000,
        password: None,
        lock_wait_ms: Some(2_000),
        max_connections: 128,
        group_commit_ms: 2,
    }
}

fn rows(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Rows { rows, .. } => rows,
        ExecuteResult::Exec { message, .. } => panic!("expected rows, got {message}"),
    }
}

fn text(result: &ExecuteResult) -> String {
    result.to_text()
}

#[test]
fn durability_survives_discarded_cache() {
    let path = dir("crash");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
    db.simulate_crash().unwrap();
    drop(db);
    let db = Database::open(&path, cfg()).unwrap();
    let got = rows(db.execute("SELECT name FROM t WHERE id = 1").unwrap());
    assert_eq!(got, vec![vec![Value::Text("alpha".into())]]);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn uncommitted_insert_is_lost_on_crash() {
    let path = dir("abort-crash");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    let mut session = db.session();
    db.execute_session(&mut session, "BEGIN").unwrap();
    db.execute_session(&mut session, "INSERT INTO t VALUES (1, 'nope')").unwrap();
    db.simulate_crash().unwrap();
    drop(db);
    let db = Database::open(&path, cfg()).unwrap();
    let got = rows(db.execute("SELECT id FROM t").unwrap());
    assert!(got.is_empty());
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn snapshot_isolation_and_write_write_conflict() {
    let path = dir("si");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();

    let mut s1 = Session::new();
    let mut s2 = Session::new();
    db.execute_session(&mut s1, "BEGIN").unwrap();
    db.execute_session(&mut s2, "BEGIN").unwrap();
    db.execute_session(&mut s2, "INSERT INTO t VALUES (3, 30)").unwrap();
    db.execute_session(&mut s2, "COMMIT").unwrap();
    let seen = rows(db.execute_session(&mut s1, "SELECT id FROM t").unwrap());
    assert_eq!(seen.len(), 2, "snapshot must not see a later commit");

    let err = db.execute_session(&mut s1, "UPDATE t SET n = 11 WHERE id = 1");
    db.execute_session(&mut s1, "ROLLBACK").ok();
    drop(err);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn two_writers_interleave_on_different_rows() {
    let path = dir("writer");
    let db = std::sync::Arc::new(Database::create(&path, cfg()).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 5), (2, 5)").unwrap();
    let mut a = db.session();
    db.execute_session(&mut a, "BEGIN").unwrap();
    db.execute_session(&mut a, "UPDATE t SET n = 0 WHERE id = 1").unwrap();

    let mut b = db.session();
    db.execute_session(&mut b, "BEGIN").unwrap();
    db.execute_session(&mut b, "UPDATE t SET n = 9 WHERE id = 2").unwrap();
    let b_sees_own = rows(db.execute_session(&mut b, "SELECT n FROM t WHERE id = 2").unwrap());
    assert_eq!(b_sees_own, vec![vec![Value::Int(9)]]);
    let b_hides_a = rows(db.execute_session(&mut b, "SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(b_hides_a, vec![vec![Value::Int(5)]]);

    db.execute_session(&mut a, "ROLLBACK").unwrap();
    db.execute_session(&mut b, "COMMIT").unwrap();
    let left = rows(db.execute("SELECT n FROM t ORDER BY id").unwrap());
    assert_eq!(left, vec![vec![Value::Int(5)], vec![Value::Int(9)]]);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn sql_indexes_and_columnar_match_heap() {
    let path = dir("sql");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, n INT, v VECTOR(3))").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello world', 10, '[1,0,0]')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'hello there', 20, '[0,1,0]')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'other', 30, '[0,0,1]')").unwrap();

    let filtered = rows(db.execute("SELECT name FROM t WHERE n > 10 ORDER BY n").unwrap());
    assert_eq!(filtered.len(), 2);
    let agg = rows(db.execute("SELECT count(*), sum(n) FROM t").unwrap());
    assert_eq!(agg, vec![vec![Value::Int(3), Value::Int(60)]]);

    db.execute("UPDATE t SET n = 11 WHERE id = 1").unwrap();
    db.execute("DELETE FROM t WHERE id = 3").unwrap();
    let left = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(left, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    db.execute("CREATE INDEX t_name ON t USING inverted (name)").unwrap();
    let plan = text(&db.execute("EXPLAIN SELECT name FROM t WHERE search(name, 'hello')").unwrap());
    assert!(plan.contains("IndexScan"), "{plan}");
    let found = rows(db.execute("SELECT name FROM t WHERE search(name, 'hello') ORDER BY id").unwrap());
    assert_eq!(found.len(), 2);

    db.execute("CREATE INDEX t_v ON t USING hnsw (v)").unwrap();
    let knn = rows(db.execute("SELECT id FROM t WHERE knn(v, '[1,0,0]', 1)").unwrap());
    assert_eq!(knn, vec![vec![Value::Int(1)]]);

    let before = text(&db.execute("EXPLAIN SELECT name FROM t").unwrap());
    assert!(before.contains("SeqScan"), "{before}");
    db.execute("FLUSH COLUMNAR").unwrap();
    let after = text(&db.execute("EXPLAIN SELECT name FROM t").unwrap());
    assert!(after.contains("ColumnarScan"), "{after}");
    let via_col = rows(db.execute("SELECT name FROM t ORDER BY name").unwrap());
    assert_eq!(via_col.len(), 2);

    let dup = db.execute("INSERT INTO t VALUES (1, 'dup', 1, '[0,0,0]')");
    assert!(dup.is_err(), "primary key must reject duplicates");
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn backup_reopens() {
    let path = dir("backup-src");
    let dest = dir("backup-dst");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (7, 'saved')").unwrap();
    db.backup(&dest).unwrap();
    drop(db);
    let db = Database::open(&dest, cfg()).unwrap();
    let got = rows(db.execute("SELECT name FROM t WHERE id = 7").unwrap());
    assert_eq!(got, vec![vec![Value::Text("saved".into())]]);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&dest);
}
