use std::sync::{Arc, Barrier};
use std::thread;

use strata::{Config, Database, ExecuteResult, Value};

fn dir(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "strata-group-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
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
        group_commit_ms: 500,
    }
}

fn rows(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Rows { rows, .. } => rows,
        ExecuteResult::Exec { message, .. } => panic!("expected rows, got {message}"),
    }
}

#[test]
fn two_commits_share_one_fsync_and_both_survive_reopen() {
    let path = dir("pair");
    let db = Arc::new(Database::create(&path, cfg()).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0), (2, 0)").unwrap();
    let before = db.wal_syncs().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for id in [1_i64, 2] {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut session = db.session();
            db.execute_session(&mut session, "BEGIN").unwrap();
            db.execute_session(
                &mut session,
                &format!("UPDATE t SET n = {id} WHERE id = {id}"),
            )
            .unwrap();
            barrier.wait();
            db.execute_session(&mut session, "COMMIT").unwrap();
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let after = db.wal_syncs().unwrap();
    assert_eq!(after - before, 1, "wal syncs {before} -> {after}");
    let got = rows(db.execute("SELECT n FROM t ORDER BY id").unwrap());
    assert_eq!(got, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    drop(db);
    let db = Database::open(&path, cfg()).unwrap();
    let got = rows(db.execute("SELECT n FROM t ORDER BY id").unwrap());
    assert_eq!(got, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    let _ = std::fs::remove_dir_all(&path);
}
