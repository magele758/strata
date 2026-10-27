use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use strata::cluster::RangeCluster;
use strata::pgwire::serve_simple;
use strata::replicate::catch_up;
use strata::{Config, Database, ExecuteResult, Value};

fn dir(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "strata-gap-{name}-{}-{}",
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
    }
}

fn rows(result: ExecuteResult) -> Vec<Vec<Value>> {
    match result {
        ExecuteResult::Rows { rows, .. } => rows,
        ExecuteResult::Exec { message, .. } => panic!("expected rows, got {message}"),
    }
}

#[test]
fn standby_replays_committed_pages() {
    let primary = dir("primary");
    let replica = dir("replica");
    let db = Database::create(&primary, cfg()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'copied')").unwrap();
    let lsn = catch_up(&primary, &replica).unwrap();
    assert!(lsn > 0);
    let copy = Database::open(&replica, cfg()).unwrap();
    let got = rows(copy.execute("SELECT name FROM t WHERE id = 1").unwrap());
    assert_eq!(got, vec![vec![Value::Text("copied".into())]]);
    let _ = std::fs::remove_dir_all(&primary);
    let _ = std::fs::remove_dir_all(&replica);
}

#[test]
fn range_cluster_routes_integer_primary_keys() {
    let left = dir("shard0");
    let right = dir("shard1");
    let cluster = RangeCluster::create(&[left.clone(), right.clone()], &[10], cfg()).unwrap();
    assert_eq!(cluster.shard_count(), 2);
    cluster
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    cluster.execute("INSERT INTO t VALUES (1, 'low')").unwrap();
    cluster.execute("INSERT INTO t VALUES (20, 'high')").unwrap();
    let low = rows(cluster.execute("SELECT name FROM t WHERE id = 1").unwrap());
    let high = rows(cluster.execute("SELECT name FROM t WHERE id = 20").unwrap());
    assert_eq!(low, vec![vec![Value::Text("low".into())]]);
    assert_eq!(high, vec![vec![Value::Text("high".into())]]);
    let all = rows(cluster.execute("SELECT id FROM t").unwrap());
    assert_eq!(all.len(), 2);
    let denied = cluster.execute("BEGIN");
    assert!(denied.unwrap_err().to_string().contains("cross-shard"));
    let _ = std::fs::remove_dir_all(&left);
    let _ = std::fs::remove_dir_all(&right);
}

#[test]
fn pgwire_simple_query_roundtrip() {
    let path = dir("pg");
    let db = Arc::new(Database::create(&path, cfg()).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'wire')").unwrap();
    let addr = "127.0.0.1:17632";
    let server = Arc::clone(&db);
    thread::spawn(move || {
        let _ = serve_simple(server, addr);
    });
    thread::sleep(Duration::from_millis(50));
    let mut sock = TcpStream::connect(addr).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut startup = Vec::new();
    startup.extend_from_slice(&0i32.to_be_bytes());
    startup.extend_from_slice(&196608i32.to_be_bytes());
    startup.extend_from_slice(b"user\0strata\0\0");
    let len = startup.len() as i32;
    startup[..4].copy_from_slice(&len.to_be_bytes());
    sock.write_all(&startup).unwrap();
    let mut pending = Vec::new();
    loop {
        let mut buf = [0u8; 256];
        let n = sock.read(&mut buf).unwrap();
        pending.extend_from_slice(&buf[..n]);
        if pending.windows(6).any(|w| w == [b'Z', 0, 0, 0, 5, b'I']) {
            break;
        }
        if pending.len() > 4096 {
            panic!("startup did not finish: {pending:?}");
        }
    }
    let sql = "SELECT name FROM t WHERE id = 3\0";
    let mut query = vec![b'Q'];
    let qlen = (4 + sql.len()) as i32;
    query.extend_from_slice(&qlen.to_be_bytes());
    query.extend_from_slice(sql.as_bytes());
    sock.write_all(&query).unwrap();
    let mut got = Vec::new();
    loop {
        let mut buf = [0u8; 256];
        let n = sock.read(&mut buf).unwrap();
        got.extend_from_slice(&buf[..n]);
        if got.windows(4).any(|w| w == b"wire") {
            break;
        }
        if got.len() > 4096 {
            panic!("query reply missing row: {}", String::from_utf8_lossy(&got));
        }
    }
    let _ = std::fs::remove_dir_all(&path);
}
