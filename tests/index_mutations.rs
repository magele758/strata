//! Index membership after DELETE / UPDATE.
//!
//! Inverted and HNSW structures are add-only: DELETE and UPDATE do not remove
//! old postings or graph nodes. Stale entries linger until VACUUM rebuilds the
//! indexes from live heap rows. Search must still agree with heap visibility —
//! a deleted RID is not a result, and knn returns up to k *live* neighbors.
//!
//! BM25 corpus stats (doc_count / total_tokens) may still count deleted docs
//! until VACUUM. Membership is required here; score exactness after delete is
//! a known softer gap and is not asserted.

use std::path::PathBuf;

use strata::{Config, Database, ExecuteResult, Value};

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
        max_connections: 8,
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

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    rows(db.execute(sql).unwrap())
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Int(n)] => *n,
            other => panic!("expected one int column, got {other:?}"),
        })
        .collect()
}

#[test]
fn inverted_delete_hides_doc_and_keeps_index_scan() {
    let path = dir("inv-del");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'red fox'), (2, 'red apple'), (3, 'blue sky')")
        .unwrap();
    db.execute("CREATE INDEX docs_body ON docs USING inverted (body)").unwrap();

    db.execute("DELETE FROM docs WHERE id = 1").unwrap();

    let plan = text(&db.execute("EXPLAIN SELECT id FROM docs WHERE search(body, 'red')").unwrap());
    assert!(plan.contains("IndexScan"), "{plan}");

    let mut found = ids(&db, "SELECT id FROM docs WHERE search(body, 'red')");
    found.sort();
    assert_eq!(found, vec![2], "deleted doc must not match; live match must");

    let mut blue = ids(&db, "SELECT id FROM docs WHERE search(body, 'blue')");
    blue.sort();
    assert_eq!(blue, vec![3]);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn inverted_update_replaces_terms() {
    let path = dir("inv-upd");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'old terms'), (2, 'other doc')").unwrap();
    db.execute("CREATE INDEX docs_body ON docs USING inverted (body)").unwrap();

    db.execute("UPDATE docs SET body = 'fresh words' WHERE id = 1").unwrap();

    let old = ids(&db, "SELECT id FROM docs WHERE search(body, 'old')");
    assert!(old.is_empty(), "updated-away terms must not match, got {old:?}");
    let terms = ids(&db, "SELECT id FROM docs WHERE search(body, 'terms')");
    assert!(terms.is_empty(), "updated-away terms must not match, got {terms:?}");

    let mut fresh = ids(&db, "SELECT id FROM docs WHERE search(body, 'fresh')");
    fresh.sort();
    assert_eq!(fresh, vec![1]);

    let mut other = ids(&db, "SELECT id FROM docs WHERE search(body, 'other')");
    other.sort();
    assert_eq!(other, vec![2]);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn hnsw_delete_nearest_returns_next_live() {
    let path = dir("hnsw-del");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE pts (id INT PRIMARY KEY, v VECTOR(2))").unwrap();
    // Query is the origin. id 1 is the unique nearest, then 2, then 3.
    db.execute("INSERT INTO pts VALUES (1, '[0.1,0]'), (2, '[1,0]'), (3, '[3,0]')")
        .unwrap();
    db.execute("CREATE INDEX pts_v ON pts USING hnsw (v)").unwrap();

    db.execute("DELETE FROM pts WHERE id = 1").unwrap();
    assert_eq!(
        ids(&db, "SELECT id FROM pts WHERE knn(v, '[0,0]', 1)"),
        vec![2],
        "k=1 must skip the deleted nearest and return the next live neighbor"
    );

    // Deleting a farther point must not change the true live nearest.
    db.execute("DELETE FROM pts WHERE id = 3").unwrap();
    assert_eq!(ids(&db, "SELECT id FROM pts WHERE knn(v, '[0,0]', 1)"), vec![2]);

    db.execute("VACUUM").unwrap();
    assert_eq!(
        ids(&db, "SELECT id FROM pts WHERE knn(v, '[0,0]', 1)"),
        vec![2],
        "VACUUM rebuilds from live rows; the remaining neighbor stays nearest"
    );
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn hnsw_update_moves_vector_to_nearest() {
    let path = dir("hnsw-upd");
    let db = Database::create(&path, cfg()).unwrap();
    db.execute("CREATE TABLE pts (id INT PRIMARY KEY, v VECTOR(2))").unwrap();
    db.execute("INSERT INTO pts VALUES (1, '[5,0]'), (2, '[1,0]')").unwrap();
    db.execute("CREATE INDEX pts_v ON pts USING hnsw (v)").unwrap();

    assert_eq!(ids(&db, "SELECT id FROM pts WHERE knn(v, '[0,0]', 1)"), vec![2]);

    db.execute("UPDATE pts SET v = '[0.1,0]' WHERE id = 1").unwrap();
    assert_eq!(
        ids(&db, "SELECT id FROM pts WHERE knn(v, '[0,0]', 1)"),
        vec![1],
        "the updated vector is now the unique nearest live neighbor"
    );
    let _ = std::fs::remove_dir_all(&path);
}
