# Strata

Single-node composable database. One transactional heap, several access methods, and a columnar replica fed by the commit log. It is not a distributed engine and it does not try to beat ClickHouse, Qdrant, or Postgres at their own jobs.

## Run

```bash
cargo run -- init ./demo
cargo run -- exec ./demo --sql "CREATE TABLE t (id INT PRIMARY KEY, name TEXT); INSERT INTO t VALUES (1, 'a'); SELECT * FROM t;"
cargo run -- shell ./demo
cargo run -- check ./demo
cargo run -- backup ./demo ./demo.bak
cargo run -- serve ./demo --addr 127.0.0.1:7432 --password secret
cargo run -- replicate ./demo ./demo-replica
```

`exec` is auto-commit. `shell` and `serve` keep a session, so `BEGIN` / `COMMIT` stick on that connection.

Library entry points are `Database::create`, `Database::open`, `Database::execute`, and `Database::execute_session`.

## SQL

`CREATE/DROP TABLE`, `CREATE/DROP INDEX ... USING btree|inverted|hnsw`, `INSERT`, `UPDATE`, `DELETE`, `SELECT` with `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`, aggregates `count/sum/avg/min/max`, `EXPLAIN`, `BEGIN/COMMIT/ROLLBACK`, `VACUUM`, `FLUSH COLUMNAR`, `CHECKPOINT`, `SHOW TABLES`, `DESCRIBE`, `SHOW STATS`.

Types: `INT`, `FLOAT`, `BOOL`, `TEXT`, `VECTOR(n)`. A vector literal is the text `'[1,0,0]'`.

```sql
CREATE INDEX t_name ON t USING inverted (name);
SELECT name FROM t WHERE search(name, 'hello');

CREATE INDEX t_v ON t USING hnsw (v);
SELECT id FROM t WHERE knn(v, '[1,0,0]', 5);
```

`search` is conjunctive BM25. `knn` is L2. HNSW dimensions are 1..=768.

## What is actually durable

- 4KB pages, checksummed. Torn pages fail the checksum.
- Commit appends the commit record, releases the database lock, then one leader fsyncs every commit that joined within `group_commit_ms` (default 2). The commit timestamp is published only after that fsync.
- Recovery replays page images from the last checkpoint, then undoes a loser's tuples. It does not restore that loser's whole page when the mutation recorded a logical operation.
- `Database::simulate_crash` drops the cache without a checkpoint. The next `open` must rebuild from the WAL. Normal drop checkpoints.
- One process holds `strata.lock`. A second open fails.
- Two write transactions can change different rows at the same time. Undo clears that transaction's tuple stamps and leaves the other row in place. Two updates of the same row still conflict. The statement itself still holds the database lock.

`replicate` copies every committed page image from the primary WAL into another directory. That standby can be opened and read. It does not tail the log live, and it does not elect a new leader.

`strata::cluster::RangeCluster` routes an integer primary key named `id` across local shard directories. It is not a distributed transaction and it is not Raft. `BEGIN`, joins, and updates that change `id` return a `cross-shard` error.

`serve` accepts at most 128 connections. `strata::pgwire::serve_simple` speaks a Postgres simple-query subset, without TLS and without the extended protocol.

Isolation is snapshot isolation, not serializable.

## Access methods and the columnar replica

B+tree keys include the row id, so an update can leave the old version visible to older snapshots. Uniqueness is checked by visibility, not by a single key.

Inverted and HNSW indexes live in the same page file and the same WAL group as the heap. They are not side databases.

`FLUSH COLUMNAR` applies committed logical operations to Parquet segments under `col/`. A scan uses that replica only when it has caught up to the WAL. Otherwise it reads the heap. `EXPLAIN` prints `SeqScan`, `IndexScan`, or `ColumnarScan`.

## Layout

```
demo/
  data.pages
  wal.log
  catalog.bin
  strata.lock
  col/
```

License: Apache-2.0.
