use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use fs2::FileExt;

use crate::am::{Row, Store};
use crate::btree::BTree;
use crate::buffer::BufferPool;
use crate::columnar::ColumnarReplica;
use crate::error::{Error, Result};
use crate::heap::Heap;
use crate::hnsw::HnswIndex;
use crate::inverted::InvertedIndex;
use crate::tuple::{self, DecodedTuple};
use crate::types::{Catalog, ColumnDef, IndexDef, IndexMethod, LogicalOp, PageId, Rid, TableDef};
use crate::value::{encode_key, key_successor, Config, DbStats, ExecuteResult, Value};
use crate::Session;

struct ActiveTxn {
    ops: Vec<LogicalOp>,
}

struct Inner {
    pool: BufferPool,
    catalog: RefCell<Catalog>,
    active: RefCell<HashMap<u64, ActiveTxn>>,
    writer: Cell<Option<u64>>,
    path: PathBuf,
    config: Config,
    columnar: RefCell<ColumnarReplica>,
    commits_since_flush: Cell<u64>,
}

pub struct Database {
    inner: Mutex<Option<Inner>>,
    lock: Option<File>,
    crashed: AtomicBool,
    gate: WriterGate,
    next_gate: AtomicU64,
}

impl Database {
    pub fn create(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() && path.read_dir()?.next().is_some() {
            return Err(Error::Exists(path.display().to_string()));
        }
        std::fs::create_dir_all(path)?;
        let lock = lock_dir(path)?;
        let pool = BufferPool::create(path, config.buffer_pages)?;
        let columnar = ColumnarReplica::open(&path.join("col"))?;
        let inner = Inner {
            pool,
            catalog: RefCell::new(Catalog::empty()),
            active: RefCell::new(HashMap::new()),
            writer: Cell::new(None),
            path: path.to_path_buf(),
            config,
            columnar: RefCell::new(columnar),
            commits_since_flush: Cell::new(0),
        };
        Ok(Self {
            inner: Mutex::new(Some(inner)),
            lock: Some(lock),
            crashed: AtomicBool::new(false),
            gate: WriterGate::new(),
            next_gate: AtomicU64::new(1),
        })
    }

    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let path = path.as_ref();
        let lock = lock_dir(path)?;
        let (pool, recovery) = BufferPool::open(path, config.buffer_pages)?;
        let columnar = ColumnarReplica::open(&path.join("col"))?;
        let inner = Inner {
            pool,
            catalog: RefCell::new(recovery.catalog),
            active: RefCell::new(HashMap::new()),
            writer: Cell::new(None),
            path: path.to_path_buf(),
            config,
            columnar: RefCell::new(columnar),
            commits_since_flush: Cell::new(0),
        };
        Ok(Self {
            inner: Mutex::new(Some(inner)),
            lock: Some(lock),
            crashed: AtomicBool::new(false),
            gate: WriterGate::new(),
            next_gate: AtomicU64::new(1),
        })
    }

    pub fn session(&self) -> Session {
        let mut session = Session::new();
        session.gate_id = self.next_gate.fetch_add(1, Ordering::Relaxed);
        session
    }

    pub fn execute(&self, sql: &str) -> Result<ExecuteResult> {
        let mut session = self.session();
        let result = self.execute_session(&mut session, sql);
        if session.txn_id.is_some() {
            let _ = self.rollback_session(&mut session);
        }
        self.release_if_idle(&mut session);
        result
    }

    pub fn execute_session(&self, session: &mut Session, sql: &str) -> Result<ExecuteResult> {
        self.ensure_gate_id(session);
        if sql_may_write(sql) && !session.holding_write {
            self.gate.acquire(session.gate_id, self.lock_wait())?;
            session.holding_write = true;
        }
        let result = {
            let guard = self.lock_inner()?;
            let inner = guard.as_ref().ok_or(Error::Closed)?;
            crate::exec::run_sql(inner, session, sql)
        };
        if session.txn_id.is_none() {
            self.release_if_idle(session);
        }
        result
    }

    pub fn rollback_session(&self, session: &mut Session) -> Result<()> {
        let result = {
            let guard = self.lock_inner()?;
            let inner = guard.as_ref().ok_or(Error::Closed)?;
            inner.rollback(session)
        };
        self.release_if_idle(session);
        result
    }

    fn ensure_gate_id(&self, session: &mut Session) {
        if session.gate_id == 0 {
            session.gate_id = self.next_gate.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn release_if_idle(&self, session: &mut Session) {
        if session.holding_write && session.txn_id.is_none() {
            self.gate.release(session.gate_id);
            session.holding_write = false;
        }
    }

    fn lock_wait(&self) -> Option<Duration> {
        let Ok(guard) = self.inner.lock() else {
            return None;
        };
        guard.as_ref().and_then(|inner| inner.config.lock_wait_ms.map(Duration::from_millis))
    }

    pub fn checkpoint(&self) -> Result<()> {
        let guard = self.lock_inner()?;
        let inner = guard.as_ref().ok_or(Error::Closed)?;
        inner.checkpoint()
    }

    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        let dest = dest.as_ref();
        self.checkpoint()?;
        let guard = self.lock_inner()?;
        let inner = guard.as_ref().ok_or(Error::Closed)?;
        std::fs::create_dir_all(dest)?;
        for name in ["data.pages", "wal.log", "catalog.bin"] {
            let from = inner.path.join(name);
            if from.exists() {
                std::fs::copy(&from, dest.join(name))?;
            }
        }
        copy_dir(&inner.path.join("col"), &dest.join("col"))?;
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if let Some(inner) = guard.as_mut() {
            if inner.active.borrow().is_empty() {
                let _ = inner.checkpoint();
            }
        }
        *guard = None;
        Ok(())
    }

    /// Drop dirty pages without a checkpoint. The next open must recover from the WAL.
    pub fn simulate_crash(&self) -> Result<()> {
        let guard = self.lock_inner()?;
        let inner = guard.as_ref().ok_or(Error::Closed)?;
        inner.pool.discard_dirty()?;
        self.crashed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, Option<Inner>>> {
        self.inner.lock().map_err(|_| Error::Corrupt {
            detail: "database lock poisoned".into(),
        })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.crashed.load(Ordering::SeqCst) {
            return;
        }
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(inner) = guard.as_mut() {
                if inner.active.borrow().is_empty() {
                    let _ = inner.checkpoint();
                }
            }
        }
    }
}

impl Inner {
    fn mutate<T>(&self, txn: u64, f: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        let before = self.catalog.borrow().encode()?;
        self.pool.begin_group()?;
        match f(self) {
            Ok(v) => {
                let after = self.catalog.borrow().encode()?;
                self.pool.end_group(txn, &before, &after, None)?;
                Ok(v)
            }
            Err(e) => {
                let _ = self.pool.abort_group();
                if let Ok(cat) = Catalog::decode(&before) {
                    *self.catalog.borrow_mut() = cat;
                }
                Err(e)
            }
        }
    }

    fn alloc_txn(&self) -> u64 {
        let mut cat = self.catalog.borrow_mut();
        let id = cat.next_txn_id;
        cat.next_txn_id += 1;
        id
    }

    fn claim_writer(&self, txn: u64) -> Result<()> {
        match self.writer.get() {
            Some(id) if id != txn => Err(Error::Txn(
                "another transaction is writing; commit or roll it back first".into(),
            )),
            Some(_) => Ok(()),
            None => {
                self.writer.set(Some(txn));
                Ok(())
            }
        }
    }

    fn ddl(&self, f: impl FnOnce(&Self) -> Result<()>) -> Result<()> {
        if !self.active.borrow().is_empty() {
            return Err(Error::Txn("DDL requires no open transaction".into()));
        }
        let txn = self.alloc_txn();
        self.mutate(txn, f)?;
        let ts = self.catalog.borrow().next_commit_ts + 1;
        self.pool.append_commit(txn, ts, &[])?;
        self.pool.flush_wal()?;
        self.catalog.borrow_mut().next_commit_ts = ts;
        Ok(())
    }

    fn table(&self, name: &str) -> Result<TableDef> {
        self.catalog.borrow().table(&norm(name)).cloned()
    }

    fn note_commit(&self) -> Result<()> {
        let n = self.commits_since_flush.get() + 1;
        self.commits_since_flush.set(n);
        if n >= self.config.columnar_flush_every {
            self.flush_columnar()?;
        }
        Ok(())
    }
}

impl Store for Inner {
    fn catalog(&self) -> Catalog {
        self.catalog.borrow().clone()
    }

    fn begin(&self, session: &mut Session) -> Result<()> {
        if session.txn_id.is_some() {
            return Err(Error::Txn("transaction already open".into()));
        }
        let id = self.alloc_txn();
        let snapshot = self.catalog.borrow().next_commit_ts;
        session.txn_id = Some(id);
        session.snapshot_ts = snapshot;
        self.active.borrow_mut().insert(id, ActiveTxn { ops: Vec::new() });
        Ok(())
    }

    fn commit(&self, session: &mut Session) -> Result<()> {
        let txn = session.txn_id.take().ok_or_else(|| Error::Txn("no transaction".into()))?;
        let active = self
            .active
            .borrow_mut()
            .remove(&txn)
            .ok_or_else(|| Error::Txn("transaction is not active".into()))?;
        if self.writer.get() == Some(txn) {
            self.writer.set(None);
        }
        if active.ops.is_empty() {
            return Ok(());
        }
        let ts = self.catalog.borrow().next_commit_ts + 1;
        self.mutate(txn, |this| {
            for op in &active.ops {
                match op {
                    LogicalOp::Insert { rid_page, rid_slot, table_id, .. } => {
                        Heap::write_header_u64(this.pool(), rid(*rid_page, *rid_slot), 8, ts)?;
                        bump_count(&mut this.catalog.borrow_mut(), *table_id, 1);
                    }
                    LogicalOp::Delete { rid_page, rid_slot, table_id, .. } => {
                        Heap::write_header_u64(this.pool(), rid(*rid_page, *rid_slot), 24, ts)?;
                        bump_count(&mut this.catalog.borrow_mut(), *table_id, -1);
                    }
                    LogicalOp::Update { old_page, old_slot, new_page, new_slot, table_id, .. } => {
                        Heap::write_header_u64(this.pool(), rid(*old_page, *old_slot), 24, ts)?;
                        Heap::write_header_u64(this.pool(), rid(*new_page, *new_slot), 8, ts)?;
                        let _ = table_id;
                    }
                }
            }
            this.catalog.borrow_mut().next_commit_ts = ts;
            Ok(())
        })?;
        self.pool.append_commit(txn, ts, &active.ops)?;
        self.pool.flush_wal()?;
        session.snapshot_ts = ts;
        self.note_commit()?;
        Ok(())
    }

    fn rollback(&self, session: &mut Session) -> Result<()> {
        let Some(txn) = session.txn_id.take() else {
            return Err(Error::Txn("no transaction".into()));
        };
        self.active.borrow_mut().remove(&txn);
        if self.writer.get() == Some(txn) {
            self.writer.set(None);
        }
        self.pool.undo_txn(txn)?;
        Ok(())
    }

    fn scan_table(&self, table: &str, snapshot: u64, own: Option<u64>) -> Result<Vec<Row>> {
        let table = self.table(table)?;
        let raw = Heap::scan(self.pool(), PageId(table.heap_root))?;
        let mut rows = Vec::new();
        for (id, bytes) in raw {
            let dec = tuple::decode_tuple(&bytes)?;
            if visible(&dec, snapshot, own) {
                rows.push(Row { rid: id, values: dec.values });
            }
        }
        Ok(rows)
    }

    fn index_eq(
        &self,
        table: &str,
        index: &str,
        key: &[Value],
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<Row>> {
        let end = key_successor(&encode_key(key)?);
        self.index_range(table, index, key, end.map(|_| Vec::new()), snapshot, own)
            .or_else(|_| self.index_prefix(table, index, key, snapshot, own))
    }

    fn index_range(
        &self,
        table: &str,
        index: &str,
        start: &[Value],
        end_exclusive: Option<Vec<Value>>,
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<Row>> {
        let (_table, index) = self.find_index(table, index)?;
        if index.method != IndexMethod::Btree {
            return Err(Error::Plan(format!("{} is not a btree index", index.name)));
        }
        let start_b = encode_key(start)?;
        let end_b = match &end_exclusive {
            Some(v) if v.is_empty() => key_successor(&start_b),
            Some(v) => Some(encode_key(v)?),
            None => None,
        };
        let pairs = BTree::range(self.pool(), PageId(index.root), &start_b, end_b.as_deref())?;
        self.rows_from_pairs(&pairs, snapshot, own)
    }

    fn search_text(
        &self,
        table: &str,
        index: &str,
        query: &str,
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<(Row, f64)>> {
        let (_table, index) = self.find_index(table, index)?;
        if index.method != IndexMethod::Inverted {
            return Err(Error::Plan(format!("{} is not an inverted index", index.name)));
        }
        let hits = InvertedIndex::search(self.pool(), PageId(index.root), query)?;
        let mut out = Vec::new();
        for (id, score) in hits {
            if let Some(row) = self.visible_row(id, snapshot, own)? {
                out.push((row, score));
            }
        }
        out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(out)
    }

    fn search_knn(
        &self,
        table: &str,
        index: &str,
        vector: &[f32],
        k: usize,
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<(Row, f32)>> {
        let (_table, index) = self.find_index(table, index)?;
        if index.method != IndexMethod::Hnsw {
            return Err(Error::Plan(format!("{} is not an hnsw index", index.name)));
        }
        // Indexes are add-only: DELETE/UPDATE leave the old RID in the graph
        // until VACUUM rebuilds it. Search returns the k nearest index hits,
        // so a deleted neighbor can fill the top slot and hide a live one.
        // Overfetch and retry until k visible rows are collected or the index
        // is exhausted. HNSW search already walks ef = max(64, k) candidates.
        let mut fetch = k.max(1);
        let mut out = Vec::new();
        loop {
            let hits = HnswIndex::search(self.pool(), PageId(index.root), vector, fetch)?;
            out.clear();
            for (id, dist) in &hits {
                if let Some(row) = self.visible_row(*id, snapshot, own)? {
                    out.push((row, *dist));
                    if out.len() == k {
                        return Ok(out);
                    }
                }
            }
            if hits.len() < fetch {
                return Ok(out);
            }
            let next = fetch.saturating_mul(2);
            if next == fetch {
                return Ok(out);
            }
            fetch = next;
        }
    }

    fn scan_columnar(&self, table: &str, snapshot: u64, own_writes: bool) -> Result<Option<Vec<Row>>> {
        if own_writes || self.columnar.borrow().applied_lsn() < self.pool.wal_lsn()? {
            return Ok(None);
        }
        let table = self.table(table)?;
        let Some(rows) = self.columnar.borrow().scan(table.id, snapshot)? else {
            return Ok(None);
        };
        Ok(Some(
            rows.into_iter()
                .map(|values| Row {
                    rid: Rid { page: 0, slot: 0 },
                    values,
                })
                .collect(),
        ))
    }

    fn insert_row(&self, table: &str, values: &[Value], txn: u64) -> Result<()> {
        self.claim_writer(txn)?;
        let table = self.table(table)?;
        self.check_values(&table, values)?;
        let tuple = tuple::encode_tuple(txn, 0, 0, 0, values)?;
        let rid = self.mutate(txn, |this| {
            this.ensure_unique(&table, values, txn, None)?;
            let id = Heap::insert(this.pool(), PageId(table.heap_root), &tuple)?;
            this.maintain_insert(&table, id, values)?;
            Ok(id)
        })?;
        self.push_op(
            txn,
            LogicalOp::Insert {
                table_id: table.id,
                rid_page: rid.page,
                rid_slot: rid.slot,
                values: values.to_vec(),
            },
        )?;
        Ok(())
    }

    fn delete_rids(&self, table: &str, rids: &[Rid], txn: u64, snapshot: u64) -> Result<u64> {
        self.claim_writer(txn)?;
        let table = self.table(table)?;
        let mut n = 0;
        for id in rids {
            let bytes = Heap::read(self.pool(), *id)?;
            let dec = tuple::decode_tuple(&bytes)?;
            claim(&dec, txn, snapshot)?;
            self.mutate(txn, |this| {
                Heap::write_header_u64(this.pool(), *id, 16, txn)?;
                Ok(())
            })?;
            self.push_op(
                txn,
                LogicalOp::Delete {
                    table_id: table.id,
                    rid_page: id.page,
                    rid_slot: id.slot,
                    values: dec.values,
                },
            )?;
            n += 1;
        }
        Ok(n)
    }

    fn update_rids(
        &self,
        table: &str,
        rids: &[Rid],
        values: &[Vec<Value>],
        txn: u64,
        snapshot: u64,
    ) -> Result<u64> {
        if rids.len() != values.len() {
            return Err(Error::Type("update row count mismatch".into()));
        }
        self.claim_writer(txn)?;
        let table = self.table(table)?;
        let mut n = 0;
        for (id, new_values) in rids.iter().zip(values) {
            self.check_values(&table, new_values)?;
            let old = tuple::decode_tuple(&Heap::read(self.pool(), *id)?)?;
            claim(&old, txn, snapshot)?;
            self.ensure_unique(&table, new_values, txn, Some(*id))?;
            let tuple = tuple::encode_tuple(txn, 0, 0, 0, new_values)?;
            let new_rid = self.mutate(txn, |this| {
                Heap::write_header_u64(this.pool(), *id, 16, txn)?;
                let new_rid = Heap::insert(this.pool(), PageId(table.heap_root), &tuple)?;
                this.maintain_insert(&table, new_rid, new_values)?;
                Ok(new_rid)
            })?;
            self.push_op(
                txn,
                LogicalOp::Update {
                    table_id: table.id,
                    old_page: id.page,
                    old_slot: id.slot,
                    new_page: new_rid.page,
                    new_slot: new_rid.slot,
                    values: new_values.clone(),
                },
            )?;
            n += 1;
        }
        Ok(n)
    }

    fn create_table(&self, name: &str, columns: &[ColumnDef], if_not_exists: bool) -> Result<()> {
        let name = norm(name);
        self.ddl(|this| {
            if this.catalog.borrow().tables.iter().any(|t| t.name == name) {
                if if_not_exists {
                    return Ok(());
                }
                return Err(Error::Exists(name.clone()));
            }
            if columns.is_empty() {
                return Err(Error::Type("table needs a column".into()));
            }
            let root = Heap::create(this.pool())?;
            let mut indexes = Vec::new();
            let pk: Vec<String> = columns
                .iter()
                .filter(|c| c.primary_key)
                .map(|c| c.name.clone())
                .collect();
            let table_id = this.catalog.borrow().next_table_id;
            let mut next_index = this.catalog.borrow().next_index_id;
            if !pk.is_empty() {
                let iroot = BTree::create(this.pool())?;
                indexes.push(IndexDef {
                    id: next_index,
                    name: format!("{name}_pkey"),
                    columns: pk,
                    method: IndexMethod::Btree,
                    root: iroot.0,
                    unique: true,
                });
                next_index += 1;
            }
            let mut cat = this.catalog.borrow_mut();
            cat.tables.push(TableDef {
                id: table_id,
                name,
                columns: columns.to_vec(),
                heap_root: root.0,
                indexes,
                row_count: 0,
            });
            cat.next_table_id = table_id + 1;
            cat.next_index_id = next_index;
            Ok(())
        })
    }

    fn drop_table(&self, name: &str, if_exists: bool) -> Result<()> {
        let name = norm(name);
        self.ddl(|this| {
            let mut cat = this.catalog.borrow_mut();
            let before = cat.tables.len();
            cat.tables.retain(|t| t.name != name);
            if cat.tables.len() == before && !if_exists {
                return Err(Error::NotFound);
            }
            Ok(())
        })
    }

    fn create_index(
        &self,
        name: &str,
        table: &str,
        columns: &[String],
        method: IndexMethod,
        if_not_exists: bool,
    ) -> Result<()> {
        let name = norm(name);
        let table_name = norm(table);
        let columns: Vec<String> = columns.iter().map(|c| norm(c)).collect();
        self.ddl(|this| {
            if this.catalog.borrow().index(&name).is_ok() {
                if if_not_exists {
                    return Ok(());
                }
                return Err(Error::Exists(name.clone()));
            }
            let table = this.table(&table_name)?;
            if columns.is_empty() {
                return Err(Error::Type("index needs a column".into()));
            }
            for col in &columns {
                if !table.columns.iter().any(|c| &c.name == col) {
                    return Err(Error::Plan(format!("unknown column {col}")));
                }
            }
            let root = match method {
                IndexMethod::Btree => BTree::create(this.pool())?,
                IndexMethod::Inverted => {
                    if columns.len() != 1 || !matches!(col_type(&table, &columns[0])?, crate::types::DataType::Text)
                    {
                        return Err(Error::Type("inverted index requires one TEXT column".into()));
                    }
                    InvertedIndex::create(this.pool())?
                }
                IndexMethod::Hnsw => {
                    let crate::types::DataType::Vector(dim) = col_type(&table, &columns[0])? else {
                        return Err(Error::Type("hnsw index requires one VECTOR column".into()));
                    };
                    if columns.len() != 1 {
                        return Err(Error::Type("hnsw index requires one VECTOR column".into()));
                    }
                    HnswIndex::create(this.pool(), dim)?
                }
            };
            let snapshot = this.catalog.borrow().next_commit_ts;
            let rows = this.scan_table(&table.name, snapshot, None)?;
            for row in &rows {
                this.maintain_one(&table, method, root, &columns, row.rid, &row.values)?;
            }
            let id = this.catalog.borrow().next_index_id;
            let mut cat = this.catalog.borrow_mut();
            let table = cat.table_mut(&table_name)?;
            table.indexes.push(IndexDef {
                id,
                name,
                columns,
                method,
                root: root.0,
                unique: false,
            });
            cat.next_index_id = id + 1;
            Ok(())
        })
    }

    fn drop_index(&self, name: &str, if_exists: bool) -> Result<()> {
        let name = norm(name);
        self.ddl(|this| {
            let mut found = false;
            let mut cat = this.catalog.borrow_mut();
            for table in &mut cat.tables {
                let before = table.indexes.len();
                table.indexes.retain(|i| i.name != name);
                if table.indexes.len() != before {
                    found = true;
                }
            }
            if !found && !if_exists {
                return Err(Error::NotFound);
            }
            Ok(())
        })
    }

    fn vacuum(&self) -> Result<()> {
        self.ddl(|this| {
            let tables = this.catalog.borrow().tables.clone();
            let snapshot = this.catalog.borrow().next_commit_ts;
            for table in tables {
                let raw = Heap::scan(this.pool(), PageId(table.heap_root))?;
                let mut live = Vec::new();
                for (id, bytes) in raw {
                    let dec = tuple::decode_tuple(&bytes)?;
                    if dec.xmin_ts != 0 && dec.xmin_ts <= snapshot && dec.xmax_ts == 0 {
                        live.push((id, bytes, dec.values));
                    }
                }
                let new_root = Heap::create(this.pool())?;
                let mut indexes = table.indexes.clone();
                for index in &mut indexes {
                    index.root = match index.method {
                        IndexMethod::Btree => BTree::create(this.pool())?.0,
                        IndexMethod::Inverted => InvertedIndex::create(this.pool())?.0,
                        IndexMethod::Hnsw => {
                            let crate::types::DataType::Vector(dim) = col_type(&table, &index.columns[0])? else {
                                return Err(Error::Type("hnsw vacuum type".into()));
                            };
                            HnswIndex::create(this.pool(), dim)?.0
                        }
                    };
                }
                for (old_rid, bytes, values) in &live {
                    let new_rid = Heap::insert(this.pool(), new_root, bytes)?;
                    for index in &indexes {
                        this.maintain_one(
                            &table,
                            index.method,
                            PageId(index.root),
                            &index.columns,
                            new_rid,
                            values,
                        )?;
                    }
                    let _ = old_rid;
                }
                let mut cat = this.catalog.borrow_mut();
                let slot = cat.table_mut(&table.name)?;
                slot.heap_root = new_root.0;
                slot.indexes = indexes;
                slot.row_count = live.len() as u64;
            }
            Ok(())
        })
    }

    fn flush_columnar(&self) -> Result<u64> {
        let applied = self.columnar.borrow().applied_lsn();
        let ops = self.pool.committed_ops_since(applied)?;
        self.columnar.borrow_mut().apply(&ops)?;
        let lsn = self.pool.wal_lsn()?;
        self.columnar.borrow_mut().set_applied_lsn(lsn);
        self.commits_since_flush.set(0);
        Ok(lsn)
    }

    fn checkpoint(&self) -> Result<()> {
        if !self.active.borrow().is_empty() {
            return Err(Error::Txn("checkpoint requires no open transaction".into()));
        }
        self.flush_columnar()?;
        let bytes = self.catalog.borrow().encode()?;
        std::fs::write(self.path.join("catalog.bin"), &bytes)?;
        self.pool.checkpoint(&bytes)?;
        Ok(())
    }

    fn stats(&self) -> Result<DbStats> {
        Ok(DbStats {
            pages: self.pool.page_count()?,
            wal_lsn: self.pool.wal_lsn()?,
            buffered: self.pool.buffered()?,
            tables: self.catalog.borrow().tables.len(),
            commit_ts: self.catalog.borrow().next_commit_ts,
            columnar_applied_lsn: self.columnar.borrow().applied_lsn(),
        })
    }

    fn indexes(&self, table: &str) -> Result<Vec<IndexDef>> {
        Ok(self.table(table)?.indexes)
    }
}

impl Inner {
    fn pool(&self) -> &BufferPool {
        &self.pool
    }

    fn index_prefix(
        &self,
        table: &str,
        index: &str,
        key: &[Value],
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<Row>> {
        let (_table, index) = self.find_index(table, index)?;
        let start = encode_key(key)?;
        let end = key_successor(&start);
        let pairs = BTree::range(self.pool(), PageId(index.root), &start, end.as_deref())?;
        self.rows_from_pairs(&pairs, snapshot, own)
    }

    fn rows_from_pairs(
        &self,
        pairs: &[(Vec<u8>, Vec<u8>)],
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        for (_key, val) in pairs {
            let id = Rid::decode(val)?;
            if let Some(row) = self.visible_row(id, snapshot, own)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn visible_row(&self, id: Rid, snapshot: u64, own: Option<u64>) -> Result<Option<Row>> {
        let dec = tuple::decode_tuple(&Heap::read(self.pool(), id)?)?;
        if visible(&dec, snapshot, own) {
            Ok(Some(Row { rid: id, values: dec.values }))
        } else {
            Ok(None)
        }
    }

    fn find_index(&self, table: &str, index: &str) -> Result<(TableDef, IndexDef)> {
        let table = self.table(table)?;
        let index = table
            .indexes
            .iter()
            .find(|i| i.name == norm(index))
            .cloned()
            .ok_or(Error::NotFound)?;
        Ok((table, index))
    }

    fn check_values(&self, table: &TableDef, values: &[Value]) -> Result<()> {
        if values.len() != table.columns.len() {
            return Err(Error::Type(format!(
                "{} values for {} columns",
                values.len(),
                table.columns.len()
            )));
        }
        for (col, value) in table.columns.iter().zip(values) {
            if value.is_null() && !col.nullable {
                return Err(Error::Type(format!("{} cannot be null", col.name)));
            }
            let _ = value.clone().coerce(&col.ty)?;
        }
        Ok(())
    }

    fn ensure_unique(
        &self,
        table: &TableDef,
        values: &[Value],
        txn: u64,
        ignore: Option<Rid>,
    ) -> Result<()> {
        let snapshot = self.catalog.borrow().next_commit_ts;
        for index in &table.indexes {
            if !index.unique || index.method != IndexMethod::Btree {
                continue;
            }
            let cols = project_cols(table, &index.columns, values)?;
            let start = encode_key(&cols)?;
            let end = key_successor(&start);
            let pairs = BTree::range(self.pool(), PageId(index.root), &start, end.as_deref())?;
            for (_k, val) in pairs {
                let id = Rid::decode(&val)?;
                if ignore == Some(id) {
                    continue;
                }
                if key_taken(self.pool(), id, snapshot, txn)? {
                    return Err(Error::Exists(index.name.clone()));
                }
            }
        }
        Ok(())
    }

    fn maintain_insert(&self, table: &TableDef, id: Rid, values: &[Value]) -> Result<()> {
        for index in &table.indexes {
            self.maintain_one(table, index.method, PageId(index.root), &index.columns, id, values)?;
        }
        Ok(())
    }

    fn maintain_one(
        &self,
        table: &TableDef,
        method: IndexMethod,
        root: PageId,
        columns: &[String],
        id: Rid,
        values: &[Value],
    ) -> Result<()> {
        match method {
            IndexMethod::Btree => {
                let cols = project_cols(table, columns, values)?;
                // Rid suffix lets old and new versions coexist. Uniqueness is
                // enforced by visibility, not by a single btree key.
                let key = crate::value::encode_key_with_rid(&cols, id)?;
                let new_root = BTree::insert(self.pool(), root, &key, &id.encode())?;
                if new_root.0 != root.0 {
                    let mut cat = self.catalog.borrow_mut();
                    if let Ok(slot) = cat.table_mut(&table.name) {
                        for index in &mut slot.indexes {
                            if index.root == root.0 {
                                index.root = new_root.0;
                            }
                        }
                    }
                }
            }
            IndexMethod::Inverted => {
                let Value::Text(text) = &project_cols(table, columns, values)?[0] else {
                    return Err(Error::Type("inverted index column is not text".into()));
                };
                InvertedIndex::add(self.pool(), root, text, id)?;
            }
            IndexMethod::Hnsw => {
                let Value::Vector(vec) = &project_cols(table, columns, values)?[0] else {
                    return Err(Error::Type("hnsw index column is not a vector".into()));
                };
                HnswIndex::add(self.pool(), root, vec, id)?;
            }
        }
        Ok(())
    }

    fn push_op(&self, txn: u64, op: LogicalOp) -> Result<()> {
        let mut active = self.active.borrow_mut();
        let txn = active
            .get_mut(&txn)
            .ok_or_else(|| Error::Txn("transaction is not active".into()))?;
        txn.ops.push(op);
        Ok(())
    }
}

fn visible(dec: &DecodedTuple, snapshot: u64, own: Option<u64>) -> bool {
    if let Some(own) = own {
        if dec.xmin_txn == own {
            return dec.xmax_txn != own;
        }
        if dec.xmax_txn == own {
            return false;
        }
    }
    if dec.xmin_ts == 0 || dec.xmin_ts > snapshot {
        return false;
    }
    if dec.xmax_ts == 0 {
        true
    } else {
        dec.xmax_ts > snapshot
    }
}

fn claim(dec: &DecodedTuple, own: u64, snapshot: u64) -> Result<()> {
    if dec.xmax_txn != 0 && dec.xmax_txn != own && (dec.xmax_ts == 0 || dec.xmax_ts > snapshot) {
        return Err(Error::Conflict);
    }
    Ok(())
}

fn key_taken(pool: &BufferPool, id: Rid, snapshot: u64, own: u64) -> Result<bool> {
    let dec = tuple::decode_tuple(&Heap::read(pool, id)?)?;
    if dec.xmin_txn == own && dec.xmax_txn != own {
        return Ok(true);
    }
    Ok(dec.xmin_ts != 0 && dec.xmin_ts <= snapshot && (dec.xmax_ts == 0 || dec.xmax_ts > snapshot))
}

fn project_cols(table: &TableDef, columns: &[String], values: &[Value]) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = table
            .columns
            .iter()
            .position(|c| &c.name == name)
            .ok_or_else(|| Error::Plan(format!("unknown column {name}")))?;
        out.push(values.get(idx).cloned().unwrap_or(Value::Null));
    }
    Ok(out)
}

fn col_type(table: &TableDef, name: &str) -> Result<crate::types::DataType> {
    table
        .columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.ty.clone())
        .ok_or_else(|| Error::Plan(format!("unknown column {name}")))
}

fn bump_count(cat: &mut Catalog, table_id: u32, delta: i64) {
    if let Some(table) = cat.tables.iter_mut().find(|t| t.id == table_id) {
        if delta >= 0 {
            table.row_count += delta as u64;
        } else {
            table.row_count = table.row_count.saturating_sub((-delta) as u64);
        }
    }
}

fn rid(page: u32, slot: u16) -> Rid {
    Rid { page, slot }
}

fn norm(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn lock_dir(path: &Path) -> Result<File> {
    std::fs::create_dir_all(path)?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path.join("strata.lock"))?;
    file.try_lock_exclusive().map_err(|_| {
        Error::Txn(format!("{} is locked by another process", path.display()))
    })?;
    Ok(file)
}

fn copy_dir(src: &Path, dest: &Path) -> Result<()> {
    if !src.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(dest)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dest.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}

struct WriterGate {
    owner: Mutex<Option<u64>>,
    cv: Condvar,
}

impl WriterGate {
    fn new() -> Self {
        Self {
            owner: Mutex::new(None),
            cv: Condvar::new(),
        }
    }

    fn acquire(&self, id: u64, timeout: Option<Duration>) -> Result<()> {
        let mut owner = self.owner.lock().map_err(|_| Error::Corrupt {
            detail: "writer gate poisoned".into(),
        })?;
        let started = Instant::now();
        loop {
            match *owner {
                Some(held) if held == id => return Ok(()),
                None => {
                    *owner = Some(id);
                    return Ok(());
                }
                Some(_) => {
                    owner = match timeout {
                        None => self.cv.wait(owner).map_err(|_| Error::Corrupt {
                            detail: "writer gate poisoned".into(),
                        })?,
                        Some(limit) => {
                            let left = limit.saturating_sub(started.elapsed());
                            if left.is_zero() {
                                return Err(Error::Limit(
                                    "timed out waiting for the writer".into(),
                                ));
                            }
                            let (guard, status) = self.cv.wait_timeout(owner, left).map_err(|_| {
                                Error::Corrupt {
                                    detail: "writer gate poisoned".into(),
                                }
                            })?;
                            if status.timed_out() {
                                if guard.as_ref() == Some(&id) || guard.is_none() {
                                    owner = guard;
                                    continue;
                                }
                                return Err(Error::Limit(
                                    "timed out waiting for the writer".into(),
                                ));
                            }
                            guard
                        }
                    };
                }
            }
        }
    }

    fn release(&self, id: u64) {
        if let Ok(mut owner) = self.owner.lock() {
            if *owner == Some(id) {
                *owner = None;
                self.cv.notify_one();
            }
        }
    }
}

fn sql_may_write(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.len() >= 7 && trimmed[..7].eq_ignore_ascii_case("explain") {
        return false;
    }
    let lower = sql.to_ascii_lowercase();
    for token in lower.split(|c: char| !c.is_ascii_alphanumeric()) {
        if matches!(
            token,
            "insert" | "update" | "delete" | "create" | "drop" | "vacuum" | "flush" | "checkpoint" | "alter"
        ) {
            return true;
        }
    }
    false
}
