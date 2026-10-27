use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::page::PageStore;
use crate::types::{Catalog, LogicalOp, Page, PageId, PageKind, PAGE_SIZE};
use crate::wal::{self, Wal, WalKind, WalRecord};

struct Frame {
    page_id: u32,
    page: Page,
    pin: u32,
    dirty: bool,
    valid: bool,
    referenced: bool,
}

struct Group {
    befores: HashMap<u32, Page>,
}

struct PoolState {
    store: PageStore,
    frames: Vec<Frame>,
    table: HashMap<u32, usize>,
    clock: usize,
    wal: Wal,
    records: Vec<WalRecord>,
    group: Option<Group>,
}

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<Mutex<PoolState>>,
}

pub struct Pin {
    pool: Arc<Mutex<PoolState>>,
    id: u32,
    alive: bool,
}

pub struct Recovery {
    pub catalog: Catalog,
}

impl BufferPool {
    pub fn create(dir: &Path, capacity: usize) -> Result<Self> {
        let store = PageStore::create(dir)?;
        let mut wal = Wal::create(&wal::wal_path(dir))?;
        let catalog = Catalog::empty().encode()?;
        let checkpoint = WalRecord {
            lsn: 0,
            kind: WalKind::Checkpoint,
            txn_id: 0,
            commit_ts: 0,
            pages: Vec::new(),
            catalog_before: Vec::new(),
            catalog_after: catalog,
            logical: None,
            ops: Vec::new(),
        };
        let lsn = wal.append(&checkpoint)?;
        wal.flush()?;
        let mut checkpoint = checkpoint;
        checkpoint.lsn = lsn;
        let pool = Self::from_parts(store, wal, vec![checkpoint], capacity)?;
        Ok(pool)
    }

    pub fn open(dir: &Path, capacity: usize) -> Result<(Self, Recovery)> {
        let store = PageStore::open(dir)?;
        let (wal, records) = Wal::open(&wal::wal_path(dir))?;
        let pool = Self::from_parts(store, wal, records, capacity)?;
        let catalog = pool.recover()?;
        Ok((pool, Recovery { catalog }))
    }

    fn from_parts(store: PageStore, wal: Wal, records: Vec<WalRecord>, capacity: usize) -> Result<Self> {
        let capacity = capacity.max(8);
        let frames = (0..capacity)
            .map(|_| Frame {
                page_id: 0,
                page: Page::blank(PageId::META, PageKind::Meta),
                pin: 0,
                dirty: false,
                valid: false,
                referenced: false,
            })
            .collect();
        Ok(Self {
            inner: Arc::new(Mutex::new(PoolState {
                store,
                frames,
                table: HashMap::new(),
                clock: 0,
                wal,
                records,
                group: None,
            })),
        })
    }

    pub fn alloc(&self, kind: PageKind) -> Result<PageId> {
        let mut st = lock(&self.inner)?;
        let page = st.store.alloc(kind)?;
        let id = page.id().0;
        if let Some(idx) = st.table.remove(&id) {
            st.frames[idx].valid = false;
            st.frames[idx].dirty = false;
            st.frames[idx].pin = 0;
        }
        Ok(PageId(id))
    }

    pub fn pin(&self, id: PageId) -> Result<Pin> {
        if id.0 == 0 {
            return Err(Error::Corrupt {
                detail: "page 0 is not a buffer-pool page".into(),
            });
        }
        let mut st = lock(&self.inner)?;
        st.load(id.0)?;
        Ok(Pin {
            pool: self.inner.clone(),
            id: id.0,
            alive: true,
        })
    }

    pub fn begin_group(&self) -> Result<()> {
        let mut st = lock(&self.inner)?;
        if st.group.is_some() {
            return Err(Error::Txn("mutation group already open".into()));
        }
        st.group = Some(Group {
            befores: HashMap::new(),
        });
        Ok(())
    }

    pub fn abort_group(&self) -> Result<()> {
        let mut st = lock(&self.inner)?;
        let Some(group) = st.group.take() else {
            return Ok(());
        };
        for (id, before) in group.befores {
            if let Some(&idx) = st.table.get(&id) {
                st.frames[idx].page = before;
                st.frames[idx].dirty = false;
            }
        }
        Ok(())
    }

    pub fn end_group(
        &self,
        txn_id: u64,
        catalog_before: &[u8],
        catalog_after: &[u8],
        logical: Option<&LogicalOp>,
    ) -> Result<u64> {
        let mut st = lock(&self.inner)?;
        let Some(group) = st.group.take() else {
            return Err(Error::Txn("no mutation group".into()));
        };
        if group.befores.is_empty() && catalog_before == catalog_after && logical.is_none() {
            return Ok(st.wal.next_lsn());
        }
        let lsn = st.wal.next_lsn();
        let mut pages = Vec::new();
        for (id, before) in &group.befores {
            let idx = st.table.get(id).copied().ok_or_else(|| Error::Corrupt {
                detail: format!("dirty page {id} missing from cache"),
            })?;
            st.frames[idx].page.set_lsn(lsn);
            st.frames[idx].page.seal();
            pages.push(wal::PageImage {
                page_id: *id,
                before: before.as_bytes().to_vec(),
                after: st.frames[idx].page.as_bytes().to_vec(),
            });
            st.frames[idx].dirty = true;
        }
        let mut rec = WalRecord {
            lsn: 0,
            kind: WalKind::Mutation,
            txn_id,
            commit_ts: 0,
            pages,
            catalog_before: catalog_before.to_vec(),
            catalog_after: catalog_after.to_vec(),
            logical: logical.cloned(),
            ops: Vec::new(),
        };
        let assigned = st.wal.append(&rec)?;
        rec.lsn = assigned;
        st.records.push(rec);
        Ok(assigned)
    }

    pub fn append_commit(&self, txn_id: u64, commit_ts: u64, ops: &[LogicalOp]) -> Result<u64> {
        let mut st = lock(&self.inner)?;
        let mut rec = WalRecord {
            lsn: 0,
            kind: WalKind::Commit,
            txn_id,
            commit_ts,
            pages: Vec::new(),
            catalog_before: Vec::new(),
            catalog_after: Vec::new(),
            logical: None,
            ops: ops.to_vec(),
        };
        let lsn = st.wal.append(&rec)?;
        rec.lsn = lsn;
        st.records.push(rec);
        Ok(lsn)
    }

    pub fn append_abort(&self, txn_id: u64) -> Result<u64> {
        let mut st = lock(&self.inner)?;
        let mut rec = WalRecord {
            lsn: 0,
            kind: WalKind::Abort,
            txn_id,
            commit_ts: 0,
            pages: Vec::new(),
            catalog_before: Vec::new(),
            catalog_after: Vec::new(),
            logical: None,
            ops: Vec::new(),
        };
        let lsn = st.wal.append(&rec)?;
        rec.lsn = lsn;
        st.records.push(rec);
        Ok(lsn)
    }

    pub fn flush_wal(&self) -> Result<()> {
        lock(&self.inner)?.wal.flush()
    }

    pub fn checkpoint(&self, catalog: &[u8]) -> Result<()> {
        let mut st = lock(&self.inner)?;
        if st.group.is_some() {
            return Err(Error::Txn("cannot checkpoint inside a mutation".into()));
        }
        st.flush_dirty()?;
        st.store.sync()?;
        let lsn = st.wal.recycle_checkpoint(catalog)?;
        st.records.clear();
        st.records.push(WalRecord {
            lsn,
            kind: WalKind::Checkpoint,
            txn_id: 0,
            commit_ts: 0,
            pages: Vec::new(),
            catalog_before: Vec::new(),
            catalog_after: catalog.to_vec(),
            logical: None,
            ops: Vec::new(),
        });
        let path = st.wal_dir_catalog();
        if let Some(path) = path {
            std::fs::write(path, catalog)?;
        }
        Ok(())
    }

    pub fn committed_ops_since(&self, lsn: u64) -> Result<Vec<crate::types::CommittedOp>> {
        let st = lock(&self.inner)?;
        Ok(wal::commit_ops(&st.records, lsn))
    }

    pub fn discard_dirty(&self) -> Result<()> {
        let mut st = lock(&self.inner)?;
        let mut drop_ids = Vec::new();
        for frame in &mut st.frames {
            if frame.dirty {
                drop_ids.push(frame.page_id);
                frame.valid = false;
                frame.dirty = false;
                frame.pin = 0;
            }
        }
        for id in drop_ids {
            st.table.remove(&id);
        }
        Ok(())
    }

    /// Restore this transaction's before-images and append Abort. Idempotent with recovery.
    pub fn undo_txn(&self, txn_id: u64) -> Result<()> {
        let mut st = lock(&self.inner)?;
        let recs: Vec<WalRecord> = st
            .records
            .iter()
            .filter(|r| r.kind == WalKind::Mutation && r.txn_id == txn_id)
            .cloned()
            .collect();
        for rec in recs.iter().rev() {
            for image in rec.pages.iter().rev() {
                st.force_image(image.page_id, &image.before)?;
            }
        }
        let already = st
            .records
            .iter()
            .any(|r| r.kind == WalKind::Abort && r.txn_id == txn_id);
        if !already {
            let mut abort = WalRecord {
                lsn: 0,
                kind: WalKind::Abort,
                txn_id,
                commit_ts: 0,
                pages: Vec::new(),
                catalog_before: Vec::new(),
                catalog_after: Vec::new(),
                logical: None,
                ops: Vec::new(),
            };
            let lsn = st.wal.append(&abort)?;
            abort.lsn = lsn;
            st.records.push(abort);
        }
        st.wal.flush()?;
        Ok(())
    }

    pub fn page_count(&self) -> Result<u32> {
        Ok(lock(&self.inner)?.store.page_count())
    }

    pub fn wal_lsn(&self) -> Result<u64> {
        Ok(lock(&self.inner)?.wal.next_lsn().saturating_sub(1))
    }

    pub fn buffered(&self) -> Result<usize> {
        let st = lock(&self.inner)?;
        Ok(st.frames.iter().filter(|f| f.valid).count())
    }

    pub fn pin_count(&self, id: PageId) -> Result<u32> {
        let st = lock(&self.inner)?;
        Ok(st
            .table
            .get(&id.0)
            .map(|idx| st.frames[*idx].pin)
            .unwrap_or(0))
    }

    fn recover(&self) -> Result<Catalog> {
        let mut st = lock(&self.inner)?;
        let checkpoint_at = st
            .records
            .iter()
            .rposition(|r| r.kind == WalKind::Checkpoint)
            .unwrap_or(0);
        let mut catalog = if st.records.is_empty() {
            Catalog::empty()
        } else {
            Catalog::decode(&st.records[checkpoint_at].catalog_after)?
        };
        let tail = st.records[checkpoint_at + 1..].to_vec();
        let committed: std::collections::HashSet<u64> = tail
            .iter()
            .filter(|r| r.kind == WalKind::Commit)
            .map(|r| r.txn_id)
            .collect();
        for rec in &tail {
            if rec.kind != WalKind::Mutation {
                continue;
            }
            for image in &rec.pages {
                st.install_image(image.page_id, &image.after, rec.lsn)?;
            }
        }
        let mut losers: Vec<u64> = tail
            .iter()
            .filter(|r| r.kind == WalKind::Mutation && !committed.contains(&r.txn_id))
            .map(|r| r.txn_id)
            .collect();
        losers.sort_unstable();
        losers.dedup();
        for rec in tail.iter().rev() {
            if rec.kind != WalKind::Mutation || committed.contains(&rec.txn_id) {
                continue;
            }
            for image in rec.pages.iter().rev() {
                st.force_image(image.page_id, &image.before)?;
            }
        }
        for txn_id in &losers {
            let already = tail.iter().any(|r| r.kind == WalKind::Abort && r.txn_id == *txn_id);
            if !already {
                let mut abort = WalRecord {
                    lsn: 0,
                    kind: WalKind::Abort,
                    txn_id: *txn_id,
                    commit_ts: 0,
                    pages: Vec::new(),
                    catalog_before: Vec::new(),
                    catalog_after: Vec::new(),
                    logical: None,
                    ops: Vec::new(),
                };
                let lsn = st.wal.append(&abort)?;
                abort.lsn = lsn;
                st.records.push(abort);
            }
        }
        if !losers.is_empty() {
            st.wal.flush()?;
            st.flush_dirty()?;
            st.store.sync()?;
        }
        for rec in &tail {
            if rec.kind == WalKind::Mutation && committed.contains(&rec.txn_id) && !rec.catalog_after.is_empty()
            {
                catalog = Catalog::decode(&rec.catalog_after)?;
            }
        }
        let max_txn = st.records.iter().map(|r| r.txn_id).max().unwrap_or(0);
        if catalog.next_txn_id <= max_txn {
            catalog.next_txn_id = max_txn + 1;
        }
        let max_ts = st
            .records
            .iter()
            .map(|r| r.commit_ts)
            .max()
            .unwrap_or(0);
        if catalog.next_commit_ts < max_ts {
            catalog.next_commit_ts = max_ts;
        }
        Ok(catalog)
    }
}

impl PoolState {
    fn load(&mut self, id: u32) -> Result<()> {
        if let Some(&idx) = self.table.get(&id) {
            self.frames[idx].pin += 1;
            self.frames[idx].referenced = true;
            return Ok(());
        }
        let idx = self.evict_frame()?;
        let page = self.store.read(PageId(id))?;
        self.frames[idx] = Frame {
            page_id: id,
            page,
            pin: 1,
            dirty: false,
            valid: true,
            referenced: true,
        };
        self.table.insert(id, idx);
        Ok(())
    }

    fn evict_frame(&mut self) -> Result<usize> {
        let n = self.frames.len();
        for _ in 0..(n * 2) {
            let idx = self.clock % n;
            self.clock = self.clock.wrapping_add(1);
            let frame = &self.frames[idx];
            if !frame.valid {
                return Ok(idx);
            }
            if frame.pin > 0 || self.group_holds(frame.page_id) {
                continue;
            }
            if frame.referenced {
                self.frames[idx].referenced = false;
                continue;
            }
            self.write_back(idx)?;
            let id = self.frames[idx].page_id;
            self.table.remove(&id);
            self.frames[idx].valid = false;
            return Ok(idx);
        }
        Err(Error::Full)
    }

    fn group_holds(&self, id: u32) -> bool {
        self.group
            .as_ref()
            .map(|g| g.befores.contains_key(&id))
            .unwrap_or(false)
    }

    fn write_back(&mut self, idx: usize) -> Result<()> {
        if !self.frames[idx].dirty {
            return Ok(());
        }
        let lsn = self.frames[idx].page.lsn();
        if lsn > self.wal.flushed_lsn() {
            self.wal.flush()?;
        }
        let page = self.frames[idx].page.duplicate();
        self.store.write(&page)?;
        self.frames[idx].dirty = false;
        Ok(())
    }

    fn flush_dirty(&mut self) -> Result<()> {
        let dirty: Vec<usize> = self
            .frames
            .iter()
            .enumerate()
            .filter(|(_, f)| f.valid && f.dirty)
            .map(|(i, _)| i)
            .collect();
        for idx in dirty {
            self.write_back(idx)?;
        }
        Ok(())
    }

    fn install_image(&mut self, page_id: u32, bytes: &[u8], lsn: u64) -> Result<()> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::Corrupt {
                detail: "bad page image length".into(),
            });
        }
        let mut raw = [0u8; PAGE_SIZE];
        raw.copy_from_slice(bytes);
        let page = Page::from_sealed(raw).map_err(|_| Error::Corrupt {
            detail: format!("wal page image {page_id} failed checksum"),
        })?;
        let existing_lsn = if page_id < self.store.page_count() {
            match self.store.read(PageId(page_id)) {
                Ok(current) => Some(current.lsn()),
                Err(_) => None,
            }
        } else {
            None
        };
        if existing_lsn.is_some() && existing_lsn.unwrap() >= lsn && existing_lsn.unwrap() != 0 {
            return Ok(());
        }
        self.store.write(&page)?;
        if let Some(&idx) = self.table.get(&page_id) {
            self.frames[idx].page = page;
            self.frames[idx].dirty = false;
        }
        Ok(())
    }
    fn force_image(&mut self, page_id: u32, bytes: &[u8]) -> Result<()> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::Corrupt {
                detail: "bad page image length".into(),
            });
        }
        let mut raw = [0u8; PAGE_SIZE];
        raw.copy_from_slice(bytes);
        let page = Page::from_sealed(raw)?;
        self.store.write(&page)?;
        if let Some(&idx) = self.table.get(&page_id) {
            self.frames[idx].page = page;
            self.frames[idx].dirty = false;
        }
        Ok(())
    }

    fn wal_dir_catalog(&self) -> Option<std::path::PathBuf> {
        None
    }
}

impl Pin {
    pub fn id(&self) -> PageId {
        PageId(self.id)
    }

    pub fn read(&self) -> Result<Page> {
        let st = lock(&self.pool)?;
        let idx = st.table.get(&self.id).copied().ok_or_else(|| Error::Corrupt {
            detail: format!("pin missing page {}", self.id),
        })?;
        Ok(st.frames[idx].page.duplicate())
    }

    pub fn modify(&self, f: impl FnOnce(&mut Page) -> Result<()>) -> Result<()> {
        let mut st = lock(&self.pool)?;
        let group_open = st.group.is_some();
        if !group_open {
            return Err(Error::Txn(
                "page modify requires an open mutation group".into(),
            ));
        }
        let idx = st.table.get(&self.id).copied().ok_or_else(|| Error::Corrupt {
            detail: format!("pin missing page {}", self.id),
        })?;
        let before = st.frames[idx].page.duplicate();
        if let Some(group) = st.group.as_mut() {
            group.befores.entry(self.id).or_insert(before);
        }
        f(&mut st.frames[idx].page)?;
        st.frames[idx].dirty = true;
        Ok(())
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        if !self.alive {
            return;
        }
        self.alive = false;
        if let Ok(mut st) = self.pool.lock() {
            if let Some(&idx) = st.table.get(&self.id) {
                st.frames[idx].pin = st.frames[idx].pin.saturating_sub(1);
            }
        }
    }
}

fn lock(inner: &Arc<Mutex<PoolState>>) -> Result<MutexGuard<'_, PoolState>> {
    inner.lock().map_err(|_| Error::Corrupt {
        detail: "buffer pool lock poisoned".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PageKind;

    #[test]
    fn pinned_page_is_not_evicted() {
        let dir = std::env::temp_dir().join(format!("strata-pin-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let pool = BufferPool::create(&dir, 8).unwrap();
        let mut pins = Vec::new();
        for _ in 0..8 {
            let id = pool.alloc(PageKind::Heap).unwrap();
            pins.push(pool.pin(id).unwrap());
        }
        let err = pool.alloc(PageKind::Heap).unwrap();
        let load = pool.pin(err);
        assert!(matches!(load, Err(Error::Full)));
        drop(pins);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
