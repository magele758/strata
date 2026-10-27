//! Hierarchical navigable small-world index persisted in buffer-pool pages.
//!
//! `M = 16`, layer 0 allows `2M` links, `ef_construction = 64`,
//! `ef_search = 64`, level multiplier `1/ln(M)`. Levels are chosen by
//! hashing the node id so insertion order is deterministic. Nodes are
//! packed into chained `PageKind::Hnsw` pages; a directory chain maps
//! node ids to `(page, offset, size)`. Neighbor updates and new nodes
//! are written through `Pin::modify`.
//!
//! Search walks the graph. If the entry point is missing, it falls back
//! to a full scan so a tiny or damaged index still returns true nearest
//! neighbors. Distance is L2.

use std::collections::{HashMap, HashSet};

use crate::buffer::BufferPool;
use crate::error::{Error, Result};
use crate::types::{
    read_u16, read_u32, write_u16, write_u32, Page, PageId, PageKind, Rid, PAYLOAD_LEN,
};

const MAGIC: u32 = 0x484E_5357;
const VERSION: u32 = 1;
const M: usize = 16;
const M0: usize = 32;
const EF_CONSTRUCTION: usize = 64;
const EF_SEARCH: usize = 64;
const MAX_LEVEL: u8 = 5;
const LAYER0_BYTES: usize = 4 + 4 * M0;
const LAYER_BYTES: usize = 4 + 4 * M;
const CHAIN_LIMIT: u32 = 1_000_000;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_DIM: usize = 8;
const OFF_COUNT: usize = 12;
const OFF_ENTRY: usize = 16;
const OFF_MAX_LEVEL: usize = 20;
const OFF_DIR_HEAD: usize = 24;
const OFF_DIR_TAIL: usize = 28;
const OFF_DATA_HEAD: usize = 32;
const OFF_DATA_TAIL: usize = 36;

pub struct HnswIndex;

#[derive(Clone, Copy)]
struct DirEnt {
    page: u32,
    offset: u16,
    size: u16,
}

struct Store<'a> {
    pool: &'a BufferPool,
    root: PageId,
    dim: u32,
    count: u32,
    entry: u32,
    max_level: u8,
    dir_head: u32,
    dir_tail: u32,
    data_head: u32,
    data_tail: u32,
    dir: Vec<DirEnt>,
    pages: HashMap<u32, Page>,
    dirty: HashSet<u32>,
}

impl HnswIndex {
    pub fn create(pool: &BufferPool, dim: u32) -> Result<PageId> {
        if dim == 0 || dim > 64 {
            return Err(Error::Type(format!(
                "hnsw dimension {dim} is outside 1..=64"
            )));
        }
        let id = pool.alloc(PageKind::Hnsw)?;
        let mut page = read_page(pool, id)?;
        write_root_fields(&mut page, dim, 0, u32::MAX, 0, 0, 0, 0, 0)?;
        persist(pool, id, &page)?;
        Ok(id)
    }

    pub fn add(pool: &BufferPool, root: PageId, vector: &[f32], rid: Rid) -> Result<()> {
        let mut store = Store::open(pool, root)?;
        if vector.len() != store.dim as usize {
            return Err(Error::Type(format!(
                "vector dimension {} does not match index dimension {}",
                vector.len(),
                store.dim
            )));
        }
        if store.count == u32::MAX {
            return Err(Error::Limit("hnsw node id overflow".into()));
        }
        let level = level_for(store.count);
        if store.count == 0 {
            let empty = vec![Vec::new(); level as usize + 1];
            let id = store.append_node(vector, rid, level, &empty)?;
            store.entry = id;
            store.max_level = level;
            store.commit()?;
            return Ok(());
        }
        let neighbors = if store.entry == u32::MAX {
            store.neighbors_by_scan(vector, level)?
        } else {
            store.neighbors_by_graph(vector, level)?
        };
        let new_id = store.append_node(vector, rid, level, &neighbors)?;
        for lc in 0..=level {
            let max_m = max_links(lc);
            for &nid in &neighbors[lc as usize] {
                store.add_neighbor(nid, lc, new_id, vector, max_m)?;
            }
        }
        if store.entry == u32::MAX || level > store.max_level {
            store.entry = new_id;
            store.max_level = store.max_level.max(level);
        }
        store.commit()?;
        Ok(())
    }

    pub fn search(
        pool: &BufferPool,
        root: PageId,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(Rid, f32)>> {
        let mut store = Store::open(pool, root)?;
        if query.len() != store.dim as usize {
            return Err(Error::Type(format!(
                "vector dimension {} does not match index dimension {}",
                query.len(),
                store.dim
            )));
        }
        if k == 0 || store.count == 0 {
            return Ok(Vec::new());
        }
        if store.entry == u32::MAX || store.entry >= store.count {
            return store.brute(query, k);
        }
        let mut ep = store.entry;
        for lc in (1..=store.max_level).rev() {
            let found = search_layer(&mut store, query, ep, 1, lc)?;
            if let Some((id, _)) = found.first() {
                ep = *id;
            }
        }
        let ef = EF_SEARCH.max(k);
        let found = search_layer(&mut store, query, ep, ef, 0)?;
        if found.len() < k.min(store.count as usize) {
            return store.brute(query, k);
        }
        let mut out = Vec::with_capacity(k.min(found.len()));
        for (id, dist) in found.into_iter().take(k) {
            out.push((store.rid_of(id)?, dist));
        }
        out.sort_by(|a, b| {
            a.1.total_cmp(&b.1)
                .then(a.0.page.cmp(&b.0.page))
                .then(a.0.slot.cmp(&b.0.slot))
        });
        Ok(out)
    }
}

impl<'a> Store<'a> {
    fn open(pool: &'a BufferPool, root: PageId) -> Result<Self> {
        let page = read_page(pool, root)?;
        check_root(&page)?;
        let p = page.payload();
        let dim = read_u32(p, OFF_DIM)?;
        if dim == 0 || dim > 64 {
            return Err(corrupt(format!("stored hnsw dim {dim} is invalid")));
        }
        let max_level = p[OFF_MAX_LEVEL];
        if max_level > MAX_LEVEL {
            return Err(corrupt(format!("stored hnsw max level {max_level}")));
        }
        let mut store = Self {
            pool,
            root,
            dim,
            count: read_u32(p, OFF_COUNT)?,
            entry: read_u32(p, OFF_ENTRY)?,
            max_level,
            dir_head: read_u32(p, OFF_DIR_HEAD)?,
            dir_tail: read_u32(p, OFF_DIR_TAIL)?,
            data_head: read_u32(p, OFF_DATA_HEAD)?,
            data_tail: read_u32(p, OFF_DATA_TAIL)?,
            dir: Vec::new(),
            pages: HashMap::new(),
            dirty: HashSet::new(),
        };
        store.pages.insert(root.0, page);
        if store.dir_head == root.0 || store.data_head == root.0 {
            return Err(corrupt("hnsw root linked as a child page"));
        }
        store.load_dir()?;
        if store.entry != u32::MAX && store.entry >= store.count {
            return Err(corrupt("hnsw entry point out of range"));
        }
        Ok(store)
    }

    fn commit(&mut self) -> Result<()> {
        self.flush_meta()?;
        let ids: Vec<u32> = self.dirty.iter().copied().collect();
        for id in ids {
            let page = self
                .pages
                .get(&id)
                .ok_or_else(|| corrupt(format!("dirty hnsw page {id} is not cached")))?
                .clone();
            persist(self.pool, PageId(id), &page)?;
        }
        self.dirty.clear();
        Ok(())
    }

    fn neighbors_by_graph(&mut self, vector: &[f32], level: u8) -> Result<Vec<Vec<u32>>> {
        let top = self.max_level;
        let mut ep = self.entry;
        if level < top {
            for lc in ((level + 1)..=top).rev() {
                let found = search_layer(self, vector, ep, 1, lc)?;
                if let Some((id, _)) = found.first() {
                    ep = *id;
                }
            }
        }
        let insert_top = level.min(top);
        let mut layers = vec![Vec::new(); level as usize + 1];
        for lc in (0..=insert_top).rev() {
            let found = search_layer(self, vector, ep, EF_CONSTRUCTION, lc)?;
            let max_m = max_links(lc);
            if let Some((best, _)) = found.first() {
                ep = *best;
            }
            layers[lc as usize] = found.into_iter().take(max_m).map(|(id, _)| id).collect();
        }
        Ok(layers)
    }

    fn neighbors_by_scan(&mut self, vector: &[f32], level: u8) -> Result<Vec<Vec<u32>>> {
        let mut layers = vec![Vec::new(); level as usize + 1];
        for lc in 0..=level {
            let mut scored = Vec::new();
            for id in 0..self.count {
                if self.level_of(id)? < lc {
                    continue;
                }
                let existing = self.vector(id)?;
                scored.push((id, l2(vector, &existing)));
            }
            scored.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            scored.truncate(max_links(lc));
            layers[lc as usize] = scored.into_iter().map(|(id, _)| id).collect();
        }
        Ok(layers)
    }

    fn add_neighbor(
        &mut self,
        node: u32,
        layer: u8,
        new_id: u32,
        new_vec: &[f32],
        max_m: usize,
    ) -> Result<()> {
        if node == new_id {
            return Ok(());
        }
        if self.level_of(node)? < layer {
            return Err(corrupt("hnsw neighbor link above node level"));
        }
        let mut nbs = self.neighbors(node, layer)?;
        if nbs.contains(&new_id) {
            return Ok(());
        }
        nbs.push(new_id);
        if nbs.len() > max_m {
            let origin = self.vector(node)?;
            let mut scored = Vec::with_capacity(nbs.len());
            for nid in nbs {
                let dist = if nid == new_id {
                    l2(&origin, new_vec)
                } else {
                    let existing = self.vector(nid)?;
                    l2(&origin, &existing)
                };
                scored.push((nid, dist));
            }
            scored.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            scored.truncate(max_m);
            if max_m > 0 && !scored.iter().any(|(id, _)| *id == new_id) {
                scored.pop();
                scored.push((new_id, l2(&origin, new_vec)));
            }
            nbs = scored.into_iter().map(|(id, _)| id).collect();
        }
        self.set_neighbors(node, layer, &nbs)
    }

    fn append_node(
        &mut self,
        vector: &[f32],
        rid: Rid,
        level: u8,
        neighbors: &[Vec<u32>],
    ) -> Result<u32> {
        let id = self.count;
        let bytes = encode_node(self.dim, rid, level, vector, neighbors)?;
        let (page, offset) = self.place_bytes(&bytes)?;
        self.push_dir(DirEnt {
            page,
            offset,
            size: bytes.len() as u16,
        })?;
        self.count = id + 1;
        Ok(id)
    }

    fn brute(&mut self, query: &[f32], k: usize) -> Result<Vec<(Rid, f32)>> {
        let mut all = Vec::with_capacity(self.count as usize);
        for id in 0..self.count {
            let existing = self.vector(id)?;
            all.push((self.rid_of(id)?, l2(query, &existing)));
        }
        all.sort_by(|a, b| {
            a.1.total_cmp(&b.1)
                .then(a.0.page.cmp(&b.0.page))
                .then(a.0.slot.cmp(&b.0.slot))
        });
        all.truncate(k);
        Ok(all)
    }

    fn load_dir(&mut self) -> Result<()> {
        let mut id = self.dir_head;
        let mut hops = 0u32;
        while id != 0 {
            hops += 1;
            if hops > CHAIN_LIMIT {
                return Err(corrupt("hnsw directory cycle"));
            }
            self.load_page(id)?;
            let page = self
                .pages
                .get(&id)
                .ok_or_else(|| corrupt("hnsw directory page missing"))?
                .clone();
            let count = read_u16(page.payload(), 0)? as usize;
            let cap = (PAYLOAD_LEN - 4) / 8;
            if count > cap {
                return Err(corrupt("hnsw directory page overfull"));
            }
            for i in 0..count {
                let at = 4 + i * 8;
                self.dir.push(DirEnt {
                    page: read_u32(page.payload(), at)?,
                    offset: read_u16(page.payload(), at + 4)?,
                    size: read_u16(page.payload(), at + 6)?,
                });
            }
            id = page.next();
        }
        if self.dir.len() != self.count as usize {
            return Err(corrupt(format!(
                "hnsw directory len {} != count {}",
                self.dir.len(),
                self.count
            )));
        }
        Ok(())
    }

    fn push_dir(&mut self, ent: DirEnt) -> Result<()> {
        self.ensure_dir_tail()?;
        if self.dir_head == 0 {
            let id = self.alloc_page(PageKind::Hnsw)?;
            self.dir_head = id;
            self.dir_tail = id;
        }
        loop {
            let tail = self.dir_tail;
            self.load_page(tail)?;
            let count = read_u16(self.pages.get(&tail).unwrap().payload(), 0)? as usize;
            if 4 + (count + 1) * 8 <= PAYLOAD_LEN {
                {
                    let page = self.pages.get_mut(&tail).unwrap();
                    let payload = page.payload_mut();
                    let at = 4 + count * 8;
                    write_u32(payload, at, ent.page);
                    write_u16(payload, at + 4, ent.offset);
                    write_u16(payload, at + 6, ent.size);
                    write_u16(payload, 0, (count + 1) as u16);
                }
                self.dirty.insert(tail);
                self.dir.push(ent);
                return Ok(());
            }
            let id = self.alloc_page(PageKind::Hnsw)?;
            self.pages.get_mut(&tail).unwrap().set_next(id);
            self.dirty.insert(tail);
            self.dir_tail = id;
        }
    }

    fn place_bytes(&mut self, bytes: &[u8]) -> Result<(u32, u16)> {
        if bytes.len() > PAYLOAD_LEN - 4 || bytes.len() > u16::MAX as usize {
            return Err(Error::Limit("hnsw node exceeds a page".into()));
        }
        self.ensure_data_tail()?;
        if self.data_head == 0 {
            let id = self.alloc_page(PageKind::Hnsw)?;
            self.data_head = id;
            self.data_tail = id;
        }
        loop {
            let tail = self.data_tail;
            self.load_page(tail)?;
            let used = free_off(self.pages.get(&tail).unwrap())?;
            if used + bytes.len() <= PAYLOAD_LEN {
                let off = used as u16;
                {
                    let page = self.pages.get_mut(&tail).unwrap();
                    let payload = page.payload_mut();
                    payload[used..used + bytes.len()].copy_from_slice(bytes);
                    write_u16(payload, 0, (used + bytes.len()) as u16);
                    let n = read_u16(payload, 2)?;
                    write_u16(payload, 2, n + 1);
                }
                self.dirty.insert(tail);
                return Ok((tail, off));
            }
            let id = self.alloc_page(PageKind::Hnsw)?;
            self.pages.get_mut(&tail).unwrap().set_next(id);
            self.dirty.insert(tail);
            self.data_tail = id;
        }
    }

    fn ensure_data_tail(&mut self) -> Result<()> {
        if self.data_head == 0 {
            return Ok(());
        }
        let start = if self.data_tail == 0 {
            self.data_head
        } else {
            self.data_tail
        };
        self.data_tail = walk_tail(self, start)?;
        Ok(())
    }

    fn ensure_dir_tail(&mut self) -> Result<()> {
        if self.dir_head == 0 {
            return Ok(());
        }
        let start = if self.dir_tail == 0 {
            self.dir_head
        } else {
            self.dir_tail
        };
        self.dir_tail = walk_tail(self, start)?;
        Ok(())
    }

    fn node_bytes(&mut self, id: u32) -> Result<Vec<u8>> {
        if id >= self.count || id as usize >= self.dir.len() {
            return Err(corrupt(format!("hnsw node {id} out of range")));
        }
        let ent = self.dir[id as usize];
        self.load_page(ent.page)?;
        let page = self
            .pages
            .get(&ent.page)
            .ok_or_else(|| corrupt("hnsw node page missing"))?;
        let start = ent.offset as usize;
        let end = start + ent.size as usize;
        if end > page.payload().len() {
            return Err(corrupt("hnsw node offset out of page"));
        }
        let bytes = page.payload()[start..end].to_vec();
        if bytes.len() < 8 {
            return Err(corrupt("hnsw node header truncated"));
        }
        let level = bytes[6];
        if level > MAX_LEVEL || bytes.len() != node_bytes(self.dim, level) {
            return Err(corrupt(format!(
                "hnsw node {id} size {} does not match level {level}",
                bytes.len()
            )));
        }
        Ok(bytes)
    }

    fn vector(&mut self, id: u32) -> Result<Vec<f32>> {
        let bytes = self.node_bytes(id)?;
        let mut out = Vec::with_capacity(self.dim as usize);
        for i in 0..self.dim as usize {
            out.push(read_f32(&bytes, 8 + i * 4)?);
        }
        Ok(out)
    }

    fn rid_of(&mut self, id: u32) -> Result<Rid> {
        let bytes = self.node_bytes(id)?;
        Rid::decode(&bytes[..6])
    }

    fn level_of(&mut self, id: u32) -> Result<u8> {
        Ok(self.node_bytes(id)?[6])
    }

    fn neighbors(&mut self, id: u32, layer: u8) -> Result<Vec<u32>> {
        let bytes = self.node_bytes(id)?;
        let level = bytes[6];
        if layer > level {
            return Ok(Vec::new());
        }
        let off = layer_offset(self.dim, layer);
        let max = max_links(layer);
        if off + layer_bytes(layer) > bytes.len() {
            return Err(corrupt("hnsw neighbor region truncated"));
        }
        let ids = read_neighbors(&bytes[off..], max)?;
        for nid in &ids {
            if *nid >= self.count {
                return Err(corrupt(format!("hnsw neighbor {nid} out of range")));
            }
        }
        Ok(ids)
    }

    fn set_neighbors(&mut self, id: u32, layer: u8, ids: &[u32]) -> Result<()> {
        let max = max_links(layer);
        if ids.len() > max {
            return Err(Error::Limit("too many hnsw neighbors".into()));
        }
        let ent = self.dir[id as usize];
        let off = ent.offset as usize + layer_offset(self.dim, layer);
        self.load_page(ent.page)?;
        {
            let page = self.pages.get_mut(&ent.page).unwrap();
            let payload = page.payload_mut();
            if off + layer_bytes(layer) > payload.len() {
                return Err(corrupt("hnsw neighbor write out of page"));
            }
            write_u16(payload, off, ids.len() as u16);
            for i in 0..max {
                let value = ids.get(i).copied().unwrap_or(0);
                write_u32(payload, off + 4 + i * 4, value);
            }
        }
        self.dirty.insert(ent.page);
        Ok(())
    }

    fn flush_meta(&mut self) -> Result<()> {
        let root = self.root.0;
        self.load_page(root)?;
        {
            let page = self.pages.get_mut(&root).unwrap();
            write_root_fields(
                page,
                self.dim,
                self.count,
                self.entry,
                self.max_level,
                self.dir_head,
                self.dir_tail,
                self.data_head,
                self.data_tail,
            )?;
        }
        self.dirty.insert(root);
        Ok(())
    }

    fn load_page(&mut self, id: u32) -> Result<()> {
        if id == 0 {
            return Err(corrupt("hnsw page 0"));
        }
        if self.pages.contains_key(&id) {
            return Ok(());
        }
        let page = read_page(self.pool, PageId(id))?;
        self.pages.insert(id, page);
        Ok(())
    }

    fn alloc_page(&mut self, kind: PageKind) -> Result<u32> {
        let id = self.pool.alloc(kind)?;
        let page = read_page(self.pool, id)?;
        self.pages.insert(id.0, page);
        self.dirty.insert(id.0);
        Ok(id.0)
    }
}

fn search_layer(
    store: &mut Store<'_>,
    query: &[f32],
    entry: u32,
    ef: usize,
    layer: u8,
) -> Result<Vec<(u32, f32)>> {
    if ef == 0 {
        return Ok(Vec::new());
    }
    if entry >= store.count {
        return Err(corrupt("hnsw search entry out of range"));
    }
    let mut visited = HashSet::new();
    visited.insert(entry);
    let d0 = l2(query, &store.vector(entry)?);
    let mut candidates = vec![(entry, d0)];
    let mut results = vec![(entry, d0)];
    while !candidates.is_empty() {
        candidates.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        let (current, current_dist) = candidates.remove(0);
        let farthest = results
            .iter()
            .map(|(_, dist)| *dist)
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(f32::MAX);
        if current_dist > farthest && results.len() >= ef {
            break;
        }
        for nid in store.neighbors(current, layer)? {
            if !visited.insert(nid) {
                continue;
            }
            let dist = l2(query, &store.vector(nid)?);
            let farthest = results
                .iter()
                .map(|(_, d)| *d)
                .max_by(|a, b| a.total_cmp(b))
                .unwrap_or(f32::MAX);
            if results.len() < ef || dist < farthest {
                candidates.push((nid, dist));
                results.push((nid, dist));
                if results.len() > ef {
                    drop_farthest(&mut results);
                }
            }
        }
    }
    results.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    Ok(results)
}
fn drop_farthest(results: &mut Vec<(u32, f32)>) {
    let Some((idx, _)) = results
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
    else {
        return;
    };
    results.swap_remove(idx);
}

fn walk_tail(store: &mut Store<'_>, start: u32) -> Result<u32> {
    let mut id = start;
    let mut hops = 0u32;
    loop {
        store.load_page(id)?;
        let next = store
            .pages
            .get(&id)
            .ok_or_else(|| corrupt(format!("hnsw page {id} missing from cache")))?
            .next();
        if next == 0 {
            return Ok(id);
        }
        hops += 1;
        if hops > CHAIN_LIMIT {
            return Err(corrupt("hnsw page chain cycle"));
        }
        id = next;
    }
}

fn encode_node(
    dim: u32,
    rid: Rid,
    level: u8,
    vector: &[f32],
    neighbors: &[Vec<u32>],
) -> Result<Vec<u8>> {
    if level > MAX_LEVEL {
        return Err(corrupt("hnsw level above cap"));
    }
    if vector.len() != dim as usize {
        return Err(Error::Type(format!(
            "vector dimension {} does not match index dimension {dim}",
            vector.len()
        )));
    }
    if neighbors.len() != level as usize + 1 {
        return Err(corrupt("hnsw neighbor layers do not match level"));
    }
    let mut buf = vec![0u8; node_bytes(dim, level)];
    buf[..4].copy_from_slice(&rid.page.to_le_bytes());
    buf[4..6].copy_from_slice(&rid.slot.to_le_bytes());
    buf[6] = level;
    buf[7] = 1;
    for (i, value) in vector.iter().enumerate() {
        buf[8 + i * 4..12 + i * 4].copy_from_slice(&value.to_le_bytes());
    }
    for lc in 0..=level {
        let ids = &neighbors[lc as usize];
        let max = max_links(lc);
        if ids.len() > max {
            return Err(Error::Limit("too many hnsw neighbors".into()));
        }
        let off = layer_offset(dim, lc);
        write_u16(&mut buf, off, ids.len() as u16);
        for (i, nid) in ids.iter().enumerate() {
            write_u32(&mut buf, off + 4 + i * 4, *nid);
        }
    }
    Ok(buf)
}

fn read_neighbors(buf: &[u8], max: usize) -> Result<Vec<u32>> {
    let count = read_u16(buf, 0)? as usize;
    if count > max {
        return Err(corrupt("hnsw neighbor count exceeds M"));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        out.push(read_u32(buf, 4 + i * 4)?);
    }
    Ok(out)
}

fn node_bytes(dim: u32, level: u8) -> usize {
    8 + dim as usize * 4 + LAYER0_BYTES + level as usize * LAYER_BYTES
}

fn layer_offset(dim: u32, layer: u8) -> usize {
    let base = 8 + dim as usize * 4;
    if layer == 0 {
        base
    } else {
        base + LAYER0_BYTES + (layer as usize - 1) * LAYER_BYTES
    }
}

fn layer_bytes(layer: u8) -> usize {
    if layer == 0 {
        LAYER0_BYTES
    } else {
        LAYER_BYTES
    }
}

fn max_links(layer: u8) -> usize {
    if layer == 0 {
        M0
    } else {
        M
    }
}

fn free_off(page: &Page) -> Result<usize> {
    let raw = read_u16(page.payload(), 0)? as usize;
    if raw < 4 {
        Ok(4)
    } else if raw > PAYLOAD_LEN {
        Err(corrupt("hnsw page free offset past payload"))
    } else {
        Ok(raw)
    }
}

fn level_for(node_id: u32) -> u8 {
    let mut z = node_id.wrapping_add(0x9E37_79B9);
    z = z.wrapping_mul(0x85EB_CA6B);
    z ^= z >> 16;
    z = z.wrapping_mul(0xC2B2_AE35);
    z ^= z >> 16;
    let unit = (f64::from(z) + 1.0) / (f64::from(u32::MAX) + 2.0);
    let ml = 1.0 / (M as f64).ln();
    let raw = (-unit.ln() * ml).floor();
    if raw <= 0.0 {
        0
    } else if raw >= f64::from(MAX_LEVEL) {
        MAX_LEVEL
    } else {
        raw as u8
    }
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    let mut sum = 0.0f32;
    for i in 0..n {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum.sqrt()
}

fn read_f32(buf: &[u8], at: usize) -> Result<f32> {
    Ok(f32::from_le_bytes(read_u32(buf, at)?.to_le_bytes()))
}

fn check_root(page: &Page) -> Result<()> {
    let kind = page.kind()?;
    if kind != PageKind::Hnsw {
        return Err(corrupt(format!("hnsw root has kind {kind:?}")));
    }
    let magic = read_u32(page.payload(), OFF_MAGIC)?;
    if magic != MAGIC {
        return Err(corrupt(format!("bad hnsw magic {magic:#x}")));
    }
    let version = read_u32(page.payload(), OFF_VERSION)?;
    if version != VERSION {
        return Err(corrupt(format!("unsupported hnsw version {version}")));
    }
    Ok(())
}

fn write_root_fields(
    page: &mut Page,
    dim: u32,
    count: u32,
    entry: u32,
    max_level: u8,
    dir_head: u32,
    dir_tail: u32,
    data_head: u32,
    data_tail: u32,
) -> Result<()> {
    let p = page.payload_mut();
    write_u32(p, OFF_MAGIC, MAGIC);
    write_u32(p, OFF_VERSION, VERSION);
    write_u32(p, OFF_DIM, dim);
    write_u32(p, OFF_COUNT, count);
    write_u32(p, OFF_ENTRY, entry);
    p[OFF_MAX_LEVEL] = max_level;
    write_u32(p, OFF_DIR_HEAD, dir_head);
    write_u32(p, OFF_DIR_TAIL, dir_tail);
    write_u32(p, OFF_DATA_HEAD, data_head);
    write_u32(p, OFF_DATA_TAIL, data_tail);
    Ok(())
}

fn read_page(pool: &BufferPool, id: PageId) -> Result<Page> {
    if id.0 == 0 {
        return Err(corrupt("page 0 is not a data page"));
    }
    let pin = pool.pin(id)?;
    pin.read()
}

fn persist(pool: &BufferPool, id: PageId, page: &Page) -> Result<()> {
    let payload = page.payload().to_vec();
    let next = page.next();
    let extra = page.extra();
    let pin = pool.pin(id)?;
    pin.modify(move |p| {
        if payload.len() != p.payload().len() {
            return Err(corrupt("payload length mismatch"));
        }
        p.payload_mut().copy_from_slice(&payload);
        p.set_next(next);
        p.set_extra(extra);
        Ok(())
    })
}

fn corrupt(detail: impl Into<String>) -> Error {
    Error::Corrupt {
        detail: detail.into(),
    }
}
