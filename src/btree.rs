//! B+ tree. Leaf payload is `u16` count, then `u16 key_len, key, u16 val_len, value`
//! entries, sorted by raw key bytes. `Page::next` is the right sibling leaf, or 0.
//! Internal payload is `u16` key count, `u32` leftmost child, then
//! `u16 key_len, key, u32 right child`. A separator is the first key of the right
//! subtree; a search key strictly less than the separator goes left.

use crate::buffer::BufferPool;
use crate::error::{Error, Result};
use crate::types::{
    read_u16, read_u32, write_u16, write_u32, Page, PageId, PageKind, MAX_INDEX_VAL, MAX_KEY,
    PAYLOAD_LEN,
};

const LEAF_HEADER: usize = 2;
const INTERNAL_HEADER: usize = 6;

pub struct BTree;

impl BTree {
    pub fn create(pool: &BufferPool) -> Result<PageId> {
        let id = pool.alloc(PageKind::BtreeLeaf)?;
        rewrite_leaf(pool, id, &[], 0)?;
        Ok(id)
    }

    pub fn insert(pool: &BufferPool, root: PageId, key: &[u8], value: &[u8]) -> Result<PageId> {
        check_limits(key, value)?;
        let (leaf_id, stack) = descend(pool, root, key)?;
        let page = read_page(pool, leaf_id)?;
        let next = page.next();
        let mut entries = decode_leaf(&page)?;
        let idx = entries.partition_point(|(k, _)| k.as_slice() < key);
        if idx < entries.len() && entries[idx].0.as_slice() == key {
            return Err(Error::Exists("duplicate key".into()));
        }
        entries.insert(idx, (key.to_vec(), value.to_vec()));
        if leaf_fits(&entries) {
            rewrite_leaf(pool, leaf_id, &entries, next)?;
            return Ok(root);
        }
        let parts = split_leaf(entries)?;
        if parts.len() < 2 || parts.iter().any(Vec::is_empty) {
            return Err(Error::Corrupt {
                detail: "btree leaf split produced no right sibling".into(),
            });
        }
        let mut new_ids = Vec::with_capacity(parts.len() - 1);
        for _ in 1..parts.len() {
            new_ids.push(pool.alloc(PageKind::BtreeLeaf)?);
        }
        for i in (0..parts.len()).rev() {
            let id = if i == 0 { leaf_id } else { new_ids[i - 1] };
            let sibling = if i + 1 == parts.len() {
                next
            } else {
                new_ids[i].0
            };
            rewrite_leaf(pool, id, &parts[i], sibling)?;
        }
        let seps = new_ids
            .iter()
            .enumerate()
            .map(|(i, id)| Separator {
                key: parts[i + 1][0].0.clone(),
                child: *id,
            })
            .collect();
        insert_separators(pool, stack, leaf_id, seps, root)
    }

    pub fn delete_key(pool: &BufferPool, root: PageId, key: &[u8]) -> Result<bool> {
        let (leaf_id, _) = descend(pool, root, key)?;
        let page = read_page(pool, leaf_id)?;
        let next = page.next();
        let mut entries = decode_leaf(&page)?;
        let idx = entries.partition_point(|(k, _)| k.as_slice() < key);
        if idx >= entries.len() || entries[idx].0.as_slice() != key {
            return Ok(false);
        }
        entries.remove(idx);
        rewrite_leaf(pool, leaf_id, &entries, next)?;
        Ok(true)
    }

    pub fn get(pool: &BufferPool, root: PageId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (leaf_id, _) = descend(pool, root, key)?;
        let page = read_page(pool, leaf_id)?;
        let entries = decode_leaf(&page)?;
        let idx = entries.partition_point(|(k, _)| k.as_slice() < key);
        if idx < entries.len() && entries[idx].0.as_slice() == key {
            Ok(Some(entries[idx].1.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn range(
        pool: &BufferPool,
        root: PageId,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (mut leaf_id, _) = descend(pool, root, start)?;
        let hops = hop_limit(pool)?;
        let mut out = Vec::new();
        for _ in 0..hops {
            let page = read_page(pool, leaf_id)?;
            if page.kind()? != PageKind::BtreeLeaf {
                return Err(Error::Corrupt {
                    detail: format!("btree sibling {} is not a leaf", leaf_id.0),
                });
            }
            for (k, v) in decode_leaf(&page)? {
                if k.as_slice() < start {
                    continue;
                }
                if let Some(end) = end {
                    if k.as_slice() >= end {
                        return Ok(out);
                    }
                }
                out.push((k, v));
            }
            let next = page.next();
            if next == 0 {
                return Ok(out);
            }
            leaf_id = PageId(next);
        }
        Err(Error::Corrupt {
            detail: "btree leaf chain is cyclic".into(),
        })
    }
}

#[derive(Clone)]
struct Separator {
    key: Vec<u8>,
    child: PageId,
}

struct InternalNode {
    leftmost: PageId,
    seps: Vec<Separator>,
}

fn check_limits(key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY {
        return Err(Error::Limit(format!(
            "key length {} exceeds {MAX_KEY}",
            key.len()
        )));
    }
    if value.len() > MAX_INDEX_VAL {
        return Err(Error::Limit(format!(
            "value length {} exceeds {MAX_INDEX_VAL}",
            value.len()
        )));
    }
    Ok(())
}

fn hop_limit(pool: &BufferPool) -> Result<usize> {
    Ok(pool.page_count()?.max(1) as usize + 1)
}

fn read_page(pool: &BufferPool, id: PageId) -> Result<Page> {
    if id.0 == 0 {
        return Err(Error::Corrupt {
            detail: "btree references page 0".into(),
        });
    }
    let pin = pool.pin(id)?;
    let page = pin.read()?;
    drop(pin);
    Ok(page)
}

fn descend(pool: &BufferPool, root: PageId, key: &[u8]) -> Result<(PageId, Vec<PageId>)> {
    let hops = hop_limit(pool)?;
    let mut stack = Vec::new();
    let mut current = root;
    for _ in 0..hops {
        let page = read_page(pool, current)?;
        match page.kind()? {
            PageKind::BtreeLeaf => return Ok((current, stack)),
            PageKind::BtreeInternal => {
                let (leftmost, seps) = decode_internal(&page)?;
                let child = child_for(leftmost, &seps, key);
                stack.push(current);
                current = child;
            }
            other => {
                return Err(Error::Corrupt {
                    detail: format!("page {} has kind {other:?}, not a btree page", current.0),
                });
            }
        }
    }
    Err(Error::Corrupt {
        detail: "btree descent exceeded page count".into(),
    })
}

fn child_for(leftmost: PageId, seps: &[Separator], key: &[u8]) -> PageId {
    let mut child = leftmost;
    for sep in seps {
        if key < sep.key.as_slice() {
            return child;
        }
        child = sep.child;
    }
    child
}

fn insert_separators(
    pool: &BufferPool,
    mut stack: Vec<PageId>,
    left_page: PageId,
    seps: Vec<Separator>,
    root: PageId,
) -> Result<PageId> {
    if seps.is_empty() {
        return Ok(root);
    }
    let Some(parent_id) = stack.pop() else {
        return build_root(pool, left_page, seps, 0);
    };
    let page = read_page(pool, parent_id)?;
    let (leftmost, mut existing) = decode_internal(&page)?;
    merge_separators(&mut existing, seps)?;
    if internal_fits(&existing) {
        rewrite_internal(pool, parent_id, leftmost, &existing)?;
        return Ok(root);
    }
    let (nodes, promoted) = split_internal(InternalNode {
        leftmost,
        seps: existing,
    })?;
    if nodes.len() < 2 || promoted.len() + 1 != nodes.len() {
        return Err(Error::Corrupt {
            detail: "btree internal split failed".into(),
        });
    }
    let mut ids = Vec::with_capacity(nodes.len());
    ids.push(parent_id);
    for _ in 1..nodes.len() {
        ids.push(pool.alloc(PageKind::BtreeInternal)?);
    }
    for (id, node) in ids.iter().zip(nodes.iter()) {
        rewrite_internal(pool, *id, node.leftmost, &node.seps)?;
    }
    let up = promoted
        .into_iter()
        .enumerate()
        .map(|(i, key)| Separator {
            key,
            child: ids[i + 1],
        })
        .collect();
    insert_separators(pool, stack, ids[0], up, root)
}

fn build_root(
    pool: &BufferPool,
    leftmost: PageId,
    seps: Vec<Separator>,
    depth: usize,
) -> Result<PageId> {
    if depth > hop_limit(pool)? {
        return Err(Error::Corrupt {
            detail: "btree root split did not converge".into(),
        });
    }
    if internal_fits(&seps) {
        let id = pool.alloc(PageKind::BtreeInternal)?;
        rewrite_internal(pool, id, leftmost, &seps)?;
        return Ok(id);
    }
    let (nodes, promoted) = split_internal(InternalNode { leftmost, seps })?;
    if nodes.len() < 2 || promoted.len() + 1 != nodes.len() {
        return Err(Error::Corrupt {
            detail: "btree root split failed".into(),
        });
    }
    let mut ids = Vec::with_capacity(nodes.len());
    for _ in 0..nodes.len() {
        ids.push(pool.alloc(PageKind::BtreeInternal)?);
    }
    for (id, node) in ids.iter().zip(nodes.iter()) {
        rewrite_internal(pool, *id, node.leftmost, &node.seps)?;
    }
    let up = promoted
        .into_iter()
        .enumerate()
        .map(|(i, key)| Separator {
            key,
            child: ids[i + 1],
        })
        .collect();
    build_root(pool, ids[0], up, depth + 1)
}

fn merge_separators(existing: &mut Vec<Separator>, incoming: Vec<Separator>) -> Result<()> {
    for sep in incoming {
        let idx = existing.partition_point(|s| s.key.as_slice() < sep.key.as_slice());
        if idx < existing.len() && existing[idx].key == sep.key {
            return Err(Error::Corrupt {
                detail: "duplicate btree separator".into(),
            });
        }
        existing.insert(idx, sep);
    }
    Ok(())
}

/// Left keeps the lower half of the sorted entries (at least one). Right gets
/// the rest. If variable-length entries make that cut overflow a page, pick
/// another cut; if no two-page cut fits, pack into the minimum number of leaves.
fn split_leaf(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Vec<Vec<(Vec<u8>, Vec<u8>)>>> {
    if entries.len() < 2 {
        return Err(Error::Limit("btree entry exceeds page".into()));
    }
    for (k, v) in &entries {
        if LEAF_HEADER + entry_len(k, v) > PAYLOAD_LEN {
            return Err(Error::Limit("btree entry exceeds page".into()));
        }
    }
    let mid = entries.len() / 2;
    if leaf_fits(&entries[..mid]) && leaf_fits(&entries[mid..]) {
        let mut left = entries;
        let right = left.split_off(mid);
        return Ok(vec![left, right]);
    }
    let target = entries.len() / 2;
    let mut best: Option<(usize, usize)> = None;
    for i in 1..entries.len() {
        if leaf_fits(&entries[..i]) && leaf_fits(&entries[i..]) {
            let dist = i.abs_diff(target);
            if best.map(|(_, d)| dist < d).unwrap_or(true) {
                best = Some((i, dist));
            }
        }
    }
    if let Some((i, _)) = best {
        let mut left = entries;
        let right = left.split_off(i);
        return Ok(vec![left, right]);
    }
    let mut rest = entries;
    let mut pages = Vec::new();
    while !rest.is_empty() {
        let mut n = 1;
        while n < rest.len() && leaf_fits(&rest[..n + 1]) {
            n += 1;
        }
        if !leaf_fits(&rest[..n]) {
            return Err(Error::Limit("btree entry exceeds page".into()));
        }
        pages.push(rest.drain(..n).collect());
    }
    Ok(pages)
}

/// Promote one separator so each side fits. The promoted key is not stored in
/// either node: it is the first key of the right node's leftmost subtree.
fn split_internal(node: InternalNode) -> Result<(Vec<InternalNode>, Vec<Vec<u8>>)> {
    let InternalNode { leftmost, seps } = node;
    if seps.is_empty() {
        return Err(Error::Corrupt {
            detail: "cannot split an internal node with no separators".into(),
        });
    }
    if seps.iter().any(|s| INTERNAL_HEADER + sep_len(&s.key) > PAYLOAD_LEN) {
        return Err(Error::Limit("btree separator exceeds page".into()));
    }
    if seps.len() < 2 {
        return Err(Error::Corrupt {
            detail: "internal node does not need a split".into(),
        });
    }
    if let Some(split) = promote_if_fits(leftmost, &seps, seps.len() / 2) {
        return Ok(split);
    }
    let target = seps.len() / 2;
    let mut best: Option<usize> = None;
    for i in 0..seps.len() {
        if sides_fit(&seps, i) {
            best = Some(match best {
                Some(prev) if prev.abs_diff(target) <= i.abs_diff(target) => prev,
                _ => i,
            });
        }
    }
    if let Some(i) = best {
        return Ok(promote_at(leftmost, &seps, i));
    }
    let mut pages = Vec::new();
    let mut promoted = Vec::new();
    let mut left = leftmost;
    let mut rest = seps.as_slice();
    while !internal_fits(rest) {
        if rest.len() < 2 || !internal_fits(&rest[..1]) {
            return Err(Error::Limit("btree separator exceeds page".into()));
        }
        let mut k = 1;
        while k < rest.len() - 1 && internal_fits(&rest[..k + 1]) {
            k += 1;
        }
        pages.push(InternalNode {
            leftmost: left,
            seps: rest[..k].to_vec(),
        });
        promoted.push(rest[k].key.clone());
        left = rest[k].child;
        rest = &rest[k + 1..];
    }
    pages.push(InternalNode {
        leftmost: left,
        seps: rest.to_vec(),
    });
    Ok((pages, promoted))
}

fn sides_fit(seps: &[Separator], promote: usize) -> bool {
    promote < seps.len()
        && internal_fits(&seps[..promote])
        && internal_fits(&seps[promote + 1..])
}

fn promote_if_fits(
    leftmost: PageId,
    seps: &[Separator],
    promote: usize,
) -> Option<(Vec<InternalNode>, Vec<Vec<u8>>)> {
    if sides_fit(seps, promote) {
        Some(promote_at(leftmost, seps, promote))
    } else {
        None
    }
}

fn promote_at(
    leftmost: PageId,
    seps: &[Separator],
    promote: usize,
) -> (Vec<InternalNode>, Vec<Vec<u8>>) {
    let left = InternalNode {
        leftmost,
        seps: seps[..promote].to_vec(),
    };
    let right = InternalNode {
        leftmost: seps[promote].child,
        seps: seps[promote + 1..].to_vec(),
    };
    (vec![left, right], vec![seps[promote].key.clone()])
}

fn entry_len(key: &[u8], value: &[u8]) -> usize {
    2 + key.len() + 2 + value.len()
}

fn leaf_page_len(entries: &[(Vec<u8>, Vec<u8>)]) -> usize {
    LEAF_HEADER + entries.iter().map(|(k, v)| entry_len(k, v)).sum::<usize>()
}

fn leaf_fits(entries: &[(Vec<u8>, Vec<u8>)]) -> bool {
    leaf_page_len(entries) <= PAYLOAD_LEN
}

fn sep_len(key: &[u8]) -> usize {
    2 + key.len() + 4
}

fn internal_page_len(seps: &[Separator]) -> usize {
    INTERNAL_HEADER + seps.iter().map(|s| sep_len(&s.key)).sum::<usize>()
}

fn internal_fits(seps: &[Separator]) -> bool {
    internal_page_len(seps) <= PAYLOAD_LEN
}

fn rewrite_leaf(
    pool: &BufferPool,
    id: PageId,
    entries: &[(Vec<u8>, Vec<u8>)],
    next: u32,
) -> Result<()> {
    let encoded = encode_leaf(entries)?;
    let pin = pool.pin(id)?;
    pin.modify(move |page| install(page, &encoded, PageKind::BtreeLeaf, next, 0))?;
    drop(pin);
    Ok(())
}

fn rewrite_internal(
    pool: &BufferPool,
    id: PageId,
    leftmost: PageId,
    seps: &[Separator],
) -> Result<()> {
    let encoded = encode_internal(leftmost, seps)?;
    let pin = pool.pin(id)?;
    pin.modify(move |page| install(page, &encoded, PageKind::BtreeInternal, 0, leftmost.0))?;
    drop(pin);
    Ok(())
}

fn install(page: &mut Page, encoded: &[u8], kind: PageKind, next: u32, extra: u32) -> Result<()> {
    let id = page.id().0;
    {
        let payload = page.payload_mut();
        if encoded.len() > payload.len() {
            return Err(Error::Corrupt {
                detail: format!("btree encoding exceeds payload on page {id}"),
            });
        }
        payload.fill(0);
        payload[..encoded.len()].copy_from_slice(encoded);
    }
    page.set_kind(kind);
    page.set_next(next);
    page.set_extra(extra);
    Ok(())
}

fn encode_leaf(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<u8>> {
    ensure_strictly_increasing(entries.iter().map(|(k, _)| k.as_slice()), "leaf")?;
    let len = leaf_page_len(entries);
    if len > PAYLOAD_LEN {
        return Err(Error::Limit("btree leaf page overflow".into()));
    }
    let count = u16::try_from(entries.len()).map_err(|_| {
        Error::Limit(format!("too many leaf entries ({})", entries.len()))
    })?;
    let mut buf = vec![0u8; len];
    put_u16(&mut buf, 0, count)?;
    let mut at = LEAF_HEADER;
    for (k, v) in entries {
        let klen = u16::try_from(k.len()).map_err(|_| Error::Limit("key exceeds u16".into()))?;
        let vlen = u16::try_from(v.len()).map_err(|_| Error::Limit("value exceeds u16".into()))?;
        put_u16(&mut buf, at, klen)?;
        at += 2;
        put_bytes(&mut buf, at, k)?;
        at += k.len();
        put_u16(&mut buf, at, vlen)?;
        at += 2;
        put_bytes(&mut buf, at, v)?;
        at += v.len();
    }
    if at != len {
        return Err(Error::Corrupt {
            detail: "leaf encoder length mismatch".into(),
        });
    }
    Ok(buf)
}

fn encode_internal(leftmost: PageId, seps: &[Separator]) -> Result<Vec<u8>> {
    if leftmost.0 == 0 {
        return Err(Error::Corrupt {
            detail: "btree leftmost is page 0".into(),
        });
    }
    ensure_strictly_increasing(seps.iter().map(|s| s.key.as_slice()), "internal")?;
    for sep in seps {
        if sep.child.0 == 0 {
            return Err(Error::Corrupt {
                detail: "btree child is page 0".into(),
            });
        }
    }
    let len = internal_page_len(seps);
    if len > PAYLOAD_LEN {
        return Err(Error::Limit("btree internal page overflow".into()));
    }
    let count = u16::try_from(seps.len()).map_err(|_| {
        Error::Limit(format!("too many separators ({})", seps.len()))
    })?;
    let mut buf = vec![0u8; len];
    put_u16(&mut buf, 0, count)?;
    put_u32(&mut buf, 2, leftmost.0)?;
    let mut at = INTERNAL_HEADER;
    for sep in seps {
        let klen = u16::try_from(sep.key.len()).map_err(|_| Error::Limit("key exceeds u16".into()))?;
        put_u16(&mut buf, at, klen)?;
        at += 2;
        put_bytes(&mut buf, at, &sep.key)?;
        at += sep.key.len();
        put_u32(&mut buf, at, sep.child.0)?;
        at += 4;
    }
    if at != len {
        return Err(Error::Corrupt {
            detail: "internal encoder length mismatch".into(),
        });
    }
    Ok(buf)
}

fn ensure_strictly_increasing<'a>(
    keys: impl IntoIterator<Item = &'a [u8]>,
    kind: &str,
) -> Result<()> {
    let mut prev: Option<&[u8]> = None;
    for key in keys {
        if let Some(prev) = prev {
            if key <= prev {
                return Err(Error::Corrupt {
                    detail: format!("refusing to write unsorted {kind} keys"),
                });
            }
        }
        prev = Some(key);
    }
    Ok(())
}

fn put_u16(buf: &mut [u8], at: usize, v: u16) -> Result<()> {
    let end = at.checked_add(2).ok_or_else(|| Error::Corrupt {
        detail: "btree encode overflow".into(),
    })?;
    if end > buf.len() {
        return Err(Error::Corrupt {
            detail: "btree encode overflow".into(),
        });
    }
    write_u16(buf, at, v);
    Ok(())
}

fn put_u32(buf: &mut [u8], at: usize, v: u32) -> Result<()> {
    let end = at.checked_add(4).ok_or_else(|| Error::Corrupt {
        detail: "btree encode overflow".into(),
    })?;
    if end > buf.len() {
        return Err(Error::Corrupt {
            detail: "btree encode overflow".into(),
        });
    }
    write_u32(buf, at, v);
    Ok(())
}

fn put_bytes(buf: &mut [u8], at: usize, bytes: &[u8]) -> Result<()> {
    let end = at.checked_add(bytes.len()).ok_or_else(|| Error::Corrupt {
        detail: "btree encode overflow".into(),
    })?;
    if end > buf.len() {
        return Err(Error::Corrupt {
            detail: "btree encode overflow".into(),
        });
    }
    buf[at..end].copy_from_slice(bytes);
    Ok(())
}

fn decode_leaf(page: &Page) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if page.kind()? != PageKind::BtreeLeaf {
        return Err(Error::Corrupt {
            detail: format!("page {} is not a btree leaf", page.id().0),
        });
    }
    let id = page.id();
    let payload = page.payload();
    let count = read_u16(payload, 0)? as usize;
    let mut at = LEAF_HEADER;
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(count.min(PAYLOAD_LEN / 4));
    for i in 0..count {
        let klen = read_u16(payload, at)? as usize;
        at += 2;
        let key = take_bytes(payload, &mut at, klen, id, "leaf key", i)?;
        let vlen = read_u16(payload, at)? as usize;
        at += 2;
        let val = take_bytes(payload, &mut at, vlen, id, "leaf value", i)?;
        if let Some((prev, _)) = entries.last() {
            if key.as_slice() <= prev.as_slice() {
                return Err(Error::Corrupt {
                    detail: format!("unsorted leaf keys on page {}", id.0),
                });
            }
        }
        entries.push((key, val));
    }
    Ok(entries)
}

fn decode_internal(page: &Page) -> Result<(PageId, Vec<Separator>)> {
    if page.kind()? != PageKind::BtreeInternal {
        return Err(Error::Corrupt {
            detail: format!("page {} is not a btree internal node", page.id().0),
        });
    }
    let id = page.id();
    let payload = page.payload();
    let count = read_u16(payload, 0)? as usize;
    let leftmost = PageId(read_u32(payload, 2)?);
    if leftmost.0 == 0 {
        return Err(Error::Corrupt {
            detail: format!("internal page {} has null leftmost", id.0),
        });
    }
    let mut at = INTERNAL_HEADER;
    let mut seps: Vec<Separator> = Vec::with_capacity(count.min(PAYLOAD_LEN / 6));
    for i in 0..count {
        let klen = read_u16(payload, at)? as usize;
        at += 2;
        let key = take_bytes(payload, &mut at, klen, id, "separator", i)?;
        let child = PageId(read_u32(payload, at)?);
        at += 4;
        if child.0 == 0 {
            return Err(Error::Corrupt {
                detail: format!("internal page {} has null child", id.0),
            });
        }
        if let Some(prev) = seps.last() {
            if key.as_slice() <= prev.key.as_slice() {
                return Err(Error::Corrupt {
                    detail: format!("unsorted separators on page {}", id.0),
                });
            }
        }
        seps.push(Separator { key, child });
    }
    Ok((leftmost, seps))
}

fn take_bytes(
    payload: &[u8],
    at: &mut usize,
    len: usize,
    page: PageId,
    what: &str,
    index: usize,
) -> Result<Vec<u8>> {
    let end = at.checked_add(len).ok_or_else(|| Error::Corrupt {
        detail: format!("{what} length overflow on page {}", page.0),
    })?;
    let bytes = payload.get(*at..end).ok_or_else(|| Error::Corrupt {
        detail: format!("{what} {index} truncated on page {}", page.0),
    })?;
    *at = end;
    Ok(bytes.to_vec())
}
