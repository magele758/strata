//! Slotted heap: a linked list of `PageKind::Heap` pages.
//!
//! Payload directory (little-endian):
//!   0..2 slot_count, 2..4 free_start (initial 8), 4..6 free_end (initial
//!   PAYLOAD_LEN), 6..8 flags.
//! Tuples grow up from offset 8. The slot directory grows down from
//! `free_end`. Slot `i` is at `PAYLOAD_LEN - (i + 1) * 4` and stores
//! `u16 offset` then `u16 len`. Offset 0 and len 0 is a reusable hole.
//!
//! A tuple longer than `PAYLOAD_LEN - 16` keeps its 32-byte header in the
//! slot plus a 12-byte overflow stub. Bytes after that header live on a
//! `PageKind::Overflow` chain. `read` and `scan` reassemble; `write_header_u64`
//! patches only the in-slot header and does not follow overflow.

use std::collections::HashSet;

use crate::buffer::BufferPool;
use crate::error::{Error, Result};
use crate::types::{
    read_u16, read_u32, write_u16, write_u32, write_u64, Page, PageId, PageKind, Rid, MAX_TUPLE,
    PAYLOAD_LEN,
};

const DIR_LEN: usize = 8;
const SLOT_LEN: usize = 4;
const TUPLE_HEADER: usize = 32;
const OVERFLOW_STUB: usize = 12;
const OVERFLOW_SLOT: usize = TUPLE_HEADER + OVERFLOW_STUB;
const OVERFLOW_MAGIC: u32 = 0x4F56_4552;
const INLINE_LIMIT: usize = PAYLOAD_LEN - 16;
const OVERFLOW_DATA: usize = PAYLOAD_LEN - 4;

pub struct Heap;

impl Heap {
    pub fn create(pool: &crate::buffer::BufferPool) -> crate::error::Result<crate::types::PageId> {
        create(pool)
    }

    pub fn insert(
        pool: &crate::buffer::BufferPool,
        root: crate::types::PageId,
        tuple: &[u8],
    ) -> crate::error::Result<crate::types::Rid> {
        insert(pool, root, tuple)
    }

    pub fn read(
        pool: &crate::buffer::BufferPool,
        rid: crate::types::Rid,
    ) -> crate::error::Result<Vec<u8>> {
        read(pool, rid)
    }

    pub fn write_header_u64(
        pool: &crate::buffer::BufferPool,
        rid: crate::types::Rid,
        offset: usize,
        value: u64,
    ) -> crate::error::Result<()> {
        write_header_u64(pool, rid, offset, value)
    }

    pub fn scan(
        pool: &crate::buffer::BufferPool,
        root: crate::types::PageId,
    ) -> crate::error::Result<Vec<(crate::types::Rid, Vec<u8>)>> {
        scan(pool, root)
    }
}

pub fn create(pool: &BufferPool) -> Result<PageId> {
    let id = pool.alloc(PageKind::Heap)?;
    let pin = pool.pin(id)?;
    pin.modify(|page| {
        init_heap(page);
        Ok(())
    })?;
    Ok(id)
}

pub fn insert(pool: &BufferPool, root: PageId, tuple: &[u8]) -> Result<Rid> {
    if tuple.is_empty() {
        return Err(Error::Corrupt {
            detail: "empty tuple".into(),
        });
    }
    if tuple.len() > MAX_TUPLE {
        return Err(Error::Limit(format!(
            "tuple exceeds {MAX_TUPLE} bytes"
        )));
    }
    let overflow = tuple.len() > INLINE_LIMIT;
    if overflow && tuple.len() < TUPLE_HEADER {
        return Err(Error::Corrupt {
            detail: "overflow tuple shorter than header".into(),
        });
    }
    let item_len = if overflow { OVERFLOW_SLOT } else { tuple.len() };
    let spot = find_spot(pool, root, item_len)?;
    if overflow {
        let body = &tuple[TUPLE_HEADER..];
        if body.len() > u32::MAX as usize {
            return Err(Error::Limit("overflow body exceeds u32".into()));
        }
        let head = write_overflow(pool, body)?;
        let item = overflow_slot(&tuple[..TUPLE_HEADER], head, body.len() as u32);
        commit_insert(pool, spot, &item)
    } else {
        commit_insert(pool, spot, tuple)
    }
}

pub fn read(pool: &BufferPool, rid: Rid) -> Result<Vec<u8>> {
    let pin = pool.pin(PageId(rid.page))?;
    let page = pin.read()?;
    let bytes = live_slot_bytes(&page, rid.slot)?.to_vec();
    drop(page);
    drop(pin);
    reassemble(pool, &bytes)
}

pub fn write_header_u64(pool: &BufferPool, rid: Rid, offset: usize, value: u64) -> Result<()> {
    if !matches!(offset, 0 | 8 | 16 | 24) {
        return Err(Error::Corrupt {
            detail: format!("tuple header offset {offset} is not 0, 8, 16, or 24"),
        });
    }
    let pin = pool.pin(PageId(rid.page))?;
    let page = pin.read()?;
    let (off, len) = live_slot_range(&page, rid.slot)?;
    if (len as usize) < TUPLE_HEADER || offset + 8 > len as usize {
        return Err(Error::Corrupt {
            detail: "tuple slot has no 32-byte header".into(),
        });
    }
    let at = off as usize + offset;
    drop(page);
    // Patch the in-slot header only. Overflow bodies are not visited.
    pin.modify(|page| {
        let payload = page.payload_mut();
        let (off2, len2) = read_slot(payload, rid.slot)?;
        if off2 != off || len2 != len || at + 8 > payload.len() {
            return Err(Error::Corrupt {
                detail: "tuple slot moved".into(),
            });
        }
        write_u64(payload, at, value);
        Ok(())
    })
}

pub fn scan(pool: &BufferPool, root: PageId) -> Result<Vec<(Rid, Vec<u8>)>> {
    if root.is_none() {
        return Err(Error::Corrupt {
            detail: "heap root is page 0".into(),
        });
    }
    let mut out = Vec::new();
    let mut id = root;
    let mut seen = HashSet::new();
    while !id.is_none() {
        if !seen.insert(id.0) {
            return Err(Error::Corrupt {
                detail: "heap page cycle".into(),
            });
        }
        let pin = pool.pin(id)?;
        let page = pin.read()?;
        require_heap(&page)?;
        let next = PageId(page.next());
        let payload = page.payload();
        let dir = read_dir(payload)?;
        let mut raw = Vec::new();
        for slot in 0..dir.slot_count {
            let (off, len) = read_slot(payload, slot)?;
            if len == 0 {
                continue;
            }
            raw.push((slot, slot_bytes(payload, dir.free_end, off, len)?.to_vec()));
        }
        drop(page);
        drop(pin);
        for (slot, bytes) in raw {
            let tuple = reassemble(pool, &bytes)?;
            out.push((Rid { page: id.0, slot }, tuple));
        }
        id = next;
    }
    Ok(out)
}

struct Dir {
    slot_count: u16,
    free_start: u16,
    free_end: u16,
}

enum Spot {
    Page { id: PageId, slot: u16, is_new: bool },
    Extend { tail: PageId },
}

fn init_heap(page: &mut Page) {
    page.set_kind(PageKind::Heap);
    page.set_next(0);
    page.set_extra(0);
    let payload = page.payload_mut();
    payload.fill(0);
    write_u16(payload, 0, 0);
    write_u16(payload, 2, DIR_LEN as u16);
    write_u16(payload, 4, PAYLOAD_LEN as u16);
    write_u16(payload, 6, 0);
}

fn find_spot(pool: &BufferPool, root: PageId, item_len: usize) -> Result<Spot> {
    if root.is_none() {
        return Err(Error::Corrupt {
            detail: "heap root is page 0".into(),
        });
    }
    let mut id = root;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(id.0) {
            return Err(Error::Corrupt {
                detail: "heap page cycle".into(),
            });
        }
        let pin = pool.pin(id)?;
        let page = pin.read()?;
        require_heap(&page)?;
        let next = page.next();
        let payload = page.payload();
        let dir = read_dir(payload)?;
        let hole = find_hole(payload, dir.slot_count)?;
        let slot_count = dir.slot_count;
        let can_reuse = hole.filter(|_| fits(&dir, item_len, false));
        let can_append = fits(&dir, item_len, true);
        drop(page);
        drop(pin);
        if let Some(slot) = can_reuse {
            return Ok(Spot::Page {
                id,
                slot,
                is_new: false,
            });
        }
        if can_append {
            return Ok(Spot::Page {
                id,
                slot: slot_count,
                is_new: true,
            });
        }
        if next == 0 {
            return Ok(Spot::Extend { tail: id });
        }
        id = PageId(next);
    }
}

fn commit_insert(pool: &BufferPool, spot: Spot, item: &[u8]) -> Result<Rid> {
    match spot {
        Spot::Page { id, slot, is_new } => {
            let pin = pool.pin(id)?;
            pin.modify(|page| place_item(page, slot, is_new, item))?;
            Ok(Rid { page: id.0, slot })
        }
        Spot::Extend { tail } => {
            let id = pool.alloc(PageKind::Heap)?;
            let pin = pool.pin(id)?;
            pin.modify(|page| {
                init_heap(page);
                place_item(page, 0, true, item)?;
                Ok(())
            })?;
            drop(pin);
            link_tail(pool, tail, id)?;
            Ok(Rid { page: id.0, slot: 0 })
        }
    }
}

fn link_tail(pool: &BufferPool, tail: PageId, next: PageId) -> Result<()> {
    let pin = pool.pin(tail)?;
    pin.modify(|page| {
        require_heap(page)?;
        if page.next() != 0 {
            return Err(Error::Corrupt {
                detail: "heap tail already linked".into(),
            });
        }
        page.set_next(next.0);
        Ok(())
    })
}

fn place_item(page: &mut Page, slot: u16, is_new: bool, item: &[u8]) -> Result<()> {
    if item.is_empty() || item.len() > u16::MAX as usize {
        return Err(Error::Corrupt {
            detail: "tuple does not fit in a heap slot".into(),
        });
    }
    let item_len = item.len() as u16;
    let payload = page.payload_mut();
    let dir = read_dir(payload)?;
    if is_new {
        if slot != dir.slot_count || !fits(&dir, item.len(), true) {
            return Err(Error::Corrupt {
                detail: "heap page has no room for a new slot".into(),
            });
        }
    } else {
        if slot >= dir.slot_count || !fits(&dir, item.len(), false) {
            return Err(Error::Corrupt {
                detail: "heap page has no room to reuse a slot".into(),
            });
        }
        let (off, len) = read_slot(payload, slot)?;
        if off != 0 || len != 0 {
            return Err(Error::Corrupt {
                detail: "refusing to overwrite a live heap slot".into(),
            });
        }
    }
    let start = dir.free_start as usize;
    let end = start + item.len();
    let new_free_start = dir
        .free_start
        .checked_add(item_len)
        .ok_or_else(|| Error::Corrupt {
            detail: "heap free_start overflow".into(),
        })?;
    let pos = slot_position(slot)?;
    let new_count = if is_new {
        Some(dir.slot_count.checked_add(1).ok_or_else(|| Error::Corrupt {
            detail: "heap slot count overflow".into(),
        })?)
    } else {
        None
    };
    if end > payload.len() || pos + SLOT_LEN > payload.len() || (is_new && end > pos) {
        return Err(Error::Corrupt {
            detail: "heap slot write exceeds page".into(),
        });
    }
    payload[start..end].copy_from_slice(item);
    write_u16(payload, pos, dir.free_start);
    write_u16(payload, pos + 2, item_len);
    if let Some(count) = new_count {
        write_u16(payload, 0, count);
        write_u16(payload, 4, pos as u16);
    }
    write_u16(payload, 2, new_free_start);
    Ok(())
}

fn write_overflow(pool: &BufferPool, body: &[u8]) -> Result<PageId> {
    if body.is_empty() {
        return Err(Error::Corrupt {
            detail: "empty overflow body".into(),
        });
    }
    let n_pages = (body.len() + OVERFLOW_DATA - 1) / OVERFLOW_DATA;
    let mut ids = Vec::with_capacity(n_pages);
    for _ in 0..n_pages {
        ids.push(pool.alloc(PageKind::Overflow)?);
    }
    for (i, id) in ids.iter().enumerate() {
        let start = i * OVERFLOW_DATA;
        let end = (start + OVERFLOW_DATA).min(body.len());
        let chunk = body[start..end].to_vec();
        let next = if i + 1 < ids.len() { ids[i + 1].0 } else { 0 };
        let pin = pool.pin(*id)?;
        pin.modify(move |page| {
            page.set_kind(PageKind::Overflow);
            page.set_next(next);
            page.set_extra(0);
            let payload = page.payload_mut();
            payload.fill(0);
            write_u32(payload, 0, chunk.len() as u32);
            payload[4..4 + chunk.len()].copy_from_slice(&chunk);
            Ok(())
        })?;
    }
    ids.first().copied().ok_or_else(|| Error::Corrupt {
        detail: "empty overflow chain".into(),
    })
}

fn overflow_slot(header: &[u8], head: PageId, remaining: u32) -> [u8; OVERFLOW_SLOT] {
    let mut rec = [0u8; OVERFLOW_SLOT];
    rec[..TUPLE_HEADER].copy_from_slice(header);
    write_u32(&mut rec, TUPLE_HEADER, OVERFLOW_MAGIC);
    write_u32(&mut rec, TUPLE_HEADER + 4, head.0);
    write_u32(&mut rec, TUPLE_HEADER + 8, remaining);
    rec
}

fn reassemble(pool: &BufferPool, item: &[u8]) -> Result<Vec<u8>> {
    if item.len() != OVERFLOW_SLOT {
        return Ok(item.to_vec());
    }
    let magic = read_u32(item, TUPLE_HEADER)?;
    if magic != OVERFLOW_MAGIC {
        return Ok(item.to_vec());
    }
    let head = PageId(read_u32(item, TUPLE_HEADER + 4)?);
    let remaining = read_u32(item, TUPLE_HEADER + 8)?;
    if head.is_none() {
        return Err(Error::Corrupt {
            detail: "overflow head is page 0".into(),
        });
    }
    let body = read_overflow(pool, head, remaining)?;
    let mut out = Vec::with_capacity(TUPLE_HEADER + body.len());
    out.extend_from_slice(&item[..TUPLE_HEADER]);
    out.extend_from_slice(&body);
    if out.len() > MAX_TUPLE {
        return Err(Error::Limit(format!(
            "tuple exceeds {MAX_TUPLE} bytes"
        )));
    }
    Ok(out)
}

fn read_overflow(pool: &BufferPool, head: PageId, remaining: u32) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(remaining as usize);
    let mut id = head;
    let mut seen = HashSet::new();
    loop {
        if id.is_none() {
            break;
        }
        if !seen.insert(id.0) {
            return Err(Error::Corrupt {
                detail: "overflow page cycle".into(),
            });
        }
        let pin = pool.pin(id)?;
        let page = pin.read()?;
        if page.kind()? != PageKind::Overflow {
            return Err(Error::Corrupt {
                detail: format!("page {} is not an overflow page", id.0),
            });
        }
        let next = page.next();
        let payload = page.payload();
        let n = read_u32(payload, 0)? as usize;
        if n > OVERFLOW_DATA || 4 + n > payload.len() {
            return Err(Error::Corrupt {
                detail: "overflow length exceeds page".into(),
            });
        }
        if out.len().saturating_add(n) > remaining as usize {
            return Err(Error::Corrupt {
                detail: "overflow longer than declared".into(),
            });
        }
        out.extend_from_slice(&payload[4..4 + n]);
        drop(page);
        drop(pin);
        if next == 0 {
            break;
        }
        id = PageId(next);
    }
    if out.len() != remaining as usize {
        return Err(Error::Corrupt {
            detail: format!("overflow length {} != {remaining}", out.len()),
        });
    }
    Ok(out)
}

fn require_heap(page: &Page) -> Result<()> {
    if page.kind()? != PageKind::Heap {
        return Err(Error::Corrupt {
            detail: format!("page {} is not a heap page", page.id().0),
        });
    }
    Ok(())
}

fn live_slot_bytes(page: &Page, slot: u16) -> Result<&[u8]> {
    require_heap(page)?;
    let payload = page.payload();
    let dir = read_dir(payload)?;
    if slot >= dir.slot_count {
        return Err(Error::NotFound);
    }
    let (off, len) = read_slot(payload, slot)?;
    if len == 0 {
        return Err(Error::NotFound);
    }
    slot_bytes(payload, dir.free_end, off, len)
}

fn live_slot_range(page: &Page, slot: u16) -> Result<(u16, u16)> {
    require_heap(page)?;
    let payload = page.payload();
    let dir = read_dir(payload)?;
    if slot >= dir.slot_count {
        return Err(Error::NotFound);
    }
    let (off, len) = read_slot(payload, slot)?;
    if len == 0 {
        return Err(Error::NotFound);
    }
    let _ = slot_bytes(payload, dir.free_end, off, len)?;
    Ok((off, len))
}

fn read_dir(payload: &[u8]) -> Result<Dir> {
    if payload.len() < PAYLOAD_LEN {
        return Err(Error::Corrupt {
            detail: "short heap payload".into(),
        });
    }
    let slot_count = read_u16(payload, 0)?;
    let free_start = read_u16(payload, 2)?;
    let free_end = read_u16(payload, 4)?;
    if (free_start as usize) < DIR_LEN || (free_end as usize) > PAYLOAD_LEN || free_start > free_end
    {
        return Err(Error::Corrupt {
            detail: "bad heap directory".into(),
        });
    }
    let dir_bytes = (slot_count as usize)
        .checked_mul(SLOT_LEN)
        .ok_or_else(|| Error::Corrupt {
            detail: "slot count overflow".into(),
        })?;
    if DIR_LEN + dir_bytes > PAYLOAD_LEN {
        return Err(Error::Corrupt {
            detail: "slot directory exceeds page".into(),
        });
    }
    if free_end as usize != PAYLOAD_LEN - dir_bytes {
        return Err(Error::Corrupt {
            detail: "heap free_end does not match slot count".into(),
        });
    }
    Ok(Dir {
        slot_count,
        free_start,
        free_end,
    })
}

fn fits(dir: &Dir, item_len: usize, new_slot: bool) -> bool {
    if item_len == 0 || item_len > u16::MAX as usize {
        return false;
    }
    let mut need = item_len;
    if new_slot {
        if slot_position(dir.slot_count).is_err() {
            return false;
        }
        need += SLOT_LEN;
    }
    let start = dir.free_start as usize;
    let end = dir.free_end as usize;
    end >= start && end - start >= need
}

fn find_hole(payload: &[u8], slot_count: u16) -> Result<Option<u16>> {
    for slot in 0..slot_count {
        let (off, len) = read_slot(payload, slot)?;
        if off == 0 && len == 0 {
            return Ok(Some(slot));
        }
    }
    Ok(None)
}

fn read_slot(payload: &[u8], slot: u16) -> Result<(u16, u16)> {
    let pos = slot_position(slot)?;
    let off = read_u16(payload, pos)?;
    let len = read_u16(payload, pos + 2)?;
    Ok((off, len))
}

fn slot_position(slot: u16) -> Result<usize> {
    let slots_end = (slot as usize + 1)
        .checked_mul(SLOT_LEN)
        .ok_or_else(|| Error::Corrupt {
            detail: "slot index overflow".into(),
        })?;
    if slots_end > PAYLOAD_LEN - DIR_LEN {
        return Err(Error::Corrupt {
            detail: format!("slot {slot} past directory"),
        });
    }
    Ok(PAYLOAD_LEN - slots_end)
}

fn slot_bytes(payload: &[u8], free_end: u16, off: u16, len: u16) -> Result<&[u8]> {
    if len == 0 {
        return Err(Error::NotFound);
    }
    let start = off as usize;
    let end = start.checked_add(len as usize).ok_or_else(|| Error::Corrupt {
        detail: "slot length overflow".into(),
    })?;
    if start < DIR_LEN || end > free_end as usize || end > payload.len() {
        return Err(Error::Corrupt {
            detail: "slot points outside heap tuple space".into(),
        });
    }
    Ok(&payload[start..end])
}
