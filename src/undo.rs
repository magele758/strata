//! Tuple-level undo. Never restores a page image.

use crate::buffer::BufferPool;
use crate::error::Result;
use crate::heap::Heap;
use crate::types::{LogicalOp, Rid};

/// Reverse `op` for `txn` if its stamp is still uncommitted (timestamp 0).
///
/// Idempotent: a missing slot, a foreign stamp, or an already-committed stamp
/// is a no-op. Insert undo clears only that slot. Delete undo clears only
/// `xmax_txn`. Update undoes the old delete, then the new insert.
pub fn undo_op(pool: &BufferPool, txn: u64, op: &LogicalOp) -> Result<()> {
    if !pending(pool, txn, op)? {
        return Ok(());
    }
    pool.begin_group()?;
    if let Err(err) = apply(pool, txn, op) {
        let _ = pool.abort_group();
        return Err(err);
    }
    pool.end_group(txn, &[], &[], Some(op))?;
    Ok(())
}

fn pending(pool: &BufferPool, txn: u64, op: &LogicalOp) -> Result<bool> {
    match op {
        LogicalOp::Insert { rid_page, rid_slot, .. } => {
            insert_pending(pool, txn, Rid { page: *rid_page, slot: *rid_slot })
        }
        LogicalOp::Delete { rid_page, rid_slot, .. } => {
            delete_pending(pool, txn, Rid { page: *rid_page, slot: *rid_slot })
        }
        LogicalOp::Update { old_page, old_slot, new_page, new_slot, .. } => Ok(
            delete_pending(pool, txn, Rid { page: *old_page, slot: *old_slot })?
                || insert_pending(pool, txn, Rid { page: *new_page, slot: *new_slot })?,
        ),
    }
}

fn apply(pool: &BufferPool, txn: u64, op: &LogicalOp) -> Result<()> {
    match op {
        LogicalOp::Insert { rid_page, rid_slot, .. } => {
            undo_insert(pool, txn, Rid { page: *rid_page, slot: *rid_slot })
        }
        LogicalOp::Delete { rid_page, rid_slot, .. } => {
            undo_delete(pool, txn, Rid { page: *rid_page, slot: *rid_slot })
        }
        LogicalOp::Update { old_page, old_slot, new_page, new_slot, .. } => {
            undo_delete(pool, txn, Rid { page: *old_page, slot: *old_slot })?;
            undo_insert(pool, txn, Rid { page: *new_page, slot: *new_slot })
        }
    }
}

fn undo_insert(pool: &BufferPool, txn: u64, rid: Rid) -> Result<()> {
    match Heap::header(pool, rid)? {
        Some((xmin_txn, xmin_ts, _, _)) if xmin_txn == txn && xmin_ts == 0 => {
            Heap::clear_slot(pool, rid)
        }
        _ => Ok(()),
    }
}

fn undo_delete(pool: &BufferPool, txn: u64, rid: Rid) -> Result<()> {
    match Heap::header(pool, rid)? {
        Some((_, _, xmax_txn, xmax_ts)) if xmax_txn == txn && xmax_ts == 0 => {
            Heap::write_header_u64(pool, rid, 16, 0)
        }
        _ => Ok(()),
    }
}

fn insert_pending(pool: &BufferPool, txn: u64, rid: Rid) -> Result<bool> {
    Ok(matches!(
        Heap::header(pool, rid)?,
        Some((xmin_txn, xmin_ts, _, _)) if xmin_txn == txn && xmin_ts == 0
    ))
}

fn delete_pending(pool: &BufferPool, txn: u64, rid: Rid) -> Result<bool> {
    Ok(matches!(
        Heap::header(pool, rid)?,
        Some((_, _, xmax_txn, xmax_ts)) if xmax_txn == txn && xmax_ts == 0
    ))
}
