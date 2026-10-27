use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::engine::Database;
use crate::error::{Error, Result};
use crate::page::PageStore;
use crate::types::{Page, PAGE_SIZE};
use crate::value::Config;
use crate::wal::{wal_path, Wal, WalKind, WalRecord};

const LSN_FILE: &str = "replica.lsn";

/// Copy every committed page image from the primary WAL onto `replica_dir`.
///
/// Returns the LSN of the last applied commit, or 0 when the primary has none.
pub fn catch_up(primary_dir: &Path, replica_dir: &Path) -> Result<u64> {
    if !replica_dir.exists() {
        let db = Database::create(replica_dir, Config::default())?;
        db.close()?;
    }

    let records = {
        let (_wal, records) = Wal::open(&wal_path(primary_dir))?;
        records
    };

    let committed: HashSet<u64> = records
        .iter()
        .filter(|rec| rec.kind == WalKind::Commit)
        .map(|rec| rec.txn_id)
        .collect();

    let mut pages = PageStore::open(replica_dir)?;
    for rec in &records {
        if rec.kind != WalKind::Mutation || !committed.contains(&rec.txn_id) {
            continue;
        }
        for image in &rec.pages {
            let Some(page) = sealed_page(&image.after) else {
                continue;
            };
            pages.write(&page)?;
        }
    }

    let catalog = catalog_bytes(&records, &committed);
    let wal_file = wal_path(replica_dir);
    if wal_file.exists() {
        std::fs::remove_file(&wal_file)?;
    }
    {
        let mut wal = Wal::create(&wal_file)?;
        wal.recycle_checkpoint(&catalog)?;
    }

    pages.sync()?;
    let lsn = last_commit_lsn(&records);
    write_lsn(replica_dir, lsn)?;
    Ok(lsn)
}

/// LSN recorded by the last successful [`catch_up`], or 0 if `replica.lsn` is absent.
pub fn last_applied_lsn(replica_dir: &Path) -> Result<u64> {
    let path = replica_dir.join(LSN_FILE);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    text.trim()
        .parse()
        .map_err(|_| Error::Corrupt {
            detail: format!("replica.lsn is not a decimal LSN: {text:?}"),
        })
}

fn sealed_page(bytes: &[u8]) -> Option<Page> {
    if bytes.len() != PAGE_SIZE {
        return None;
    }
    let mut raw = [0u8; PAGE_SIZE];
    raw.copy_from_slice(bytes);
    Page::from_sealed(raw).ok()
}

fn catalog_bytes(records: &[WalRecord], committed: &HashSet<u64>) -> Vec<u8> {
    let checkpoint_at = records
        .iter()
        .rposition(|rec| rec.kind == WalKind::Checkpoint);
    let mut catalog = checkpoint_at
        .map(|at| records[at].catalog_after.clone())
        .unwrap_or_default();
    let start = checkpoint_at.map(|at| at + 1).unwrap_or(0);
    for rec in &records[start..] {
        if rec.kind == WalKind::Mutation
            && committed.contains(&rec.txn_id)
            && !rec.catalog_after.is_empty()
        {
            catalog = rec.catalog_after.clone();
        }
    }
    catalog
}

fn last_commit_lsn(records: &[WalRecord]) -> u64 {
    records
        .iter()
        .rev()
        .find(|rec| rec.kind == WalKind::Commit)
        .map(|rec| rec.lsn)
        .unwrap_or(0)
}

fn write_lsn(replica_dir: &Path, lsn: u64) -> Result<()> {
    let path = replica_dir.join(LSN_FILE);
    let mut file = File::create(&path)?;
    writeln!(file, "{lsn}")?;
    file.sync_all()?;
    Ok(())
}
