use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::types::{Page, PageId, PageKind, PAGE_SIZE};

pub fn pages_path(dir: &Path) -> PathBuf {
    dir.join("data.pages")
}

pub struct PageStore {
    file: File,
    page_count: u32,
    freelist_head: u32,
}

impl PageStore {
    pub fn create(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = pages_path(dir);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut store = Self {
            file,
            page_count: 0,
            freelist_head: u32::MAX,
        };
        let mut meta = Page::blank(PageId::META, PageKind::Meta);
        store.write_meta_fields(&mut meta);
        meta.seal();
        store.write_raw(&meta)?;
        store.page_count = 1;
        store.sync()?;
        Ok(store)
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let path = pages_path(dir);
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let len = file.metadata()?.len();
        if len % PAGE_SIZE as u64 != 0 || len < PAGE_SIZE as u64 {
            return Err(Error::Corrupt {
                detail: format!("page file length {len} is not a multiple of {PAGE_SIZE}"),
            });
        }
        let page_count = (len / PAGE_SIZE as u64) as u32;
        let mut store = Self {
            file,
            page_count,
            freelist_head: u32::MAX,
        };
        let meta = store.read(PageId::META)?;
        if meta.kind()? != PageKind::Meta {
            return Err(Error::Corrupt {
                detail: "page 0 is not meta".into(),
            });
        }
        store.freelist_head = read_freelist(&meta)?;
        Ok(store)
    }

    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    pub fn alloc(&mut self, kind: PageKind) -> Result<Page> {
        if self.freelist_head != u32::MAX {
            let id = PageId(self.freelist_head);
            let old = self.read(id)?;
            self.freelist_head = old.next();
            let mut page = Page::blank(id, kind);
            page.seal();
            self.write_raw(&page)?;
            self.persist_freelist()?;
            return Ok(page);
        }
        let id = PageId(self.page_count);
        let mut page = Page::blank(id, kind);
        page.seal();
        self.write_raw(&page)?;
        self.page_count += 1;
        Ok(page)
    }

    pub fn free(&mut self, id: PageId) -> Result<()> {
        if id.0 == 0 || id.0 >= self.page_count {
            return Err(Error::Corrupt {
                detail: format!("cannot free page {}", id.0),
            });
        }
        let mut page = Page::blank(id, PageKind::Freelist);
        page.set_next(self.freelist_head);
        page.seal();
        self.write_raw(&page)?;
        self.freelist_head = id.0;
        self.persist_freelist()?;
        Ok(())
    }

    pub fn read(&mut self, id: PageId) -> Result<Page> {
        if id.0 >= self.page_count {
            return Err(Error::Corrupt {
                detail: format!("page {} past end {}", id.0, self.page_count),
            });
        }
        let mut buf = [0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset(id)))?;
        self.file.read_exact(&mut buf)?;
        Page::from_sealed(buf)
    }

    pub fn write(&mut self, page: &Page) -> Result<()> {
        self.ensure_slot(page.id())?;
        self.write_raw(page)
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn ensure_slot(&mut self, id: PageId) -> Result<()> {
        while self.page_count <= id.0 {
            let blank_id = PageId(self.page_count);
            let mut blank = Page::blank(blank_id, PageKind::Freelist);
            blank.seal();
            self.write_raw(&blank)?;
            self.page_count += 1;
        }
        Ok(())
    }

    fn write_raw(&mut self, page: &Page) -> Result<()> {
        self.file.seek(SeekFrom::Start(offset(page.id())))?;
        self.file.write_all(page.as_bytes())?;
        Ok(())
    }

    fn persist_freelist(&mut self) -> Result<()> {
        let mut meta = self.read(PageId::META)?;
        self.write_meta_fields(&mut meta);
        meta.seal();
        self.write_raw(&meta)
    }

    fn write_meta_fields(&self, meta: &mut Page) {
        meta.payload_mut()[..4].copy_from_slice(&self.freelist_head.to_le_bytes());
    }
}

fn read_freelist(meta: &Page) -> Result<u32> {
    let p = meta.payload();
    if p.len() < 4 {
        return Err(Error::Corrupt {
            detail: "short meta".into(),
        });
    }
    Ok(u32::from_le_bytes(p[0..4].try_into().unwrap()))
}

fn offset(id: PageId) -> u64 {
    id.0 as u64 * PAGE_SIZE as u64
}
