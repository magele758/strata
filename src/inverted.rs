//! BM25 inverted index persisted in buffer-pool pages.
//!
//! Root (`PageKind::Inverted`) holds collection stats and chain heads.
//! Dictionary pages (`Inverted`) and posting / document-length pages
//! (`Overflow`) are linked with `Page::next`. Every mutation is applied
//! through `Pin::modify` so an open engine mutation group logs it.
//!
//! Tokenization splits on non-alphanumeric characters, lowercases, and
//! drops empty tokens. Search is conjunctive: a query term with no
//! posting yields an empty result.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::buffer::BufferPool;
use crate::error::{Error, Result};
use crate::types::{
    read_u16, read_u32, read_u64, write_u16, write_u32, write_u64, Page, PageId, PageKind, Rid,
    PAYLOAD_LEN,
};

const MAGIC: u32 = 0x494E_5654;
const VERSION: u32 = 1;
const K1: f64 = 1.2;
const B: f64 = 0.75;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_DOCS: usize = 8;
const OFF_TOKENS: usize = 12;
const OFF_DOCLEN_HEAD: usize = 20;
const OFF_DOCLEN_TAIL: usize = 24;
const OFF_DICT_HEAD: usize = 28;
const OFF_DICT_TAIL: usize = 32;
const OFF_POST_HEAD: usize = 36;
const OFF_POST_TAIL: usize = 40;
const OFF_NEXT_TERM: usize = 44;

const DOCLEN_REC: usize = 10;
const POST_REC: usize = 14;
const CHAIN_LIMIT: u32 = 1_000_000;

pub struct InvertedIndex;

struct Meta {
    doc_count: u32,
    total_tokens: u64,
    doclen_head: u32,
    doclen_tail: u32,
    dict_head: u32,
    dict_tail: u32,
    post_head: u32,
    post_tail: u32,
    next_term_id: u32,
}

impl InvertedIndex {
    pub fn create(pool: &BufferPool) -> Result<PageId> {
        let id = pool.alloc(PageKind::Inverted)?;
        let mut page = read_page(pool, id)?;
        write_meta(&mut page, &Meta::empty())?;
        persist(pool, id, &page)?;
        Ok(id)
    }

    pub fn add(pool: &BufferPool, root: PageId, text: &str, rid: Rid) -> Result<()> {
        let tokens = tokenize(text);
        let dl = u32::try_from(tokens.len()).map_err(|_| limit("document exceeds u32 tokens"))?;
        let mut tf = BTreeMap::<String, u32>::new();
        for token in tokens {
            let count = tf.entry(token).or_insert(0);
            *count = count.checked_add(1).ok_or_else(|| limit("term frequency overflow"))?;
        }

        let mut page = read_page(pool, root)?;
        check_root(&page)?;
        let mut meta = read_meta(&page)?;
        let dict = load_dict(pool, meta.dict_head)?;

        let mut fresh = Vec::new();
        let mut posts = Vec::with_capacity(tf.len() * POST_REC);
        for (term, count) in &tf {
            let term_id = if let Some(id) = dict.get(term) {
                *id
            } else {
                let id = meta.next_term_id;
                meta.next_term_id = meta
                    .next_term_id
                    .checked_add(1)
                    .ok_or_else(|| limit("term id overflow"))?;
                fresh.push((id, term.clone()));
                id
            };
            posts.extend_from_slice(&encode_posting(term_id, rid, *count));
        }

        append_dict(pool, &mut meta.dict_head, &mut meta.dict_tail, &fresh)?;
        append_fixed(
            pool,
            &mut meta.post_head,
            &mut meta.post_tail,
            PageKind::Overflow,
            POST_REC,
            &posts,
        )?;
        append_fixed(
            pool,
            &mut meta.doclen_head,
            &mut meta.doclen_tail,
            PageKind::Overflow,
            DOCLEN_REC,
            &encode_doclen(rid, dl),
        )?;

        meta.doc_count = meta
            .doc_count
            .checked_add(1)
            .ok_or_else(|| limit("document count overflow"))?;
        meta.total_tokens = meta
            .total_tokens
            .checked_add(u64::from(dl))
            .ok_or_else(|| limit("token counter overflow"))?;
        write_meta(&mut page, &meta)?;
        persist(pool, root, &page)?;
        Ok(())
    }

    pub fn search(pool: &BufferPool, root: PageId, query: &str) -> Result<Vec<(Rid, f64)>> {
        let page = read_page(pool, root)?;
        check_root(&page)?;
        let meta = read_meta(&page)?;
        let terms = query_terms(query);
        if terms.is_empty() || meta.doc_count == 0 {
            return Ok(Vec::new());
        }

        let dict = load_dict(pool, meta.dict_head)?;
        let mut wanted = Vec::with_capacity(terms.len());
        for term in &terms {
            match dict.get(term) {
                Some(id) => wanted.push(*id),
                None => return Ok(Vec::new()),
            }
        }

        let doclens = load_doclens(pool, meta.doclen_head)?;
        let mut by_term: HashMap<u32, HashMap<Rid, u32>> = HashMap::new();
        for id in &wanted {
            by_term.insert(*id, HashMap::new());
        }
        for_each_page(pool, meta.post_head, |_id, page| {
            for (term_id, rid, tf) in read_postings(page)? {
                if let Some(slot) = by_term.get_mut(&term_id) {
                    let acc = slot.entry(rid).or_insert(0);
                    *acc = acc.saturating_add(tf);
                }
            }
            Ok(())
        })?;

        let n = f64::from(meta.doc_count);
        let avgdl = if meta.doc_count == 0 {
            0.0
        } else {
            meta.total_tokens as f64 / n
        };
        let mut acc: Option<HashMap<Rid, f64>> = None;
        for term_id in &wanted {
            let list = by_term.get(term_id).map(|m| m.as_slice_pairs()).unwrap_or_default();
            if list.is_empty() {
                return Ok(Vec::new());
            }
            let df = list.len() as f64;
            let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
            let mut scores = HashMap::with_capacity(list.len());
            for (rid, tf) in list {
                let dl = match doclens.get(&rid) {
                    Some(len) => f64::from(*len),
                    None => {
                        return Err(corrupt(format!(
                            "missing document length for rid {}:{}",
                            rid.page, rid.slot
                        )))
                    }
                };
                let norm = if avgdl > 0.0 { dl / avgdl } else { 0.0 };
                let tf = f64::from(tf);
                let denom = tf + K1 * (1.0 - B + B * norm);
                let score = idf * (tf * (K1 + 1.0)) / denom;
                scores.insert(rid, score);
            }
            acc = Some(match acc {
                None => scores,
                Some(prev) => prev
                    .into_iter()
                    .filter_map(|(rid, score)| scores.get(&rid).map(|more| (rid, score + *more)))
                    .collect(),
            });
            if acc.as_ref().is_some_and(|m| m.is_empty()) {
                return Ok(Vec::new());
            }
        }

        let mut out: Vec<(Rid, f64)> = acc.unwrap_or_default().into_iter().collect();
        out.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then(a.0.page.cmp(&b.0.page))
                .then(a.0.slot.cmp(&b.0.slot))
        });
        Ok(out)
    }
}

trait AsSlicePairs {
    fn as_slice_pairs(&self) -> Vec<(Rid, u32)>;
}

impl AsSlicePairs for HashMap<Rid, u32> {
    fn as_slice_pairs(&self) -> Vec<(Rid, u32)> {
        self.iter().map(|(rid, tf)| (*rid, *tf)).collect()
    }
}

impl Meta {
    fn empty() -> Self {
        Self {
            doc_count: 0,
            total_tokens: 0,
            doclen_head: 0,
            doclen_tail: 0,
            dict_head: 0,
            dict_tail: 0,
            post_head: 0,
            post_tail: 0,
            next_term_id: 1,
        }
    }
}

fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            for lower in ch.to_lowercase() {
                cur.push(lower);
            }
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn query_terms(text: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for term in tokenize(text) {
        if seen.insert(term.clone()) {
            out.push(term);
        }
    }
    out
}

fn check_root(page: &Page) -> Result<()> {
    let kind = page.kind()?;
    if kind != PageKind::Inverted {
        return Err(corrupt(format!("inverted root has kind {kind:?}")));
    }
    let magic = read_u32(page.payload(), OFF_MAGIC)?;
    if magic != MAGIC {
        return Err(corrupt(format!("bad inverted magic {magic:#x}")));
    }
    let version = read_u32(page.payload(), OFF_VERSION)?;
    if version != VERSION {
        return Err(corrupt(format!("unsupported inverted version {version}")));
    }
    Ok(())
}

fn read_meta(page: &Page) -> Result<Meta> {
    let p = page.payload();
    Ok(Meta {
        doc_count: read_u32(p, OFF_DOCS)?,
        total_tokens: read_u64(p, OFF_TOKENS)?,
        doclen_head: read_u32(p, OFF_DOCLEN_HEAD)?,
        doclen_tail: read_u32(p, OFF_DOCLEN_TAIL)?,
        dict_head: read_u32(p, OFF_DICT_HEAD)?,
        dict_tail: read_u32(p, OFF_DICT_TAIL)?,
        post_head: read_u32(p, OFF_POST_HEAD)?,
        post_tail: read_u32(p, OFF_POST_TAIL)?,
        next_term_id: read_u32(p, OFF_NEXT_TERM)?,
    })
}

fn write_meta(page: &mut Page, meta: &Meta) -> Result<()> {
    let p = page.payload_mut();
    write_u32(p, OFF_MAGIC, MAGIC);
    write_u32(p, OFF_VERSION, VERSION);
    write_u32(p, OFF_DOCS, meta.doc_count);
    write_u64(p, OFF_TOKENS, meta.total_tokens);
    write_u32(p, OFF_DOCLEN_HEAD, meta.doclen_head);
    write_u32(p, OFF_DOCLEN_TAIL, meta.doclen_tail);
    write_u32(p, OFF_DICT_HEAD, meta.dict_head);
    write_u32(p, OFF_DICT_TAIL, meta.dict_tail);
    write_u32(p, OFF_POST_HEAD, meta.post_head);
    write_u32(p, OFF_POST_TAIL, meta.post_tail);
    write_u32(p, OFF_NEXT_TERM, meta.next_term_id);
    Ok(())
}

fn encode_doclen(rid: Rid, len: u32) -> [u8; DOCLEN_REC] {
    let mut buf = [0u8; DOCLEN_REC];
    buf[..6].copy_from_slice(&rid.encode());
    buf[6..].copy_from_slice(&len.to_le_bytes());
    buf
}

fn encode_posting(term_id: u32, rid: Rid, tf: u32) -> [u8; POST_REC] {
    let mut buf = [0u8; POST_REC];
    buf[..4].copy_from_slice(&term_id.to_le_bytes());
    buf[4..10].copy_from_slice(&rid.encode());
    buf[10..].copy_from_slice(&tf.to_le_bytes());
    buf
}

fn read_postings(page: &Page) -> Result<Vec<(u32, Rid, u32)>> {
    let count = read_u16(page.payload(), 0)? as usize;
    let cap = (PAYLOAD_LEN - 4) / POST_REC;
    if count > cap {
        return Err(corrupt("posting page overfull"));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let at = 4 + i * POST_REC;
        let term_id = read_u32(page.payload(), at)?;
        let rid = Rid::decode(&page.payload()[at + 4..at + 10])?;
        let tf = read_u32(page.payload(), at + 10)?;
        out.push((term_id, rid, tf));
    }
    Ok(out)
}

fn load_doclens(pool: &BufferPool, head: u32) -> Result<HashMap<Rid, u32>> {
    let mut out = HashMap::new();
    for_each_page(pool, head, |_id, page| {
        let count = read_u16(page.payload(), 0)? as usize;
        let cap = (PAYLOAD_LEN - 4) / DOCLEN_REC;
        if count > cap {
            return Err(corrupt("doclen page overfull"));
        }
        for i in 0..count {
            let at = 4 + i * DOCLEN_REC;
            let rid = Rid::decode(&page.payload()[at..at + 6])?;
            let len = read_u32(page.payload(), at + 6)?;
            out.insert(rid, len);
        }
        Ok(())
    })?;
    Ok(out)
}

fn load_dict(pool: &BufferPool, head: u32) -> Result<HashMap<String, u32>> {
    let mut out = HashMap::new();
    for_each_page(pool, head, |_id, page| {
        let count = read_u16(page.payload(), 0)? as usize;
        let end = dict_end(page)?;
        let mut at = 4usize;
        for _ in 0..count {
            if at + 6 > end {
                return Err(corrupt("dictionary entry truncated"));
            }
            let term_id = read_u32(page.payload(), at)?;
            let len = read_u16(page.payload(), at + 4)? as usize;
            at += 6;
            if at + len > end {
                return Err(corrupt("dictionary term truncated"));
            }
            let bytes = &page.payload()[at..at + len];
            let term = std::str::from_utf8(bytes)
                .map_err(|_| corrupt("dictionary term is not utf-8"))?
                .to_string();
            at += len;
            out.insert(term, term_id);
        }
        if at != end {
            return Err(corrupt("dictionary page end mismatch"));
        }
        Ok(())
    })?;
    Ok(out)
}

fn dict_end(page: &Page) -> Result<usize> {
    let end = read_u16(page.payload(), 2)? as usize;
    if end < 4 {
        Ok(4)
    } else if end > PAYLOAD_LEN {
        Err(corrupt("dictionary end past payload"))
    } else {
        Ok(end)
    }
}

fn append_dict(
    pool: &BufferPool,
    head: &mut u32,
    tail: &mut u32,
    entries: &[(u32, String)],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    for (_, term) in entries {
        if term.len() > u16::MAX as usize || 6 + term.len() > PAYLOAD_LEN - 4 {
            return Err(limit(format!(
                "term longer than a page: {} bytes",
                term.len()
            )));
        }
    }
    ensure_chain(pool, head, tail, PageKind::Inverted)?;
    let mut rest = entries;
    while !rest.is_empty() {
        let mut page = read_page(pool, PageId(*tail))?;
        let mut count = read_u16(page.payload(), 0)? as usize;
        let mut end = dict_end(&page)?;
        let mut placed = 0usize;
        for (term_id, term) in rest.iter() {
            let need = 6 + term.len();
            if end + need > PAYLOAD_LEN {
                break;
            }
            {
                let payload = page.payload_mut();
                write_u32(payload, end, *term_id);
                write_u16(payload, end + 4, term.len() as u16);
                payload[end + 6..end + need].copy_from_slice(term.as_bytes());
            }
            end += need;
            count += 1;
            placed += 1;
        }
        if placed == 0 {
            let id = pool.alloc(PageKind::Inverted)?;
            page.set_next(id.0);
            persist(pool, PageId(*tail), &page)?;
            *tail = id.0;
            continue;
        }
        {
            let payload = page.payload_mut();
            write_u16(payload, 0, count as u16);
            write_u16(payload, 2, end as u16);
        }
        let more = placed < rest.len();
        if more {
            let id = pool.alloc(PageKind::Inverted)?;
            page.set_next(id.0);
            persist(pool, PageId(*tail), &page)?;
            *tail = id.0;
        } else {
            persist(pool, PageId(*tail), &page)?;
        }
        rest = &rest[placed..];
    }
    Ok(())
}

fn append_fixed(
    pool: &BufferPool,
    head: &mut u32,
    tail: &mut u32,
    kind: PageKind,
    record: usize,
    mut bytes: &[u8],
) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    if record == 0 || bytes.len() % record != 0 {
        return Err(corrupt("bad fixed-record span"));
    }
    let cap = (PAYLOAD_LEN - 4) / record;
    if cap == 0 || cap > u16::MAX as usize {
        return Err(limit("record does not fit in a page"));
    }
    ensure_chain(pool, head, tail, kind)?;
    while !bytes.is_empty() {
        let mut page = read_page(pool, PageId(*tail))?;
        let count = read_u16(page.payload(), 0)? as usize;
        if count > cap {
            return Err(corrupt("fixed-record page overfull"));
        }
        let space = cap - count;
        if space == 0 {
            let id = pool.alloc(kind)?;
            page.set_next(id.0);
            persist(pool, PageId(*tail), &page)?;
            *tail = id.0;
            continue;
        }
        let n = space.min(bytes.len() / record);
        let take = n * record;
        let dest = 4 + count * record;
        {
            let payload = page.payload_mut();
            payload[dest..dest + take].copy_from_slice(&bytes[..take]);
            write_u16(payload, 0, (count + n) as u16);
        }
        persist(pool, PageId(*tail), &page)?;
        bytes = &bytes[take..];
    }
    Ok(())
}

fn ensure_chain(pool: &BufferPool, head: &mut u32, tail: &mut u32, kind: PageKind) -> Result<()> {
    if *head == 0 {
        let id = pool.alloc(kind)?;
        *head = id.0;
        *tail = id.0;
        return Ok(());
    }
    *tail = chain_end(pool, if *tail == 0 { *head } else { *tail })?;
    Ok(())
}

fn chain_end(pool: &BufferPool, start: u32) -> Result<u32> {
    let mut id = start;
    let mut hops = 0u32;
    loop {
        let page = read_page(pool, PageId(id))?;
        let next = page.next();
        if next == 0 {
            return Ok(id);
        }
        hops += 1;
        if hops > CHAIN_LIMIT {
            return Err(corrupt("page chain cycle"));
        }
        id = next;
    }
}

fn for_each_page(
    pool: &BufferPool,
    head: u32,
    mut f: impl FnMut(u32, &Page) -> Result<()>,
) -> Result<()> {
    let mut id = head;
    let mut hops = 0u32;
    while id != 0 {
        hops += 1;
        if hops > CHAIN_LIMIT {
            return Err(corrupt("page chain cycle"));
        }
        let page = read_page(pool, PageId(id))?;
        let next = page.next();
        f(id, &page)?;
        id = next;
    }
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

fn limit(detail: impl Into<String>) -> Error {
    Error::Limit(detail.into())
}
