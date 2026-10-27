use crate::error::Result;
use crate::types::{Catalog, ColumnDef, IndexDef, IndexMethod, Rid};
use crate::value::{DbStats, Value};

#[derive(Clone, Debug)]
pub struct Row {
    pub rid: Rid,
    pub values: Vec<Value>,
}

/// Object-safe storage surface. SQL never touches pages.
pub trait Store {
    fn catalog(&self) -> Catalog;
    fn begin(&self, session: &mut crate::types::Session) -> Result<()>;
    fn commit(&self, session: &mut crate::types::Session) -> Result<()>;
    fn rollback(&self, session: &mut crate::types::Session) -> Result<()>;
    fn scan_table(&self, table: &str, snapshot: u64, own: Option<u64>) -> Result<Vec<Row>>;
    fn index_eq(
        &self,
        table: &str,
        index: &str,
        key: &[Value],
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<Row>>;
    fn index_range(
        &self,
        table: &str,
        index: &str,
        start: &[Value],
        end_exclusive: Option<Vec<Value>>,
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<Row>>;
    fn search_text(
        &self,
        table: &str,
        index: &str,
        query: &str,
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<(Row, f64)>>;
    fn search_knn(
        &self,
        table: &str,
        index: &str,
        vector: &[f32],
        k: usize,
        snapshot: u64,
        own: Option<u64>,
    ) -> Result<Vec<(Row, f32)>>;
    fn scan_columnar(
        &self,
        table: &str,
        snapshot: u64,
        own_writes: bool,
    ) -> Result<Option<Vec<Row>>>;
    fn insert_row(&self, table: &str, values: &[Value], txn: u64) -> Result<()>;
    fn delete_rids(
        &self,
        table: &str,
        rids: &[Rid],
        txn: u64,
        snapshot: u64,
    ) -> Result<u64>;
    fn update_rids(
        &self,
        table: &str,
        rids: &[Rid],
        values: &[Vec<Value>],
        txn: u64,
        snapshot: u64,
    ) -> Result<u64>;
    fn create_table(&self, name: &str, columns: &[ColumnDef], if_not_exists: bool) -> Result<()>;
    fn drop_table(&self, name: &str, if_exists: bool) -> Result<()>;
    fn create_index(
        &self,
        name: &str,
        table: &str,
        columns: &[String],
        method: IndexMethod,
        if_not_exists: bool,
    ) -> Result<()>;
    fn drop_index(&self, name: &str, if_exists: bool) -> Result<()>;
    fn vacuum(&self) -> Result<()>;
    fn flush_columnar(&self) -> Result<u64>;
    fn checkpoint(&self) -> Result<()>;
    fn stats(&self) -> Result<DbStats>;
    fn indexes(&self, table: &str) -> Result<Vec<IndexDef>>;
}
