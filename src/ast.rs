use crate::types::{ColumnDef, IndexMethod};

#[derive(Clone, Debug)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        method: IndexMethod,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    Query(Query),
    Begin,
    Commit,
    Rollback,
    Explain(Box<Statement>),
    Vacuum,
    FlushColumnar,
    Checkpoint,
    ShowTables,
    ShowStats,
    Describe {
        name: String,
    },
}

#[derive(Clone, Debug)]
pub struct Query {
    pub projection: Vec<SelectItem>,
    pub from: Option<From>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
}

#[derive(Clone, Debug)]
pub enum From {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<From>,
        right: Box<From>,
        kind: JoinKind,
        on: Expr,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Debug)]
pub enum SelectItem {
    Star {
        table: Option<String>,
    },
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub struct OrderItem {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Column {
        table: Option<String>,
        name: String,
    },
    Literal(crate::value::Value),
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    Func {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Plus,
    Minus,
    Mul,
    Div,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Clone, Debug)]
pub struct PlannedExplain {
    pub text: String,
}
