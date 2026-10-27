use crate::am::Store;
use crate::ast::{Expr, From, Query, SelectItem};
use crate::error::{Error, Result};
use crate::types::IndexMethod;
use crate::value::Value;
use crate::Session;

#[derive(Clone, Debug)]
pub enum Access {
    Seq,
    Columnar,
    Index { name: String, method: IndexMethod },
}

pub fn explain(store: &dyn Store, session: &Session, query: &Query) -> Result<String> {
    let mut lines = Vec::new();
    match &query.from {
        Some(from) => lines.push(access_line(store, session, from, query.selection.as_ref())?),
        None => lines.push("Project".into()),
    }
    if query.selection.is_some() {
        lines.push("Filter".into());
    }
    if has_aggregate(query) {
        lines.push("Aggregate".into());
    }
    if !query.order_by.is_empty() {
        lines.push("Sort".into());
    }
    if query.limit.is_some() {
        lines.push("Limit".into());
    }
    lines.push("Project".into());
    Ok(lines.join("\n"))
}

pub fn access_line(
    store: &dyn Store,
    session: &Session,
    from: &From,
    selection: Option<&Expr>,
) -> Result<String> {
    match from {
        From::Join { kind, .. } => Ok(format!(
            "{}",
            match kind {
                crate::ast::JoinKind::Inner => "NestedLoopJoin",
                crate::ast::JoinKind::Left => "NestedLoopJoin",
            }
        )),
        From::Table { name, .. } => {
            let access = choose_access(store, session, name, selection, false)?;
            Ok(match access {
                Access::Seq => "SeqScan".into(),
                Access::Columnar => "ColumnarScan".into(),
                Access::Index { .. } => "IndexScan".into(),
            })
        }
    }
}

pub fn choose_access(
    store: &dyn Store,
    session: &Session,
    table: &str,
    selection: Option<&Expr>,
    for_write: bool,
) -> Result<Access> {
    if let Some(expr) = selection {
        if let Some(access) = index_access(store, table, expr)? {
            return Ok(access);
        }
    }
    if !for_write {
        let snapshot = if session.txn_id.is_some() {
            session.snapshot_ts
        } else {
            store.catalog().next_commit_ts
        };
        if store.scan_columnar(table, snapshot, false)?.is_some() {
            return Ok(Access::Columnar);
        }
    }
    Ok(Access::Seq)
}

fn index_access(store: &dyn Store, table: &str, expr: &Expr) -> Result<Option<Access>> {
    let indexes = store.indexes(table)?;
    match expr {
        Expr::Func { name, args } if name == "search" && args.len() == 2 => {
            let col = column_name(&args[0])?;
            let found = indexes.into_iter().find(|i| {
                i.method == IndexMethod::Inverted && i.columns.iter().any(|c| c == &col)
            });
            Ok(found.map(|i| Access::Index {
                name: i.name,
                method: i.method,
            }))
        }
        Expr::Func { name, args } if name == "knn" && args.len() == 3 => {
            let col = column_name(&args[0])?;
            let found = indexes.into_iter().find(|i| {
                i.method == IndexMethod::Hnsw && i.columns.iter().any(|c| c == &col)
            });
            Ok(found.map(|i| Access::Index {
                name: i.name,
                method: i.method,
            }))
        }
        Expr::Binary { op, left, right } if matches!(op, crate::ast::BinOp::Eq | crate::ast::BinOp::Gt | crate::ast::BinOp::Ge | crate::ast::BinOp::Lt | crate::ast::BinOp::Le)
        => {
            let (col, _) = column_and_literal(left, right);
            let Some(col) = col else {
                return Ok(None);
            };
            let found = indexes.into_iter().find(|i| {
                i.method == IndexMethod::Btree && i.columns.first().is_some_and(|c| c == &col)
            });
            Ok(found.map(|i| Access::Index {
                name: i.name,
                method: i.method,
            }))
        }
        Expr::Binary { op, left, right } if matches!(op, crate::ast::BinOp::And) => {
            if let Some(access) = index_access(store, table, left)? {
                return Ok(Some(access));
            }
            index_access(store, table, right)
        }
        _ => Ok(None),
    }
}

pub fn column_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Column { name, .. } => Ok(name.clone()),
        _ => Err(Error::Plan("expected a column".into())),
    }
}

pub fn column_and_literal<'a>(left: &'a Expr, right: &'a Expr) -> (Option<String>, Option<&'a Value>) {
    match (left, right) {
        (Expr::Column { name, .. }, Expr::Literal(v)) => (Some(name.clone()), Some(v)),
        (Expr::Literal(v), Expr::Column { name, .. }) => (Some(name.clone()), Some(v)),
        _ => (None, None),
    }
}

pub fn has_aggregate(query: &Query) -> bool {
    query.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_has_agg(expr),
        SelectItem::Star { .. } => false,
    }) || !query.group_by.is_empty()
}

fn expr_has_agg(expr: &Expr) -> bool {
    match expr {
        Expr::Func { name, .. } if is_agg(name) => true,
        Expr::Func { args, .. } => args.iter().any(expr_has_agg),
        Expr::Binary { left, right, .. } => expr_has_agg(left) || expr_has_agg(right),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => expr_has_agg(expr),
        Expr::Like { expr, pattern, .. } => expr_has_agg(expr) || expr_has_agg(pattern),
        _ => false,
    }
}

pub fn is_agg(name: &str) -> bool {
    matches!(name, "count" | "sum" | "avg" | "min" | "max")
}
