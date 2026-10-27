use crate::am::{Row, Store};
use crate::ast::{
    BinOp, Expr, From, JoinKind, OrderItem, Query, SelectItem, Statement, UnaryOp,
};
use crate::error::{Error, Result};
use crate::planner::{self, Access};
use crate::types::{ColumnDef, DataType, IndexMethod, Rid};
use crate::value::{parse_vector, Value};
use crate::Session;
use crate::ExecuteResult;

struct Rec {
    table: String,
    names: Vec<String>,
    values: Vec<Value>,
    rid: Rid,
}

pub fn run_sql(store: &dyn Store, session: &mut Session, sql: &str) -> Result<ExecuteResult> {
    let stmts = crate::parser::parse(sql)?;
    if stmts.is_empty() {
        return Ok(exec_msg(0, "OK"));
    }
    let mut last = exec_msg(0, "OK");
    for stmt in stmts {
        last = exec_one(store, session, stmt)?;
    }
    Ok(last)
}

fn exec_one(store: &dyn Store, session: &mut Session, stmt: Statement) -> Result<ExecuteResult> {
    match stmt {
        Statement::Explain(inner) => explain(store, session, inner.as_ref()),
        Statement::Begin => {
            store.begin(session)?;
            Ok(exec_msg(0, "BEGIN"))
        }
        Statement::Commit => {
            store.commit(session)?;
            Ok(exec_msg(0, "COMMIT"))
        }
        Statement::Rollback => {
            store.rollback(session)?;
            Ok(exec_msg(0, "ROLLBACK"))
        }
        Statement::CreateTable { name, columns, if_not_exists } => {
            reject_open_txn(session)?;
            store.create_table(&name, &columns, if_not_exists)?;
            Ok(exec_msg(0, &format!("CREATE TABLE {name}")))
        }
        Statement::DropTable { name, if_exists } => {
            reject_open_txn(session)?;
            store.drop_table(&name, if_exists)?;
            Ok(exec_msg(0, &format!("DROP TABLE {name}")))
        }
        Statement::CreateIndex { name, table, columns, method, if_not_exists } => {
            reject_open_txn(session)?;
            store.create_index(&name, &table, &columns, method, if_not_exists)?;
            Ok(exec_msg(0, &format!("CREATE INDEX {name}")))
        }
        Statement::DropIndex { name, if_exists } => {
            reject_open_txn(session)?;
            store.drop_index(&name, if_exists)?;
            Ok(exec_msg(0, &format!("DROP INDEX {name}")))
        }
        Statement::Vacuum => {
            reject_open_txn(session)?;
            store.vacuum()?;
            Ok(exec_msg(0, "VACUUM"))
        }
        Statement::FlushColumnar => {
            reject_open_txn(session)?;
            store.flush_columnar()?;
            Ok(exec_msg(0, "FLUSH"))
        }
        Statement::Checkpoint => {
            reject_open_txn(session)?;
            store.checkpoint()?;
            Ok(exec_msg(0, "CHECKPOINT"))
        }
        Statement::ShowTables => {
            let mut names: Vec<String> = store.catalog().tables.iter().map(|t| t.name.clone()).collect();
            names.sort();
            Ok(ExecuteResult::Rows {
                columns: vec!["table".into()],
                rows: names.into_iter().map(|n| vec![Value::Text(n)]).collect(),
            })
        }
        Statement::Describe { name } => describe(store, &name),
        Statement::ShowStats => {
            let stats = store.stats()?;
            Ok(ExecuteResult::Rows {
                columns: vec![
                    "pages".into(),
                    "wal_lsn".into(),
                    "buffered".into(),
                    "tables".into(),
                    "commit_ts".into(),
                    "columnar_applied_lsn".into(),
                ],
                rows: vec![vec![
                    Value::Int(stats.pages as i64),
                    Value::Int(stats.wal_lsn as i64),
                    Value::Int(stats.buffered as i64),
                    Value::Int(stats.tables as i64),
                    Value::Int(stats.commit_ts as i64),
                    Value::Int(stats.columnar_applied_lsn as i64),
                ]],
            })
        }
        Statement::Insert { table, columns, rows } => exec_insert(store, session, &table, columns, rows),
        Statement::Update { table, assignments, filter } => {
            exec_update(store, session, &table, assignments, filter)
        }
        Statement::Delete { table, filter } => exec_delete(store, session, &table, filter),
        Statement::Query(query) => exec_query(store, session, &query, false),
    }
}

fn explain(store: &dyn Store, session: &Session, stmt: &Statement) -> Result<ExecuteResult> {
    let text = match stmt {
        Statement::Query(query) => planner::explain(store, session, query)?,
        other => format!("{other:?}"),
    };
    Ok(ExecuteResult::Rows {
        columns: vec!["plan".into()],
        rows: vec![vec![Value::Text(text)]],
    })
}

fn exec_insert(
    store: &dyn Store,
    session: &mut Session,
    table: &str,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
) -> Result<ExecuteResult> {
    let def = table_def(store, table)?;
    let started = begin_if_needed(store, session)?;
    let txn = session.txn_id.ok_or_else(|| Error::Txn("no transaction".into()))?;
    let result = (|| {
        let mut n = 0u64;
        for row in rows {
            let values = align_insert(&def, columns.as_deref(), &row)?;
            store.insert_row(&def.name, &values, txn)?;
            n += 1;
        }
        Ok(exec_msg(n, &format!("INSERT {n}")))
    })();
    finish(store, session, started, result)
}

fn exec_update(
    store: &dyn Store,
    session: &mut Session,
    table: &str,
    assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
) -> Result<ExecuteResult> {
    let def = table_def(store, table)?;
    let started = begin_if_needed(store, session)?;
    let result = (|| {
        let recs = load_table(store, session, &def.name, filter.as_ref(), true)?.0;
        let txn = session.txn_id.ok_or_else(|| Error::Txn("no transaction".into()))?;
        let mut rids = Vec::new();
        let mut new_rows = Vec::new();
        for rec in recs {
            let mut values = rec.values.clone();
            for (col, expr) in &assignments {
                let idx = col_index(&def.columns, col)?;
                let evaluated = eval(expr, &rec)?;
                values[idx] = evaluated.coerce(&def.columns[idx].ty)?;
            }
            rids.push(rec.rid);
            new_rows.push(values);
        }
        let n = store.update_rids(&def.name, &rids, &new_rows, txn, session.snapshot_ts)?;
        Ok(exec_msg(n, &format!("UPDATE {n}")))
    })();
    finish(store, session, started, result)
}

fn exec_delete(
    store: &dyn Store,
    session: &mut Session,
    table: &str,
    filter: Option<Expr>,
) -> Result<ExecuteResult> {
    let def = table_def(store, table)?;
    let started = begin_if_needed(store, session)?;
    let result = (|| {
        let recs = load_table(store, session, &def.name, filter.as_ref(), true)?.0;
        let txn = session.txn_id.ok_or_else(|| Error::Txn("no transaction".into()))?;
        let rids: Vec<Rid> = recs.into_iter().map(|r| r.rid).collect();
        let n = store.delete_rids(&def.name, &rids, txn, session.snapshot_ts)?;
        Ok(exec_msg(n, &format!("DELETE {n}")))
    })();
    finish(store, session, started, result)
}

fn exec_query(
    store: &dyn Store,
    session: &mut Session,
    query: &Query,
    started_outer: bool,
) -> Result<ExecuteResult> {
    let started = if started_outer {
        true
    } else {
        begin_if_needed(store, session)?
    };
    let own_writes = !started && session.txn_id.is_some();
    let result = (|| run_query(store, session, query, own_writes))();
    if started_outer {
        return result;
    }
    finish(store, session, started, result)
}

fn run_query(
    store: &dyn Store,
    session: &Session,
    query: &Query,
    own_writes: bool,
) -> Result<ExecuteResult> {
    let (mut distinct, mut offset, projection) = strip_sentinels(&query.projection);
    let Some(from) = query.from.clone() else {
        let rec = Rec {
            table: String::new(),
            names: Vec::new(),
            values: Vec::new(),
            rid: Rid { page: 0, slot: 0 },
        };
        let values = project_one(store, &projection, &rec)?;
        return Ok(ExecuteResult::Rows {
            columns: values.0,
            rows: vec![values.1],
        });
    };
    let mut recs = load_from(store, session, &from, query.selection.as_ref(), own_writes)?;
    if let Some(filter) = &query.selection {
        if !is_index_only(filter) {
            recs.retain(|rec| eval_bool(filter, rec).unwrap_or(false));
        }
    }
    if !query.order_by.is_empty() {
        sort_recs(&mut recs, &query.order_by)?;
    }
    if offset > 0 {
        if offset as usize >= recs.len() {
            recs.clear();
        } else {
            recs.drain(0..offset as usize);
        }
    }
    if let Some(limit) = query.limit {
        recs.truncate(limit as usize);
    }
    if planner::has_aggregate(query) {
        return aggregate(store, &projection, &query.group_by, &recs);
    }
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for rec in &recs {
        let (cols, vals) = project_one(store, &projection, rec)?;
        if columns.is_empty() {
            columns = cols;
        }
        rows.push(vals);
    }
    if columns.is_empty() {
        columns = projection_names(store, &from, &projection)?;
    }
    if distinct {
        rows.sort_by(|a, b| a.partial_cmp_values(b));
        rows.dedup();
    }
    let _ = distinct;
    Ok(ExecuteResult::Rows { columns, rows })
}

fn load_from(
    store: &dyn Store,
    session: &Session,
    from: &From,
    selection: Option<&Expr>,
    own_writes: bool,
) -> Result<Vec<Rec>> {
    match from {
        From::Table { name, alias } => {
            load_table(store, session, name, selection, false)?.0
                .into_iter()
                .map(|mut rec| {
                    if let Some(alias) = alias {
                        rec.table = alias.clone();
                    }
                    Ok(rec)
                })
                .collect()
        }
        From::Join { left, right, kind, on } => {
            let ls = load_from(store, session, left, None, own_writes)?;
            let rs = load_from(store, session, right, None, own_writes)?;
            let mut out = Vec::new();
            for l in &ls {
                let mut matched = false;
                for r in &rs {
                    let joined = join_rec(l, r);
                    if eval_bool(on, &joined)? {
                        out.push(joined);
                        matched = true;
                    }
                }
                if !matched && *kind == JoinKind::Left {
                    out.push(join_rec(l, &null_rec(right)?));
                }
            }
            let _ = selection;
            Ok(out)
        }
    }
}

fn load_table(
    store: &dyn Store,
    session: &Session,
    table: &str,
    selection: Option<&Expr>,
    for_write: bool,
) -> Result<(Vec<Rec>, String)> {
    let def = table_def(store, table)?;
    let snapshot = session.snapshot_ts;
    let own = session.txn_id;
    let access = planner::choose_access(store, session, &def.name, selection, for_write)?;
    let rows = match &access {
        Access::Index { name, method } => match method {
            IndexMethod::Inverted => {
                let query = literal_text(&func_arg(selection, 1)?)?;
                store
                    .search_text(&def.name, name, &query, snapshot, own)?
                    .into_iter()
                    .map(|(row, _)| row)
                    .collect()
            }
            IndexMethod::Hnsw => {
                let vector = vector_arg(&func_arg(selection, 1)?)?;
                let k = match func_arg(selection, 2)? {
                    Expr::Literal(Value::Int(n)) if n > 0 => n as usize,
                    _ => return Err(Error::Type("knn k must be a positive integer".into())),
                };
                store
                    .search_knn(&def.name, name, &vector, k, snapshot, own)?
                    .into_iter()
                    .map(|(row, _)| row)
                    .collect()
            }
            IndexMethod::Btree => {
                let (col, lit) = selection
                    .and_then(|e| match e {
                        Expr::Binary { left, right, .. } => Some(planner::column_and_literal(left, right)),
                        _ => None,
                    })
                    .unwrap_or((None, None));
                let Some(lit) = lit else {
                    return Ok((rows_to_recs(&def.name, &def.columns, &store.scan_table(&def.name, snapshot, own)?), "SeqScan".into()));
                };
                let key = vec![lit.clone()];
                if matches!(selection, Some(Expr::Binary { op: BinOp::Eq, .. })) {
                    store.index_eq(&def.name, name, &key, snapshot, own)?
                } else {
                    store.index_range(&def.name, name, &key, None, snapshot, own)?
                }
            }
        },
        Access::Columnar => store
            .scan_columnar(&def.name, snapshot, false)?
            .unwrap_or_default(),
        Access::Seq => store.scan_table(&def.name, snapshot, own)?,
    };
    let label = match access {
        Access::Seq => "SeqScan",
        Access::Columnar => "ColumnarScan",
        Access::Index { .. } => "IndexScan",
    };
    Ok((rows_to_recs(&def.name, &def.columns, &rows), label.into()))
}

fn aggregate(
    store: &dyn Store,
    projection: &[SelectItem],
    group_by: &[Expr],
    recs: &[Rec],
) -> Result<ExecuteResult> {
    let mut groups: Vec<(Vec<Value>, Vec<Rec>)> = Vec::new();
    for rec in recs {
        let key: Vec<Value> = group_by.iter().map(|e| eval(e, rec)).collect::<Result<_>>()?;
        if let Some((_, bucket)) = groups.iter_mut().find(|(k, _)| k == &key) {
            bucket.push(rec.clone());
        } else {
            groups.push((key, vec![rec.clone()]));
        }
    }
    if groups.is_empty() && group_by.is_empty() {
        groups.push((Vec::new(), Vec::new()));
    }
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for (_, bucket) in groups {
        let mut vals = Vec::new();
        let mut cols = Vec::new();
        for item in projection {
            if is_sentinel(item) {
                continue;
            }
            match item {
                SelectItem::Star { .. } => {
                    return Err(Error::Plan("SELECT * cannot mix with aggregates".into()));
                }
                SelectItem::Expr { expr, alias } => {
                    cols.push(alias.clone().unwrap_or_else(|| expr_label(expr)));
                    vals.push(eval_agg(expr, &bucket)?);
                }
            }
        }
        if columns.is_empty() {
            columns = cols;
        }
        rows.push(vals);
    }
    let _ = store;
    Ok(ExecuteResult::Rows { columns, rows })
}

fn eval_agg(expr: &Expr, recs: &[Rec]) -> Result<Value> {
    match expr {
        Expr::Func { name, args } if planner::is_agg(name) => {
            if name == "count" {
                return Ok(Value::Int(recs.len() as i64));
            }
            let arg = args.first().ok_or_else(|| Error::Plan(format!("{name} needs an argument")))?;
            let mut nums: Vec<Value> = Vec::new();
            for rec in recs {
                let v = eval(arg, rec)?;
                if !v.is_null() {
                    nums.push(v);
                }
            }
            if nums.is_empty() {
                return Ok(Value::Null);
            }
            match name.as_str() {
                "sum" => sum_values(&nums),
                "avg" => {
                    let total = sum_values(&nums)?;
                    let n = nums.len() as f64;
                    match total {
                        Value::Int(v) => Ok(Value::Float(v as f64 / n)),
                        Value::Float(v) => Ok(Value::Float(v / n)),
                        other => Err(Error::Type(format!("cannot average {other}"))),
                    }
                }
                "min" => Ok(nums.into_iter().min_by(cmp_value).unwrap_or(Value::Null)),
                "max" => Ok(nums.into_iter().max_by(cmp_value).unwrap_or(Value::Null)),
                _ => Err(Error::Plan(format!("unknown aggregate {name}"))),
            }
        }
        Expr::Column { .. } | Expr::Literal(_) => {
            if let Some(rec) = recs.first() {
                eval(expr, rec)
            } else {
                Ok(Value::Null)
            }
        }
        other => {
            if let Some(rec) = recs.first() {
                eval(other, rec)
            } else {
                Ok(Value::Null)
            }
        }
    }
}

fn project_one(store: &dyn Store, items: &[SelectItem], rec: &Rec) -> Result<(Vec<String>, Vec<Value>)> {
    let mut cols = Vec::new();
    let mut vals = Vec::new();
    for item in items {
        if is_sentinel(item) {
            continue;
        }
        match item {
            SelectItem::Star { table } => {
                for (i, name) in rec.names.iter().enumerate() {
                    if table.as_ref().is_some_and(|t| !name.starts_with(&format!("{t}."))) && table.is_some() {
                        continue;
                    }
                    cols.push(name.clone());
                    vals.push(rec.values.get(i).cloned().unwrap_or(Value::Null));
                }
            }
            SelectItem::Expr { expr, alias } => {
                cols.push(alias.clone().unwrap_or_else(|| expr_label(expr)));
                vals.push(eval(expr, rec)?);
            }
        }
    }
    let _ = store;
    Ok((cols, vals))
}

fn eval(expr: &Expr, rec: &Rec) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column { table, name } => lookup(rec, table.as_deref(), name),
        Expr::Unary { op, expr } => {
            let v = eval(expr, rec)?;
            match op {
                UnaryOp::Not => Ok(Value::Bool(!is_true(&v))),
                UnaryOp::Neg => match v {
                    Value::Int(n) => Ok(Value::Int(n.checked_neg().ok_or_else(|| Error::Type("overflow".into()))?)),
                    Value::Float(n) => Ok(Value::Float(-n)),
                    Value::Null => Ok(Value::Null),
                    other => Err(Error::Type(format!("cannot negate {other}"))),
                },
            }
        }
        Expr::IsNull { expr, negated } => {
            let v = eval(expr, rec)?;
            Ok(Value::Bool(v.is_null() ^ negated))
        }
        Expr::Like { expr, pattern, negated } => {
            let value = eval(expr, rec)?;
            let pat = eval(pattern, rec)?;
            match (value, pat) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Text(s), Value::Text(p)) => Ok(Value::Bool(like(&s, &p) ^ negated)),
                _ => Err(Error::Type("LIKE needs text".into())),
            }
        }
        Expr::Binary { op, left, right } => eval_binary(*op, eval(left, rec)?, eval(right, rec)?),
        Expr::Func { name, args } => eval_func(name, args, rec),
    }
}

fn eval_bool(expr: &Expr, rec: &Rec) -> Result<bool> {
    Ok(is_true(&eval(expr, rec)?))
}

fn eval_binary(op: BinOp, left: Value, right: Value) -> Result<Value> {
    match op {
        BinOp::And => Ok(tri_and(&left, &right)),
        BinOp::Or => Ok(tri_or(&left, &right)),
        BinOp::Plus | BinOp::Minus | BinOp::Mul | BinOp::Div => arith(op, left, right),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => cmp_op(op, left, right),
    }
}

fn eval_func(name: &str, args: &[Expr], rec: &Rec) -> Result<Value> {
    let mut values = Vec::new();
    for arg in args {
        values.push(eval(arg, rec)?);
    }
    match name {
        "lower" => match values.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_lowercase())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Type("lower needs text".into())),
        },
        "upper" => match values.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_uppercase())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Type("upper needs text".into())),
        },
        "length" => match values.first() {
            Some(Value::Text(s)) => Ok(Value::Int(s.chars().count() as i64)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Type("length needs text".into())),
        },
        "abs" => match values.first() {
            Some(Value::Int(n)) => Ok(Value::Int(n.abs())),
            Some(Value::Float(n)) => Ok(Value::Float(n.abs())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Type("abs needs a number".into())),
        },
        "search" => {
            let text = match values.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Bool(false)),
                _ => return Err(Error::Type("search needs text".into())),
            };
            let query = match values.get(1) {
                Some(Value::Text(s)) => s.clone(),
                _ => return Err(Error::Type("search needs a query string".into())),
            };
            Ok(Value::Bool(text_matches(&text, &query)))
        }
        "knn" => Ok(Value::Bool(true)),
        other if planner::is_agg(other) => Err(Error::Plan(format!("{other} is an aggregate"))),
        other => Err(Error::Plan(format!("unknown function {other}"))),
    }
}

fn align_insert(def: &crate::types::TableDef, columns: Option<&[String]>, row: &[Expr]) -> Result<Vec<Value>> {
    let rec = Rec {
        table: def.name.clone(),
        names: Vec::new(),
        values: Vec::new(),
        rid: Rid { page: 0, slot: 0 },
    };
    let mut values = vec![Value::Null; def.columns.len()];
    if let Some(columns) = columns {
        if columns.len() != row.len() {
            return Err(Error::Type("INSERT column count mismatch".into()));
        }
        for (col, expr) in columns.iter().zip(row) {
            let idx = col_index(&def.columns, col)?;
            values[idx] = eval(expr, &rec)?.coerce(&def.columns[idx].ty)?;
        }
    } else {
        if row.len() != def.columns.len() {
            return Err(Error::Type(format!(
                "{} values for {} columns",
                row.len(),
                def.columns.len()
            )));
        }
        for (idx, expr) in row.iter().enumerate() {
            values[idx] = eval(expr, &rec)?.coerce(&def.columns[idx].ty)?;
        }
    }
    for (col, value) in def.columns.iter().zip(&values) {
        if value.is_null() && !col.nullable {
            return Err(Error::Type(format!("{} cannot be null", col.name)));
        }
    }
    Ok(values)
}

fn describe(store: &dyn Store, name: &str) -> Result<ExecuteResult> {
    let def = table_def(store, name)?;
    let rows = def
        .columns
        .iter()
        .map(|c| {
            vec![
                Value::Text(c.name.clone()),
                Value::Text(c.ty.name()),
                Value::Bool(c.nullable),
                Value::Bool(c.primary_key),
            ]
        })
        .collect();
    Ok(ExecuteResult::Rows {
        columns: vec!["column".into(), "type".into(), "nullable".into(), "primary_key".into()],
        rows,
    })
}

fn table_def(store: &dyn Store, name: &str) -> Result<crate::types::TableDef> {
    let name = name.to_ascii_lowercase();
    store
        .catalog()
        .tables
        .into_iter()
        .find(|t| t.name == name)
        .ok_or(Error::NotFound)
}

fn rows_to_recs(table: &str, columns: &[ColumnDef], rows: &[Row]) -> Vec<Rec> {
    rows.iter()
        .map(|row| Rec {
            table: table.to_string(),
            names: columns.iter().map(|c| c.name.clone()).collect(),
            values: row.values.clone(),
            rid: row.rid,
        })
        .collect()
}

fn lookup(rec: &Rec, table: Option<&str>, name: &str) -> Result<Value> {
    if name == "*" {
        return Ok(Value::Null);
    }
    if let Some(table) = table {
        if rec.table != table && !rec.names.iter().any(|n| n.starts_with(&format!("{table}."))) {
            return Err(Error::Plan(format!("unknown column {table}.{name}")));
        }
    }
    rec.names
        .iter()
        .position(|n| n == name || n.ends_with(&format!(".{name}")))
        .map(|i| rec.values.get(i).cloned().unwrap_or(Value::Null))
        .ok_or_else(|| Error::Plan(format!("unknown column {name}")))
}

fn join_rec(left: &Rec, right: &Rec) -> Rec {
    let mut names = Vec::new();
    let mut values = Vec::new();
    for (name, value) in left.names.iter().zip(&left.values) {
        names.push(if name.contains('.') { name.clone() } else { format!("{}.{}", left.table, name) });
        values.push(value.clone());
    }
    for (name, value) in right.names.iter().zip(&right.values) {
        names.push(if name.contains('.') { name.clone() } else { format!("{}.{}", right.table, name) });
        values.push(value.clone());
    }
    Rec {
        table: left.table.clone(),
        names,
        values,
        rid: left.rid,
    }
}

fn null_rec(from: &From) -> Result<Rec> {
    match from {
        From::Table { name, alias } => Ok(Rec {
            table: alias.clone().unwrap_or_else(|| name.clone()),
            names: Vec::new(),
            values: Vec::new(),
            rid: Rid { page: 0, slot: 0 },
        }),
        From::Join { .. } => Err(Error::Plan("cannot null-extend a join".into())),
    }
}

fn sort_recs(recs: &mut [Rec], order: &[OrderItem]) -> Result<()> {
    let mut error = None;
    recs.sort_by(|a, b| {
        for item in order {
            let av = eval(&item.expr, a);
            let bv = eval(&item.expr, b);
            match (av, bv) {
                (Ok(av), Ok(bv)) => {
                    let cmp = cmp_value(&av, &bv);
                    if cmp != std::cmp::Ordering::Equal {
                        return if item.asc { cmp } else { cmp.reverse() };
                    }
                }
                (Err(e), _) | (_, Err(e)) => {
                    error = Some(e);
                    return std::cmp::Ordering::Equal;
                }
            }
        }
        std::cmp::Ordering::Equal
    });
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn begin_if_needed(store: &dyn Store, session: &mut Session) -> Result<bool> {
    if session.txn_id.is_some() {
        return Ok(false);
    }
    store.begin(session)?;
    Ok(true)
}

fn finish(
    store: &dyn Store,
    session: &mut Session,
    started: bool,
    result: Result<ExecuteResult>,
) -> Result<ExecuteResult> {
    match result {
        Ok(v) => {
            if started {
                store.commit(session)?;
            }
            Ok(v)
        }
        Err(e) => {
            if started {
                let _ = store.rollback(session);
            }
            Err(e)
        }
    }
}

fn reject_open_txn(session: &Session) -> Result<()> {
    if session.txn_id.is_some() {
        Err(Error::Txn("DDL commits implicitly; end the transaction first".into()))
    } else {
        Ok(())
    }
}

fn exec_msg(affected: u64, message: &str) -> ExecuteResult {
    ExecuteResult::Exec {
        affected,
        message: message.to_string(),
    }
}

fn col_index(columns: &[ColumnDef], name: &str) -> Result<usize> {
    columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| Error::Plan(format!("unknown column {name}")))
}

fn is_true(value: &Value) -> bool {
    matches!(value, Value::Bool(true))
}

fn tri_and(left: &Value, right: &Value) -> Value {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
        (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
        _ => Value::Null,
    }
}

fn tri_or(left: &Value, right: &Value) -> Value {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
        (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
        _ => Value::Null,
    }
}

fn arith(op: BinOp, left: Value, right: Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    let (lf, rf, integral) = match (left, right) {
        (Value::Int(a), Value::Int(b)) => (a as f64, b as f64, true),
        (Value::Int(a), Value::Float(b)) => (a as f64, b, false),
        (Value::Float(a), Value::Int(b)) => (a, b as f64, false),
        (Value::Float(a), Value::Float(b)) => (a, b, false),
        _ => return Err(Error::Type("arithmetic needs numbers".into())),
    };
    let result = match op {
        BinOp::Plus => lf + rf,
        BinOp::Minus => lf - rf,
        BinOp::Mul => lf * rf,
        BinOp::Div => {
            if rf == 0.0 {
                return Err(Error::Type("division by zero".into()));
            }
            lf / rf
        }
        _ => return Err(Error::Type("not arithmetic".into())),
    };
    if integral && op != BinOp::Div {
        Ok(Value::Int(result as i64))
    } else if integral && op == BinOp::Div && result.fract() == 0.0 {
        Ok(Value::Int(result as i64))
    } else {
        Ok(Value::Float(result))
    }
}

fn cmp_op(op: BinOp, left: Value, right: Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    let ord = cmp_value(&left, &right);
    let eq = matches!(ord, std::cmp::Ordering::Equal);
    Ok(Value::Bool(match op {
        BinOp::Eq => eq,
        BinOp::Ne => !eq,
        BinOp::Lt => ord == std::cmp::Ordering::Less,
        BinOp::Le => ord != std::cmp::Ordering::Greater,
        BinOp::Gt => ord == std::cmp::Ordering::Greater,
        BinOp::Ge => ord != std::cmp::Ordering::Less,
        _ => return Err(Error::Type("not a comparison".into())),
    }))
}

fn cmp_value(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (a, b) => a.to_string().cmp(&b.to_string()),
    }
}

fn like(text: &str, pattern: &str) -> bool {
    fn rec(t: &[u8], p: &[u8]) -> bool {
        let mut ti = 0;
        let mut pi = 0;
        while pi < p.len() {
            if p[pi] == b'%' {
                if pi + 1 == p.len() {
                    return true;
                }
                for k in ti..=t.len() {
                    if rec(&t[k..], &p[pi + 1..]) {
                        return true;
                    }
                }
                return false;
            }
            if ti >= t.len() {
                return false;
            }
            if p[pi] != b'_' && p[pi] != t[ti] {
                return false;
            }
            ti += 1;
            pi += 1;
        }
        ti == t.len()
    }
    rec(text.as_bytes(), pattern.as_bytes())
}

fn text_matches(text: &str, query: &str) -> bool {
    let hay = text.to_ascii_lowercase();
    query
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .all(|term| hay.contains(&term.to_ascii_lowercase()))
}

fn literal_text(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(Value::Text(s)) => Ok(s.clone()),
        other => Err(Error::Type(format!("expected a string, got {other:?}"))),
    }
}

fn vector_arg(expr: &Expr) -> Result<Vec<f32>> {
    match expr {
        Expr::Literal(Value::Text(s)) => parse_vector(s, s.trim().trim_start_matches('[').trim_end_matches(']').split(',').filter(|p| !p.trim().is_empty()).count() as u32),
        Expr::Literal(Value::Vector(v)) => Ok(v.clone()),
        _ => Err(Error::Type("knn vector must be a literal".into())),
    }
}

fn func_arg(selection: Option<&Expr>, idx: usize) -> Result<Expr> {
    match selection {
        Some(Expr::Func { args, .. }) => args.get(idx).cloned().ok_or_else(|| Error::Plan("missing function argument".into())),
        _ => Err(Error::Plan("expected a function predicate".into())),
    }
}

fn is_index_only(expr: &Expr) -> bool {
    matches!(expr, Expr::Func { name, .. } if name == "search" || name == "knn")
}

fn strip_sentinels(items: &[SelectItem]) -> (bool, u64, Vec<SelectItem>) {
    let mut distinct = false;
    let mut offset = 0u64;
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Expr { expr: Expr::Func { name, args }, .. } if name == "__distinct" => {
                distinct = true;
            }
            SelectItem::Expr { expr: Expr::Func { name, args }, .. } if name == "__offset" => {
                if let Some(Expr::Literal(Value::Int(n))) = args.first() {
                    offset = *n as u64;
                }
            }
            other => out.push(other.clone()),
        }
    }
    (distinct, offset, out)
}

fn is_sentinel(item: &SelectItem) -> bool {
    matches!(item, SelectItem::Expr { expr: Expr::Func { name, .. }, .. } if name == "__distinct" || name == "__offset")
}

fn expr_label(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Func { name, .. } => name.clone(),
        _ => "expr".into(),
    }
}

fn projection_names(store: &dyn Store, from: &From, items: &[SelectItem]) -> Result<Vec<String>> {
    let _ = (store, from, items);
    Ok(vec!["column".into()])
}

fn sum_values(values: &[Value]) -> Result<Value> {
    let mut acc = 0f64;
    let mut integral = true;
    for v in values {
        match v {
            Value::Int(n) => acc += *n as f64,
            Value::Float(n) => {
                integral = false;
                acc += *n;
            }
            other => return Err(Error::Type(format!("cannot sum {other}"))),
        }
    }
    if integral {
        Ok(Value::Int(acc as i64))
    } else {
        Ok(Value::Float(acc))
    }
}

trait ValueSliceCmp {
    fn partial_cmp_values(&self, other: &Self) -> std::cmp::Ordering;
}

impl ValueSliceCmp for Vec<Value> {
    fn partial_cmp_values(&self, other: &Self) -> std::cmp::Ordering {
        for (a, b) in self.iter().zip(other) {
            let cmp = cmp_value(a, b);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        self.len().cmp(&other.len())
    }
}

impl Clone for Rec {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            names: self.names.clone(),
            values: self.values.clone(),
            rid: self.rid,
        }
    }
}
