use std::path::PathBuf;
use std::sync::Mutex;

use crate::ast::{BinOp, Expr, From, Statement, UnaryOp};
use crate::engine::Database;
use crate::error::{Error, Result};
use crate::types::DataType;
use crate::value::{Config, ExecuteResult, Value};

/// Range-routed set of independent databases. Not Raft and not a distributed transaction.
pub struct RangeCluster {
    bounds: Vec<i64>,
    shards: Mutex<Vec<Database>>,
}

impl RangeCluster {
    pub fn create(dirs: &[PathBuf], upper_bounds: &[i64], config: Config) -> Result<Self> {
        Self::open_shards(dirs, upper_bounds, config, true)
    }

    pub fn open(dirs: &[PathBuf], upper_bounds: &[i64], config: Config) -> Result<Self> {
        Self::open_shards(dirs, upper_bounds, config, false)
    }

    pub fn execute(&self, sql: &str) -> Result<ExecuteResult> {
        let stmts = crate::parser::parse(sql)?;
        if stmts.is_empty() {
            return Ok(exec_ok(0, "OK"));
        }
        let texts = split_statements(sql);
        if texts.len() != stmts.len() {
            return Err(Error::Parse(
                "could not split statements for routing".into(),
            ));
        }
        let shards = self.shards.lock().map_err(|_| {
            Error::Txn("cluster lock poisoned".into())
        })?;
        let mut last = exec_ok(0, "OK");
        for (stmt, text) in stmts.iter().zip(texts.iter()) {
            last = route(&shards, &self.bounds, stmt, text)?;
        }
        Ok(last)
    }

    pub fn shard_count(&self) -> usize {
        self.bounds.len() + 1
    }

    fn open_shards(
        dirs: &[PathBuf],
        upper_bounds: &[i64],
        config: Config,
        create: bool,
    ) -> Result<Self> {
        if dirs.len() != upper_bounds.len() + 1 {
            return Err(Error::Plan(format!(
                "expected {} shard directories for {} upper bounds, got {}",
                upper_bounds.len() + 1,
                upper_bounds.len(),
                dirs.len()
            )));
        }
        for pair in upper_bounds.windows(2) {
            if pair[0] >= pair[1] {
                return Err(Error::Plan(
                    "upper bounds must be strictly increasing".into(),
                ));
            }
        }
        let mut opened = Vec::with_capacity(dirs.len());
        for dir in dirs {
            let db = if create {
                Database::create(dir, config.clone())?
            } else {
                Database::open(dir, config.clone())?
            };
            opened.push(db);
        }
        Ok(Self {
            bounds: upper_bounds.to_vec(),
            shards: Mutex::new(opened),
        })
    }
}

fn route(
    shards: &[Database],
    bounds: &[i64],
    stmt: &Statement,
    text: &str,
) -> Result<ExecuteResult> {
    match stmt {
        Statement::CreateTable { columns, .. } => {
            let ok = columns.iter().any(|col| {
                col.primary_key
                    && col.name.eq_ignore_ascii_case("id")
                    && col.ty == DataType::Int
            });
            if !ok {
                return Err(cross("CREATE TABLE requires INT PRIMARY KEY named id"));
            }
            broadcast(shards, text)
        }
        Statement::DropTable { .. } | Statement::CreateIndex { .. } | Statement::DropIndex { .. } => {
            broadcast(shards, text)
        }
        Statement::Insert {
            table,
            columns,
            rows,
        } => insert_rows(shards, bounds, table, columns.as_deref(), rows),
        Statement::Update {
            assignments,
            filter,
            ..
        } => {
            if assignments
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("id"))
            {
                return Err(cross("UPDATE must not change id"));
            }
            let id = point_id(filter.as_ref()).ok_or_else(|| cross("UPDATE requires id = <int>"))?;
            shards[shard_for(id, bounds)].execute(text)
        }
        Statement::Delete { filter, .. } => {
            let id = point_id(filter.as_ref()).ok_or_else(|| cross("DELETE requires id = <int>"))?;
            shards[shard_for(id, bounds)].execute(text)
        }
        Statement::Query(query) => {
            if query.from.as_ref().is_some_and(from_has_join) || query.from.is_none() {
                return Err(cross("SELECT must be a single table"));
            }
            if let Some(id) = point_id(query.selection.as_ref()) {
                shards[shard_for(id, bounds)].execute(text)
            } else {
                scatter_select(shards, text)
            }
        }
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Err(cross("transactions are not distributed"))
        }
        _ => Err(cross("statement is not routable")),
    }
}

fn insert_rows(
    shards: &[Database],
    bounds: &[i64],
    table: &str,
    columns: Option<&[String]>,
    rows: &[Vec<Expr>],
) -> Result<ExecuteResult> {
    let mut affected = 0u64;
    for row in rows {
        let id = row_id(columns, row).ok_or_else(|| cross("INSERT requires an integer id"))?;
        let sql = render_insert(table, columns, row)?;
        match shards[shard_for(id, bounds)].execute(&sql)? {
            ExecuteResult::Exec { affected: n, .. } => affected += n,
            ExecuteResult::Rows { .. } => {
                return Err(cross("INSERT returned rows"));
            }
        }
    }
    Ok(exec_ok(affected, &format!("INSERT {affected}")))
}

fn broadcast(shards: &[Database], sql: &str) -> Result<ExecuteResult> {
    let mut last = exec_ok(0, "OK");
    for shard in shards {
        last = shard.execute(sql)?;
    }
    Ok(last)
}

fn scatter_select(shards: &[Database], sql: &str) -> Result<ExecuteResult> {
    let mut columns = None;
    let mut rows = Vec::new();
    for shard in shards {
        match shard.execute(sql)? {
            ExecuteResult::Rows {
                columns: cols,
                rows: part,
            } => {
                if columns.is_none() {
                    columns = Some(cols);
                }
                rows.extend(part);
            }
            ExecuteResult::Exec { .. } => {
                return Err(cross("SELECT did not return rows"));
            }
        }
    }
    Ok(ExecuteResult::Rows {
        columns: columns.unwrap_or_default(),
        rows,
    })
}

fn shard_for(id: i64, bounds: &[i64]) -> usize {
    bounds
        .iter()
        .position(|bound| id < *bound)
        .unwrap_or(bounds.len())
}

fn point_id(filter: Option<&Expr>) -> Option<i64> {
    let filter = filter?;
    let mut found = None;
    for conjunct in and_chain(filter) {
        if let Some(id) = eq_id(conjunct) {
            if found.is_some_and(|prev| prev != id) {
                return None;
            }
            found = Some(id);
        }
    }
    found
}

fn and_chain(expr: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    fn walk<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
        if let Expr::Binary {
            op: BinOp::And,
            left,
            right,
        } = expr
        {
            walk(left, out);
            walk(right, out);
        } else {
            out.push(expr);
        }
    }
    walk(expr, &mut out);
    out
}

fn eq_id(expr: &Expr) -> Option<i64> {
    let Expr::Binary {
        op: BinOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column { name, .. }, other) | (other, Expr::Column { name, .. })
            if name.eq_ignore_ascii_case("id") =>
        {
            int_lit(other)
        }
        _ => None,
    }
}

fn int_lit(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Value::Int(v)) => Some(*v),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => int_lit(expr)?.checked_neg(),
        _ => None,
    }
}

fn row_id(columns: Option<&[String]>, row: &[Expr]) -> Option<i64> {
    let idx = match columns {
        None => 0,
        Some(cols) => cols.iter().position(|c| c.eq_ignore_ascii_case("id"))?,
    };
    int_lit(row.get(idx)?)
}

fn render_insert(table: &str, columns: Option<&[String]>, row: &[Expr]) -> Result<String> {
    let mut sql = format!("INSERT INTO {} ", quote_ident(table));
    if let Some(cols) = columns {
        sql.push('(');
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&quote_ident(col));
        }
        sql.push_str(") ");
    }
    sql.push_str("VALUES (");
    for (i, expr) in row.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&render_expr(expr)?);
    }
    sql.push(')');
    Ok(sql)
}

fn render_expr(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(value) => Ok(render_value(value)),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => Ok(format!("-({})", render_expr(expr)?)),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Ok(format!("NOT ({})", render_expr(expr)?)),
        _ => Err(cross("INSERT value is not a literal")),
    }
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Int(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Bool(true) => "TRUE".into(),
        Value::Bool(false) => "FALSE".into(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Vector(v) => {
            let body = v
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("'[{body}]'")
        }
    }
}

fn quote_ident(name: &str) -> String {
    if !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
    {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn from_has_join(from: &From) -> bool {
    match from {
        From::Table { .. } => false,
        From::Join { .. } => true,
    }
}

fn cross(detail: &str) -> Error {
    Error::Plan(format!("cross-shard: {detail}"))
}

fn exec_ok(affected: u64, message: &str) -> ExecuteResult {
    ExecuteResult::Exec {
        affected,
        message: message.to_string(),
    }
}

fn split_statements(sql: &str) -> Vec<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                cur.push(chars[i]);
                i += 1;
            }
            continue;
        }
        if chars[i] == '\'' || chars[i] == '"' || chars[i] == '`' {
            let quote = chars[i];
            cur.push(quote);
            i += 1;
            while i < chars.len() {
                cur.push(chars[i]);
                if chars[i] == quote {
                    if quote == '\'' && chars.get(i + 1) == Some(&'\'') {
                        cur.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if chars[i] == ';' {
            let stmt = cur.trim().to_string();
            if !stmt.is_empty() {
                out.push(stmt);
            }
            cur.clear();
            i += 1;
            continue;
        }
        cur.push(chars[i]);
        i += 1;
    }
    let stmt = cur.trim().to_string();
    if !stmt.is_empty() {
        out.push(stmt);
    }
    out
}
