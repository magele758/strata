//! SQL parser. Identifiers are folded to lowercase. A few statements are
//! recognized before `sqlparser` because GenericDialect drops or rejects them.

use sqlparser::ast::{self as S};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::ast::{
    BinOp, Expr, From, JoinKind, OrderItem, Query, SelectItem, Statement, UnaryOp,
};
use crate::error::{Error, Result};
use crate::types::{ColumnDef, DataType, IndexMethod};
use crate::value::Value;

pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    let mut out = Vec::new();
    for raw in split_statements(sql) {
        let stmt = raw.trim();
        if stmt.is_empty() || is_comment_only(stmt) {
            continue;
        }
        out.push(parse_one(stmt)?);
    }
    Ok(out)
}

fn parse_one(sql: &str) -> Result<Statement> {
    if let Some(stmt) = parse_builtin(sql)? {
        return Ok(stmt);
    }
    let (sql_for_parser, forced_method) = prepare_create_index(sql);
    let mut parsed = Parser::parse_sql(&GenericDialect, &sql_for_parser)
        .map_err(|e| Error::Parse(e.to_string()))?;
    if parsed.len() != 1 {
        return Err(Error::Parse(
            "expected a single statement after splitting".into(),
        ));
    }
    let mut stmt = conv_stmt(parsed.remove(0))?;
    if let (Statement::CreateIndex { method, .. }, Some(forced)) = (&mut stmt, forced_method) {
        *method = forced;
    }
    Ok(stmt)
}

fn parse_builtin(sql: &str) -> Result<Option<Statement>> {
    let cleaned = strip_comments(sql);
    let trimmed = cleaned.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let eq = |a: &str, b: &str| a.eq_ignore_ascii_case(b);
    match tokens.as_slice() {
        [a] if eq(a, "begin") => Ok(Some(Statement::Begin)),
        [a, b] if eq(a, "begin") && (eq(b, "transaction") || eq(b, "work")) => {
            Ok(Some(Statement::Begin))
        }
        [a, b] if eq(a, "start") && eq(b, "transaction") => Ok(Some(Statement::Begin)),
        [a] if eq(a, "commit") => Ok(Some(Statement::Commit)),
        [a, b] if eq(a, "commit") && (eq(b, "transaction") || eq(b, "work")) => {
            Ok(Some(Statement::Commit))
        }
        [a] if eq(a, "rollback") => Ok(Some(Statement::Rollback)),
        [a, b] if eq(a, "rollback") && (eq(b, "transaction") || eq(b, "work")) => {
            Ok(Some(Statement::Rollback))
        }
        [a] if eq(a, "vacuum") => Ok(Some(Statement::Vacuum)),
        [a] if eq(a, "checkpoint") => Ok(Some(Statement::Checkpoint)),
        [a, b] if eq(a, "flush") && eq(b, "columnar") => Ok(Some(Statement::FlushColumnar)),
        [a, b] if eq(a, "show") && eq(b, "tables") => Ok(Some(Statement::ShowTables)),
        [a, b] if eq(a, "show") && eq(b, "stats") => Ok(Some(Statement::ShowStats)),
        [a, b] if eq(a, "describe") || eq(a, "desc") => Ok(Some(Statement::Describe {
            name: unquote_ident(b),
        })),
        [a, ..] if eq(a, "explain") => {
            let rest = rest_after_keyword(trimmed, "explain");
            if rest.is_empty() {
                return Err(Error::Parse("EXPLAIN requires a statement".into()));
            }
            let inner = parse_one(rest)?;
            Ok(Some(Statement::Explain(Box::new(inner))))
        }
        _ => Ok(None),
    }
}

fn rest_after_keyword<'a>(sql: &'a str, kw: &str) -> &'a str {
    let mut parts = sql.splitn(2, char::is_whitespace);
    let first = parts.next().unwrap_or("");
    if !first.eq_ignore_ascii_case(kw) {
        return "";
    }
    parts.next().unwrap_or("").trim()
}

fn unquote_ident(raw: &str) -> String {
    let s = raw.trim().trim_end_matches(';').trim();
    let s = if s.len() >= 2 {
        let b = s.as_bytes();
        if (b[0] == b'"' && b[s.len() - 1] == b'"')
            || (b[0] == b'`' && b[s.len() - 1] == b'`')
            || (b[0] == b'[' && b[s.len() - 1] == b']')
        {
            &s[1..s.len() - 1]
        } else {
            s
        }
    } else {
        s
    };
    fold(s)
}

/// `CREATE INDEX ... USING method` is captured here because some parser
/// versions drop `USING`. The clause is stripped so GenericDialect still parses.
fn prepare_create_index(sql: &str) -> (String, Option<IndexMethod>) {
    let cleaned = strip_comments(sql);
    let mut tokens = cleaned.split_whitespace();
    let first = tokens.next().unwrap_or("");
    let second = tokens.next().unwrap_or("");
    let is_create_index = first.eq_ignore_ascii_case("create")
        && (second.eq_ignore_ascii_case("index")
            || second.eq_ignore_ascii_case("unique")
                && tokens.next().unwrap_or("").eq_ignore_ascii_case("index"));
    if !is_create_index {
        return (sql.to_string(), None);
    }
    match capture_using(&cleaned) {
        Some(method) => {
            let stripped = strip_using_clause(sql);
            (stripped, IndexMethod::parse(&method).ok())
        }
        None => (sql.to_string(), None),
    }
}

fn capture_using(sql: &str) -> Option<String> {
    let chars: Vec<char> = sql.chars().collect();
    let lower: Vec<char> = chars.iter().map(|c| c.to_ascii_lowercase()).collect();
    let needle: Vec<char> = " using ".chars().collect();
    let mut i = 0;
    while i + needle.len() < lower.len() {
        if lower[i..].starts_with(&needle) {
            let mut j = i + needle.len();
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            let start = j;
            while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j > start {
                return Some(chars[start..j].iter().collect::<String>().to_ascii_lowercase());
            }
        }
        i += 1;
    }
    None
}

fn strip_using_clause(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let lower: Vec<char> = chars.iter().map(|c| c.to_ascii_lowercase()).collect();
    let needle: Vec<char> = " using ".chars().collect();
    let mut i = 0;
    while i + needle.len() < lower.len() {
        if lower[i..].starts_with(&needle) {
            let mut j = i + needle.len();
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            while j < chars.len() && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            let mut out: String = chars[..i].iter().collect();
            out.push(' ');
            out.extend(chars[j..].iter());
            return out;
        }
        i += 1;
    }
    sql.to_string()
}

fn conv_stmt(stmt: S::Statement) -> Result<Statement> {
    match stmt {
        S::Statement::Query(q) => Ok(Statement::Query(conv_query(*q)?)),
        S::Statement::Insert(insert) => conv_insert(insert),
        S::Statement::Update {
            table,
            assignments,
            selection,
            from,
            returning,
        } => {
            if from.is_some() || returning.is_some() {
                return Err(Error::Parse(
                    "UPDATE does not support FROM or RETURNING".into(),
                ));
            }
            let name = table_with_joins_name(&table)?;
            let mut mapped = Vec::new();
            for assignment in assignments {
                let cols = assignment_columns(&assignment.target)?;
                if cols.len() != 1 {
                    return Err(Error::Parse("tuple UPDATE targets are not supported".into()));
                }
                mapped.push((cols[0].clone(), conv_expr(assignment.value)?));
            }
            Ok(Statement::Update {
                table: name,
                assignments: mapped,
                filter: selection.map(conv_expr).transpose()?,
            })
        }
        S::Statement::Delete(delete) => {
            if delete.using.is_some() || delete.returning.is_some() || !delete.order_by.is_empty() {
                return Err(Error::Parse("unsupported DELETE clause".into()));
            }
            let name = from_table_name(&delete.from)?;
            Ok(Statement::Delete {
                table: name,
                filter: delete.selection.map(conv_expr).transpose()?,
            })
        }
        S::Statement::CreateTable(create) => conv_create_table(create),
        S::Statement::CreateIndex(create) => conv_create_index(create),
        S::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            let name = single_object_name(&names)?;
            match object_type {
                S::ObjectType::Table => Ok(Statement::DropTable {
                    name,
                    if_exists,
                }),
                S::ObjectType::Index => Ok(Statement::DropIndex {
                    name,
                    if_exists,
                }),
                other => Err(Error::Parse(format!("unsupported drop target {other}"))),
            }
        }
        S::Statement::StartTransaction { .. } => Ok(Statement::Begin),
        S::Statement::Commit { .. } => Ok(Statement::Commit),
        S::Statement::Rollback { savepoint: None, .. } => Ok(Statement::Rollback),
        S::Statement::Explain { statement, .. } => {
            Ok(Statement::Explain(Box::new(conv_stmt(*statement)?)))
        }
        S::Statement::ShowTables { .. } => Ok(Statement::ShowTables),
        other => Err(Error::Parse(format!("unsupported statement: {other}"))),
    }
}

fn conv_create_table(create: S::CreateTable) -> Result<Statement> {
    if create.temporary || create.external || create.query.is_some() || create.like.is_some() {
        return Err(Error::Parse(
            "only plain CREATE TABLE is supported".into(),
        ));
    }
    let name = object_name(&create.name)?;
    let mut columns = Vec::new();
    for col in create.columns {
        columns.push(conv_column(&col)?);
    }
    for constraint in &create.constraints {
        if let S::TableConstraint::PrimaryKey { columns: pk, .. } = constraint {
            for ident in pk {
                let pk_name = fold(&ident.value);
                let col = columns
                    .iter_mut()
                    .find(|c| c.name == pk_name)
                    .ok_or_else(|| Error::Parse(format!("primary key column {pk_name} is missing")))?;
                col.primary_key = true;
                col.nullable = false;
            }
        }
    }
    if columns.is_empty() {
        return Err(Error::Parse("table needs a column".into()));
    }
    let mut seen = Vec::new();
    for col in &columns {
        if seen.iter().any(|n| n == &col.name) {
            return Err(Error::Parse(format!("duplicate column {}", col.name)));
        }
        seen.push(col.name.clone());
    }
    Ok(Statement::CreateTable {
        name,
        columns,
        if_not_exists: create.if_not_exists,
    })
}

fn conv_column(col: &S::ColumnDef) -> Result<ColumnDef> {
    let mut nullable = true;
    let mut primary_key = false;
    let mut not_null = false;
    for opt in &col.options {
        match &opt.option {
            S::ColumnOption::NotNull => not_null = true,
            S::ColumnOption::Null => nullable = true,
            S::ColumnOption::Unique { is_primary: true, .. } => primary_key = true,
            S::ColumnOption::Unique { is_primary: false, .. }
            | S::ColumnOption::Default(_)
            | S::ColumnOption::Check(_)
            | S::ColumnOption::ForeignKey { .. }
            | S::ColumnOption::Comment(_)
            | S::ColumnOption::DialectSpecific(_) => {}
            other => {
                return Err(Error::Parse(format!(
                    "unsupported column option {other}"
                )))
            }
        }
    }
    if primary_key || not_null {
        nullable = false;
    }
    Ok(ColumnDef {
        name: fold(&col.name.value),
        ty: map_type(&col.data_type)?,
        nullable,
        primary_key,
    })
}

fn map_type(ty: &S::DataType) -> Result<DataType> {
    use S::DataType as T;
    match ty {
        T::Int(_)
        | T::Integer(_)
        | T::BigInt(_)
        | T::SmallInt(_)
        | T::TinyInt(_)
        | T::Int2(_)
        | T::Int4(_)
        | T::Int8(_)
        | T::Int16
        | T::Int32
        | T::Int64
        | T::UnsignedInt(_)
        | T::UnsignedInteger(_)
        | T::UnsignedBigInt(_)
        | T::UnsignedSmallInt(_)
        | T::UnsignedTinyInt(_)
        | T::MediumInt(_)
        | T::UnsignedMediumInt(_)
        | T::UnsignedInt2(_)
        | T::UnsignedInt4(_)
        | T::UnsignedInt8(_) => Ok(DataType::Int),
        T::Float(_)
        | T::Float4
        | T::Float8
        | T::Float32
        | T::Float64
        | T::Real
        | T::Double
        | T::DoublePrecision => Ok(DataType::Float),
        T::Bool | T::Boolean => Ok(DataType::Bool),
        T::Text
        | T::String(_)
        | T::Varchar(_)
        | T::Nvarchar(_)
        | T::Char(_)
        | T::Character(_)
        | T::CharacterVarying(_)
        | T::CharVarying(_) => Ok(DataType::Text),
        T::Custom(name, mods) => {
            let n = name
                .0
                .last()
                .map(|i| fold(&i.value))
                .unwrap_or_default();
            if n == "vector" {
                if mods.len() != 1 {
                    return Err(Error::Parse("VECTOR requires one dimension".into()));
                }
                let dim: u32 = mods[0].trim().parse().map_err(|_| {
                    Error::Parse(format!("bad VECTOR dimension {}", mods[0]))
                })?;
                Ok(DataType::Vector(dim))
            } else {
                Err(Error::Parse(format!("unsupported type {n}")))
            }
        }
        other => Err(Error::Parse(format!("unsupported type {other}"))),
    }
}

fn conv_create_index(create: S::CreateIndex) -> Result<Statement> {
    let name = match create.name {
        Some(name) => object_name(&name)?,
        None => return Err(Error::Parse("index name is required".into())),
    };
    let table = object_name(&create.table_name)?;
    let mut columns = Vec::new();
    for col in create.columns {
        columns.push(expr_column_name(&col.expr)?);
    }
    if columns.is_empty() {
        return Err(Error::Parse("index needs a column".into()));
    }
    let method = match create.using {
        Some(ident) => IndexMethod::parse(&ident.value)?,
        None => IndexMethod::Btree,
    };
    Ok(Statement::CreateIndex {
        name,
        table,
        columns,
        method,
        if_not_exists: create.if_not_exists,
    })
}

fn conv_insert(insert: S::Insert) -> Result<Statement> {
    if insert.source.is_none() {
        return Err(Error::Parse("INSERT requires VALUES".into()));
    }
    let table = object_name(&insert.table_name)?;
    let columns = if insert.columns.is_empty() {
        None
    } else {
        Some(insert.columns.iter().map(|c| fold(&c.value)).collect())
    };
    let source = insert.source.unwrap();
    let rows = match source.body.as_ref() {
        S::SetExpr::Values(values) => {
            let mut rows = Vec::new();
            for row in &values.rows {
                let mut exprs = Vec::new();
                for expr in row {
                    exprs.push(conv_expr(expr.clone())?);
                }
                rows.push(exprs);
            }
            rows
        }
        _ => {
            return Err(Error::Parse(
                "only INSERT ... VALUES is supported".into(),
            ))
        }
    };
    Ok(Statement::Insert {
        table,
        columns,
        rows,
    })
}

fn conv_query(query: S::Query) -> Result<Query> {
    if query.with.is_some() || query.fetch.is_some() || !query.locks.is_empty() {
        return Err(Error::Parse("unsupported query clause".into()));
    }
    let (mut projection, from, selection, group_by, distinct) = match query.body.as_ref() {
        S::SetExpr::Select(select) => {
            if select.having.is_some() {
                return Err(Error::Parse("HAVING is not supported".into()));
            }
            if !select.from.is_empty() && select.from.len() > 1 {
                // comma-separated FROM items become a cross join below
            }
            let projection = select
                .projection
                .iter()
                .map(conv_select_item)
                .collect::<Result<Vec<_>>>()?;
            let from = conv_from_list(&select.from)?;
            let selection = select.selection.clone().map(conv_expr).transpose()?;
            let group_by = conv_group_by(&select.group_by)?;
            let distinct = select.distinct.is_some();
            (projection, from, selection, group_by, distinct)
        }
        S::SetExpr::Query(inner) => {
            return conv_query(*inner.clone());
        }
        other => {
            return Err(Error::Parse(format!(
                "unsupported query body: {other}"
            )))
        }
    };
    if distinct {
        // Recorded by wrapping the projection; exec sees it via a sentinel item
        // only when the planner asks. Distinct is carried on the query by
        // rewriting is not possible, so stash it as a leading comment-free
        // marker function the planner understands. See `query_is_distinct`.
        projection.insert(
            0,
            SelectItem::Expr {
                expr: Expr::Func {
                    name: "__distinct".into(),
                    args: Vec::new(),
                },
                alias: None,
            },
        );
    }
    let order_by = match query.order_by {
        Some(order) => order
            .exprs
            .into_iter()
            .map(|item| {
                Ok(OrderItem {
                    expr: conv_expr(item.expr)?,
                    asc: item.asc.unwrap_or(true),
                })
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    let limit = match query.limit {
        Some(expr) => Some(limit_value(&expr)?),
        None => None,
    };
    let offset = match query.offset {
        Some(offset) => limit_value(&offset.value)?,
        None => 0,
    };
    if offset > 0 {
        // OFFSET is applied by the executor via a negative-free pair stored in
        // the limit sentinel: planner reads `__offset` from selection? We fold
        // offset into a trailing OrderItem-like function on the projection so
        // the fixed Query struct can still carry it.
        projection.push(SelectItem::Expr {
            expr: Expr::Func {
                name: "__offset".into(),
                args: vec![Expr::Literal(Value::Int(offset as i64))],
            },
            alias: None,
        });
    }
    Ok(Query {
        projection,
        from,
        selection,
        group_by,
        order_by,
        limit,
    })
}

fn limit_value(expr: &S::Expr) -> Result<u64> {
    match expr {
        S::Expr::Value(S::Value::Number(n, _)) => n
            .parse::<u64>()
            .map_err(|_| Error::Parse(format!("bad LIMIT {n}"))),
        S::Expr::Value(S::Value::Placeholder(_)) => {
            Err(Error::Parse("placeholders are not supported".into()))
        }
        other => Err(Error::Parse(format!("LIMIT must be an integer, got {other}"))),
    }
}

fn conv_group_by(group: &S::GroupByExpr) -> Result<Vec<Expr>> {
    match group {
        S::GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(Error::Parse("GROUP BY modifiers are not supported".into()));
            }
            exprs.iter().cloned().map(conv_expr).collect()
        }
        S::GroupByExpr::All(_) => Err(Error::Parse("GROUP BY ALL is not supported".into())),
    }
}

fn conv_from_list(items: &[S::TableWithJoins]) -> Result<Option<From>> {
    let mut iter = items.iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    let mut left = conv_table_with_joins(first)?;
    for next in iter {
        let right = conv_table_with_joins(next)?;
        left = From::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind: JoinKind::Inner,
            on: Expr::Literal(Value::Bool(true)),
        };
    }
    Ok(Some(left))
}

fn conv_table_with_joins(item: &S::TableWithJoins) -> Result<From> {
    let mut left = conv_factor(&item.relation)?;
    for join in &item.joins {
        let right = conv_factor(&join.relation)?;
        let (kind, on) = conv_join_op(&join.join_operator, &left, &right)?;
        left = From::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind,
            on,
        };
    }
    Ok(left)
}

fn conv_join_op(op: &S::JoinOperator, left: &From, right: &From) -> Result<(JoinKind, Expr)> {
    let (kind, constraint) = match op {
        S::JoinOperator::Inner(c) => (JoinKind::Inner, c),
        S::JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
        S::JoinOperator::CrossJoin => {
            return Ok((JoinKind::Inner, Expr::Literal(Value::Bool(true))))
        }
        other => {
            return Err(Error::Parse(format!(
                "only INNER and LEFT joins are supported, got {other:?}"
            )))
        }
    };
    let on = match constraint {
        S::JoinConstraint::On(expr) => conv_expr(expr.clone())?,
        S::JoinConstraint::Using(cols) => using_predicate(left, right, cols)?,
        S::JoinConstraint::None => Expr::Literal(Value::Bool(true)),
        S::JoinConstraint::Natural => {
            return Err(Error::Parse("NATURAL JOIN is not supported".into()))
        }
    };
    Ok((kind, on))
}

fn using_predicate(left: &From, right: &From, cols: &[S::Ident]) -> Result<Expr> {
    let left_name = from_alias_or_name(left)?;
    let right_name = from_alias_or_name(right)?;
    if cols.is_empty() {
        return Err(Error::Parse("USING requires a column".into()));
    }
    let mut acc: Option<Expr> = None;
    for col in cols {
        let name = fold(&col.value);
        let eq = Expr::Binary {
            op: BinOp::Eq,
            left: Box::new(Expr::Column {
                table: Some(left_name.clone()),
                name: name.clone(),
            }),
            right: Box::new(Expr::Column {
                table: Some(right_name.clone()),
                name,
            }),
        };
        acc = Some(match acc {
            None => eq,
            Some(prev) => Expr::Binary {
                op: BinOp::And,
                left: Box::new(prev),
                right: Box::new(eq),
            },
        });
    }
    Ok(acc.expect("USING columns checked"))
}

fn from_alias_or_name(from: &From) -> Result<String> {
    match from {
        From::Table { alias, name } => Ok(alias.clone().unwrap_or_else(|| name.clone())),
        From::Join { .. } => Err(Error::Parse(
            "USING requires simple table operands".into(),
        )),
    }
}

fn conv_factor(factor: &S::TableFactor) -> Result<From> {
    match factor {
        S::TableFactor::Table {
            name,
            alias,
            args,
            ..
        } => {
            if args.is_some() {
                return Err(Error::Parse("table functions are not supported".into()));
            }
            Ok(From::Table {
                name: object_name(name)?,
                alias: alias.as_ref().map(|a| fold(&a.name.value)),
            })
        }
        S::TableFactor::NestedJoin {
            table_with_joins, ..
        } => conv_table_with_joins(table_with_joins),
        other => Err(Error::Parse(format!(
            "unsupported FROM item: {other}"
        ))),
    }
}

fn conv_select_item(item: &S::SelectItem) -> Result<SelectItem> {
    match item {
        S::SelectItem::Wildcard(_) => Ok(SelectItem::Star { table: None }),
        S::SelectItem::QualifiedWildcard(name, _) => Ok(SelectItem::Star {
            table: Some(object_name(name)?),
        }),
        S::SelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expr {
            expr: conv_expr(expr.clone())?,
            alias: None,
        }),
        S::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expr {
            expr: conv_expr(expr.clone())?,
            alias: Some(fold(&alias.value)),
        }),
    }
}

fn conv_expr(expr: S::Expr) -> Result<Expr> {
    match expr {
        S::Expr::Identifier(ident) => Ok(Expr::Column {
            table: None,
            name: fold(&ident.value),
        }),
        S::Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Ok(Expr::Column {
                    table: Some(fold(&idents[0].value)),
                    name: fold(&idents[1].value),
                })
            } else if idents.len() == 1 {
                Ok(Expr::Column {
                    table: None,
                    name: fold(&idents[0].value),
                })
            } else {
                Err(Error::Parse(
                    "only table.column qualification is supported".into(),
                ))
            }
        }
        S::Expr::Value(value) => Ok(Expr::Literal(conv_value(value)?)),
        S::Expr::Nested(inner) => conv_expr(*inner),
        S::Expr::BinaryOp { left, op, right } => {
            if matches!(op, S::BinaryOperator::StringConcat) {
                return Ok(Expr::Func {
                    name: "concat".into(),
                    args: vec![conv_expr(*left)?, conv_expr(*right)?],
                });
            }
            Ok(Expr::Binary {
                op: map_binop(op)?,
                left: Box::new(conv_expr(*left)?),
                right: Box::new(conv_expr(*right)?),
            })
        }
        S::Expr::UnaryOp { op, expr } => Ok(Expr::Unary {
            op: map_unary(op)?,
            expr: Box::new(conv_expr(*expr)?),
        }),
        S::Expr::IsNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(conv_expr(*inner)?),
            negated: false,
        }),
        S::Expr::IsNotNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(conv_expr(*inner)?),
            negated: true,
        }),
        S::Expr::IsTrue(inner) => Ok(flag_func("__is_true", conv_expr(*inner)?)),
        S::Expr::IsNotTrue(inner) => Ok(flag_func("__is_not_true", conv_expr(*inner)?)),
        S::Expr::IsFalse(inner) => Ok(flag_func("__is_false", conv_expr(*inner)?)),
        S::Expr::IsNotFalse(inner) => Ok(flag_func("__is_not_false", conv_expr(*inner)?)),
        S::Expr::IsUnknown(inner) => Ok(Expr::IsNull {
            expr: Box::new(conv_expr(*inner)?),
            negated: false,
        }),
        S::Expr::IsNotUnknown(inner) => Ok(Expr::IsNull {
            expr: Box::new(conv_expr(*inner)?),
            negated: true,
        }),
        S::Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            if escape_char.is_some() {
                return Err(Error::Parse("LIKE ESCAPE is not supported".into()));
            }
            Ok(Expr::Like {
                expr: Box::new(conv_expr(*expr)?),
                pattern: Box::new(conv_expr(*pattern)?),
                negated,
            })
        }
        S::Expr::InList {
            expr,
            list,
            negated,
        } => conv_in_list(*expr, list, negated),
        S::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => conv_between(*expr, *low, *high, negated),
        S::Expr::Function(func) => conv_function(func),
        S::Expr::Array(array) => conv_array(array),
        S::Expr::Cast {
            expr, data_type, ..
        } => {
            let ty = map_type(&data_type)?;
            Ok(Expr::Func {
                name: cast_name(&ty),
                args: vec![conv_expr(*expr)?],
            })
        }
        S::Expr::Wildcard => Err(Error::Parse("bare * is only valid in SELECT or COUNT".into())),
        other => Err(Error::Parse(format!("unsupported expression: {other}"))),
    }
}

fn flag_func(name: &str, expr: Expr) -> Expr {
    Expr::Func {
        name: name.into(),
        args: vec![expr],
    }
}

fn cast_name(ty: &DataType) -> String {
    match ty {
        DataType::Int => "__cast_int".into(),
        DataType::Float => "__cast_float".into(),
        DataType::Bool => "__cast_bool".into(),
        DataType::Text => "__cast_text".into(),
        DataType::Vector(n) => format!("__cast_vector_{n}"),
    }
}

fn conv_in_list(expr: S::Expr, list: Vec<S::Expr>, negated: bool) -> Result<Expr> {
    let left = conv_expr(expr)?;
    if list.is_empty() {
        return Ok(Expr::Literal(Value::Bool(negated)));
    }
    let mut acc: Option<Expr> = None;
    for item in list {
        let right = conv_expr(item)?;
        let cmp = Expr::Binary {
            op: if negated { BinOp::Ne } else { BinOp::Eq },
            left: Box::new(left.clone()),
            right: Box::new(right),
        };
        let op = if negated { BinOp::And } else { BinOp::Or };
        acc = Some(match acc {
            None => cmp,
            Some(prev) => Expr::Binary {
                op,
                left: Box::new(prev),
                right: Box::new(cmp),
            },
        });
    }
    Ok(acc.expect("in-list checked"))
}

fn conv_between(expr: S::Expr, low: S::Expr, high: S::Expr, negated: bool) -> Result<Expr> {
    let expr = conv_expr(expr)?;
    let low = conv_expr(low)?;
    let high = conv_expr(high)?;
    if negated {
        Ok(Expr::Binary {
            op: BinOp::Or,
            left: Box::new(Expr::Binary {
                op: BinOp::Lt,
                left: Box::new(expr.clone()),
                right: Box::new(low),
            }),
            right: Box::new(Expr::Binary {
                op: BinOp::Gt,
                left: Box::new(expr),
                right: Box::new(high),
            }),
        })
    } else {
        Ok(Expr::Binary {
            op: BinOp::And,
            left: Box::new(Expr::Binary {
                op: BinOp::Ge,
                left: Box::new(expr.clone()),
                right: Box::new(low),
            }),
            right: Box::new(Expr::Binary {
                op: BinOp::Le,
                left: Box::new(expr),
                right: Box::new(high),
            }),
        })
    }
}

fn conv_function(func: S::Function) -> Result<Expr> {
    let name = fold(&func.name.to_string());
    if func.filter.is_some() || func.over.is_some() || !func.within_group.is_empty() {
        return Err(Error::Parse(format!(
            "function {name} does not support FILTER, OVER, or WITHIN GROUP"
        )));
    }
    let (args, distinct) = function_args(&func.args)?;
    if distinct {
        return Err(Error::Parse(format!(
            "DISTINCT is not supported in {name}"
        )));
    }
    Ok(Expr::Func { name, args })
}

fn function_args(args: &S::FunctionArguments) -> Result<(Vec<Expr>, bool)> {
    match args {
        S::FunctionArguments::None => Ok((Vec::new(), false)),
        S::FunctionArguments::Subquery(_) => {
            Err(Error::Parse("subquery arguments are not supported".into()))
        }
        S::FunctionArguments::List(list) => {
            let distinct = list.duplicate_treatment.is_some();
            let mut out = Vec::new();
            for arg in &list.args {
                match arg {
                    S::FunctionArg::Unnamed(S::FunctionArgExpr::Wildcard) => {
                        out.push(Expr::Column {
                            table: None,
                            name: "*".into(),
                        });
                    }
                    S::FunctionArg::Unnamed(S::FunctionArgExpr::Expr(expr)) => {
                        out.push(conv_expr(expr.clone())?);
                    }
                    S::FunctionArg::Unnamed(S::FunctionArgExpr::QualifiedWildcard(name)) => {
                        out.push(Expr::Column {
                            table: Some(object_name(name)?),
                            name: "*".into(),
                        });
                    }
                    S::FunctionArg::Named { .. } => {
                        return Err(Error::Parse("named function arguments are not supported".into()))
                    }
                }
            }
            Ok((out, distinct))
        }
    }
}

fn conv_array(array: S::Array) -> Result<Expr> {
    let mut nums = Vec::new();
    for elem in array.elem {
        let value = conv_expr(elem)?;
        let value = fold_numeric_literal(&value)?;
        match value {
            Value::Int(n) => nums.push(n as f32),
            Value::Float(n) => nums.push(n as f32),
            Value::Vector(v) => nums.extend(v),
            other => {
                return Err(Error::Parse(format!(
                    "array elements must be numbers, got {}",
                    other.type_name()
                )))
            }
        }
    }
    Ok(Expr::Literal(Value::Vector(nums)))
}

fn fold_numeric_literal(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match fold_numeric_literal(expr)? {
            Value::Int(n) => Ok(Value::Int(-n)),
            Value::Float(n) => Ok(Value::Float(-n)),
            other => Err(Error::Parse(format!(
                "cannot negate {}",
                other.type_name()
            ))),
        },
        Expr::Unary {
            op: UnaryOp::Not, ..
        } => Err(Error::Parse("NOT is not a numeric literal".into())),
        other => Err(Error::Parse(format!("expected a literal, got {other:?}"))),
    }
}

fn conv_value(value: S::Value) -> Result<Value> {
    match value {
        S::Value::Number(n, _) => parse_number(&n),
        S::Value::SingleQuotedString(s)
        | S::Value::DoubleQuotedString(s)
        | S::Value::NationalStringLiteral(s)
        | S::Value::EscapedStringLiteral(s)
        | S::Value::UnicodeStringLiteral(s)
        | S::Value::TripleSingleQuotedString(s)
        | S::Value::TripleDoubleQuotedString(s) => Ok(Value::Text(s)),
        S::Value::Boolean(b) => Ok(Value::Bool(b)),
        S::Value::Null => Ok(Value::Null),
        S::Value::DollarQuotedString(s) => Ok(Value::Text(s.value)),
        other => Err(Error::Parse(format!("unsupported literal {other}"))),
    }
}

fn parse_number(n: &str) -> Result<Value> {
    let lower = n.to_ascii_lowercase();
    if lower.contains('.') || lower.contains('e') {
        let f: f64 = n
            .parse()
            .map_err(|_| Error::Parse(format!("bad number {n}")))?;
        if f.is_nan() {
            return Err(Error::Type("NaN is not a value".into()));
        }
        Ok(Value::Float(f))
    } else {
        let i: i64 = n
            .parse()
            .map_err(|_| Error::Parse(format!("bad integer {n}")))?;
        Ok(Value::Int(i))
    }
}

fn map_binop(op: S::BinaryOperator) -> Result<BinOp> {
    Ok(match op {
        S::BinaryOperator::Eq => BinOp::Eq,
        S::BinaryOperator::NotEq => BinOp::Ne,
        S::BinaryOperator::Lt => BinOp::Lt,
        S::BinaryOperator::LtEq => BinOp::Le,
        S::BinaryOperator::Gt => BinOp::Gt,
        S::BinaryOperator::GtEq => BinOp::Ge,
        S::BinaryOperator::And => BinOp::And,
        S::BinaryOperator::Or => BinOp::Or,
        S::BinaryOperator::Plus => BinOp::Plus,
        S::BinaryOperator::Minus => BinOp::Minus,
        S::BinaryOperator::Multiply => BinOp::Mul,
        S::BinaryOperator::Divide => BinOp::Div,
        other => return Err(Error::Parse(format!("unsupported operator {other}"))),
    })
}

fn map_unary(op: S::UnaryOperator) -> Result<UnaryOp> {
    match op {
        S::UnaryOperator::Plus => Err(Error::Parse("unary plus is not supported".into())),
        S::UnaryOperator::Minus => Ok(UnaryOp::Neg),
        S::UnaryOperator::Not => Ok(UnaryOp::Not),
        other => Err(Error::Parse(format!("unsupported unary operator {other}"))),
    }
}

fn assignment_columns(target: &S::AssignmentTarget) -> Result<Vec<String>> {
    match target {
        S::AssignmentTarget::ColumnName(name) => Ok(vec![object_name(name)?]),
        S::AssignmentTarget::Tuple(names) => names.iter().map(object_name).collect(),
    }
}

fn expr_column_name(expr: &S::Expr) -> Result<String> {
    match expr {
        S::Expr::Identifier(ident) => Ok(fold(&ident.value)),
        S::Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|i| fold(&i.value))
            .ok_or_else(|| Error::Parse("empty index column".into())),
        other => Err(Error::Parse(format!(
            "index column must be a name, got {other}"
        ))),
    }
}

fn table_with_joins_name(table: &S::TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return Err(Error::Parse("UPDATE of a join is not supported".into()));
    }
    match &table.relation {
        S::TableFactor::Table { name, .. } => object_name(name),
        other => Err(Error::Parse(format!("unsupported UPDATE target {other}"))),
    }
}

fn from_table_name(from: &S::FromTable) -> Result<String> {
    let items = match from {
        S::FromTable::WithFromKeyword(items) | S::FromTable::WithoutKeyword(items) => items,
    };
    if items.len() != 1 {
        return Err(Error::Parse("DELETE requires one table".into()));
    }
    table_with_joins_name(&items[0])
}

fn single_object_name(names: &[S::ObjectName]) -> Result<String> {
    if names.len() != 1 {
        return Err(Error::Parse("expected one object name".into()));
    }
    object_name(&names[0])
}

fn object_name(name: &S::ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(Error::Parse(format!(
            "qualified name {name} is not supported"
        )));
    }
    Ok(fold(&name.0[0].value))
}

fn fold(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn is_comment_only(sql: &str) -> bool {
    strip_comments(sql).trim().is_empty()
}

fn strip_comments(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '-' && chars.get(i + 1) == Some(&'-') {
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            out.push(' ');
            continue;
        }
        if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i = (i + 2).min(chars.len());
            out.push(' ');
            continue;
        }
        if chars[i] == '\'' {
            out.push(chars[i]);
            i += 1;
            while i < chars.len() {
                out.push(chars[i]);
                if chars[i] == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        out.push('\'');
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
        out.push(chars[i]);
        i += 1;
    }
    out
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
        if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
            cur.push('/');
            cur.push('*');
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                cur.push(chars[i]);
                i += 1;
            }
            if i + 1 < chars.len() {
                cur.push('*');
                cur.push('/');
                i += 2;
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
