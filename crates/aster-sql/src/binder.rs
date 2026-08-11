use std::collections::BTreeSet;

use aster_core::{Column, DataType, Schema, Value};

use crate::ast::{
    AggregateArgument, AggregateFunction, BinaryOp, Expr, ExprKind, Ident, SelectItem, Span,
    Spanned, Statement, TableSource, UnaryOp,
};
use crate::catalog::{Catalog, TableDef};
use crate::plan::{
    BoundColumn, BoundExpr, BoundExprKind, BoundOrder, BoundStatement, BoundTable, LogicalPlan,
    NamedExpr, ParameterSpec, SqlType, TransactionStatement,
};
use crate::{Result, SqlError, SqlErrorKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindOutput {
    pub statement: BoundStatement,
    pub parameters: Vec<ParameterSpec>,
}

pub fn bind(statement: &Spanned<Statement>, catalog: &dyn Catalog) -> Result<BindOutput> {
    Binder::new(catalog).bind(statement)
}

pub struct Binder<'a> {
    catalog: &'a dyn Catalog,
    parameters: Vec<ParameterSpec>,
}

impl<'a> Binder<'a> {
    #[must_use]
    pub const fn new(catalog: &'a dyn Catalog) -> Self {
        Self {
            catalog,
            parameters: Vec::new(),
        }
    }

    pub fn bind(mut self, statement: &Spanned<Statement>) -> Result<BindOutput> {
        let statement = self.bind_statement(statement)?;
        self.parameters.sort_by_key(|parameter| parameter.index);
        Ok(BindOutput {
            statement,
            parameters: self.parameters,
        })
    }

    fn bind_statement(&mut self, statement: &Spanned<Statement>) -> Result<BoundStatement> {
        match &statement.value {
            Statement::CreateTable(create) => {
                if self.catalog.resolve_table(&create.name).is_some()
                    || self.catalog.resolve_index(&create.name).is_some()
                {
                    return Err(Self::bind_error(
                        format!("catalog object `{}` already exists", create.name),
                        create.name.span,
                    ));
                }
                let schema = Schema {
                    columns: create
                        .columns
                        .iter()
                        .map(|column| Column {
                            name: column.name.value.clone(),
                            data_type: column.data_type,
                            nullable: column.nullable,
                            primary_key: column.primary_key,
                        })
                        .collect(),
                };
                schema.validate().map_err(|error| {
                    SqlError::new(SqlErrorKind::Constraint, error.to_string(), statement.span)
                })?;
                Ok(BoundStatement::CreateTable {
                    name: create.name.value.clone(),
                    schema,
                })
            }
            Statement::CreateIndex(create) => {
                if self.catalog.resolve_index(&create.name).is_some()
                    || self.catalog.resolve_table(&create.name).is_some()
                {
                    return Err(Self::bind_error(
                        format!("catalog object `{}` already exists", create.name),
                        create.name.span,
                    ));
                }
                let table = self.require_table(&create.table)?;
                let column_index = find_column(&table, &create.column).ok_or_else(|| {
                    Self::bind_error(
                        format!(
                            "column `{}` does not exist in table `{}`",
                            create.column, table.name
                        ),
                        create.column.span,
                    )
                })?;
                Ok(BoundStatement::CreateIndex {
                    name: create.name.value.clone(),
                    table,
                    column_index,
                })
            }
            Statement::Insert(insert) => self.bind_insert(insert),
            Statement::Update(update) => self.bind_update(update),
            Statement::Delete(delete) => self.bind_delete(delete),
            Statement::Select(select) => self.bind_select(select),
            Statement::Begin => Ok(BoundStatement::Transaction(TransactionStatement::Begin)),
            Statement::Commit => Ok(BoundStatement::Transaction(TransactionStatement::Commit)),
            Statement::Rollback => Ok(BoundStatement::Transaction(TransactionStatement::Rollback)),
            Statement::Explain(inner) => {
                let bound = self.bind_statement(inner)?;
                if matches!(bound, BoundStatement::Transaction(_)) {
                    return Err(SqlError::unsupported(
                        "EXPLAIN does not accept transaction-control statements",
                        inner.span,
                    ));
                }
                Ok(BoundStatement::Explain(Box::new(bound)))
            }
        }
    }

    fn bind_insert(&mut self, insert: &crate::ast::Insert) -> Result<BoundStatement> {
        let table = self.require_table(&insert.table)?;
        let target_columns = if let Some(columns) = &insert.columns {
            let mut seen = BTreeSet::new();
            let mut targets = Vec::with_capacity(columns.len());
            for column in columns {
                let index = find_column(&table, column).ok_or_else(|| {
                    Self::bind_error(
                        format!("column `{column}` does not exist in table `{}`", table.name),
                        column.span,
                    )
                })?;
                if !seen.insert(index) {
                    return Err(Self::bind_error(
                        format!("column `{column}` appears more than once in INSERT"),
                        column.span,
                    ));
                }
                targets.push(index);
            }
            targets
        } else {
            (0..table.schema.columns.len()).collect()
        };
        if target_columns.len() != insert.values.len() {
            return Err(SqlError::new(
                SqlErrorKind::Constraint,
                format!(
                    "INSERT names {} target columns but supplies {} values",
                    target_columns.len(),
                    insert.values.len()
                ),
                insert.table.span,
            ));
        }

        let mut normalized: Vec<Option<BoundExpr>> = vec![None; table.schema.columns.len()];
        let scope = Scope::default();
        for (&target, value) in target_columns.iter().zip(&insert.values) {
            let column = &table.schema.columns[target];
            let bound = self.bind_expr(
                value,
                &scope,
                Some(column.data_type),
                AggregatePolicy::Forbidden,
            )?;
            reject_null_for_non_nullable(&bound, column, value.span)?;
            if !column.nullable {
                require_direct_parameter_value(&bound, &mut self.parameters);
            }
            normalized[target] = Some(bound);
        }
        let mut values = Vec::with_capacity(normalized.len());
        for (index, value) in normalized.into_iter().enumerate() {
            if let Some(value) = value {
                values.push(value);
            } else {
                let column = &table.schema.columns[index];
                if !column.nullable {
                    return Err(SqlError::new(
                        SqlErrorKind::Constraint,
                        format!("INSERT omits required column `{}`", column.name),
                        insert.table.span,
                    ));
                }
                values.push(Spanned::new(
                    BoundExprKind::Literal(Value::Null),
                    insert.table.span,
                ));
            }
        }
        Ok(BoundStatement::Insert { table, values })
    }

    fn bind_update(&mut self, update: &crate::ast::Update) -> Result<BoundStatement> {
        let table = self.require_table(&update.table)?;
        let scope = Scope::single(table.clone(), table.name.clone(), 0);
        let mut seen = BTreeSet::new();
        let mut assignments = Vec::with_capacity(update.assignments.len());
        for assignment in &update.assignments {
            let column_index = find_column(&table, &assignment.column).ok_or_else(|| {
                Self::bind_error(
                    format!(
                        "column `{}` does not exist in table `{}`",
                        assignment.column, table.name
                    ),
                    assignment.column.span,
                )
            })?;
            if !seen.insert(column_index) {
                return Err(Self::bind_error(
                    format!("column `{}` is assigned more than once", assignment.column),
                    assignment.column.span,
                ));
            }
            let column = &table.schema.columns[column_index];
            let value = self.bind_expr(
                &assignment.value,
                &scope,
                Some(column.data_type),
                AggregatePolicy::Forbidden,
            )?;
            reject_null_for_non_nullable(&value, column, assignment.value.span)?;
            if !column.nullable {
                require_direct_parameter_value(&value, &mut self.parameters);
            }
            assignments.push((column_index, value));
        }
        let selection = update
            .selection
            .as_ref()
            .map(|expr| self.bind_predicate(expr, &scope, "UPDATE WHERE"))
            .transpose()?;
        Ok(BoundStatement::Update {
            table,
            assignments,
            selection,
        })
    }

    fn bind_delete(&mut self, delete: &crate::ast::Delete) -> Result<BoundStatement> {
        let table = self.require_table(&delete.table)?;
        let scope = Scope::single(table.clone(), table.name.clone(), 0);
        let selection = delete
            .selection
            .as_ref()
            .map(|expr| self.bind_predicate(expr, &scope, "DELETE WHERE"))
            .transpose()?;
        Ok(BoundStatement::Delete { table, selection })
    }

    #[allow(clippy::too_many_lines)]
    fn bind_select(&mut self, select: &crate::ast::Select) -> Result<BoundStatement> {
        let (first_table, first_bound) = self.bind_table_source(&select.from, 0)?;
        let mut scope = Scope::default();
        scope.push(first_bound.clone(), select.from.span)?;
        let mut plan = LogicalPlan::Scan(first_bound);

        for join in &select.joins {
            let offset = scope.total_columns();
            let (_, right) = self.bind_table_source(&join.right, offset)?;
            scope.push(right.clone(), join.right.span)?;
            let on = self.bind_predicate(&join.on, &scope, "JOIN ON")?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(LogicalPlan::Scan(right)),
                on,
            };
        }

        if let Some(selection) = &select.selection {
            let predicate = self.bind_predicate(selection, &scope, "SELECT WHERE")?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        let mut group_by = Vec::with_capacity(select.group_by.len());
        for expression in &select.group_by {
            group_by.push(self.bind_expr(expression, &scope, None, AggregatePolicy::Forbidden)?);
        }

        let mut output = Vec::new();
        for (position, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(span) => {
                    for table in &scope.tables {
                        for (column_index, column) in table.table.schema.columns.iter().enumerate()
                        {
                            output.push(NamedExpr {
                                expr: Spanned::new(
                                    BoundExprKind::Column(BoundColumn {
                                        table_id: table.table.id,
                                        table_name: table.table.name.clone(),
                                        table_alias: table.alias.clone(),
                                        column_index,
                                        input_slot: table.slot_offset + column_index,
                                        column: column.clone(),
                                    }),
                                    *span,
                                ),
                                name: column.name.clone(),
                            });
                        }
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    let bound = self.bind_expr(expr, &scope, None, AggregatePolicy::Allowed)?;
                    output.push(NamedExpr {
                        name: alias.as_ref().map_or_else(
                            || expression_name(&bound, position),
                            |alias| alias.value.clone(),
                        ),
                        expr: bound,
                    });
                }
            }
        }

        let mut order_by = Vec::with_capacity(select.order_by.len());
        for order in &select.order_by {
            order_by.push(BoundOrder {
                expr: self.bind_order_expression(&order.expr, &scope, &output)?,
                direction: order.direction,
            });
        }

        let mut aggregates = Vec::new();
        for named in &output {
            collect_aggregates(&named.expr, &mut aggregates);
        }
        for order in &order_by {
            collect_aggregates(&order.expr, &mut aggregates);
        }
        deduplicate_expressions(&mut aggregates);
        let aggregate_query = !aggregates.is_empty() || !group_by.is_empty();
        if aggregate_query {
            for named in &output {
                if !group_compatible(&named.expr, &group_by) {
                    return Err(SqlError::new(
                        SqlErrorKind::Type,
                        format!(
                            "projection `{}` references a non-grouped column outside an aggregate",
                            named.name
                        ),
                        named.expr.span,
                    ));
                }
            }
            for order in &order_by {
                if !group_compatible(&order.expr, &group_by) {
                    return Err(SqlError::new(
                        SqlErrorKind::Type,
                        "ORDER BY references a non-grouped column outside an aggregate",
                        order.expr.span,
                    ));
                }
            }
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };
        }
        if !order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by,
            };
        }
        if let Some(limit) = &select.limit {
            let limit = self.bind_expr(
                limit,
                &Scope::default(),
                Some(DataType::Int64),
                AggregatePolicy::Forbidden,
            )?;
            match &limit.value {
                BoundExprKind::Literal(Value::Int64(value)) if *value >= 0 => {}
                BoundExprKind::Parameter { .. } => {}
                _ => {
                    return Err(SqlError::new(
                        SqlErrorKind::Constraint,
                        "LIMIT must be a non-negative INT64 literal or parameter",
                        limit.span,
                    ));
                }
            }
            require_direct_parameter_value(&limit, &mut self.parameters);
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
            };
        }
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            expressions: output.clone(),
        };
        let _ = first_table;
        Ok(BoundStatement::Query { plan, output })
    }

    fn bind_predicate(&mut self, expr: &Expr, scope: &Scope, context: &str) -> Result<BoundExpr> {
        let bound = self.bind_expr(
            expr,
            scope,
            Some(DataType::Bool),
            AggregatePolicy::Forbidden,
        )?;
        if bound.sql_type().data_type != Some(DataType::Bool) {
            return Err(SqlError::new(
                SqlErrorKind::Type,
                format!("{context} requires a BOOL expression"),
                expr.span,
            ));
        }
        Ok(bound)
    }

    fn bind_order_expression(
        &mut self,
        expression: &Expr,
        scope: &Scope,
        output: &[NamedExpr],
    ) -> Result<BoundExpr> {
        if let ExprKind::Column {
            qualifier: None,
            name,
        } = &expression.value
        {
            let aliases: Vec<_> = output
                .iter()
                .filter(|candidate| name.matches(&candidate.name))
                .collect();
            match aliases.as_slice() {
                [alias] => {
                    let mut expression = alias.expr.clone();
                    expression.span = name.span;
                    return Ok(expression);
                }
                [_, _, ..] => {
                    return Err(SqlError::new(
                        SqlErrorKind::Bind,
                        format!("ORDER BY name `{name}` is ambiguous"),
                        name.span,
                    ));
                }
                [] => {}
            }
        }
        self.bind_expr(expression, scope, None, AggregatePolicy::Allowed)
    }

    #[allow(clippy::too_many_lines)]
    fn bind_expr(
        &mut self,
        expr: &Expr,
        scope: &Scope,
        expected: Option<DataType>,
        aggregate_policy: AggregatePolicy,
    ) -> Result<BoundExpr> {
        let mut bound = match &expr.value {
            ExprKind::Literal(value) => {
                Spanned::new(BoundExprKind::Literal(value.clone()), expr.span)
            }
            ExprKind::Parameter(index) => {
                self.record_parameter(*index, expected, expr.span)?;
                Spanned::new(
                    BoundExprKind::Parameter {
                        index: *index,
                        data_type: expected,
                    },
                    expr.span,
                )
            }
            ExprKind::Column { qualifier, name } => {
                let column = scope.resolve_column(qualifier.as_ref(), name)?;
                Spanned::new(BoundExprKind::Column(column), expr.span)
            }
            ExprKind::Unary { op, expr: inner } => {
                let required = match op {
                    UnaryOp::Not => DataType::Bool,
                    UnaryOp::Negate => DataType::Int64,
                };
                let inner = self.bind_expr(inner, scope, Some(required), aggregate_policy)?;
                Spanned::new(
                    BoundExprKind::Unary {
                        op: *op,
                        expr: Box::new(inner),
                    },
                    expr.span,
                )
            }
            ExprKind::Binary { left, op, right } if matches!(op, BinaryOp::And | BinaryOp::Or) => {
                let left = self.bind_expr(left, scope, Some(DataType::Bool), aggregate_policy)?;
                let right = self.bind_expr(right, scope, Some(DataType::Bool), aggregate_policy)?;
                Spanned::new(
                    BoundExprKind::Binary {
                        left: Box::new(left),
                        op: *op,
                        right: Box::new(right),
                    },
                    expr.span,
                )
            }
            ExprKind::Binary { left, op, right } if op.is_comparison() => {
                let mut left = self.bind_expr(left, scope, None, aggregate_policy)?;
                let mut right =
                    self.bind_expr(right, scope, left.sql_type().data_type, aggregate_policy)?;
                unify_comparison_types(&mut left, &mut right, &mut self.parameters, expr.span)?;
                Spanned::new(
                    BoundExprKind::Binary {
                        left: Box::new(left),
                        op: *op,
                        right: Box::new(right),
                    },
                    expr.span,
                )
            }
            ExprKind::Binary { .. } => unreachable!("AST has no other binary operators"),
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => {
                let inner = self.bind_expr(inner, scope, None, aggregate_policy)?;
                Spanned::new(
                    BoundExprKind::IsNull {
                        expr: Box::new(inner),
                        negated: *negated,
                    },
                    expr.span,
                )
            }
            ExprKind::Aggregate { function, argument } => {
                if aggregate_policy != AggregatePolicy::Allowed {
                    return Err(SqlError::new(
                        SqlErrorKind::Type,
                        "aggregate functions are not allowed in this clause",
                        expr.span,
                    ));
                }
                let argument = match argument {
                    AggregateArgument::Star => None,
                    AggregateArgument::Expr(argument) => {
                        if contains_aggregate(argument) {
                            return Err(SqlError::new(
                                SqlErrorKind::Type,
                                "nested aggregate functions are not supported",
                                argument.span,
                            ));
                        }
                        let required =
                            (*function == AggregateFunction::Sum).then_some(DataType::Int64);
                        Some(Box::new(self.bind_expr(
                            argument,
                            scope,
                            required,
                            AggregatePolicy::Forbidden,
                        )?))
                    }
                };
                Spanned::new(
                    BoundExprKind::Aggregate {
                        function: *function,
                        argument,
                    },
                    expr.span,
                )
            }
        };
        self.enforce_expected(&mut bound, expected)?;
        Ok(bound)
    }

    fn enforce_expected(
        &mut self,
        expression: &mut BoundExpr,
        expected: Option<DataType>,
    ) -> Result<()> {
        let Some(expected) = expected else {
            return Ok(());
        };
        if let Some(actual) = expression.sql_type().data_type {
            if actual != expected {
                return Err(SqlError::new(
                    SqlErrorKind::Type,
                    format!("expected {expected}, found {actual}"),
                    expression.span,
                ));
            }
        } else {
            constrain_direct_parameter(expression, expected, &mut self.parameters)?;
        }
        Ok(())
    }

    fn record_parameter(
        &mut self,
        index: usize,
        data_type: Option<DataType>,
        span: Span,
    ) -> Result<()> {
        if let Some(parameter) = self
            .parameters
            .iter_mut()
            .find(|parameter| parameter.index == index)
        {
            if let (Some(existing), Some(new)) = (parameter.data_type, data_type)
                && existing != new
            {
                return Err(SqlError::new(
                    SqlErrorKind::Type,
                    format!(
                        "parameter ?{} is constrained as both {existing} and {new}",
                        index + 1
                    ),
                    span,
                ));
            }
            parameter.data_type = parameter.data_type.or(data_type);
        } else {
            self.parameters.push(ParameterSpec {
                index,
                data_type,
                nullable: true,
            });
        }
        Ok(())
    }

    fn bind_table_source(
        &self,
        source: &TableSource,
        slot_offset: usize,
    ) -> Result<(TableDef, BoundTable)> {
        let table = self.require_table(&source.name)?;
        let alias = source
            .alias
            .as_ref()
            .map_or_else(|| table.name.clone(), |alias| alias.value.clone());
        Ok((
            table.clone(),
            BoundTable {
                table,
                alias,
                slot_offset,
            },
        ))
    }

    fn require_table(&self, name: &Ident) -> Result<TableDef> {
        self.catalog
            .resolve_table(name)
            .ok_or_else(|| Self::bind_error(format!("table `{name}` does not exist"), name.span))
    }

    fn bind_error(message: impl Into<String>, span: Span) -> SqlError {
        SqlError::new(SqlErrorKind::Bind, message, span)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregatePolicy {
    Allowed,
    Forbidden,
}

#[derive(Debug, Clone, Default)]
struct Scope {
    tables: Vec<BoundTable>,
}

impl Scope {
    fn single(table: TableDef, alias: String, slot_offset: usize) -> Self {
        Self {
            tables: vec![BoundTable {
                table,
                alias,
                slot_offset,
            }],
        }
    }

    fn push(&mut self, table: BoundTable, span: Span) -> Result<()> {
        if self
            .tables
            .iter()
            .any(|existing| existing.alias.eq_ignore_ascii_case(&table.alias))
        {
            return Err(SqlError::new(
                SqlErrorKind::Bind,
                format!("duplicate table alias `{}`", table.alias),
                span,
            ));
        }
        self.tables.push(table);
        Ok(())
    }

    fn total_columns(&self) -> usize {
        self.tables.last().map_or(0, |table| {
            table.slot_offset + table.table.schema.columns.len()
        })
    }

    fn resolve_column(&self, qualifier: Option<&Ident>, name: &Ident) -> Result<BoundColumn> {
        let candidates: Vec<_> = self
            .tables
            .iter()
            .filter(|table| {
                qualifier.is_none_or(|qualifier| {
                    if qualifier.quoted {
                        qualifier.value == table.alias
                    } else {
                        qualifier.value.eq_ignore_ascii_case(&table.alias)
                    }
                })
            })
            .filter_map(|table| {
                table
                    .table
                    .schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| name.matches(&column.name))
                    .map(|(column_index, column)| BoundColumn {
                        table_id: table.table.id,
                        table_name: table.table.name.clone(),
                        table_alias: table.alias.clone(),
                        column_index,
                        input_slot: table.slot_offset + column_index,
                        column: column.clone(),
                    })
            })
            .collect();
        match candidates.as_slice() {
            [column] => Ok(column.clone()),
            [] => {
                let display_name = qualifier.map_or_else(
                    || name.to_string(),
                    |qualifier| format!("{qualifier}.{name}"),
                );
                Err(SqlError::new(
                    SqlErrorKind::Bind,
                    format!("column `{display_name}` does not exist in the query scope"),
                    name.span,
                ))
            }
            _ => Err(SqlError::new(
                SqlErrorKind::Bind,
                format!("column `{name}` is ambiguous"),
                name.span,
            )),
        }
    }
}

fn find_column(table: &TableDef, name: &Ident) -> Option<usize> {
    table
        .schema
        .columns
        .iter()
        .position(|column| name.matches(&column.name))
}

fn reject_null_for_non_nullable(expr: &BoundExpr, column: &Column, span: Span) -> Result<()> {
    if !column.nullable && matches!(expr.value, BoundExprKind::Literal(Value::Null)) {
        return Err(SqlError::new(
            SqlErrorKind::Constraint,
            format!("column `{}` is not nullable", column.name),
            span,
        ));
    }
    Ok(())
}

fn constrain_direct_parameter(
    expression: &mut BoundExpr,
    expected: DataType,
    parameters: &mut [ParameterSpec],
) -> Result<()> {
    if let BoundExprKind::Parameter { index, data_type } = &mut expression.value {
        if let Some(actual) = *data_type
            && actual != expected
        {
            return Err(SqlError::new(
                SqlErrorKind::Type,
                format!("parameter ?{} expects {actual}, not {expected}", *index + 1),
                expression.span,
            ));
        }
        *data_type = Some(expected);
        if let Some(parameter) = parameters.iter_mut().find(|item| item.index == *index) {
            parameter.data_type = Some(expected);
        }
    }
    Ok(())
}

fn unify_comparison_types(
    left: &mut BoundExpr,
    right: &mut BoundExpr,
    parameters: &mut [ParameterSpec],
    span: Span,
) -> Result<()> {
    let left_type = left.sql_type().data_type;
    let right_type = right.sql_type().data_type;
    match (left_type, right_type) {
        (Some(left_type), Some(right_type)) if left_type != right_type => Err(SqlError::new(
            SqlErrorKind::Type,
            format!("cannot compare {left_type} with {right_type}"),
            span,
        )),
        (Some(data_type), None) => constrain_direct_parameter(right, data_type, parameters),
        (None, Some(data_type)) => constrain_direct_parameter(left, data_type, parameters),
        (None, None)
            if !matches!(left.value, BoundExprKind::Literal(Value::Null))
                && !matches!(right.value, BoundExprKind::Literal(Value::Null)) =>
        {
            Err(SqlError::new(
                SqlErrorKind::Type,
                "comparison operand types cannot be inferred",
                span,
            ))
        }
        _ => Ok(()),
    }
}

fn require_direct_parameter_value(expression: &BoundExpr, parameters: &mut [ParameterSpec]) {
    if let BoundExprKind::Parameter { index, .. } = expression.value
        && let Some(parameter) = parameters.iter_mut().find(|item| item.index == index)
    {
        parameter.nullable = false;
    }
}

fn contains_aggregate(expr: &Expr) -> bool {
    match &expr.value {
        ExprKind::Aggregate { .. } => true,
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => contains_aggregate(expr),
        ExprKind::Binary { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Column { .. } => false,
    }
}

fn collect_aggregates(expr: &BoundExpr, output: &mut Vec<BoundExpr>) {
    match &expr.value {
        BoundExprKind::Aggregate { .. } => output.push(expr.clone()),
        BoundExprKind::Unary { expr, .. } | BoundExprKind::IsNull { expr, .. } => {
            collect_aggregates(expr, output);
        }
        BoundExprKind::Binary { left, right, .. } => {
            collect_aggregates(left, output);
            collect_aggregates(right, output);
        }
        BoundExprKind::Literal(_) | BoundExprKind::Parameter { .. } | BoundExprKind::Column(_) => {}
    }
}

fn deduplicate_expressions(expressions: &mut Vec<BoundExpr>) {
    let mut unique = Vec::new();
    for expression in expressions.drain(..) {
        if !unique
            .iter()
            .any(|existing| same_expression(existing, &expression))
        {
            unique.push(expression);
        }
    }
    *expressions = unique;
}

fn group_compatible(expr: &BoundExpr, group_by: &[BoundExpr]) -> bool {
    if group_by.iter().any(|group| same_expression(expr, group)) {
        return true;
    }
    match &expr.value {
        BoundExprKind::Aggregate { .. }
        | BoundExprKind::Literal(_)
        | BoundExprKind::Parameter { .. } => true,
        BoundExprKind::Column(_) => false,
        BoundExprKind::Unary { expr, .. } | BoundExprKind::IsNull { expr, .. } => {
            group_compatible(expr, group_by)
        }
        BoundExprKind::Binary { left, right, .. } => {
            group_compatible(left, group_by) && group_compatible(right, group_by)
        }
    }
}

fn same_expression(left: &BoundExpr, right: &BoundExpr) -> bool {
    match (&left.value, &right.value) {
        (BoundExprKind::Literal(left), BoundExprKind::Literal(right)) => left == right,
        (
            BoundExprKind::Parameter { index: left, .. },
            BoundExprKind::Parameter { index: right, .. },
        ) => left == right,
        (BoundExprKind::Column(left), BoundExprKind::Column(right)) => {
            left.table_id == right.table_id && left.column_index == right.column_index
        }
        (
            BoundExprKind::Unary {
                op: left_op,
                expr: left,
            },
            BoundExprKind::Unary {
                op: right_op,
                expr: right,
            },
        ) => left_op == right_op && same_expression(left, right),
        (
            BoundExprKind::Binary {
                left: left_a,
                op: left_op,
                right: left_b,
            },
            BoundExprKind::Binary {
                left: right_a,
                op: right_op,
                right: right_b,
            },
        ) => {
            left_op == right_op
                && same_expression(left_a, right_a)
                && same_expression(left_b, right_b)
        }
        (
            BoundExprKind::IsNull {
                expr: left,
                negated: left_negated,
            },
            BoundExprKind::IsNull {
                expr: right,
                negated: right_negated,
            },
        ) => left_negated == right_negated && same_expression(left, right),
        (
            BoundExprKind::Aggregate {
                function: left_function,
                argument: left_argument,
            },
            BoundExprKind::Aggregate {
                function: right_function,
                argument: right_argument,
            },
        ) => {
            left_function == right_function
                && match (left_argument, right_argument) {
                    (None, None) => true,
                    (Some(left), Some(right)) => same_expression(left, right),
                    _ => false,
                }
        }
        _ => false,
    }
}

fn expression_name(expr: &BoundExpr, position: usize) -> String {
    match &expr.value {
        BoundExprKind::Column(column) => column.column.name.clone(),
        BoundExprKind::Aggregate { function, .. } => match function {
            AggregateFunction::Count => "count".into(),
            AggregateFunction::Sum => "sum".into(),
            AggregateFunction::Min => "min".into(),
            AggregateFunction::Max => "max".into(),
        },
        _ => format!("expr{}", position + 1),
    }
}

#[must_use]
pub fn output_schema(output: &[NamedExpr]) -> Vec<(String, SqlType)> {
    output
        .iter()
        .map(|expression| (expression.name.clone(), expression.expr.sql_type()))
        .collect()
}
