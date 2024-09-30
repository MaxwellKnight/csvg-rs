use crate::{config, csv::DataFrame, graph, sql};
use sqlparser::{
    ast::{AlterTableOperation, ColumnOption, Statement, TableConstraint},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{
    error::Error,
    path::{Path, PathBuf},
};

/// Parses SQL content and extracts table definitions.
pub fn parse_sql(contents: &str) -> Result<Vec<DataFrame>, Box<dyn Error>> {
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, &contents)?;
    let mut tables = ast
        .clone()
        .into_iter()
        .filter_map(parse_statement)
        .collect();

    parse_alter_table(&mut tables, &ast);

    Ok(tables)
}

fn parse_statement(statement: Statement) -> Option<DataFrame> {
    match statement {
        Statement::CreateTable(create_table) => Some(parse_create_table(&create_table)),
        _ => None,
    }
}

fn parse_create_table(create_table: &sqlparser::ast::CreateTable) -> DataFrame {
    let mut table = DataFrame::new(create_table.name.to_string().to_lowercase());
    parse_columns(&mut table, &create_table.columns);
    parse_constraints(&mut table, &create_table.constraints);
    table
}

fn parse_columns(table: &mut DataFrame, columns: &[sqlparser::ast::ColumnDef]) {
    for (i, column) in columns.iter().enumerate() {
        table.headers.insert(i, column.name.value.to_owned());
        for definition in &column.options {
            if let ColumnOption::Unique {
                is_primary: true, ..
            } = definition.option
            {
                table.primary_key = Some(column.name.value.to_lowercase().to_owned());
            }
        }
    }
}

fn parse_constraints(table: &mut DataFrame, constraints: &[TableConstraint]) {
    for constraint in constraints {
        parse_constraint(table, constraint);
    }
}

fn parse_constraint(table: &mut DataFrame, constraint: &TableConstraint) {
    if let TableConstraint::ForeignKey {
        columns,
        foreign_table,
        referred_columns,
        ..
    } = constraint
    {
        let ident = foreign_table.0.last().unwrap();
        let fks = [ident.clone()];

        table
            .foreign_keys
            .extend(columns.iter().zip(&fks).zip(referred_columns.iter()).map(
                |((src_column, dst_table), dst_column)| {
                    (
                        src_column.value.to_lowercase().to_owned(),
                        dst_table.value.to_lowercase().to_owned(),
                        dst_column.value.to_lowercase().to_owned(),
                    )
                },
            ));
    }
}

fn parse_alter_table(tables: &mut Vec<DataFrame>, ast: &Vec<Statement>) {
    for statement in ast {
        match &statement {
            Statement::AlterTable {
                name, operations, ..
            } => {
                if let Some(table_index) = tables
                    .iter()
                    .position(|t| t.name == name.0.last().unwrap().value.to_lowercase())
                {
                    for op in operations {
                        match op {
                            AlterTableOperation::AddConstraint(constraint) => {
                                let table = &mut tables[table_index];
                                parse_constraint(table, constraint);
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

pub fn process_sql_schema(
    schema_path: &Path,
    config_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_content = std::fs::read_to_string(schema_path)
        .map_err(|e| format!("Failed to read schema file: {}", e))?;
    let result =
        sql::parse_sql(&schema_content).map_err(|e| format!("Failed to parse SQL: {}", e))?;
    let g = graph::create_graph(result);
    config::write_graph_cache(&g, config_dir)
        .map_err(|e| format!("Failed to write graph cache: {}", e))?;
    println!(
        "Graph data cached in {}",
        config::display_relative_path(&config_dir.join("graph.json"))
    );
    Ok(())
}
