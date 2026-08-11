use aster_core::TableId;
use aster_engine::{CatalogMutation, Engine, Transaction};
use aster_sql::ast::Ident;
use aster_sql::{Catalog, IndexDef, TableDef};

/// Immutable SQL-planning view of the committed catalog plus catalog objects
/// staged by one explicit transaction.
#[derive(Debug, Clone, Default)]
pub(crate) struct EngineCatalog {
    tables: Vec<TableDef>,
    indexes: Vec<IndexDef>,
}

impl EngineCatalog {
    pub(crate) fn new(engine: &Engine, transaction: Option<&Transaction>) -> Self {
        let mut tables: Vec<_> = engine
            .catalog()
            .tables()
            .map(|table| TableDef {
                id: table.id,
                name: table.name.clone(),
                schema: table.schema.clone(),
            })
            .collect();
        let mut indexes: Vec<_> = engine
            .catalog()
            .indexes()
            .map(|index| IndexDef {
                id: index.id,
                name: index.name.clone(),
                table_id: index.table_id,
                column_index: index.column,
            })
            .collect();

        if let Some(transaction) = transaction {
            // Transaction intentionally exposes its staged catalog only on the
            // canonical attempt boundary. Cloning here is side-effect free.
            let attempt = transaction.clone().into_attempt(
                aster_core::ClientRequestId {
                    client_id: [0; 16],
                    sequence: 1,
                },
                0,
            );
            for mutation in attempt.catalog_mutations() {
                match mutation {
                    CatalogMutation::CreateTable(table) => tables.push(TableDef {
                        id: table.id,
                        name: table.name.clone(),
                        schema: table.schema.clone(),
                    }),
                    CatalogMutation::CreateIndex(index) => indexes.push(IndexDef {
                        id: index.id,
                        name: index.name.clone(),
                        table_id: index.table_id,
                        column_index: index.column,
                    }),
                }
            }
        }
        tables.sort_by_key(|table| table.id);
        indexes.sort_by_key(|index| index.id);
        Self { tables, indexes }
    }
}

impl Catalog for EngineCatalog {
    fn resolve_table(&self, name: &Ident) -> Option<TableDef> {
        self.tables
            .iter()
            .find(|table| name.matches(&table.name))
            .cloned()
    }

    fn resolve_index(&self, name: &Ident) -> Option<IndexDef> {
        self.indexes
            .iter()
            .find(|index| name.matches(&index.name))
            .cloned()
    }

    fn indexes_for_table(&self, table_id: TableId) -> Vec<IndexDef> {
        self.indexes
            .iter()
            .filter(|index| index.table_id == table_id)
            .cloned()
            .collect()
    }
}
