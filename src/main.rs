use std::collections::{HashMap, BTreeMap};

struct Database {
    keyspaces: HashMap<String, Keyspace>,
}

struct Keyspace {
    tables: HashMap<String, Table>,
}

struct Table {
    columns: Vec<String>,
    rows: Vec<Row>,
}

struct Row {
    values: BTreeMap<String, String>,
}

impl Database {
    fn new() -> Self {
        Database {
            keyspaces: HashMap::new(),
        }
    }

    fn create_keyspace(&mut self, name: String) {
        self.keyspaces.insert(name, Keyspace {
            tables: HashMap::new(),
        });
    }

    fn create_table(&mut self, keyspace: &str, table: String, columns: Vec<String>) {
        if let Some(keyspace) = self.keyspaces.get_mut(keyspace) {
            keyspace.tables.insert(table, Table {
                columns,
                rows: Vec::new(),
            });
        }
    }

    fn insert_row(&mut self, keyspace: &str, table: &str, row: Row) {
        if let Some(keyspace) = self.keyspaces.get_mut(keyspace) {
            if let Some(table) = keyspace.tables.get_mut(table) {
                table.rows.push(row);
            }
        }
    }

    fn select_all(&self, keyspace: &str, table: &str) -> Option<&Vec<Row>> {
        // self.keyspaces.get(keyspace)?.tables.get(table)?.rows.as_ref()
        let key = self.keyspaces.get(keyspace).unwrap();
        let tables = key.tables.get(table).unwrap();
        let rows: &Vec<Row> = tables.rows.as_ref();
        Some(rows)
    }
}

fn main() {
    let mut db = Database::new();

    db.create_keyspace("my_keyspace".to_string());
    db.create_table("my_keyspace", "users".to_string(), vec!["id".to_string(), "name".to_string()]);

    let row1 = Row {
        values: [("id".to_string(), "1".to_string()), ("name".to_string(), "John".to_string())].iter().cloned().collect(),
    };
    db.insert_row("my_keyspace", "users", row1);

    let row2 = Row {
        values: [("id".to_string(), "2".to_string()), ("name".to_string(), "Jane".to_string())].iter().cloned().collect(),
    };
    db.insert_row("my_keyspace", "users", row2);

    if let Some(rows) = db.select_all("my_keyspace", "users") {
        for row in rows {
            println!("{:?}", row.values);
        }
    }
}
