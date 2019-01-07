use crate::component::database::Database;
use crate::component::database::DatabaseError;
use crate::component::table::Table;
use crate::sql::query::NodePtr;
use crate::sql::query::QueryData;
use std::collections::HashSet;
use std::fmt;

#[derive(Debug)]
pub struct SQL {
    pub user: User,
    pub database: Database,
    pub querydata: QueryData,
    pub result_json: String,
}

#[derive(Debug)]
pub struct User {
    pub name: String,
    pub key: i32,
}
impl User {
    pub fn new(username: &str) -> User {
        User {
            name: username.to_string(),
            key: 0,
        }
    }
}

#[derive(Debug)]
pub enum SQLError {
    CauserByDatabase(DatabaseError),
    SemanticError(String),
}

impl fmt::Display for SQLError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SQLError::CauserByDatabase(ref e) => write!(f, "{}", e),
            SQLError::SemanticError(ref s) => write!(f, "semantic error: {}", s),
        }
    }
}

impl SQL {
    pub fn new(username: &str) -> Result<SQL, SQLError> {
        Ok(SQL {
            user: User::new(username),
            database: Database::new(""), // empty db
            querydata: QueryData::new(),
            result_json: "".to_string(),
        })
    }

    // Create a new database
    pub fn create_database(&mut self, db_name: &str) -> Result<(), SQLError> {
        self.database = Database::new(db_name);
        Ok(())
    }

    /// Load a database
    pub fn load_database(&mut self, db_name: &str) -> Result<(), SQLError> {
        self.database = Database::load_db(&self.user.name, db_name).map_err(|e| SQLError::CauserByDatabase(e))?;
        Ok(())
    }

    // TODO: check db delete bit
    /// Load the database and create a new table
    pub fn create_table(&mut self, table: &Table) -> Result<(), SQLError> {
        self.database.insert_new_table(table.clone());
        Ok(())
    }

    // TODO: check db, table delete bit
    /// Insert new rows into the table
    pub fn insert_into_table(
        &mut self,
        table_name: &str,
        attrs: Vec<String>,
        rows: Vec<Vec<String>>,
    ) -> Result<(), SQLError> {
        let table = self
            .database
            .tables
            .get_mut(table_name)
            .ok_or(SQLError::SemanticError("table not exists".to_string()))?;
        if table.public_key == 0 {
            table.public_key = self.user.key;
        }

        for row in rows {
            let mut row_in_pair: Vec<(&str, &str)> = Vec::new();
            for i in 0..attrs.len() {
                row_in_pair.push((&attrs[i], &row[i]));
            }
            table
                .insert_row(row_in_pair)
                .map_err(|e| SQLError::SemanticError(format!("{}", e)))?;
        }

        Ok(())
    }

    /// Handle the `select` query
    ///
    /// Syntax:
    ///
    /// ```sql
    /// (8)  SELECT (9) DISTINCT (11) TOP <top_specification> <select_list>
    /// (1)  FROM <left_table>
    /// (3)       <join_type> JOIN <right_table>
    /// (2)       ON <join_condition>
    /// (4)  WHERE <where_condition>
    /// (5)  GROUP BY <group_by_list>
    /// (6)  WITH {CUBE | ROLLUP}
    /// (7)  HAVING <having_condition>
    /// (10) ORDER BY <order_by_list>
    /// ```
    ///
    /// Process:
    /// 0. Semantic check: tables exists, fields exists, predicate is valid.
    /// 1. `FROM`: If there is no where clause or join-on clause join, the virtual table
    ///    `VT3` is that table, and go step 4. Else, it is a join. A Cartesian product
    ///    (cross join) is performed between each two tables, and as a result:
    ///     - 1-1. If the number of tables between `FROM` and `JOIN` are more than one,
    ///            it is a where-clause inner join. Cross join tables and generate virtual
    ///            table `VT3`. Go step 4.
    ///     - 1-2. Otherwise, there should be only a table between `FROM` and `JOIN`, and
    ///            there must be `JOIN ON` clause(s). If the first `JOIN ON`, cross join
    ///             `FROM` and `JOIN` to make `VT1`, else cross join `VT1` and the next
    ///            `JOIN`. Go step 2.
    /// 2. `ON`: The `ON` filter is applied to `VT1`. Only rows for which the
    ///    `<join_condition>` is `TRUE` are inserted to `VT2`.
    /// 3. `OUTER` (join): If  an `OUTER JOIN` is specified (as opposed to
    ///    an `INNER JOIN`), rows from the preserved table or tables for
    ///    which a match was not found are added to the rows from `VT2` as outer
    ///    rows, generating `VT3`. If more than two tables appear in the `FROM`
    ///    clause, steps 1 through 3 are applied repeatedly between the result
    ///    of the last join and the next table in the `FROM` clause until all
    ///    tables are processed.
    /// 4. `WHERE`: The `WHERE` filter is applied to `VT3`. Only rows for which
    ///    the `<where_condition>` is `TRUE` are inserted to `VT4`.
    /// 5. `GROUP BY`: The rows from `VT4` are arranged in groups based on the
    ///    column list specified in the `GROUP BY` clause. `VT5` is generated.
    /// 6. `CUBE | ROLLUP`: Supergroups (groups of groups) are added to the
    ///    rows from `VT5`, generating `VT6`.
    /// 7. `HAVING`: The `HAVING` filter is applied to `VT6`. Only groups for which
    ///    the `<having_condition>` is `TRUE` are inserted to `VT7`.
    /// 8. `SELECT`: The `SELECT` list is processed, generating `VT8`.
    /// 9. `DISTINCT`: Duplicate rows are removed from `VT8`. `VT9` is generated.
    /// 10. `ORDER BY`: The rows from `VT9` are sorted according to the column list
    ///     specified in the ORDER BY clause. A cursor is generated (`VC10`).
    /// 11. `TOP`: The specified number or percentage of rows is selected from
    ///     the beginning of `VC10`. Table `VT11` is generated and returned to the
    ///     caller.
    ///
    /// reference: [stack overflow #1018822](https://stackoverflow.com/a/1944492/6798649)
    pub fn select(&mut self) -> Result<(), SQLError> {
        let mut is_where_clause = false;
        let mut is_join_on_clause = false;

        // TODO: step 0

        // step 1

        // copy the first table
        let mut vt1 = self
            .database
            .tables
            .get(&self.querydata.tables[0])
            .ok_or(SQLError::SemanticError("table not exists".to_string()))?
            .clone();

        // dealing cross joins
        if self.querydata.tables.len() > 1 {
            is_where_clause = true;
        }
        if self.querydata.joins.len() > 0 {
            is_join_on_clause = true;
        }

        let mut vt3 = Table::new(""); // stand by

        match (is_where_clause, is_join_on_clause) {
            (true, false) => {
                // TODO: step 1.1
            }
            (false, true) => {
                // TODO: step 1.2
            }
            (true, true) => {
                return Err(SQLError::SemanticError(String::from(
                    "where and join on clause cannot be together",
                )))
            }
            (false, false) => {
                // No join. The virtual table is the table.
                vt1.load_all_rows_data(&self.user.name, &self.database.name)
                    .map_err(|e| SQLError::SemanticError(format!("{}", e)))?;
                vt3 = vt1;
            }
        }

        // step 4
        let mut vt4;
        if self.querydata.predicate.is_some() {
            table_predicate(&mut vt3, &mut self.querydata.predicate)?;
            let set = match self.querydata.predicate.as_ref() {
                Some(s) => s.set.clone(),
                None => HashSet::new(), // should not happen, but still set empty if ever happen
            };
            vt3.set_row_set(set);
        }
        vt4 = vt3;

        // step 8
        let data = vt4
            .select(self.querydata.fields.clone())
            .map_err(|e| SQLError::SemanticError(format!("{}", e)))?;

        self.result_json = serde_json::to_string(&data).unwrap();
        Ok(())
    }
}

fn table_predicate(tb: &mut Table, node: &mut NodePtr) -> Result<(), SQLError> {
    match node.as_mut() {
        Some(p) => {
            // due to the mechanism of borrowing, set the mutable variable first before call them.
            let left;
            let right;
            let mut left_node_root = "".to_string();
            let mut left_node_set = HashSet::new();
            let mut right_node_root = "".to_string();
            let mut right_node_set = HashSet::new();
            let mut ll = false;
            let mut lr = false;
            let mut rl = false;
            let mut rr = false;
            let this_node_root: &str = &p.root;

            // post-order traversal
            table_predicate(tb, &mut p.left)?;
            table_predicate(tb, &mut p.right)?;

            match p.left.as_mut() {
                Some(s) => {
                    left = true;
                    left_node_root = s.root.to_string();
                    left_node_set = s.set.clone();
                    ll = s.left.is_some();
                    lr = s.right.is_some();
                }
                None => left = false,
            }

            match p.right.as_mut() {
                Some(s) => {
                    right = true;
                    right_node_root = s.root.to_string();
                    right_node_set = s.set.clone();
                    rl = s.left.is_some();
                    rr = s.right.is_some();
                }
                None => right = false,
            }

            debug!("current node: {}", this_node_root);
            debug!("left node: {}", left_node_root);
            debug!("right node: {}", right_node_root);
            debug!("grandchildren nodes: {:?}", (ll, lr, rl, rr));

            if left && right {
                match (ll, lr, rl, rr) {
                    (false, false, false, false) => match this_node_root {
                        "and" => {
                            let set: HashSet<usize> = left_node_set.intersection(&right_node_set).cloned().collect();
                            (*p).set = set;
                        }
                        "or" => {
                            let set: HashSet<usize> = left_node_set.union(&right_node_set).cloned().collect();
                            (*p).set = set;
                        }
                        _ => {
                            (*p).set = tb
                                .operator_filter_rows(&left_node_root, this_node_root, &right_node_root)
                                .map_err(|e| SQLError::SemanticError(format!("{}", e)))?;

                            (*p).left = None;
                            (*p).right = None;
                        }
                    },
                    (_, _, _, _) => {}
                };
            }

            debug!("this node set: {:?}", (*p).set);
        }
        None => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::*;
    use env_logger;

    fn fake_sql() -> SQL {
        let mut sql = SQL::new("Tiger").unwrap();
        sql.create_database("db11").unwrap();

        let query = "create table t1 (a1 int, a2 char(7), a3 double);";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();

        let query = "insert into t1(a1, a2, a3) values (1, 'aaa', 2.1);";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();
        let query = "insert into t1(a1, a2, a3) values (2, 'aaa', 2.2);";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();
        let query = "insert into t1(a1, a2, a3) values (3, 'bbb', 2.3);";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();
        let query = "insert into t1(a1, a2, a3) values (4, 'bbb', 2.4);";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();
        let query = "insert into t1(a1, a2, a3) values (5, 'bbb', 2.5);";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();

        sql
    }

    #[test]
    fn test_select_where_and() {
        let mut sql = fake_sql();

        let query = "select a1, a2, a3 from t1 where a1 > 2 and a3 < 2.5;";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();

        assert_eq!(
            sql.result_json,
            "{\"fields\":[\"a1\",\"a2\",\"a3\"],\"rows\":[[\"3\",\"\'bbb\'\",\"2.3\"],[\"4\",\"\'bbb\'\",\"2.4\"]]}"
                .to_string()
        );
    }

    #[test]
    fn test_select_where_or() {
        let mut sql = fake_sql();

        let query = "select a1, a2, a3 from t1 where a1 < 2 or a3 > 2.4;";
        Parser::new(query).unwrap().parse(&mut sql).unwrap();

        assert_eq!(
            sql.result_json,
            "{\"fields\":[\"a1\",\"a2\",\"a3\"],\"rows\":[[\"1\",\"\'aaa\'\",\"2.1\"],[\"5\",\"\'bbb\'\",\"2.5\"]]}"
                .to_string()
        );
    }
}
