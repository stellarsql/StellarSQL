use crate::component::table::Row;
use crate::sql::worker::{SQLError, SQL};
use crate::storage::diskinterface::DiskError;
use crate::storage::file::File;
use std::fmt;

use std::collections::{BTreeMap, VecDeque};

/*
 * freelist: [recent use ..... least recent use]
 */
#[derive(Debug)]
pub struct Pool {
    pub max_entry: usize,
    pub freelist: VecDeque<String>,
    pub cache: BTreeMap<String, SQL>,
}

#[derive(Debug)]
pub enum PoolError {
    SQLError(SQLError),
    EntryNotExist,
    DiskError(DiskError),
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PoolError::SQLError(ref e) => write!(f, "error cause by worker: {}", e),
            PoolError::EntryNotExist => write!(f, "entry is not existed"),
            PoolError::DiskError(ref e) => write!(f, "error cause by file: {}", e),
        }
    }
}

impl Pool {
    pub fn new(entry_number: usize) -> Pool {
        Pool {
            max_entry: entry_number,
            freelist: VecDeque::new(),
            cache: BTreeMap::new(),
        }
    }
    pub fn get(&mut self, username: &str, dbname: &str, addr: String) -> Result<&mut SQL, PoolError> {
        // get username entry from cache

        // if entry is not existed, load from disk to cache
        if !self.cache.contains_key(&addr) {
            let mut sql = SQL::new(username).unwrap();
            if dbname != "" {
                match sql.load_database(dbname) {
                    Ok(_) => {}
                    Err(ret) => return Err(PoolError::SQLError(ret)),
                }
            }
            match self.insert(sql, addr.clone()) {
                Ok(_) => {}
                Err(ret) => return Err(ret),
            }
        }
        // if username entry is not in the front(most recent use), move it to the front
        if self.freelist[0] != addr {
            self.pop_from_freelist(&addr);
            let key = addr.clone();
            self.freelist.push_front(key);
        }
        Ok(self.cache.get_mut(&addr).unwrap())
    }
    pub fn insert(&mut self, sql: SQL, addr: String) -> Result<(), PoolError> {
        // if current size >= cache max size, pop and write back thr Least Recent Use(LRU) entry
        if self.cache.len() >= self.max_entry {
            let pop_addr = self.freelist.pop_back().unwrap();
            match self.write_back(pop_addr) {
                Ok(_) => {}
                Err(ret) => return Err(ret),
            }
        }
        let key = addr.clone();
        self.cache.insert(addr, sql);
        self.freelist.push_front(key);
        Ok(())
    }
    pub fn write_back(&mut self, addr: String) -> Result<(), PoolError> {
        // pop username entry, write this entry back to disk

        self.pop_from_freelist(&addr);

        let sql = match self.cache.get(&addr) {
            Some(tsql) => tsql,
            None => return Err(PoolError::EntryNotExist),
        };
        match Pool::hierarchic_check(sql) {
            Ok(_) => {}
            Err(e) => return Err(e),
        }

        // remove from cache
        self.cache.remove(&addr);
        Ok(())
    }
    fn pop_from_freelist(&mut self, addr: &String) {
        let l = self.freelist.len();
        for i in 0..l {
            if self.freelist[i] == *addr {
                self.freelist.remove(i);
                break;
            }
        }
    }
    fn hierarchic_check(sql: &SQL) -> Result<(), PoolError> {
        // 1. check dirty bit of database
        if sql.database.is_delete {
            match File::remove_db(&sql.user.name, &sql.database.name, Some(dotenv!("FILE_BASE_PATH"))) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(PoolError::DiskError(e)),
            }
        }
        if sql.database.is_dirty {
            match File::create_db(&sql.user.name, &sql.database.name, Some(dotenv!("FILE_BASE_PATH"))) {
                Ok(_) => {}
                Err(e) => return Err(PoolError::DiskError(e)),
            }
        }
        // 2. check dirty bit of tables
        for (name, table) in sql.database.tables.iter() {
            if table.is_delete {
                match File::drop_table(
                    &sql.user.name,
                    &sql.database.name,
                    &name,
                    Some(dotenv!("FILE_BASE_PATH")),
                ) {
                    Ok(_) => {}
                    Err(e) => return Err(PoolError::DiskError(e)),
                }
                continue;
            }
            if table.is_dirty {
                match File::create_table(
                    &sql.user.name,
                    &sql.database.name,
                    &table,
                    Some(dotenv!("FILE_BASE_PATH")),
                ) {
                    Ok(_) => {}
                    Err(e) => return Err(PoolError::DiskError(e)),
                }
            }
            // 3. check dirty bit of rows
            let mut new_row: Vec<Row> = table.rows.clone();
            let l = new_row.len();
            for i in 0..l {
                if !new_row[i].is_dirty {
                    // remove rows which are not dirty
                    new_row.remove(i);
                }
            }
            if !new_row.is_empty() {
                match File::append_rows(
                    &sql.user.name,
                    &sql.database.name,
                    &name,
                    &new_row,
                    Some(dotenv!("FILE_BASE_PATH")),
                ) {
                    Ok(_) => {}
                    Err(e) => return Err(PoolError::FileError(e)),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_multithread_correctness() {}

    #[test]
    fn test_pool_LRU_algorithm() {}

    #[test]
    fn test_db_writeback() {}

    #[test]
    fn test_table_writeback() {}

    #[test]
    fn test_create_row_writeback() {}

    #[test]
    fn test_pool_error() {}
}
