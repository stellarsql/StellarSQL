use std::fmt;
use std::fs::File;
use std::io::prelude::*;
use std::io::Seek;
use std::io::SeekFrom;
use std::mem;

#[derive(Debug)]
pub enum IndexError {
    OpenFileError,
    CreateFileError,
    BuildIntIndexTableError,
    ReadIntIndexTableError,
    BuildStringIndexTableError,
    ReadStringIndexTableError,
    WriteIndexError,
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IndexError::OpenFileError => write!(f, "cannot open file"),
            IndexError::CreateFileError => write!(f, "cannot create file"),
            IndexError::BuildIntIndexTableError => write!(f, "Build int index table error"),
            IndexError::ReadIntIndexTableError => write!(f, "Read int index table error"),
            IndexError::BuildStringIndexTableError => write!(f, "Build string index table error"),
            IndexError::ReadStringIndexTableError => write!(f, "Read string index table error"),
            IndexError::WriteIndexError => write!(f, "Write index table error"),
        }
    }
}

// meta data of raw table
pub struct TableMeta {
    table_name: String, // name of raw table
    key_type: String,   // type of primary key in raw table
    key_offset: u32,    // byte position of first primary key in raw table
    key_bytes: u32,     // bytes of primary key in raw table
    row_bytes: u32,     // bytes of each row in raw table
}

// row and key value pair in which key type is int
pub struct IndexDataStructureInt {
    row: u32,
    key_value: u32,
}

// row and key value pair in which key type is string
pub struct IndexDataStructureString {
    row: u32,
    key_value: Vec<u8>,
}

pub struct IndexInt(Vec<IndexDataStructureInt>);
pub struct IndexString(Vec<IndexDataStructureString>);

#[allow(dead_code)]
pub enum Index {
    IndexInt(Vec<IndexDataStructureInt>),
    IndexString(Vec<IndexDataStructureString>),
}

#[allow(dead_code)]
pub enum IndexDataStructure {
    IndexInt(IndexDataStructureInt),
    IndexString(IndexDataStructureString),
}

#[allow(dead_code)]
impl Index {
    /// build index table with raw table in which key type is int
    fn build_int_index_table(table_meta: &TableMeta) -> Result<(Vec<IndexDataStructureInt>), IndexError> {
        let mut index_arr = IndexInt(vec![]);
        let mut row = 0;
        let bytes_to_slide = table_meta.row_bytes - table_meta.key_bytes;
        let table_name = table_meta.table_name.clone();
        let mut file = File::open(table_name).map_err(|_| IndexError::OpenFileError)?;
        file.seek(SeekFrom::Start(table_meta.key_offset as u64))
            .map_err(|_| IndexError::BuildIntIndexTableError)?;
        let mut buffer = [0; 4];
        loop {
            let _bytes_read = match file.read(&mut buffer) {
                Ok(0) => break, // end-of-file
                Ok(_) => {
                    unsafe {
                        let temp = mem::transmute::<[u8; 4], u32>(buffer);
                        let index_content = IndexDataStructureInt {
                            row: row,
                            key_value: temp,
                        };
                        index_arr.0.push(index_content);
                    }
                    file.seek(SeekFrom::Current(bytes_to_slide as i64))
                        .map_err(|_| IndexError::BuildIntIndexTableError)?;
                    row = row + 1;
                }
                Err(_) => {
                    return Err(IndexError::BuildIntIndexTableError);
                }
            };
        }

        index_arr.0.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
        Ok(index_arr.0)
    }

    /// write index table into index file
    fn write_int_index_table(table_meta: &TableMeta, index_arr: &Vec<IndexDataStructureInt>) -> Result<(), IndexError> {
        let table_index_name = format!("{}.index", table_meta.table_name);
        let mut file_write = File::create(table_index_name).map_err(|_| IndexError::CreateFileError)?;
        for i in 0..index_arr.len() {
            let row_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr[i].row) };
            file_write.write(&row_temp).map_err(|_| IndexError::WriteIndexError)?;
            let key_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr[i].key_value) };
            file_write.write(&key_temp).map_err(|_| IndexError::WriteIndexError)?;
        }
        Ok(())
    }

    /// read index table from index file
    fn read_int_index_table(
        table_meta: &TableMeta,
        index_arr: &mut Vec<IndexDataStructureInt>,
    ) -> Result<(), IndexError> {
        let table_index_name = format!("{}.index", table_meta.table_name);
        let mut file = File::open(table_index_name).map_err(|_| IndexError::OpenFileError)?;
        let mut buffer_row = [0; 4];
        let mut buffer_key = [0; 4];
        loop {
            let _bytes_read = match file.read(&mut buffer_row) {
                Ok(0) => break, // end-of-file
                Ok(_) => unsafe {
                    let temp_row = mem::transmute::<[u8; 4], u32>(buffer_row);
                    file.read(&mut buffer_key)
                        .map_err(|_| IndexError::ReadIntIndexTableError)?;
                    let temp_key = mem::transmute::<[u8; 4], u32>(buffer_key);
                    let index_content = IndexDataStructureInt {
                        row: temp_row,
                        key_value: temp_key,
                    };
                    index_arr.push(index_content);
                },
                Err(_) => {
                    return Err(IndexError::ReadIntIndexTableError);
                }
            };
        }
        Ok(())
    }

    // insert into index table in which key type is int
    // if work, use b-insert
    pub fn insert_int_index_table(
        insert_value: IndexDataStructureInt,
        index_arr: &mut Vec<IndexDataStructureInt>,
    ) -> Result<(), IndexError> {
        if index_arr.is_empty() {
            index_arr.push(insert_value);
        } else {
            let mut target = 0;
            for i in 0..index_arr.len() {
                if insert_value.key_value <= index_arr[i].key_value {
                    target = i;
                    break;
                }
            }
            if target == 0 {
                index_arr.insert(target, insert_value);
            } else {
                index_arr.insert(target - 1, insert_value);
            }
        }
        Ok(())
    }

    fn build_string_index_table(table_meta: &TableMeta) -> Result<(Vec<IndexDataStructureString>), IndexError> {
        let mut index_arr = IndexString(vec![]);
        let mut row = 0;
        let bytes_to_slide = table_meta.row_bytes - table_meta.key_bytes;
        let table_name = table_meta.table_name.clone();
        let mut file = File::open(table_name).map_err(|_| IndexError::OpenFileError)?;
        file.seek(SeekFrom::Start(table_meta.key_offset as u64))
            .map_err(|_| IndexError::BuildStringIndexTableError)?;
        let mut buffer = vec![0; table_meta.key_bytes as usize];
        loop {
            let _bytes_read = match file.read(&mut buffer) {
                Ok(0) => break, // end-of-file
                Ok(_) => {
                    let index_content = IndexDataStructureString {
                        row: row,
                        key_value: buffer.clone(),
                    };
                    index_arr.0.push(index_content);
                    file.seek(SeekFrom::Current(bytes_to_slide as i64))
                        .map_err(|_| IndexError::BuildStringIndexTableError)?;
                    row = row + 1;
                }
                Err(_) => {
                    return Err(IndexError::BuildStringIndexTableError);
                }
            };
        }

        index_arr.0.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
        Ok(index_arr.0)
    }

    fn write_string_index_table(
        table_meta: &TableMeta,
        index_arr: &Vec<IndexDataStructureString>,
    ) -> Result<(), IndexError> {
        let table_index_name = format!("{}.index", table_meta.table_name);
        let mut file_write = File::create(table_index_name).map_err(|_| IndexError::CreateFileError)?;
        for i in 0..index_arr.len() {
            let row_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr[i].row) };
            file_write.write(&row_temp).map_err(|_| IndexError::WriteIndexError)?;
            file_write
                .write(&index_arr[i].key_value)
                .map_err(|_| IndexError::WriteIndexError)?;
        }
        Ok(())
    }

    fn read_string_index_table(
        table_meta: &TableMeta,
        index_arr: &mut Vec<IndexDataStructureString>,
    ) -> Result<(), IndexError> {
        let table_index_name = format!("{}.index", table_meta.table_name);
        let mut file = File::open(table_index_name).map_err(|_| IndexError::OpenFileError)?;
        let mut buffer_row = [0; 4];
        let mut buffer_key = vec![0; table_meta.key_bytes as usize];
        loop {
            let _bytes_read = match file.read(&mut buffer_row) {
                Ok(0) => break, // end-of-file
                Ok(_) => unsafe {
                    let temp_row = mem::transmute::<[u8; 4], u32>(buffer_row);
                    file.read(&mut buffer_key)
                        .map_err(|_| IndexError::ReadStringIndexTableError)?;
                    let index_content = IndexDataStructureString {
                        row: temp_row,
                        key_value: buffer_key.clone(),
                    };
                    index_arr.push(index_content);
                },
                Err(_) => {
                    return Err(IndexError::ReadStringIndexTableError);
                }
            };
        }
        Ok(())
    }

    pub fn insert_string_index_table(
        insert_value: IndexDataStructureString,
        index_arr: &mut Vec<IndexDataStructureString>,
    ) -> Result<(), IndexError> {
        if index_arr.is_empty() {
            index_arr.push(insert_value);
        } else {
            let mut target = 0;
            for i in 0..index_arr.len() {
                if insert_value.key_value <= index_arr[i].key_value {
                    target = i;
                    break;
                }
            }
            if target == 0 {
                index_arr.insert(target, insert_value);
            } else {
                index_arr.insert(target - 1, insert_value);
            }
        }
        Ok(())
    }

    pub fn build_index_table(table_meta: &TableMeta) -> Result<Index, IndexError> {
        if table_meta.key_type == "Int" {
            let index_int = Index::build_int_index_table(&table_meta)?;
            return Ok(Index::IndexInt(index_int));
        } else {
            let index_string = Index::build_string_index_table(&table_meta)?;
            return Ok(Index::IndexString(index_string));
        }
    }

    pub fn write_index_table(table_meta: &TableMeta, index_arr: &mut Index) -> Result<(), IndexError> {
        match index_arr {
            Index::IndexInt(index_int) => Index::write_int_index_table(&table_meta, &index_int),
            Index::IndexString(index_string) => Index::write_string_index_table(&table_meta, &index_string),
        }
    }

    pub fn read_index_table(table_meta: &TableMeta, index_arr: &mut Index) -> Result<(), IndexError> {
        match index_arr {
            Index::IndexInt(index_int) => Index::read_int_index_table(&table_meta, index_int),
            Index::IndexString(index_string) => Index::read_string_index_table(&table_meta, index_string),
        }
    }

    pub fn insert_index_table(insert_value: IndexDataStructure, index_arr: &mut Index) -> Result<(), IndexError> {
        match index_arr {
            Index::IndexInt(index_int) => match insert_value {
                IndexDataStructure::IndexInt(value_int) => Index::insert_int_index_table(value_int, index_int),
                IndexDataStructure::IndexString(_value_string) => Ok(()),
            },
            Index::IndexString(index_string) => match insert_value {
                IndexDataStructure::IndexInt(_value_int) => Ok(()),
                IndexDataStructure::IndexString(value_string) => {
                    Index::insert_string_index_table(value_string, index_string)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn test_construct_index() {
        let table_meta = TableMeta {
            table_name: String::from("test_data/1.in"),
            key_type: String::from("Int"),
            key_offset: 0,
            key_bytes: 4,
            row_bytes: 4,
        };
        let mut index_arr = Index::build_index_table(&table_meta).unwrap();
        Index::write_index_table(&table_meta, &mut index_arr).unwrap();
    }
}
