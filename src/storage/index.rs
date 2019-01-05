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
    InsertValueMismatchIndex,
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
            IndexError::InsertValueMismatchIndex => write!(f, "Type of the insert value mismatches index"),
        }
    }
}

pub struct Index {
    table_meta: TableMeta,
    index_data: IndexData,
}

/// meta data of raw table
#[derive(Debug)]
pub struct TableMeta {
    table_name: String, // name of raw table
    key_type: KeyType,  // type of primary key in raw table
    key_offset: u32,    // byte position of first primary key in raw table
    key_bytes: u32,     // bytes of primary key in raw table
    row_bytes: u32,     // bytes of each row in raw table
}

/// row and key value pair in which key type is int
pub struct RowPairInt {
    row: u32,
    key_value: u32,
}

impl RowPairInt {
    fn new(pair: (u32, &str)) -> RowPairInt {
        RowPairInt {
            row: pair.0,
            key_value: pair.1.parse::<u32>().unwrap(),
        }
    }
}

/// row and key value pair in which key type is string
pub struct RowPairString {
    row: u32,
    key_value: Vec<u8>,
}

impl RowPairString {
    fn new(pair: (u32, &str)) -> RowPairString {
        RowPairString {
            row: pair.0,
            key_value: pair.1.as_bytes().to_vec(),
        }
    }
}

#[allow(dead_code)]
pub enum IndexData {
    IndexInt(Vec<RowPairInt>),
    IndexString(Vec<RowPairString>),
    None,
}

#[derive(Debug, PartialEq)]
pub enum KeyType {
    Int,
    String,
}

#[allow(dead_code)]
impl Index {
    /// construct a new Index
    pub fn new(table_meta: TableMeta) -> Result<Index, IndexError> {
        Ok(Index {
            table_meta,
            index_data: IndexData::None,
        })
    }

    /// create a new index data for Index
    fn build_index(&mut self) -> Result<(), IndexError> {
        self.index_data = match self.table_meta.key_type {
            KeyType::Int => IndexData::IndexInt(self.build_int_index_table()?),
            KeyType::String => IndexData::IndexString(self.build_string_index_table()?),
        };
        Ok(())
    }

    /// Write index to storage
    pub fn write_index_table(&mut self) -> Result<(), IndexError> {
        match &self.index_data {
            IndexData::IndexInt(index_int) => self.write_int_index_table(&index_int),
            IndexData::IndexString(index_string) => self.write_string_index_table(&index_string),
            IndexData::None => Ok(()), // should not happen
        }
    }

    /// Read index from storage
    pub fn read_index_table(&mut self) -> Result<(), IndexError> {
        match self.table_meta.key_type {
            KeyType::Int => self.read_int_index_table(),
            KeyType::String => self.read_string_index_table(),
        }
    }

    /// insert a row-key pair into the index
    pub fn insert_index_table(&mut self, value_pair: (u32, &str)) -> Result<(), IndexError> {
        let key_type = match value_pair.1.parse::<u32>() {
            Ok(_) => KeyType::Int,
            Err(_) => KeyType::String,
        };

        match self.index_data {
            IndexData::IndexInt(ref mut index_int) => match key_type {
                KeyType::Int => {
                    let pair = RowPairInt::new(value_pair);
                    Index::insert_int_index_table(pair, index_int)
                }
                KeyType::String => Err(IndexError::InsertValueMismatchIndex),
            },
            IndexData::IndexString(ref mut index_string) => match key_type {
                KeyType::Int => Err(IndexError::InsertValueMismatchIndex),
                KeyType::String => {
                    let pair = RowPairString::new(value_pair);
                    Index::insert_string_index_table(pair, index_string)
                }
            },
            IndexData::None => Ok(()), // should not happen
        }
    }

    /// build index table with raw table in which key type is int
    fn build_int_index_table(&self) -> Result<(Vec<RowPairInt>), IndexError> {
        let mut index_arr = vec![];
        let mut row = 0;
        let table_meta = &self.table_meta;
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
                        let index_content = RowPairInt {
                            row: row,
                            key_value: temp,
                        };
                        index_arr.push(index_content);
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

        index_arr.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
        Ok(index_arr)
    }

    /// write index table into index file
    fn write_int_index_table(&self, index_arr: &Vec<RowPairInt>) -> Result<(), IndexError> {
        let table_index_name = format!("{}.index", self.table_meta.table_name);
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
    fn read_int_index_table(&mut self) -> Result<(), IndexError> {
        let mut index_arr = vec![];
        let table_index_name = format!("{}.index", self.table_meta.table_name);
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
                    let index_content = RowPairInt {
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
        self.index_data = IndexData::IndexInt(index_arr);
        Ok(())
    }

    /// insert into index table in which primary key type is int
    fn insert_int_index_table(insert_value: RowPairInt, index_arr: &mut Vec<RowPairInt>) -> Result<(), IndexError> {
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

    fn build_string_index_table(&self) -> Result<(Vec<RowPairString>), IndexError> {
        let mut index_arr = vec![];
        let mut row = 0;
        let table_meta = &self.table_meta;
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
                    let index_content = RowPairString {
                        row: row,
                        key_value: buffer.clone(),
                    };
                    index_arr.push(index_content);
                    file.seek(SeekFrom::Current(bytes_to_slide as i64))
                        .map_err(|_| IndexError::BuildStringIndexTableError)?;
                    row = row + 1;
                }
                Err(_) => {
                    return Err(IndexError::BuildStringIndexTableError);
                }
            };
        }

        index_arr.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
        Ok(index_arr)
    }

    fn write_string_index_table(&self, index_arr: &Vec<RowPairString>) -> Result<(), IndexError> {
        let table_index_name = format!("{}.index", self.table_meta.table_name);
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

    fn read_string_index_table(&mut self) -> Result<(), IndexError> {
        let mut index_arr = vec![];
        let table_index_name = format!("{}.index", self.table_meta.table_name);
        let mut file = File::open(table_index_name).map_err(|_| IndexError::OpenFileError)?;
        let mut buffer_row = [0; 4];
        let mut buffer_key = vec![0; self.table_meta.key_bytes as usize];
        loop {
            let _bytes_read = match file.read(&mut buffer_row) {
                Ok(0) => break, // end-of-file
                Ok(_) => unsafe {
                    let temp_row = mem::transmute::<[u8; 4], u32>(buffer_row);
                    file.read(&mut buffer_key)
                        .map_err(|_| IndexError::ReadStringIndexTableError)?;
                    let index_content = RowPairString {
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
        self.index_data = IndexData::IndexString(index_arr);
        Ok(())
    }

    fn insert_string_index_table(
        insert_value: RowPairString,
        index_arr: &mut Vec<RowPairString>,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn test_construct_index() {
        let table_meta = TableMeta {
            table_name: String::from("test_data/1.in"),
            key_type: KeyType::Int,
            key_offset: 0,
            key_bytes: 4,
            row_bytes: 4,
        };
        let mut index = Index::new(table_meta).unwrap();
        index.write_index_table().unwrap();
    }
}
