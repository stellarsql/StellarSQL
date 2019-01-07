use crate::storage::diskinterface::{DiskError, TableMeta};

use std::fmt;

pub struct Index {
    table_meta: TableMeta,
    index_data: Vec<RowPair>,
}

// /// meta data of raw table
// #[derive(Debug)]
// pub struct TableMeta {
//     table_name: String, // name of raw table
//     key_type: KeyType,  // type of primary key in raw table
//     key_offset: u32,    // byte position of first primary key in raw table
//     key_bytes: u32,     // bytes of primary key in raw table
//     row_bytes: u32,     // bytes of each row in raw table
// }

// /// row and key value pair in which key type is int
// pub struct RowPairInt {
//     row: u32,
//     key_value: u32,
// }

// impl RowPairInt {
//     fn new(pair: (u32, &str)) -> RowPairInt {
//         RowPairInt {
//             row: pair.0,
//             key_value: pair.1.parse::<u32>().unwrap(),
//         }
//     }
// }

// /// row and key value pair in which key type is string
// pub struct RowPairString {
//     row: u32,
//     key_value: Vec<u8>,
// }

// impl RowPairString {
//     fn new(pair: (u32, &str)) -> RowPairString {
//         RowPairString {
//             row: pair.0,
//             key_value: pair.1.as_bytes().to_vec(),
//         }
//     }
// }

/// (row, key_value) pair
pub struct RowPair {
    row: u32,
    key_value: Vec<u8>,
}

impl RowPair {
    fn new(pair: (u32, &Vec<u8>)) -> RowPair {
        RowPair {
            row: pair.0,
            key_value: pair.1.to_vec(),
        }
    }
}

// #[allow(dead_code)]
// pub enum IndexData {
//     IndexInt(Vec<RowPairInt>),
//     IndexString(Vec<RowPairString>),
//     None,
// }

// #[derive(Debug, PartialEq)]
// pub enum KeyType {
//     Int,
//     String,
// }

#[allow(dead_code)]
impl Index {
    /// construct a new Index
    pub fn new(table_meta: TableMeta) -> Result<Index, DiskError> {
        Ok(Index {
            table_meta,
            index_data: vec![],
        })
    }

    // build index from table bin file
    // fn build_from_file(&mut self, file_base_path: Option<&str>) -> Result<(), IndexError> {
    //     // determine file base path
    //     let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

    //     // perform storage check toward table level
    //     DiskInterface::storage_hierarchy_check(base_path, Some(username), Some(db_name), Some(table_name)).map_err(|e| e)?;

    //     // load current tables from `tables.json`
    //     let tables_json_path = format!("{}/{}/{}/{}", base_path, username, db_name, "tables.json");
    //     let tables_file = fs::File::open(&tables_json_path)?;
    //     let tables_json: TablesJson = serde_json::from_reader(tables_file)?;

    //     // locate meta of target table
    //     let idx_target = tables_json
    //         .tables
    //         .iter()
    //         .position(|table_meta| &table_meta.name == table_name);

    //     let table_meta_target: &TableMeta = match idx_target {
    //         Some(idx) => &tables_json.tables[idx],
    //         None => return Err(FileError::TableNotExists),
    //     };

    //     // load corresponding chunk of bytes from table bin
    //     let table_bin_path = format!("{}/{}/{}/{}.bin", base_path, username, db_name, table_name);
    //     let table_bin_file = fs::File::open(&table_bin_path)?;
    //     let mut buffered = BufReader::new(table_bin_file);

    //     let mut chunk_bytes = vec![];
    //     buffered.seek(SeekFrom::Start((row_range[0] * table_meta_target.row_length) as u64))?;
    //     let mut raw = buffered.take(((row_range[1] - row_range[0]) * table_meta_target.row_length) as u64);
    //     raw.read_to_end(&mut chunk_bytes)?;

    //     if chunk_bytes.len() != ((row_range[1] - row_range[0]) * table_meta_target.row_length) as usize {
    //         return Err(FileError::RangeExceedLatestRecord);
    //     }

    //     // parse chunk of bytes to vector of rows
    //     let mut rows: Vec<Row> = vec![];
    //     for row_bytes in chunk_bytes.chunks(table_meta_target.row_length as usize) {
    //         if row_bytes[0] == 0 as u8 {
    //             return Err(FileError::RangeContainsDeletedRecord);
    //         }
    //         rows.push(BytesCoder::bytes_to_row(&table_meta_target, &row_bytes.to_vec())?);
    //     }

    //     Ok(rows)
    // }

    // /// create a new index data for Index
    // fn build_index(&mut self) -> Result<(), IndexError> {
    //     self.index_data = match self.table_meta.key_type {
    //         KeyType::Int => IndexData::IndexInt(self.build_int_index_table()?),
    //         KeyType::String => IndexData::IndexString(self.build_string_index_table()?),
    //     };
    //     Ok(())
    // }

    // /// Write index to storage
    // pub fn write_index_table(&mut self) -> Result<(), IndexError> {
    //     match &self.index_data {
    //         IndexData::IndexInt(index_int) => self.write_int_index_table(&index_int),
    //         IndexData::IndexString(index_string) => self.write_string_index_table(&index_string),
    //         IndexData::None => Ok(()), // should not happen
    //     }
    // }

    // /// Read index from storage
    // pub fn read_index_table(&mut self) -> Result<(), IndexError> {
    //     match self.table_meta.key_type {
    //         KeyType::Int => self.read_int_index_table(),
    //         KeyType::String => self.read_string_index_table(),
    //     }
    // }

    // /// insert a row-key pair into the index
    // pub fn insert_index_table(&mut self, value_pair: (u32, &str)) -> Result<(), IndexError> {
    //     let key_type = match value_pair.1.parse::<u32>() {
    //         Ok(_) => KeyType::Int,
    //         Err(_) => KeyType::String,
    //     };

    //     match self.index_data {
    //         IndexData::IndexInt(ref mut index_int) => match key_type {
    //             KeyType::Int => {
    //                 let pair = RowPairInt::new(value_pair);
    //                 Index::insert_int_index_table(pair, index_int)
    //             }
    //             KeyType::String => Err(IndexError::InsertValueMismatchIndex),
    //         },
    //         IndexData::IndexString(ref mut index_string) => match key_type {
    //             KeyType::Int => Err(IndexError::InsertValueMismatchIndex),
    //             KeyType::String => {
    //                 let pair = RowPairString::new(value_pair);
    //                 Index::insert_string_index_table(pair, index_string)
    //             }
    //         },
    //         IndexData::None => Ok(()), // should not happen
    //     }
    // }

    // /// build index table with raw table in which key type is int
    // fn build_int_index_table(&self) -> Result<(Vec<RowPairInt>), IndexError> {
    //     let mut index_arr = vec![];
    //     let mut row = 0;
    //     let table_meta = &self.table_meta;
    //     let bytes_to_slide = table_meta.row_bytes - table_meta.key_bytes;
    //     let table_name = table_meta.table_name.clone();
    //     let mut file = File::open(table_name).map_err(|_| IndexError::OpenFileError)?;
    //     file.seek(SeekFrom::Start(table_meta.key_offset as u64))
    //         .map_err(|_| IndexError::BuildIntIndexTableError)?;
    //     let mut buffer = [0; 4];
    //     loop {
    //         let _bytes_read = match file.read(&mut buffer) {
    //             Ok(0) => break, // end-of-file
    //             Ok(_) => {
    //                 unsafe {
    //                     let temp = mem::transmute::<[u8; 4], u32>(buffer);
    //                     let index_content = RowPairInt {
    //                         row: row,
    //                         key_value: temp,
    //                     };
    //                     index_arr.push(index_content);
    //                 }
    //                 file.seek(SeekFrom::Current(bytes_to_slide as i64))
    //                     .map_err(|_| IndexError::BuildIntIndexTableError)?;
    //                 row = row + 1;
    //             }
    //             Err(_) => {
    //                 return Err(IndexError::BuildIntIndexTableError);
    //             }
    //         };
    //     }

    //     index_arr.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
    //     Ok(index_arr)
    // }

    // /// write index table into index file
    // fn write_int_index_table(&self, index_arr: &Vec<RowPairInt>) -> Result<(), IndexError> {
    //     let table_index_name = format!("{}.index", self.table_meta.table_name);
    //     let mut file_write = File::create(table_index_name).map_err(|_| IndexError::CreateFileError)?;
    //     for i in 0..index_arr.len() {
    //         let row_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr[i].row) };
    //         file_write.write(&row_temp).map_err(|_| IndexError::WriteIndexError)?;
    //         let key_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr[i].key_value) };
    //         file_write.write(&key_temp).map_err(|_| IndexError::WriteIndexError)?;
    //     }
    //     Ok(())
    // }

    // /// read index table from index file
    // fn read_int_index_table(&mut self) -> Result<(), IndexError> {
    //     let mut index_arr = vec![];
    //     let table_index_name = format!("{}.index", self.table_meta.table_name);
    //     let mut file = File::open(table_index_name).map_err(|_| IndexError::OpenFileError)?;
    //     let mut buffer_row = [0; 4];
    //     let mut buffer_key = [0; 4];
    //     loop {
    //         let _bytes_read = match file.read(&mut buffer_row) {
    //             Ok(0) => break, // end-of-file
    //             Ok(_) => unsafe {
    //                 let temp_row = mem::transmute::<[u8; 4], u32>(buffer_row);
    //                 file.read(&mut buffer_key)
    //                     .map_err(|_| IndexError::ReadIntIndexTableError)?;
    //                 let temp_key = mem::transmute::<[u8; 4], u32>(buffer_key);
    //                 let index_content = RowPairInt {
    //                     row: temp_row,
    //                     key_value: temp_key,
    //                 };
    //                 index_arr.push(index_content);
    //             },
    //             Err(_) => {
    //                 return Err(IndexError::ReadIntIndexTableError);
    //             }
    //         };
    //     }
    //     self.index_data = IndexData::IndexInt(index_arr);
    //     Ok(())
    // }

    // /// insert into index table in which primary key type is int
    // fn insert_int_index_table(insert_value: RowPairInt, index_arr: &mut Vec<RowPairInt>) -> Result<(), IndexError> {
    //     if index_arr.is_empty() {
    //         index_arr.push(insert_value);
    //     } else {
    //         let mut target = 0;
    //         for i in 0..index_arr.len() {
    //             if insert_value.key_value <= index_arr[i].key_value {
    //                 target = i;
    //                 break;
    //             }
    //         }
    //         if target == 0 {
    //             index_arr.insert(target, insert_value);
    //         } else {
    //             index_arr.insert(target - 1, insert_value);
    //         }
    //     }
    //     Ok(())
    // }

    // fn build_string_index_table(&self) -> Result<(Vec<RowPairString>), IndexError> {
    //     let mut index_arr = vec![];
    //     let mut row = 0;
    //     let table_meta = &self.table_meta;
    //     let bytes_to_slide = table_meta.row_bytes - table_meta.key_bytes;
    //     let table_name = table_meta.table_name.clone();
    //     let mut file = File::open(table_name).map_err(|_| IndexError::OpenFileError)?;
    //     file.seek(SeekFrom::Start(table_meta.key_offset as u64))
    //         .map_err(|_| IndexError::BuildStringIndexTableError)?;
    //     let mut buffer = vec![0; table_meta.key_bytes as usize];
    //     loop {
    //         let _bytes_read = match file.read(&mut buffer) {
    //             Ok(0) => break, // end-of-file
    //             Ok(_) => {
    //                 let index_content = RowPairString {
    //                     row: row,
    //                     key_value: buffer.clone(),
    //                 };
    //                 index_arr.push(index_content);
    //                 file.seek(SeekFrom::Current(bytes_to_slide as i64))
    //                     .map_err(|_| IndexError::BuildStringIndexTableError)?;
    //                 row = row + 1;
    //             }
    //             Err(_) => {
    //                 return Err(IndexError::BuildStringIndexTableError);
    //             }
    //         };
    //     }

    //     index_arr.sort_unstable_by(|a, b| a.key_value.cmp(&b.key_value));
    //     Ok(index_arr)
    // }

    // fn write_string_index_table(&self, index_arr: &Vec<RowPairString>) -> Result<(), IndexError> {
    //     let table_index_name = format!("{}.index", self.table_meta.table_name);
    //     let mut file_write = File::create(table_index_name).map_err(|_| IndexError::CreateFileError)?;
    //     for i in 0..index_arr.len() {
    //         let row_temp = unsafe { mem::transmute::<u32, [u8; 4]>(index_arr[i].row) };
    //         file_write.write(&row_temp).map_err(|_| IndexError::WriteIndexError)?;
    //         file_write
    //             .write(&index_arr[i].key_value)
    //             .map_err(|_| IndexError::WriteIndexError)?;
    //     }
    //     Ok(())
    // }

    // fn read_string_index_table(&mut self) -> Result<(), IndexError> {
    //     let mut index_arr = vec![];
    //     let table_index_name = format!("{}.index", self.table_meta.table_name);
    //     let mut file = File::open(table_index_name).map_err(|_| IndexError::OpenFileError)?;
    //     let mut buffer_row = [0; 4];
    //     let mut buffer_key = vec![0; self.table_meta.key_bytes as usize];
    //     loop {
    //         let _bytes_read = match file.read(&mut buffer_row) {
    //             Ok(0) => break, // end-of-file
    //             Ok(_) => unsafe {
    //                 let temp_row = mem::transmute::<[u8; 4], u32>(buffer_row);
    //                 file.read(&mut buffer_key)
    //                     .map_err(|_| IndexError::ReadStringIndexTableError)?;
    //                 let index_content = RowPairString {
    //                     row: temp_row,
    //                     key_value: buffer_key.clone(),
    //                 };
    //                 index_arr.push(index_content);
    //             },
    //             Err(_) => {
    //                 return Err(IndexError::ReadStringIndexTableError);
    //             }
    //         };
    //     }
    //     self.index_data = IndexData::IndexString(index_arr);
    //     Ok(())
    // }

    // fn insert_string_index_table(
    //     insert_value: RowPairString,
    //     index_arr: &mut Vec<RowPairString>,
    // ) -> Result<(), IndexError> {
    //     if index_arr.is_empty() {
    //         index_arr.push(insert_value);
    //     } else {
    //         let mut target = 0;
    //         for i in 0..index_arr.len() {
    //             if insert_value.key_value <= index_arr[i].key_value {
    //                 target = i;
    //                 break;
    //             }
    //         }
    //         if target == 0 {
    //             index_arr.insert(target, insert_value);
    //         } else {
    //             index_arr.insert(target - 1, insert_value);
    //         }
    //     }
    //     Ok(())
    // }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     #[test]
//     pub fn test_construct_index() {
//         let table_meta = TableMeta {
//             table_name: String::from("test_data/1.in"),
//             key_type: KeyType::Int,
//             key_offset: 0,
//             key_bytes: 4,
//             row_bytes: 4,
//         };
//         let mut index = Index::new(table_meta).unwrap();
//         index.write_index_table().unwrap();
//     }
// }
