use crate::storage::diskinterface::{DiskError, DiskInterface, TableMeta};

use std::fs;
use std::io::{BufReader, Read};

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
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RowPair {
    row: u32,
    key_value: Vec<u8>,
}

impl RowPair {
    pub fn new(row: u32, key_value: Vec<u8>) -> Self {
        RowPair { row, key_value }
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
    fn build_from_bin(&mut self, file_base_path: Option<&str>) -> Result<(), DiskError> {
        // determine file base path
        let base_path = file_base_path.unwrap_or(dotenv!("FILE_BASE_PATH"));

        // perform storage check toward table level
        DiskInterface::storage_hierarchy_check(
            base_path,
            Some(&self.table_meta.username),
            Some(&self.table_meta.db_name),
            Some(&self.table_meta.name),
        )
        .map_err(|e| e)?;

        // load table bin as chunk of bytes
        let table_bin_path = format!(
            "{}/{}/{}/{}.bin",
            base_path, self.table_meta.username, self.table_meta.db_name, self.table_meta.name
        );
        let table_bin_file = fs::File::open(&table_bin_path)?;
        let mut buffered = BufReader::new(table_bin_file);

        let mut chunk_bytes = vec![];
        buffered.read_to_end(&mut chunk_bytes)?;

        // parse chunk of bytes to vector of rows
        let mut new_index_data: Vec<RowPair> = vec![];
        for (row_id, row_bytes) in chunk_bytes.chunks(self.table_meta.row_length as usize).enumerate() {
            // ignore deleted rows
            if row_bytes[0] == 1 as u8 {
                new_index_data.push(RowPair::new(
                    row_id as u32,
                    row_bytes[self.table_meta.attr_offset_ranges[1][0] as usize
                        ..self.table_meta.attr_offset_ranges[1][1] as usize]
                        .to_vec(),
                ));
            }
        }

        new_index_data.sort_by(|rp1, rp2| rp1.key_value.cmp(&rp2.key_value));

        self.index_data = new_index_data;

        Ok(())
    }

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

    // /// write index table into index file
    // fn write_int_index_table(&self, index_arr: &Vec<RowPairInt>) -> Result<(), IndexError> {
    //     let table_index_name = format!("{}.index", self.table_meta.table_name);
    //     let mut file_write = create(table_index_name).map_err(|_| IndexError::CreateFileError)?;
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
    //     let mut file = open(table_index_name).map_err(|_| IndexError::OpenFileError)?;
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

    // fn write_string_index_table(&self, index_arr: &Vec<RowPairString>) -> Result<(), IndexError> {
    //     let table_index_name = format!("{}.index", self.table_meta.table_name);
    //     let mut file_write = create(table_index_name).map_err(|_| IndexError::CreateFileError)?;
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
    //     let mut file = open(table_index_name).map_err(|_| IndexError::OpenFileError)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::datatype::DataType;
    use crate::component::field;
    use crate::component::field::Field;
    use crate::component::table::Table;
    use std::fs;
    use std::path::Path;

    #[test]
    pub fn test_build_from_bin() {
        let file_base_path = "data9";
        if Path::new(file_base_path).exists() {
            fs::remove_dir_all(file_base_path).unwrap();
        }

        DiskInterface::create_file_base(Some(file_base_path)).unwrap();
        DiskInterface::create_username("crazyguy", Some(file_base_path)).unwrap();
        DiskInterface::create_db("crazyguy", "BookerDB", Some(file_base_path)).unwrap();

        let mut aff_table = Table::new("Affiliates");
        aff_table.fields.insert(
            "AffID".to_string(),
            Field::new_all("AffID", DataType::Int, true, None, field::Checker::None, false),
        );
        aff_table.fields.insert(
            "AffName".to_string(),
            Field::new_all(
                "AffName",
                DataType::Varchar(40),
                true,
                None,
                field::Checker::None,
                false,
            ),
        );
        aff_table.fields.insert(
            "AffEmail".to_string(),
            Field::new_all(
                "AffEmail",
                DataType::Varchar(50),
                true,
                None,
                field::Checker::None,
                false,
            ),
        );
        aff_table.fields.insert(
            "AffPhoneNum".to_string(),
            Field::new_all(
                "AffPhoneNum",
                DataType::Varchar(20),
                false,
                Some("+886900000000".to_string()),
                field::Checker::None,
                false,
            ),
        );
        aff_table.primary_key.push("AffID".to_string());

        DiskInterface::create_table("crazyguy", "BookerDB", &aff_table, Some(file_base_path)).unwrap();

        let data = vec![
            ("AffID", "2"),
            ("AffName", "Tom"),
            ("AffEmail", "tom@foo.com"),
            ("AffPhoneNum", "+886900000001"),
        ];
        aff_table.insert_row(data).unwrap();

        let data = vec![
            ("AffID", "7"),
            ("AffName", "Ben"),
            ("AffEmail", "ben@foo.com"),
            ("AffPhoneNum", "+886900000002"),
        ];
        aff_table.insert_row(data).unwrap();

        // d
        let data = vec![
            ("AffID", "6"),
            ("AffName", "Leo"),
            ("AffEmail", "leo@dee.com"),
            ("AffPhoneNum", "+886900000003"),
        ];
        aff_table.insert_row(data).unwrap();

        let data = vec![
            ("AffID", "1"),
            ("AffName", "John"),
            ("AffEmail", "john@dee.com"),
            ("AffPhoneNum", "+886900000004"),
        ];
        aff_table.insert_row(data).unwrap();

        // d
        let data = vec![
            ("AffID", "4"),
            ("AffName", "Ray"),
            ("AffEmail", "ray@dee.com"),
            ("AffPhoneNum", "+886900000005"),
        ];
        aff_table.insert_row(data).unwrap();

        // d
        let data = vec![
            ("AffID", "5"),
            ("AffName", "Bryn"),
            ("AffEmail", "bryn@dee.com"),
            ("AffPhoneNum", "+886900000006"),
        ];
        aff_table.insert_row(data).unwrap();

        let data = vec![
            ("AffID", "8"),
            ("AffName", "Eric"),
            ("AffEmail", "eric@doo.com"),
            ("AffPhoneNum", "+886900000007"),
        ];
        aff_table.insert_row(data).unwrap();

        let data = vec![
            ("AffID", "3"),
            ("AffName", "Vinc"),
            ("AffEmail", "vinc@doo.com"),
            ("AffPhoneNum", "+886900000008"),
        ];
        aff_table.insert_row(data).unwrap();

        DiskInterface::append_rows(
            "crazyguy",
            "BookerDB",
            "Affiliates",
            &aff_table.rows[..].iter().cloned().collect(),
            Some(file_base_path),
        )
        .unwrap();

        DiskInterface::delete_rows("crazyguy", "BookerDB", "Affiliates", &vec![2, 3], Some(file_base_path)).unwrap();
        DiskInterface::delete_rows("crazyguy", "BookerDB", "Affiliates", &vec![4, 6], Some(file_base_path)).unwrap();

        let table_meta =
            DiskInterface::load_table_meta("crazyguy", "BookerDB", "Affiliates", Some(file_base_path)).unwrap();
        let mut index = Index::new(table_meta).unwrap();
        index.build_from_bin(Some(file_base_path)).unwrap();

        assert_eq!(index.index_data.len(), 5);

        for i in 1..index.index_data.len() {
            assert!(index.index_data[i - 1].key_value < index.index_data[i].key_value);
        }

        // index.write_index_table().unwrap();
    }
}
