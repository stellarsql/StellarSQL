extern crate byteorder;

use crate::component::table::Row;
use crate::storage::bytescoder::BytesCoder;
use crate::storage::diskinterface::{DiskError, DiskInterface, TableMeta};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::Path;

pub struct Index {
    table_meta: TableMeta,
    index_data: Vec<RowPair>,
    num_rows: u32, // row number of the table including deleted
}

/// (row, key_value) pair
#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct RowPair {
    row: u32,
    key_value: Vec<u8>,
}

impl RowPair {
    pub fn new(row: u32, key_value: Vec<u8>) -> Self {
        RowPair { row, key_value }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, DiskError> {
        let mut bs: Vec<u8> = vec![];
        bs.write_u32::<BigEndian>(self.row)?;
        bs.extend_from_slice(&self.key_value);

        Ok(bs)
    }
}

#[allow(dead_code)]
impl Index {
    /// construct a new Index
    pub fn new(table_meta: TableMeta) -> Result<Index, DiskError> {
        Ok(Index {
            table_meta,
            index_data: vec![],
            num_rows: 0,
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
        let mut num_rows: u32 = 0;
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
            num_rows += 1;
        }

        new_index_data.sort_by(|rp1, rp2| rp1.key_value.cmp(&rp2.key_value));

        self.index_data = new_index_data;
        self.num_rows = num_rows;

        Ok(())
    }

    // save(overwrite) index table into index file
    fn save(&self, file_base_path: Option<&str>) -> Result<(), DiskError> {
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

        // create chunk of bytes to be written
        let mut chunk_bytes = vec![];
        for rp in self.index_data.iter() {
            chunk_bytes.extend_from_slice(&rp.to_bytes()?);
        }

        // write chunk of bytes to index bin
        let index_bin_path = format!(
            "{}/{}/{}/{}_{}.idx",
            base_path,
            self.table_meta.username,
            self.table_meta.db_name,
            self.table_meta.name,
            self.table_meta.primary_key[0]
        );
        let mut index_bin_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(index_bin_path)?;
        index_bin_file.write_all(&chunk_bytes)?;

        Ok(())
    }

    // Load index from storage
    pub fn load(&mut self, file_base_path: Option<&str>) -> Result<(), DiskError> {
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

        let index_bin_path = format!(
            "{}/{}/{}/{}_{}.idx",
            base_path,
            self.table_meta.username,
            self.table_meta.db_name,
            self.table_meta.name,
            self.table_meta.primary_key[0]
        );
        if !Path::new(&index_bin_path).exists() {
            return Err(DiskError::TableIdxFileNotExists);
        }
        let index_bin_file = fs::File::open(&index_bin_path)?;
        let mut buffered = BufReader::new(index_bin_file);

        let mut chunk_bytes = vec![];
        buffered.read_to_end(&mut chunk_bytes)?;

        // parse chunk of bytes to vector of rows
        let mut index_data: Vec<RowPair> = vec![];
        for rp_bytes in chunk_bytes
            .chunks((self.table_meta.attr_offset_ranges[1][1] - self.table_meta.attr_offset_ranges[1][0] + 4) as usize)
        {
            index_data.push(RowPair::new(
                (&rp_bytes[..4]).read_u32::<BigEndian>()?,
                rp_bytes[4..].to_vec(),
            ));
        }

        self.index_data = index_data;
        self.num_rows = DiskInterface::get_num_rows(
            &self.table_meta.username,
            &self.table_meta.db_name,
            &self.table_meta.name,
            Some(base_path),
        )?;

        Ok(())
    }

    // insert a row-key pair into the index
    pub fn insert(&mut self, row: &Row) -> Result<(), DiskError> {
        let new_row_pair = RowPair::new(
            self.num_rows.clone(),
            BytesCoder::attr_to_bytes(
                &self.table_meta.attrs[&self.table_meta.primary_key[0]].datatype,
                row.data
                    .get(&self.table_meta.primary_key[0])
                    .ok_or_else(|| DiskError::AttrNotExists)?,
            )?,
        );
        match self
            .index_data
            .binary_search_by(|rp| rp.key_value.cmp(&new_row_pair.key_value))
        {
            Ok(_pos) => return Err(DiskError::DuplicatedKey),
            Err(pos) => {
                self.index_data.insert(pos, new_row_pair);
                self.num_rows += 1;
            }
        }

        Ok(())
    }
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
    pub fn test_build_save_load_insert() {
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

        // to be deleted
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

        // to be deleted
        let data = vec![
            ("AffID", "4"),
            ("AffName", "Ray"),
            ("AffEmail", "ray@dee.com"),
            ("AffPhoneNum", "+886900000005"),
        ];
        aff_table.insert_row(data).unwrap();

        // to be deleted
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
        assert_eq!(index.num_rows, 8);

        for i in 1..index.index_data.len() {
            assert!(index.index_data[i - 1].key_value < index.index_data[i].key_value);
        }

        let index_data = index.index_data.to_vec();
        index.save(Some(file_base_path)).unwrap();
        index.load(Some(file_base_path)).unwrap();
        assert_eq!(index_data, index.index_data);

        let data = vec![
            ("AffID", "5"),
            ("AffName", "Allie"),
            ("AffEmail", "allie@doo.com"),
            ("AffPhoneNum", "+886900000005"),
        ];
        aff_table.insert_row(data).unwrap();

        index.insert(&aff_table.rows[aff_table.rows.len() - 1]).unwrap();
        assert_eq!(index.index_data.len(), 6);
        for i in 1..index.index_data.len() {
            assert!(index.index_data[i - 1].key_value < index.index_data[i].key_value);
        }
        assert_eq!(index.num_rows, 9);
    }
}
