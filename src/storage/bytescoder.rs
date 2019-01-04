extern crate byteorder;

use crate::component::datatype::DataType;
use crate::component::field;
use crate::component::field::Field;
use crate::component::table::Row;
use crate::storage::file::TableMeta;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::num;
use std::string;

#[derive(Debug, Clone)]
pub struct BytesCoder {
    /* definition */
// Ideally, BytesCoder is a stateless struct
}

#[derive(Debug, PartialEq, Clone)]
pub enum BytesCoderError {
    Io,
    ParseInt,
    ParseFloat,
    StringLength,
    StringDecode,
    AttrNotExists,
}

// Implement the `trim` method for byte slices
trait SliceExt {
    fn trim(&self) -> &Self;
}

impl SliceExt for [u8] {
    fn trim(&self) -> &[u8] {
        fn is_padding(c: &u8) -> bool {
            *c == 0 as u8
        }

        fn is_not_padding(c: &u8) -> bool {
            !is_padding(c)
        }

        if let Some(first) = self.iter().position(is_not_padding) {
            if let Some(last) = self.iter().rposition(is_not_padding) {
                &self[first..last + 1]
            } else {
                unreachable!();
            }
        } else {
            &[]
        }
    }
}

impl From<io::Error> for BytesCoderError {
    fn from(_err: io::Error) -> BytesCoderError {
        BytesCoderError::Io
    }
}

impl From<num::ParseIntError> for BytesCoderError {
    fn from(_err: num::ParseIntError) -> BytesCoderError {
        BytesCoderError::ParseInt
    }
}

impl From<num::ParseFloatError> for BytesCoderError {
    fn from(_err: num::ParseFloatError) -> BytesCoderError {
        BytesCoderError::ParseFloat
    }
}

impl From<string::FromUtf8Error> for BytesCoderError {
    fn from(_err: string::FromUtf8Error) -> BytesCoderError {
        BytesCoderError::StringDecode
    }
}

impl fmt::Display for BytesCoderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BytesCoderError::Io => write!(f, "Error occurred during read/write from byte slices"),
            BytesCoderError::ParseInt => write!(f, "Error occurred during parsing value from Int data type."),
            BytesCoderError::ParseFloat => {
                write!(f, "Error occurred during parsing value from Float or Double data type.")
            }
            BytesCoderError::StringLength => write!(f, "The string attempt to store exceed the size of field."),
            BytesCoderError::StringDecode => write!(f, "Error occurred during decoding utf8 String from bytes."),
            BytesCoderError::AttrNotExists => write!(f, "The row does not contain specified attribute."),
        }
    }
}

impl BytesCoder {
    pub fn attr_to_bytes(datatype: &DataType, str_val: &str) -> Result<Vec<u8>, BytesCoderError> {
        let mut bs: Vec<u8> = vec![];
        match datatype {
            DataType::Char(length) => {
                if str_val.len() > *length as usize {
                    return Err(BytesCoderError::StringLength);
                }
                bs.extend_from_slice(str_val.as_bytes());
                bs.extend_from_slice(&vec![0; *length as usize - str_val.len()])
            }
            DataType::Double => bs.write_f64::<BigEndian>(str_val.parse::<f64>()?)?,
            DataType::Float => bs.write_f32::<BigEndian>(str_val.parse::<f32>()?)?,
            DataType::Int => bs.write_i32::<BigEndian>(str_val.parse::<i32>()?)?,
            DataType::Varchar(length) => {
                if str_val.len() > *length as usize {
                    return Err(BytesCoderError::StringLength);
                }
                bs.extend_from_slice(str_val.as_bytes());
                bs.extend_from_slice(&vec![0; *length as usize - str_val.len()])
            }
        }

        Ok(bs)
    }

    pub fn bytes_to_attr(datatype: &DataType, bytes: &Vec<u8>) -> Result<String, BytesCoderError> {
        let s: String;
        match datatype {
            DataType::Char(_length) => s = String::from_utf8(bytes.trim().to_vec())?,
            DataType::Double => s = (&(*bytes)[..]).read_f64::<BigEndian>()?.to_string(),
            DataType::Float => s = (&(*bytes)[..]).read_f32::<BigEndian>()?.to_string(),
            DataType::Int => s = (&(*bytes)[..]).read_i32::<BigEndian>()?.to_string(),
            DataType::Varchar(_length) => s = String::from_utf8(bytes.trim().to_vec())?,
        }

        Ok(s)
    }

    pub fn row_to_bytes(tablemeta: &TableMeta, row: &Row) -> Result<Vec<u8>, BytesCoderError> {
        // set `__valid__` to 1
        let mut row_bytes = vec![1];
        for attr in tablemeta.attrs_order[1..].iter() {
            let attr_bytes = BytesCoder::attr_to_bytes(
                &tablemeta.attrs[attr].datatype,
                row.0.get(attr).ok_or_else(|| BytesCoderError::AttrNotExists)?,
            )?;
            row_bytes.extend_from_slice(&attr_bytes);
        }

        Ok(row_bytes)
    }

    pub fn bytes_to_row(tablemeta: &TableMeta, bytes: &Vec<u8>) -> Result<Row, BytesCoderError> {
        let mut attr_vals: Vec<String> = vec![];
        for (idx, attr) in tablemeta.attrs_order[1..].iter().enumerate() {
            let attr_bytes = bytes
                [tablemeta.attr_offset_ranges[idx + 1][0] as usize..tablemeta.attr_offset_ranges[idx + 1][1] as usize]
                .to_vec();
            let attr_val = BytesCoder::bytes_to_attr(&tablemeta.attrs[attr].datatype, &attr_bytes)?;
            attr_vals.push(attr_val);
        }
        let mut new_row = Row::new();
        for i in 0..attr_vals.len() {
            new_row
                .0
                .insert(tablemeta.attrs_order[i + 1].clone(), attr_vals[i].clone());
        }

        Ok(new_row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn test_attr_encode_decode() {
        let datatype = DataType::Char(10);
        let data = "test你好".to_string();
        assert_eq!(
            BytesCoder::bytes_to_attr(&datatype, &BytesCoder::attr_to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Double;
        let data = "3.1415926".to_string();
        assert_eq!(
            BytesCoder::bytes_to_attr(&datatype, &BytesCoder::attr_to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Float;
        let data = "2.71".to_string();
        assert_eq!(
            BytesCoder::bytes_to_attr(&datatype, &BytesCoder::attr_to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Int;
        let data = "123456543".to_string();
        assert_eq!(
            BytesCoder::bytes_to_attr(&datatype, &BytesCoder::attr_to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Varchar(100);
        let data = "abcdefghijklmnopqrstuvwxyz12345438967`+=/{}[]<>-_|%$#@!&^*()?,.".to_string();
        assert_eq!(
            BytesCoder::bytes_to_attr(&datatype, &BytesCoder::attr_to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Char(10);
        assert_eq!(
            BytesCoder::attr_to_bytes(&datatype, &data).unwrap_err(),
            BytesCoderError::StringLength
        );
    }

    #[test]
    pub fn test_row_encode_decode() {
        let mut aff_table_meta = TableMeta {
            name: "Affiliates".to_string(),
            primary_key: vec!["AffID".to_string()],
            foreign_key: vec![],
            reference_table: None,
            reference_attr: None,
            path_tsv: "Affiliates.tsv".to_string(),
            path_bin: "Affiliates.bin".to_string(),
            attr_offset_ranges: vec![vec![0, 1], vec![1, 5], vec![5, 55], vec![55, 95], vec![95, 115]],
            row_length: 115,
            // ignore attrs checking
            attrs_order: vec![
                "__valid__".to_string(),
                "AffID".to_string(),
                "AffEmail".to_string(),
                "AffName".to_string(),
                "AffPhoneNum".to_string(),
            ],
            attrs: HashMap::new(),
        };

        aff_table_meta.attrs.insert(
            "AffID".to_string(),
            Field::new_all("AffID", DataType::Int, true, None, field::Checker::None, false),
        );
        aff_table_meta.attrs.insert(
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
        aff_table_meta.attrs.insert(
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
        aff_table_meta.attrs.insert(
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

        let data = vec![
            ("AffID", "2"),
            ("AffName", "Ben"),
            ("AffEmail", "ben@foo.com"),
            ("AffPhoneNum", "+886900000002"),
        ];

        let mut row = Row::new();
        for i in 0..data.len() {
            row.0.insert(data[i].0.to_string(), data[i].1.to_string());
        }

        let reconstructed_row = BytesCoder::bytes_to_row(
            &aff_table_meta,
            &BytesCoder::row_to_bytes(&aff_table_meta, &row).unwrap(),
        )
        .unwrap();

        for (attr, val) in row.0.iter() {
            assert_eq!(val.clone(), reconstructed_row.0[attr]);
        }
    }
}
