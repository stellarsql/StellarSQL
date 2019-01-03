extern crate byteorder;

use crate::component::datatype::DataType;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
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
        }
    }
}

impl BytesCoder {
    pub fn to_bytes(datatype: &DataType, str_val: &str) -> Result<Vec<u8>, BytesCoderError> {
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

    pub fn from_bytes(datatype: &DataType, bytes: &Vec<u8>) -> Result<String, BytesCoderError> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn test_bytes_encode_decode() {
        let datatype = DataType::Char(10);
        let data = "test你好".to_string();
        assert_eq!(
            BytesCoder::from_bytes(&datatype, &BytesCoder::to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Double;
        let data = "3.1415926".to_string();
        assert_eq!(
            BytesCoder::from_bytes(&datatype, &BytesCoder::to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Float;
        let data = "2.71".to_string();
        assert_eq!(
            BytesCoder::from_bytes(&datatype, &BytesCoder::to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Int;
        let data = "123456543".to_string();
        assert_eq!(
            BytesCoder::from_bytes(&datatype, &BytesCoder::to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Varchar(100);
        let data = "abcdefghijklmnopqrstuvwxyz12345438967`+=/{}[]<>-_|%$#@!&^*()?,.".to_string();
        assert_eq!(
            BytesCoder::from_bytes(&datatype, &BytesCoder::to_bytes(&datatype, &data).unwrap()).unwrap(),
            data
        );

        let datatype = DataType::Char(10);
        assert_eq!(
            BytesCoder::to_bytes(&datatype, &data).unwrap_err(),
            BytesCoderError::StringLength
        );
    }
}
