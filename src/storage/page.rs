use crate::component::datatype::DataType;
use crate::storage::bytescoder::BytesCoder;

trait Page {
    fn new(pid: u32, block_length: usize) -> Self;
}

struct IndexPage {
    header: HeaderBytes,
    content: ContentBytes,
}

const HEADER_SIZE: usize = 20;
struct Header {
    pid: u32,
    capacity: usize,
    block_length: usize,
}

type Bytes = Vec<u8>;
struct HeaderBytes(Bytes);
struct ContentBytes(Bytes);

impl Page for IndexPage {
    fn new(pid: u32, block_length: usize) -> Self {
        let capacity = get_capacity(&block_length);

        let header = Header {
            pid,
            capacity: capacity.clone(),
            block_length: block_length.clone(),
        };

        let content = Vec::with_capacity(block_length * capacity);

        Self {
            header: header.to_bytes(),
            content: ContentBytes(content),
        }
    }
}

fn get_capacity(block_length: &usize) -> usize {
    let page_size = match dotenv!("PAGE_SIZE").parse::<usize>() {
        Ok(s) => s,
        Err(_) => 4096,
    };

    (page_size - HEADER_SIZE) / block_length
}

impl Header {
    fn to_bytes(&self) -> HeaderBytes {
        let mut bytes: Bytes = vec![];
        bytes.extend_from_slice(&BytesCoder::attr_to_bytes(&DataType::Int, &self.pid.to_string()).unwrap());
        bytes.extend_from_slice(&BytesCoder::attr_to_bytes(&DataType::Int, &self.capacity.to_string()).unwrap());
        bytes.extend_from_slice(&BytesCoder::attr_to_bytes(&DataType::Int, &self.block_length.to_string()).unwrap());

        HeaderBytes(bytes)
    }

    fn from_bytes(header_bytes: &HeaderBytes) -> Header {
        let bytes = &header_bytes.0;
        let pid = BytesCoder::bytes_to_attr(&DataType::Int, &bytes[0..4])
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let capacity = BytesCoder::bytes_to_attr(&DataType::Int, &bytes[4..8])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let block_length = BytesCoder::bytes_to_attr(&DataType::Int, &bytes[8..12])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        Header {
            pid,
            capacity,
            block_length,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_header() {
        let header = Header {
            pid: 1,
            capacity: 101,
            block_length: 128,
        };

        let header_bytes = header.to_bytes();
        let header = Header::from_bytes(&header_bytes);

        assert_eq!(header.pid, 1);
        assert_eq!(header.capacity, 101);
        assert_eq!(header.block_length, 128);
    }

    #[test]
    pub fn test_create_index_page() {
        let _index_page = IndexPage::new(0, 128);
    }
}
