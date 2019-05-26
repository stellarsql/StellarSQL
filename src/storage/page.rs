use crate::component::datatype::DataType;
use crate::index::tree::NodeType;
use crate::storage::bytescoder::BytesCoder;

trait IndexPage {
    fn new(
        pid: u32,
        capacity: usize,
        node_type: NodeType,
        key_type: DataType,
        ptr_size: usize,
        key_size: usize,
        row_ptr_size: Option<usize>,
    ) -> Self;
}

trait FilePage {
    fn new(pid: u32, block_length: usize) -> Self;
}

struct IndexInternalPage {
    header: HeaderBytes,
    content: ContentBytes,
}

struct IndexLeafPage {
    header: HeaderBytes,
    content: ContentBytes,
}

struct DataFilePage {
    header: HeaderBytes,
    content: ContentBytes,
}

trait Header {
    fn to_bytes(&self) -> HeaderBytes;
    fn from_bytes(header_bytes: &HeaderBytes) -> Self;
}

pub const FILE_HEADER_SIZE: usize = 20;
struct FileHeader {
    pid: u32,
    capacity: usize,
    block_length: usize,
}

pub const INDEX_INTERNAL_HEADER_SIZE: usize = 20;
struct IndexInternalHeader {
    pid: u32,
    capacity: usize,
    node_type: NodeType,
    key_type: DataType,
    ptr_size: usize,
    key_size: usize,
}

pub const INDEX_LEAF_HEADER_SIZE: usize = 20;
struct IndexLeafHeader {
    pid: u32,
    capacity: usize,
    node_type: NodeType,
    key_type: DataType,
    ptr_size: usize,
    key_size: usize,
    row_ptr_size: usize,
}

type Bytes = Vec<u8>;
struct HeaderBytes(Bytes);
struct ContentBytes(Bytes);

impl IndexPage for IndexInternalPage {
    fn new(
        pid: u32,
        capacity: usize,
        node_type: NodeType,
        key_type: DataType,
        ptr_size: usize,
        key_size: usize,
        row_ptr_size: Option<usize>,
    ) -> Self {
        let header = IndexInternalHeader {
            pid,
            capacity,
            node_type,
            key_type,
            ptr_size,
            key_size,
        };

        let content = Vec::with_capacity(key_size * capacity + ptr_size * (capacity + 1));

        Self {
            header: header.to_bytes(),
            content: ContentBytes(content),
        }
    }
}

impl IndexPage for IndexLeafPage {
    fn new(
        pid: u32,
        capacity: usize,
        node_type: NodeType,
        key_type: DataType,
        ptr_size: usize,
        key_size: usize,
        row_ptr_size: Option<usize>,
    ) -> Self {
        let row_ptr_size = row_ptr_size.unwrap();
        let header = IndexLeafHeader {
            pid,
            capacity,
            node_type,
            key_type,
            ptr_size,
            key_size,
            row_ptr_size,
        };

        let content = Vec::with_capacity((key_size + row_ptr_size) * capacity + ptr_size * 2);

        Self {
            header: header.to_bytes(),
            content: ContentBytes(content),
        }
    }
}

impl FilePage for DataFilePage {
    fn new(pid: u32, block_length: usize) -> Self {
        let capacity = get_file_capacity(&block_length);

        let header = FileHeader {
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

fn get_file_capacity(block_length: &usize) -> usize {
    let page_size = match dotenv!("PAGE_SIZE").parse::<usize>() {
        Ok(s) => s,
        Err(_) => 4096,
    };

    (page_size - FILE_HEADER_SIZE) / block_length
}

impl Header for FileHeader {
    fn to_bytes(&self) -> HeaderBytes {
        let mut bytes: Bytes = vec![];
        bytes.extend_from_slice(&BytesCoder::attr_to_bytes(&DataType::Int, &self.pid.to_string()).unwrap());
        bytes.extend_from_slice(&BytesCoder::attr_to_bytes(&DataType::Int, &self.capacity.to_string()).unwrap());
        bytes.extend_from_slice(&BytesCoder::attr_to_bytes(&DataType::Int, &self.block_length.to_string()).unwrap());

        HeaderBytes(bytes)
    }

    fn from_bytes(header_bytes: &HeaderBytes) -> Self {
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
        Self {
            pid,
            capacity,
            block_length,
        }
    }
}

impl Header for IndexInternalHeader {
    fn to_bytes(&self) -> HeaderBytes {
        let mut bytes: Bytes = vec![];
        // TODO
        HeaderBytes(bytes)
    }

    fn from_bytes(header_bytes: &HeaderBytes) -> Self {
        // TODO
        let bytes = &header_bytes.0;
        let pid = 0;
        let capacity = 0;
        let node_type = NodeType::Internal;
        let key_type = DataType::Int;
        let ptr_size = 0;
        let key_size = 0;
        Self {
            pid,
            capacity,
            node_type,
            key_type,
            ptr_size,
            key_size,
        }
    }
}

impl Header for IndexLeafHeader {
    fn to_bytes(&self) -> HeaderBytes {
        let mut bytes: Bytes = vec![];
        // TODO
        HeaderBytes(bytes)
    }

    fn from_bytes(header_bytes: &HeaderBytes) -> Self {
        // TODO
        let bytes = &header_bytes.0;
        let pid = 0;
        let capacity = 0;
        let node_type = NodeType::Leaf;
        let key_type = DataType::Int;
        let ptr_size = 0;
        let key_size = 0;
        let row_ptr_size = 0;
        Self {
            pid,
            capacity,
            node_type,
            key_type,
            ptr_size,
            key_size,
            row_ptr_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_file_header() {
        let header = FileHeader {
            pid: 1,
            capacity: 101,
            block_length: 128,
        };

        let header_bytes = header.to_bytes();
        let header = FileHeader::from_bytes(&header_bytes);

        assert_eq!(header.pid, 1);
        assert_eq!(header.capacity, 101);
        assert_eq!(header.block_length, 128);
    }

    #[test]
    pub fn test_create_file_page() {
        let _file_page = DataFilePage::new(0, 128);
    }
}
