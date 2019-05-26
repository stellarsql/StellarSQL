use crate::component::datatype::DataType;
use crate::index::tree::{NodeType, PagePtr, RowPtr, Tree};
use crate::storage::page::{INDEX_INTERNAL_HEADER_SIZE, INDEX_LEAF_HEADER_SIZE};

/// B+ Tree
///
/// Internal Node
/// ```
/// +---------------------------+
/// |P0|K1||P1|K1|| ...  ||Pn|Kn|
/// +---------------------------+
/// K: Key Value
/// P(n-1): Pointer to the page with Kn-1 < Value < Kn
/// ```
///
/// Leaf Node
/// ```
/// +-----------------------------------+
/// |P0||R0|K0||R1|K1|| ...  ||Rn|Kn||P1|
/// +-----------------------------------+
/// K: Key Value
/// R: Record pointer
/// P0: Previous page pointer
/// P1: Next page pointer
/// ```
pub struct BPlusTree<T> {
    pid: u32,
    node_type: NodeType,
    key_type: DataType,
    capacity: usize,
    ptr_size: usize,
    key_size: usize,
    row_ptr_size: Option<usize>,
    ptrs: Vec<PagePtr>,
    keys: Vec<T>,
    rows: Option<Vec<RowPtr>>,
    nodes: Vec<Box<Self>>,
}

impl<T> Tree<T> for BPlusTree<T> {
    fn new(
        pid: u32,
        node_type: NodeType,
        key_type: DataType,
        ptr_size: usize,
        key_size: usize,
        row_ptr_size: Option<usize>,
    ) -> Self {
        match node_type {
            NodeType::Internal => {
                let capacity = get_internal_capacity(ptr_size, key_size);
                let ptrs: Vec<PagePtr> = Vec::with_capacity(capacity);
                let keys: Vec<T> = Vec::with_capacity(capacity);
                let rows = None;
                let nodes: Vec<Box<Self>> = Vec::with_capacity(capacity);
                Self {
                    pid,
                    node_type,
                    key_type,
                    capacity,
                    ptr_size,
                    key_size,
                    row_ptr_size,
                    ptrs,
                    keys,
                    rows,
                    nodes,
                }
            }
            NodeType::Leaf => {
                let capacity = get_leaf_capacity(ptr_size, key_size, row_ptr_size.unwrap());
                let ptrs: Vec<PagePtr> = Vec::with_capacity(2);
                let keys: Vec<T> = Vec::with_capacity(capacity);
                let rows: Option<Vec<RowPtr>> = Some(Vec::with_capacity(capacity));
                let nodes: Vec<Box<Self>> = Vec::with_capacity(2);
                Self {
                    pid,
                    node_type,
                    key_type,
                    capacity,
                    ptr_size,
                    key_size,
                    row_ptr_size,
                    ptrs,
                    keys,
                    rows,
                    nodes,
                }
            }
        }
    }
    fn height(&self) -> u32 {
        0
    }
    fn insert(&mut self, val: T) {}
    fn delete(&mut self, val: T) {}
    fn search(&self, val: T) -> RowPtr {
        (0, 0)
    }
}

fn get_internal_capacity(ptr_size: usize, key_size: usize) -> usize {
    let page_size = match dotenv!("PAGE_SIZE").parse::<usize>() {
        Ok(s) => s,
        Err(_) => 4096,
    };
    // page_size - header_size > n(key_size) + (n+1)(ptr_size)
    (page_size - INDEX_INTERNAL_HEADER_SIZE - ptr_size) / (ptr_size + key_size)
}

fn get_leaf_capacity(ptr_size: usize, key_size: usize, row_ptr_size: usize) -> usize {
    let page_size = match dotenv!("PAGE_SIZE").parse::<usize>() {
        Ok(s) => s,
        Err(_) => 4096,
    };
    // page_size - header_size > n(key_size + row_ptr_size) + 2(ptr_size)
    (page_size - INDEX_LEAF_HEADER_SIZE - 2 * ptr_size) / (row_ptr_size + key_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_new_b_plus_tree() {
        let _internal_tree: BPlusTree<u32> = BPlusTree::new(0, NodeType::Internal, DataType::Int, 4, 8, None);
        let _leaf_tree: BPlusTree<String> = BPlusTree::new(0, NodeType::Leaf, DataType::Char(10), 4, 128, Some(8));
    }
}
