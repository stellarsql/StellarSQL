use crate::component::datatype::DataType;
use crate::index::tree::{NodeType, PagePtr, RowPtr, Tree};
use crate::storage::page::{INDEX_INTERNAL_HEADER_SIZE, INDEX_LEAF_HEADER_SIZE};
use std::cmp::PartialOrd;

/// B+ Tree
///
/// Internal Node
/// ```
/// +---------------------------+
/// |P0|K1||P1|K1|| ...  ||Pn|Kn|
/// +---------------------------+
/// K: Key Value
/// P(n-1): Pointer to the page with Kn-1 <= Value < Kn
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
    height: u32,
}

impl<T: PartialOrd> Tree<T> for BPlusTree<T> {
    fn new(
        pid: u32,
        node_type: NodeType,
        key_type: DataType,
        ptr_size: usize,
        key_size: usize,
        row_ptr_size: Option<usize>,
    ) -> Box<Self> {
        match node_type {
            NodeType::Internal => {
                let capacity = get_internal_capacity(ptr_size, key_size);
                let ptrs: Vec<PagePtr> = Vec::with_capacity(capacity + 1);
                let keys: Vec<T> = Vec::with_capacity(capacity);
                let rows = None;
                let nodes: Vec<Box<Self>> = Vec::with_capacity(capacity);
                Box::new(Self {
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
                    height: 1,
                })
            }
            NodeType::Leaf => {
                let capacity = get_leaf_capacity(ptr_size, key_size, row_ptr_size.unwrap());
                let ptrs: Vec<PagePtr> = Vec::with_capacity(2);
                let keys: Vec<T> = Vec::with_capacity(capacity);
                let rows: Option<Vec<RowPtr>> = Some(Vec::with_capacity(capacity));
                let nodes: Vec<Box<Self>> = Vec::with_capacity(2);
                Box::new(Self {
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
                    height: 1,
                })
            }
        }
    }
    fn height(&self) -> u32 {
        self.height
    }
    fn insert(&mut self, val: T) {}
    fn delete(&mut self, val: T) {}
    fn search(&self, val: T) -> RowPtr {
        (0, 0)
    }
}

impl<T: PartialOrd> BPlusTree<T> {
    fn node_type(&self) -> &NodeType {
        &self.node_type
    }

    /// find_ptr: upper-bounded binary searching the key to find the page
    ///
    /// Internal Node:
    /// ```
    /// +-------------+
    /// |P0| Key=2 |P1|
    /// +-------------+
    /// ```
    /// - Find Key=1 -> P0
    /// - Find Key=2 -> P1
    /// - Find Key=3 -> P1
    fn find_ptr(arr: &Vec<T>, left: usize, right: usize, val: T) -> usize {
        let mut l = left as i32;
        let mut r = right as i32;
        let mut pos = 0;
        while l < r {
            let m = l + (r - l) / 2;
            if arr[m as usize] > val {
                r = m;
                pos = r;
            } else {
                l = m + 1;
                pos = l;
            }
        }
        pos as usize
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
    fn test_new_b_plus_tree() {
        let _internal_tree: Box<BPlusTree<i32>> = BPlusTree::new(0, NodeType::Internal, DataType::Int, 4, 8, None);
        let _leaf_tree: Box<BPlusTree<String>> = BPlusTree::new(0, NodeType::Leaf, DataType::Char(10), 4, 128, Some(8));
    }

    #[test]
    fn test_find_ptr() {
        let arr = vec![vec![10], vec![10, 20], vec![10, 20, 30]];
        for i in 0..arr.len() {
            let pos = BPlusTree::find_ptr(&arr[i], 0, arr[i].len(), 11);
            assert_eq!(pos, 1); // greater
            let pos = BPlusTree::find_ptr(&arr[i], 0, arr[i].len(), 10);
            assert_eq!(pos, 1); // equal
            let pos = BPlusTree::find_ptr(&arr[i], 0, arr[i].len(), 1);
            assert_eq!(pos, 0); // smaller
        }
        let arr = vec![1, 3, 5, 7, 9];
        let pos = BPlusTree::find_ptr(&arr, 0, arr.len(), 11);
        assert_eq!(pos, 5); // right boundary
        let pos = BPlusTree::find_ptr(&arr, 0, arr.len(), 9);
        assert_eq!(pos, 5);
        let pos = BPlusTree::find_ptr(&arr, 0, arr.len(), 8);
        assert_eq!(pos, 4);
        let pos = BPlusTree::find_ptr(&arr, 0, arr.len(), 1);
        assert_eq!(pos, 1);
        let pos = BPlusTree::find_ptr(&arr, 0, arr.len(), 0);
        assert_eq!(pos, 0); // left boundary
    }

}
