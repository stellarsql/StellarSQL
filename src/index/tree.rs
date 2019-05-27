use crate::component::datatype::DataType;

pub type PagePtr = u32;
pub type PageOffset = usize;
pub type RowPtr = (PagePtr, PageOffset);

pub trait Tree<T> {
    fn new(
        pid: u32,
        node_type: NodeType,
        key_type: DataType,
        ptr_size: usize,
        key_size: usize,
        row_ptr_size: Option<usize>,
    ) -> Box<Self>;
    fn height(&self) -> u32;
    fn insert(&mut self, val: T);
    fn delete(&mut self, val: T);
    fn search(&self, val: T) -> RowPtr;
}

#[derive(PartialEq)]
pub enum NodeType {
    Internal,
    Leaf,
}
