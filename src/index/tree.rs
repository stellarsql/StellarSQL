pub type PagePtr = u32;
pub type PageOffset = usize;
pub type RowPtr = (PagePtr, PageOffset);

pub trait Tree<T> {
    fn new(pid: u32, node_type: NodeType, ptr_size: usize, key_size: usize, row_ptr_size: Option<usize>) -> Self;
    fn height(&self) -> u32;
    fn insert(&mut self, val: T);
    fn delete(&mut self, val: T);
    fn search(&self, val: T) -> RowPtr;
}

pub enum NodeType {
    Internal,
    Leaf,
}
