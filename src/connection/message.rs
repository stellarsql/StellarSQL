#[derive(Debug)]
pub struct Message {
    /// The TCP socket.
    socket: TcpStream,

    /// Buffer used when reading from the socket. Data is not returned from this
    /// buffer until an entire message has been read.
    rd: BytesMut,

    /// Buffer used to stage data before writing it to the socket.
    wr: BytesMut,
}

impl Message {
    /// Create a new `Message` codec backed by the socket
    fn new(socket: TcpStream) -> Self {
        Message {
            socket,
            rd: BytesMut::new(),
            wr: BytesMut::new(),
        }
    }
}