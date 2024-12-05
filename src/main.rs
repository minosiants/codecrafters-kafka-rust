#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;
use bytes::{BufMut, Bytes};


struct Header {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
}
impl Header {
    fn new(request_api_key: i16, request_api_version: i16, correlation_id: i32) -> Self {
        Self {
            request_api_key,
            request_api_version,
            correlation_id,
        }
    }
}
impl Into<Vec<u8>> for Header {
    fn into(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.request_api_key.to_be_bytes());
        bytes.extend(self.request_api_version.to_be_bytes());
        bytes.extend(self.correlation_id.to_be_bytes());
        bytes
    }
}

impl From<&[u8]> for Header {
    fn from(bytes: &[u8]) -> Self {
        let request_api_key = i16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let request_api_version = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        Header::new(request_api_key, request_api_version, correlation_id)
    }
}
struct Request {
    header: Header,
}
impl Request {
    fn new(header: Header) -> Self {
        Self {
            header,
        }
    }
}
impl Into<Vec<u8>> for Request {
    fn into(self) -> Vec<u8> {
        let header: Vec<u8> = self.header.into();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(header.as_slice());
        bytes
    }
}

impl From<&[u8]> for Request {
    fn from(bytes: &[u8]) -> Self {
        let header: Header = Header::from(&bytes[0..8]);
        Request::new(header)
    }
}
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                /* let message_size:i32 = 35;
                 let request_api_key:i16 = 18;
                 let request_api_version:i16 = 4;
                 let correletion_id:i32 = 1870644833;*/
                let mut message_size = [0;4];
                stream.read_exact(&mut message_size).unwrap();
                let message_size = i32::from_be_bytes(message_size) as usize;
                let mut buffer = vec![0; message_size];
                stream.read_exact(&mut buffer).unwrap();
                let req: Request = Request::from(buffer.as_slice());
                let error_code:i16 = 35;
                let mut resp: Vec<u8> = Vec::with_capacity(8);
                resp.put_i32(0);
                resp.put_i32(req.header.correlation_id);
                resp.put_i16(error_code);
                stream.write_all(&resp).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
