#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;
use bytes::{BufMut, Bytes, Buf};


struct Header {
    request_api_key:i16,
    request_api_version:i16,
    correlation_id:i32,
}
impl Header {
    fn new(request_api_key:i16, request_api_version:i16, correlation_id:i32)-> Self {
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

impl From<&[u8]>  for Header{
    fn from(bytes: &[u8]) -> Self {
        let request_api_key = i16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let request_api_version = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        Header::new(request_api_key, request_api_version, correlation_id)
    }
}
struct Request {
    message_size:i32,
    header:Header,
}
impl Request {
    fn new(message_size:i32, header:Header) -> Self {
        Self{
            message_size,
            header
        }
    }
}
impl Into<Vec<u8>> for Request {
    fn into(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.message_size.to_be_bytes());
        let header:Vec<u8> = self.header.into();
        bytes.extend_from_slice(header.as_slice());
        bytes
    }
}

impl From<&[u8]> for Request {
    fn from(bytes: &[u8]) -> Self {
        let correlation_id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let header:Header  = Header::from(&bytes[4..12]);
        Request::new(correlation_id, header)
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
                let mut len = [0; 4];
                stream.read_exact(&mut len).unwrap();
                let len = i32::from_be_bytes(len) as usize;
                let mut request = vec![0; len];
                stream.read_exact(&mut request).unwrap();
                let mut request = request.as_slice();
                let _request_api_key = request.get_i16();
                let _request_api_version = request.get_i16();
                let correlation_id = request.get_i32();
                let mut response = Vec::with_capacity(8);
                response.put_i32(0);
                response.put_i32(correlation_id);
                stream.write_all(&response).unwrap();


            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
