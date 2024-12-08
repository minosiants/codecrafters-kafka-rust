#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;
use bytes::{BufMut, Bytes};
use std::mem;
use std::fmt;


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
#[derive(Debug, Clone)]
struct ApiVersion {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    tag_buffer:i8,
}
impl ApiVersion {
    fn new(api_key: i16,
           min_version: i16,
           max_version: i16,) -> Self {
        Self{
            api_key,
            min_version,
            max_version,
            tag_buffer:0,
        }
    }
}

impl Into<Vec<u8>> for &ApiVersion {
    fn into(self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.put_i16(self.api_key);
        bytes.put_i16(self.min_version);
        bytes.put_i16(self.max_version);
        bytes.put_i8(self.tag_buffer);
        bytes
    }
}
#[derive(Debug, Clone)]
struct Response {
    message_size: i32,
    correlation_id: i32,
    error_code: i16,
    num_of_api_keys:i8,
    api_versions: Vec<ApiVersion>,
    throttle_time_ms:i32,
    tagged_fields:i8,
}

impl Response {
    fn new(correlation_id: i32,
           error_code: i16,
           api_versions: Vec<ApiVersion>) -> Self {
        Self {
            message_size : 7 + 4 + 2 + 1 + 4 + 1,
            correlation_id,
            error_code,
            num_of_api_keys:api_versions.len() as i8 +1,
            api_versions,
            throttle_time_ms:0,
            tagged_fields:0,
        }
    }
}
impl From<Response> for Vec<u8> {
    fn from(value: Response) -> Self {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.put_i32(value.message_size);
        //bytes.put_i32(0);
        bytes.put_i32(value.correlation_id);
        bytes.put_i16(value.error_code);
        bytes.put_i8(value.num_of_api_keys);
        let api_versions: Vec<u8> = value.api_versions.iter().flat_map::<Vec<u8>,_>(|e| e.into()).collect();
        bytes.extend(api_versions);
        bytes.put_i32(value.throttle_time_ms);
        bytes.put_i8(value.tagged_fields);
        bytes
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
                let mut message_size = [0; 4];
                stream.read_exact(&mut message_size).unwrap();
                let message_size = i32::from_be_bytes(message_size) as usize;
                let mut buffer = vec![0; message_size];
                stream.read_exact(&mut buffer).unwrap();
                let req: Request = Request::from(buffer.as_slice());
                if req.header.request_api_version > 4 || req.header.request_api_version < 0{
                    let message_size = 10;
                    let error_code:i16 = 35;
                    let mut error:Vec<u8> = Vec::new();
                    error.put_i32(message_size);
                    error.put_i32(req.header.correlation_id);
                    error.put_i16(error_code);
                    stream.write_all(&error).unwrap();
                } else {
                    let api_version = ApiVersion::new(18, 0, 4);
                    let resp:Vec<u8> = Response::new(req.header.correlation_id, 0, vec![api_version]).into();
                    stream.write(&resp).unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
