#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;
use bytes::Bytes;

struct Header {
   // message_size:i32,
    correletion_id:i32,
}
impl Header {
    fn new(message_size:i32, correletion_id:i32)-> Header {
        Header {
     //       message_size,
            correletion_id,
        }
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
       // bytes.extend(self.message_size.to_be_bytes());
        bytes.extend(self.correletion_id.to_be_bytes());
        return bytes
    }
    fn from_bytes(bytes:&[u8]) -> Self {
        //let message_size = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let correletion_id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        Self{
          //  message_size,
            correletion_id
        }
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
            Ok(mut _stream) => {
                println!("accepted new connection");
                let message_size:i32 = 0;
                let correletion_id:i32 = 7;
                let header = Header::new(message_size, correletion_id);
               match _stream.write(&*header.to_bytes()) {
                    Ok(_) => println!("response sent to the client"),
                   Err(_) =>eprintln!("Failed to write to connection")

               }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
