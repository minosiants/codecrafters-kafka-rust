use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;
use bytes::BufMut;

use codecrafters_kafka::{Api, ApiKey, Version, Context, CorrelationId, Error, ErrorCode, MessageSize, Request, Response, Result, TaggedFields};

fn error_response(correlation_id: &CorrelationId) -> Vec<u8> {
    let mut error: Vec<u8> = Vec::new();
    error.put_i32(*MessageSize::new(10));
    error.put_i32(**correlation_id);
    error.put_i16(*ErrorCode::UnsupportedVersion);
    error
}
fn process_request(request:&Request) -> Vec<u8> {
    Response::new(request.header().correlation_id(),ErrorCode::NoError, vec![
        Api::new(ApiKey::ApiVersions, Version::V0, Version::V4, TaggedFields::new(0)),
        Api::new(ApiKey::DescribeTopicPartitions, Version::V0, Version::V0, TaggedFields::new(0))
    ]).into()
}
fn process_stream(stream:&mut TcpStream) -> Result<Vec<u8>> {
    println!("accepted new connection");
    let req:Result<Request> = stream.try_into();
    println!("req {:?}", req);
    let resp = match req {
        Ok(request) => {
            println!("ok {:?}", request);
            process_request(&request)
        }
        Err(Error::UnsupportedApiVersion(_, Some(id))) => {
            println!("unsupoeted api version: ");
            error_response(&id)
        }
        Err(Error::UnsupportedApiKey(_, Some(id))) => {
            println!("unsuported api key");
            error_response(&id)
        }
        Err(Error::GeneralError(txt, err)) => {
            println!("txt: {}", txt);
            println!("err: {:?}", err);
            Vec::new()
        }
        Err(e) => {
            println!("err {}", e);
            Vec::new()
        }
    };
    Ok(resp)
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").with_context(||"Unable to create tcp listener")?;
    let mut handlers = vec![];
    for stream in listener.incoming() {
        let handler = thread::spawn(move ||{
            match stream {
                Ok(mut stream) => {
                    while let Ok(resp) = process_stream(&mut stream) {
                        stream.write(resp.as_ref()).with_context(|| "").unwrap();
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
        handlers.push(handler);
    }
    handlers.into_iter().for_each(|i| i.join().unwrap());
    Ok(())
}
