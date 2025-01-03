use std::io::Write;
use std::net::{TcpListener, TcpStream};

use bytes::BufMut;

use codecrafters_kafka::{Api, ApiKey, ApiVersion, Context, CorrelationId, Error, ErrorCode, MessageSize, Request, Response, Result, TaggedFields};

fn error_response(correlation_id: &CorrelationId) -> Vec<u8> {
    let mut error: Vec<u8> = Vec::new();
    error.put_i32(*MessageSize::new(10));
    error.put_i32(**correlation_id);
    error.put_i16(*ErrorCode::UnsupportedVersion);
    error
}
fn process_request(request:&Request) -> Vec<u8> {
    let api = Api::new(ApiKey::ApiVersions,ApiVersion::V0,ApiVersion::V4,TaggedFields::new(0));
    Response::new(request.header().correlation_id(),ErrorCode::NoError, vec![api]).into()
}
fn process_stream(stream:&mut TcpStream) -> Result<Vec<u8>> {
    println!("accepted new connection");
    let req:Result<Request> = stream.try_into();
    let resp = match req {
        Ok(request) => {
            process_request(&request)
        }
        Err(Error::UnsupportedApiVersion(_, Some(id))) => {
            error_response(&id)
        }
        Err(Error::UnsupportedApiKey(_, Some(id))) => {
            error_response(&id)
        }
        Err(Error::GeneralError(_,_)) => {
            Vec::new()
        }
        Err(_) => {
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

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let resp = process_stream(&mut stream)?;
                stream.write(resp.as_ref()).with_context(||"")?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}


