use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;

use bytes::BufMut;
use codecrafters_kafka::{
    Context, CorrelationId, Error, ErrorCode, MessageSize, Request, Response,
    Result,
};

fn error_response(correlation_id: &CorrelationId) -> Vec<u8> {
    let mut error: Vec<u8> = Vec::new();
    error.put_u32(*MessageSize::new(10));
    error.put_u32(**correlation_id);
    error.put_i16(*ErrorCode::UnsupportedVersion);
    error
}

fn process_stream(stream: &mut TcpStream) -> Result<Vec<u8>> {
    println!("accepted new connection");
    let req: Result<Request> = stream.try_into();
    let res: Vec<u8> = req
        .and_then(|r| Response::response(&r))
        .map(|v| v.into())
        .unwrap_or_else(|e| match e {
            Error::UnsupportedApiVersion(_, Some(id)) => error_response(&id),
            Error::UnsupportedApiKey(_, Some(id)) => error_response(&id),
            Error::ErrorWrapper(txt, err) => Vec::new(),
            e => Vec::new(),
        });
    Ok(res)
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092")
        .with_context(|| "Unable to create tcp listener")?;
    let mut handlers = vec![];
    for stream in listener.incoming() {
        let handler = thread::spawn(move || match stream {
            Ok(mut stream) =>
                while let Ok(resp) = process_stream(&mut stream) {
                    stream.write(resp.as_ref()).context("").unwrap();
                },
            Err(e) => {
                println!("error: {}", e);
            }
        });
        handlers.push(handler);
    }
    handlers.into_iter().for_each(|i| i.join().unwrap());
    Ok(())
}
