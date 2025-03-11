use crate::{Context, Result};
use std::fs::File;
use std::io::Read;
pub fn read(path: &str) -> Result<Vec<u8>> {
    let mut file = File::open(path).context({ "Open file {path}" })?; // Open the file
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).context("Reading file")?; // Read file contents into buffer
    Ok(buffer)
}
