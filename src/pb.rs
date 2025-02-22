use crate::{Error, MapTupleTwo, Result};

#[derive(Debug, Clone)]
pub struct SignedVarInt{
    value:i64,
    n_bytes:usize
}

impl SignedVarInt {
    pub fn new(value:i64, n_bytes:usize) -> Self {
        Self{value, n_bytes}
    }
    pub fn decode(v:&[u8]) -> Result<(Self,&[u8])> {
        VarInt::decode(v).map_tuple(
            |v| Self::new(Self::zig_zag_decode(v.value),v.n_bytes)
        )
    }
    pub fn value(&self)-> i64 {
        self.value
    }


    fn zig_zag_decode(n:u64) -> i64 {
        ((n >> 1) as i64) ^ -((n & 1) as i64)
    }


}
#[derive(Debug, Clone)]
pub struct VarInt{
    value:u64,
    n_bytes:usize
}
impl VarInt {
    fn new(value:u64, n_bytes:usize) -> Self {
        Self{value, n_bytes}
    }
    pub fn decode(bytes: &[u8]) -> Result<(Self,&[u8])> {
        let mut value: u64 = 0;
        let mut shift = 0;
        let mut n_bytes = 0;

        for &byte in bytes {
            let part = (byte & 0x7F) as u64; // Extract lower 7 bits
            value |= part << shift;          // Shift and accumulate
            n_bytes += 1;

            if (byte & 0x80) == 0 { // If MSB is 0, this is the last byte
                return Ok((Self{value, n_bytes}, &bytes[n_bytes..]));
            }

            shift += 7;
            if shift >= 64 {
                return Err(Error::general("Overflow"));
            }
        }
        Err(Error::general("Incomplete varint"))
    }

    pub fn encode(v:u64) -> Vec<u8> {
        let mut value = v;
        let mut buf = Vec::new();
        while value >= 0x80 {
            buf.push(((value & 0x7F) as u8) | 0x80);
            value >>= 7;
        }
        buf.push(value as u8);
        buf
    }

    pub fn value(&self) -> usize {
        self.value as usize
    }

}

#[cfg(test)]
mod test {
    #[test]
    fn test_encode(){
        let num: u64 = 0b1011_0010_1110_1101_0001;
        let num1: u64 = 0b1011_0010_1110_1101_0001;

        let first_bits = (num) ;
        let first_bits2 = (num >> 7) ;
        let first_bits3 = (num >> 14) ;
        let last_7_bits = first_bits & 0b111_1111;
        let last_7_bits2 = first_bits2 & 0b111_1111;
        let last_7_bits3 = first_bits3 & 0b111_1111;


        println!("{:07b}", last_7_bits); // Output in binary
        println!("{:07b}", last_7_bits2); // Output in binary
        println!("{:07b}", last_7_bits3); // Output in binary
    }
}