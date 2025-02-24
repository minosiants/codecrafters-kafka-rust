use std::str::from_utf8;
use bytes::Buf;
use crate::{Context, Error, MapTupleTwo, Result};

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

pub trait BytesOps {
    type R;
    fn  extract_u32(self)-> Result<(u32, Self::R)>;
    fn  extract_u64(self)-> Result<(u64, Self::R)>;
    fn  extract_u8(self)-> Result<(u8, Self::R)>;
    fn  extract_u16(self)-> Result<(u16, Self::R)>;
    fn extract_u16_into<T>(self, f: impl FnOnce(u16) -> T) -> Result<(T, Self::R)> where Self: Sized {
        Self::extract_u16(self).map_tuple(f)
    }
    fn extract_u32_into<T>(self, f: impl FnOnce(u32) -> T) -> Result<(T, Self::R)>  where Self: Sized{
        Self::extract_u32(self).map_tuple(f)
    }
    fn extract_u64_into<T>(self, f: impl FnOnce(u64) -> T) -> Result<(T, Self::R)>  where Self: Sized{
        Self::extract_u64(self).map_tuple(f)
    }
    fn extract_u8_into<T>(self, f: impl FnOnce(u8) -> T) -> Result<(T, Self::R)>  where Self: Sized{
        Self::extract_u8(self).map_tuple(f)
    }
    fn extract_array<T:Clone>(self, f: impl FnMut(u32) -> T) -> Result<(Vec<T>, Self::R)>;
    fn extract_array_into<T: TryExtract>(self) -> Result<(Vec<T>, Self::R)>;
    fn drop<'a>(self, num:usize)->Result<(&'a [u8],&'a [u8])>;
    fn extract_compact_str(self) -> Result<(String, &'static [u8])>;
}

impl BytesOps for &[u8] {
    type R = &'static[u8];

    fn extract_u32(self)-> Result<(u32, Self::R)> {
        self.drop(4).map_tuple(move |mut l| {
            l.get_u32()
        })
    }

    fn extract_u64(self) -> Result<(u64, Self::R)> {
        self.drop(8).map_tuple(|mut l| {
            l.get_u64()
        })
    }

    fn extract_u8(self) -> Result<(u8, Self::R)> {
        self.drop(1).map_tuple(|mut l| {
            l.get_u8()
        })
    }

    fn extract_u16(self) -> Result<(u16, Self::R)> {
        self.drop(2).map_tuple(|mut l| {
            l.get_u16()
        })
    }

    fn extract_array<T:Clone>(self, mut f: impl FnMut(u32) -> T) -> Result<(Vec<T>, Self::R)>  {

        let (len, rest) = VarInt::decode(self).map_tuple(|v|
            v.value() - 1
        )?;

        rest.drop(len*4).map_tuple(|replicas| {
            let r: Vec<T> = replicas.chunks(32).map(|mut rep| {
                f(rep.get_u32())
            }).collect();
            r
        })
    }


    fn extract_array_into<T: TryExtract>(self) -> Result<(Vec<T>, Self::R)> {
        fn do_split<T: TryExtract>(v: &[u8], mut result: Vec<T>) -> Result<Vec<T>> {
            if v.is_empty() {
                Ok(result)
            } else {
                let (value,rest)= T::try_extract(v)?;
                result.push(value);
                do_split(rest, result)
            }
        }
        let (array, rest)= VarInt::decode(&self).and_then(|(v,rest)| rest.drop(v.value() -1))?;
        Ok((do_split(&array, vec![])?,rest))
    }

    fn drop<'a>(self, num: usize) -> Result<(&'a [u8],&'a [u8])> {
        self.split_at_checked(num).context("drop")
    }

    fn extract_compact_str(self) -> Result<(String, &'static [u8])> {
        let (str, rest)= VarInt::decode(&self).and_then(|(v,rest)| rest.drop(v.value() -1))?;
        Ok((from_utf8(str).map(|v|v.to_string())?,rest))
    }


}

pub trait TryExtract {
    fn try_extract(v:&[u8]) -> Result<(Self, &'static[u8])> where Self:Sized;
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