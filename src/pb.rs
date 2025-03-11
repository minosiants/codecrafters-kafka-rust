use crate::{Context, Error, FetchTopic, MapTupleTwo, ReplicaNode, Result, TopicName};
use bytes::{Buf, BufMut};
use pretty_hex::simple_hex;
use std::ops::Deref;
use std::str::from_utf8;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SignedVarInt {
    value: i64,
    n_bytes: usize,
}

impl SignedVarInt {
    pub fn new(value: i64, n_bytes: usize) -> Self {
        Self {
            value,
            n_bytes,
        }
    }
    pub fn decode(v: &[u8]) -> Result<(Self, &[u8])> {
        VarInt::decode(v)
            .map_tuple(|v| Self::new(Self::zig_zag_decode(v.value), v.n_bytes))
    }
    pub fn value(&self) -> i64 {
        self.value
    }

    fn zig_zag_decode(n: u64) -> i64 {
        ((n >> 1) as i64) ^ -((n & 1) as i64)
    }
    fn zig_zag_encode(n:i64) -> u64 {
        ((n << 1) ^ (n >> 63)) as u64
    }
    pub fn encode(n:i64) -> Vec<u8> {
        VarInt::encode(Self::zig_zag_encode(n))
    }
}
#[derive(Debug, Clone)]
pub struct VarInt {
    value: u64,
    n_bytes: usize,
}
impl VarInt {
    fn new(value: u64, n_bytes: usize) -> Self {
        Self {
            value,
            n_bytes,
        }
    }
    pub fn decode(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let mut value: u64 = 0;
        let mut shift = 0;
        let mut n_bytes = 0;

        for &byte in bytes {
            let part = (byte & 0x7F) as u64; // Extract lower 7 bits
            value |= part << shift; // Shift and accumulate
            n_bytes += 1;

            if (byte & 0x80) == 0 {
                // If MSB is 0, this is the last byte
                return Ok((
                    Self {
                        value,
                        n_bytes,
                    },
                    &bytes[n_bytes..],
                ));
            }

            shift += 7;
            if shift >= 64 {
                return Err(Error::general("Overflow"));
            }
        }
        Err(Error::general("Incomplete varint"))
    }

    pub fn encode(v: u64) -> Vec<u8> {
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
    fn extract_u32(&self) -> Result<(u32, &[u8])>;
    fn extract_u32_as_option(&self) -> Result<(Option<u32>, &[u8])> {
        self.extract_u32().map_tuple(|v| match v {
            0xffffffff => None,
            _ => Some(v),
        })
    }
    fn extract_u32_as_option_into<T>(
        &self,
        f: impl FnOnce(u32) -> T,
    ) -> Result<(Option<T>, &[u8])> {
        self.extract_u32_as_option().map_tuple(|v| v.map(f))
    }
    fn extract_u64(&self) -> Result<(u64, &[u8])>;
    fn extract_u64_as_option(&self) -> Result<(Option<u64>, &[u8])> {
        self.extract_u64().map_tuple(|v| match v {
            0xffffffffffffffff => None,
            _ => Some(v),
        })
    }
    fn extract_u64_as_option_into<T>(
        &self,
        f: impl FnOnce(u64) -> T,
    ) -> Result<(Option<T>, &[u8])> {
        self.extract_u64_as_option().map_tuple(|v| v.map(f))
    }
    fn extract_u8(&self) -> Result<(u8, &[u8])>;
    fn extract_i8(&self) -> Result<(i8, &[u8])> {
        self.extract_u8().map_tuple(|v| v as i8)
    }
    fn extract_u16(&self) -> Result<(u16, &[u8])>;
    fn extract_u16_as_option(&self) -> Result<(Option<u16>, &[u8])> {
        self.extract_u16().map_tuple(|v| match v {
            0xffff => None,
            _ => Some(v),
        })
    }
    fn extract_u16_as_option_into<T>(
        &self,
        f: impl FnOnce(u16) -> T,
    ) -> Result<(Option<T>, &[u8])> {
        self.extract_u16_as_option().map_tuple(|v| v.map(f))
    }
    fn extract_u16_into<T>(
        &self,
        f: impl FnOnce(u16) -> T,
    ) -> Result<(T, &[u8])> {
        self.extract_u16().map_tuple(f)
    }
    fn extract_u32_into<T>(
        &self,
        f: impl FnOnce(u32) -> T,
    ) -> Result<(T, &[u8])> {
        Self::extract_u32(self).map_tuple(f)
    }
    fn extract_u64_into<T>(
        &self,
        f: impl FnOnce(u64) -> T,
    ) -> Result<(T, &[u8])> {
        Self::extract_u64(self).map_tuple(f)
    }
    fn extract_u8_into<T>(
        &self,
        f: impl FnOnce(u8) -> T,
    ) -> Result<(T, &[u8])> {
        Self::extract_u8(self).map_tuple(f)
    }
    fn extract_array<T: Clone>(
        &self,
        f: impl FnMut(u32) -> T,
    ) -> Result<(Vec<T>, &[u8])>;
    fn extract_array_into<T: TryExtract>(&self) -> Result<(Vec<T>, &[u8])>;
    fn drop(&self, num: usize) -> Result<(&[u8], &[u8])>;
    fn extract_compact_str(&self) -> Result<(String, &[u8])>;

    fn extract_str(&self, size: usize) -> Result<(&str, &[u8])>;

    fn extract_uuid(&self) -> Result<(Uuid, &[u8])> {
        self.drop(16).fmap_tuple(|v|Uuid::from_slice(v).context(""))

    }
    fn extract_uuid_into<T>(
        &self,
        f: impl FnOnce(Uuid) -> T,
    ) -> Result<(T, &[u8])> {
        self.extract_uuid().map_tuple(f)
    }

    fn extract_signed_var_int(&self) -> Result<(SignedVarInt, &[u8])>;
}

impl BytesOps for [u8] {
    fn extract_u32(&self) -> Result<(u32, &[u8])> {
        self.drop(4).map_tuple(move |mut l| l.get_u32())
    }

    fn extract_u64(&self) -> Result<(u64, &[u8])> {
        self.drop(8).map_tuple(|mut l| l.get_u64())
    }

    fn extract_u8(&self) -> Result<(u8, &[u8])> {
        self.drop(1).map_tuple(|mut l| l.get_u8())
    }

    fn extract_u16(&self) -> Result<(u16, &[u8])> {
        self.drop(2).map_tuple(|l| l.clone().get_u16())
    }

    fn extract_array<T: Clone>(
        &self,
        mut f: impl FnMut(u32) -> T,
    ) -> Result<(Vec<T>, &[u8])> {
        let (len, rest) = VarInt::decode(self).map_tuple(|v| v.value() - 1)?;
        rest.drop(len * 4).map_tuple(|replicas| {
            let r: Vec<T> =
                replicas.chunks(32).map(|mut rep| f(rep.get_u32())).collect();
            r
        })
    }

    fn extract_array_into<T: TryExtract>(&self) -> Result<(Vec<T>, &[u8])> {
        fn do_split<T: TryExtract>(
            v: &[u8],
            len: usize,
            mut result: Vec<T>,
        ) -> Result<(Vec<T>, &[u8])> {
            if len == 0 {
                Ok((result, v))
            } else {
                let (value, rest) = T::try_extract(v)?;
                result.push(value);
                do_split(rest, len - 1, result)
            }
        }
        println!("extract_array: {:?}", simple_hex(&self));
        let (len, rest) = VarInt::decode(&self).map_tuple(|v| v.value() - 1)?;
        println!("len: {:?}", len);
        do_split(&rest, len, vec![])
    }

    fn drop(&self, num: usize) -> Result<(&[u8], &[u8])> {
        self.split_at_checked(num).context("drop")
    }

    fn extract_compact_str(&self) -> Result<(String, &[u8])> {
        let (length, rest) = VarInt::decode(&self)?;
        if length.value == 0 {
            Ok(("".to_string(),rest))
        } else {
            let (str, rest) = rest.drop(length.value() - 1)?;
            Ok((from_utf8(str).map(|v| v.to_string())?, rest))
        }
    }

    fn extract_str(&self, size: usize) -> Result<(&str, &[u8])> {
        let (str, rest) = self.drop(size)?;
        Ok((from_utf8(str)?, rest))
    }

    fn extract_signed_var_int(&self) -> Result<(SignedVarInt, &[u8])> {
        SignedVarInt::decode(self)
    }
}
pub trait TryExtract {
    fn try_extract(v: &[u8]) -> Result<(Self, &[u8])>
    where
        Self: Sized;
}
pub trait ToCompactString {
    fn to_compact_string(&self) -> Vec<u8>;
}

impl ToCompactString for String {
    fn to_compact_string(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(VarInt::encode((&self.len() + 1) as u64));
        bytes.put_slice(&self.as_bytes());
        bytes
    }
}
impl ToCompactString for TopicName {
    fn to_compact_string(&self) -> Vec<u8> {
        self.deref().to_compact_string()
    }
}

impl TryExtract for Uuid {
    fn try_extract(v: &[u8]) -> Result<(Self, &[u8])>
    where
        Self: Sized
    {
        v.extract_uuid()
    }
}

pub trait ToArray {
    fn to_pb_array(&self) -> Result<Vec<u8>>;
}


impl ToArray for Vec<u32>{
    fn to_pb_array(&self) -> Result<Vec<u8>> {
        let mut res = VarInt::encode((self.len() +1)as u64);
        let v:Vec<u8> = self.iter().flat_map(|v| v.to_be_bytes().into_iter()).collect();
        res.extend(v);
        Ok(res)
    }
}
impl ToArray for Vec<Uuid>{
    fn to_pb_array(&self) -> Result<Vec<u8>> {
        let mut res = VarInt::encode((self.len() +1)as u64);
        let v:Vec<u8> = self.iter().flat_map(|v| v.into_bytes().into_iter()).collect();
        res.extend(v);
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use crate::{BytesOps, FetchTopic, MapTupleTwo, Result};
    use hex::decode;

    #[test]
    fn test_encode() {
        let num: u64 = 0b1011_0010_1110_1101_0001;
        let num1: u64 = 0b1011_0010_1110_1101_0001;

        let first_bits = (num);
        let first_bits2 = (num >> 7);
        let first_bits3 = (num >> 14);
        let last_7_bits = first_bits & 0b111_1111;
        let last_7_bits2 = first_bits2 & 0b111_1111;
        let last_7_bits3 = first_bits3 & 0b111_1111;

        println!("{:07b}", last_7_bits); // Output in binary
        println!("{:07b}", last_7_bits2); // Output in binary
        println!("{:07b}", last_7_bits3); // Output in binary
    }

    #[test]
    fn test_extract_array_into() {
        use super::*;
        let data: String = vec![
            "02 00 00 00  00 00 00 00  00 00 00 00  00 00 00 53",
            "21 02 00 00  00 00 ff ff  ff ff 00 00  00 00 00 00",
            "00 00 ff ff  ff ff ff ff  ff ff ff ff  ff ff 00 10",
            "00 00 00 00  01 01 00",
        ]
        .join("")
        .replace(" ", "");
        let bytes = decode(data).expect("");
        let topics: Result<Vec<FetchTopic>> =
            <[u8]>::extract_array_into(&bytes).first();

        println!("bytes {:?}", simple_hex(&bytes));
        println!("topics {:?}", topics)
    }
}
