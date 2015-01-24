extern crate serialize;

use serialize::{Encoder, Decoder};
use std::io;
use std::io::{IoResult, Reader, Writer, IoError};
use std::char::from_u32;

pub struct BinaryDecoder<R: Reader> { reader: R }

impl<R: Reader> serialize::Decoder for BinaryDecoder<R>
{
    pub type Error = IoError;

    fn read_nil(&mut self)  -> IoResult<()> { Ok(()) }

    fn read_uint(&mut self) -> IoResult<usize>   { self.reader.read_le_uint() }
    fn read_u64(&mut self)  -> IoResult<u64>    { self.reader.read_le_u64() }
    fn read_u32(&mut self)  -> IoResult<u32>    { self.reader.read_le_u32() }
    fn read_u16(&mut self)  -> IoResult<u16>    { self.reader.read_le_u16() }
    fn read_u8(&mut self)   -> IoResult<u8>     { self.reader.read_u8() }

    fn read_int(&mut self)  -> IoResult<isize>    { self.reader.read_le_int() }
    fn read_i64(&mut self)  -> IoResult<i64>    { self.reader.read_le_i64() }
    fn read_i32(&mut self)  -> IoResult<i32>    { self.reader.read_le_i32() }
    fn read_i16(&mut self)  -> IoResult<i16>    { self.reader.read_le_i16() }
    fn read_i8(&mut self)   -> IoResult<i8>     { self.reader.read_i8() }
    fn read_bool(&mut self) -> IoResult<bool>   { Ok(try!(self.reader.read_u8()) != 0) }

    fn read_f64(&mut self)  -> IoResult<f64>    { self.reader.read_le_f64() }
    fn read_f32(&mut self)  -> IoResult<f32>    { self.reader.read_le_f32() }

    fn read_char(&mut self) -> IoResult<char>   { self.reader.read_le_u32().map(|x| from_u32(x).unwrap()) }
    fn read_str(&mut self)  -> IoResult<String> { self.reader.read_to_string() }

    fn read_enum<T, F>(&mut self, _name: &str, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }
    fn read_enum_variant<T, F>(&mut self, _names: &[&str], f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>, usize) -> IoResult<T>
        { let variant = try!(self.reader.read_le_uint()); f(self, variant) }
    fn read_enum_variant_arg<T, F>(&mut self, _a_idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }
    fn read_enum_struct_variant<T, F>(&mut self, _names: &[&str], f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>, usize) -> IoResult<T>
        { let variant = try!(self.reader.read_le_uint()); f(self, variant) }
    fn read_enum_struct_variant_field<T, F>(&mut self, _f_name: &str, _f_idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T>
        { f(self) }

    fn read_struct<T, F>(&mut self, _s_name: &str, _len: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T>
        { f(self) }
    fn read_struct_field<T, F>(&mut self, _f_name: &str, _f_idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }

    fn read_tuple<T, F>(&mut self, _len: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }
    fn read_tuple_arg<T, F>(&mut self, _a_idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }
    fn read_tuple_struct<T, F>(&mut self, _s_name: &str, _len: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }
    fn read_tuple_struct_arg<T, F>(&mut self, _a_idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }

    fn read_option<T, F>(&mut self, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>, bool) -> IoResult<T>
        { let some = try!(self.reader.read_u8()); f(self, some != 0) }

    fn read_seq<T, F>(&mut self, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>, usize) -> IoResult<T>
        { let len = try!(self.reader.read_le_uint()); f(self, len) }
    fn read_seq_elt<T, F>(&mut self, _idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }

    fn read_map<T, F>(&mut self, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>, usize) -> IoResult<T>
        { let len = try!(self.reader.read_le_uint()); f(self, len) }
    fn read_map_elt_key<T, F>(&mut self, _idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }
    fn read_map_elt_val<T, F>(&mut self, _idx: usize, f: F) -> IoResult<T>
        where F: FnOnce(&mut BinaryDecoder<R>) -> IoResult<T> { f(self) }

    fn error(&mut self, _err: &str) -> io::IoError { IoError::last_error() }
}

pub struct BinaryEncoder<R: Writer> { writer: R }

impl<R: Writer> serialize::Encoder for BinaryEncoder<R>
{
    pub type Error = IoError;

    fn emit_nil(&mut self)            -> IoResult<()> { Ok(()) }

    fn emit_uint(&mut self, v: usize)  -> IoResult<()> { self.writer.write_le_uint(v) }
    fn emit_u64(&mut self, v: u64)    -> IoResult<()> { self.writer.write_le_u64(v) }
    fn emit_u32(&mut self, v: u32)    -> IoResult<()> { self.writer.write_le_u32(v) }
    fn emit_u16(&mut self, v: u16)    -> IoResult<()> { self.writer.write_le_u16(v) }
    fn emit_u8(&mut self, v: u8)      -> IoResult<()> { self.writer.write_u8(v) }

    fn emit_int(&mut self, v: isize)    -> IoResult<()> { self.writer.write_le_int(v) }
    fn emit_i64(&mut self, v: i64)    -> IoResult<()> { self.writer.write_le_i64(v) }
    fn emit_i32(&mut self, v: i32)    -> IoResult<()> { self.writer.write_le_i32(v) }
    fn emit_i16(&mut self, v: i16)    -> IoResult<()> { self.writer.write_le_i16(v) }
    fn emit_i8(&mut self, v: i8)      -> IoResult<()> { self.writer.write_i8(v) }

    fn emit_bool(&mut self, v: bool)  -> IoResult<()> { self.writer.write_u8(if v { 1 } else { 0 }) }

    fn emit_f64(&mut self, v: f64)    -> IoResult<()> { self.writer.write_le_f64(v) }
    fn emit_f32(&mut self, v: f32)    -> IoResult<()> { self.writer.write_le_f32(v) }

    fn emit_char(&mut self, _v: char) -> IoResult<()> { self.writer.write_le_u32(_v as u32) }
    fn emit_str(&mut self, v: &str)   -> IoResult<()> { self.writer.write_str(v) }

    fn emit_enum<F>(&mut self, _name: &str, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_enum_variant<F>(&mut self, _v_name: &str, v_id: usize, _len: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()>
        { try!(self.writer.write_le_uint(v_id)); f(self) }
    fn emit_enum_variant_arg<F>(&mut self, _a_idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_enum_struct_variant<F>(&mut self, _v_name: &str, v_id: usize, _len: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()>
        { try!(self.writer.write_le_uint(v_id)); f(self) }
    fn emit_enum_struct_variant_field<F>(&mut self, _f_name: &str, _f_idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()>
        { f(self) }

    fn emit_struct<F>(&mut self, _name: &str, _len: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_struct_field<F>(&mut self, _f_name: &str, _f_idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }

    fn emit_tuple<F>(&mut self, _len: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_tuple_arg<F>(&mut self, _idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_tuple_struct<F>(&mut self, _name: &str, _len: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_tuple_struct_arg<F>(&mut self, _f_idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }

    fn emit_option<F>(&mut self, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_option_none(&mut self) -> IoResult<()> { try!(self.writer.write_u8(0)); Ok(()) }
    fn emit_option_some<F>(&mut self, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()>
        { try!(self.writer.write_u8(1)); f(self) }

    fn emit_seq<F>(&mut self, _len: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()>
        { try!(self.writer.write_le_uint(_len)); f(self) }
    fn emit_seq_elt<F>(&mut self, _idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }

    fn emit_map<F>(&mut self, _len: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()>
        { try!(self.writer.write_le_uint(_len)); f(self) }
    fn emit_map_elt_key<F>(&mut self, _idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
    fn emit_map_elt_val<F>(&mut self, _idx: usize, f: F) -> IoResult<()>
        where F: FnOnce(&mut BinaryEncoder<R>) -> IoResult<()> { f(self) }
}
