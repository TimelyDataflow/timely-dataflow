extern crate serialize;

use serialize::{Encoder, Decoder};
use std::io;
use std::io::{IoResult, Reader, Writer, IoError};
use std::char::from_u32;

pub struct BinaryDecoder<R: Reader> { reader: R }

impl<R: Reader> serialize::Decoder<io::IoError> for BinaryDecoder<R>
{
    fn read_nil(&mut self)  -> IoResult<()> { Ok(()) }

    fn read_uint(&mut self) -> IoResult<uint>   { self.reader.read_le_uint() }
    fn read_u64(&mut self)  -> IoResult<u64>    { self.reader.read_le_u64() }
    fn read_u32(&mut self)  -> IoResult<u32>    { self.reader.read_le_u32() }
    fn read_u16(&mut self)  -> IoResult<u16>    { self.reader.read_le_u16() }
    fn read_u8(&mut self)   -> IoResult<u8>     { self.reader.read_u8() }

    fn read_int(&mut self)  -> IoResult<int>    { self.reader.read_le_int() }
    fn read_i64(&mut self)  -> IoResult<i64>    { self.reader.read_le_i64() }
    fn read_i32(&mut self)  -> IoResult<i32>    { self.reader.read_le_i32() }
    fn read_i16(&mut self)  -> IoResult<i16>    { self.reader.read_le_i16() }
    fn read_i8(&mut self)   -> IoResult<i8>     { self.reader.read_i8() }
    fn read_bool(&mut self) -> IoResult<bool>   { Ok(try!(self.reader.read_u8()) != 0) }

    fn read_f64(&mut self)  -> IoResult<f64>    { self.reader.read_le_f64() }
    fn read_f32(&mut self)  -> IoResult<f32>    { self.reader.read_le_f32() }

    fn read_char(&mut self) -> IoResult<char>   { self.reader.read_le_u32().map(|x| from_u32(x).unwrap()) }
    fn read_str(&mut self)  -> IoResult<String> { self.reader.read_to_string() }

    fn read_enum<T>(&mut self, _name: &str, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }
    fn read_enum_variant<T>(&mut self, _names: &[&str], f: |&mut BinaryDecoder<R>, uint| -> IoResult<T>) -> IoResult<T>
        { let variant = try!(self.reader.read_le_uint()); f(self, variant) }
    fn read_enum_variant_arg<T>(&mut self, _a_idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }
    fn read_enum_struct_variant<T>(&mut self, _names: &[&str], f: |&mut BinaryDecoder<R>, uint| -> IoResult<T>) -> IoResult<T>
        { let variant = try!(self.reader.read_le_uint()); f(self, variant) }
    fn read_enum_struct_variant_field<T>(&mut self, _f_name: &str, _f_idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T>
        { f(self) }

    fn read_struct<T>(&mut self, _s_name: &str, _len: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }
    fn read_struct_field<T>(&mut self, _f_name: &str, _f_idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }

    fn read_tuple<T>(&mut self, _len: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }
    fn read_tuple_arg<T>(&mut self, _a_idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }
    fn read_tuple_struct<T>(&mut self, _s_name: &str, _len: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }
    fn read_tuple_struct_arg<T>(&mut self, _a_idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }

    fn read_option<T>(&mut self, f: |&mut BinaryDecoder<R>, bool| -> IoResult<T>) -> IoResult<T>
        { let some = try!(self.reader.read_u8()); f(self, some != 0) }

    fn read_seq<T>(&mut self, f: |&mut BinaryDecoder<R>, uint| -> IoResult<T>) -> IoResult<T>
        { let len = try!(self.reader.read_le_uint()); f(self, len) }
    fn read_seq_elt<T>(&mut self, _idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }

    fn read_map<T>(&mut self, f: |&mut BinaryDecoder<R>, uint| -> IoResult<T>) -> IoResult<T>
        { let len = try!(self.reader.read_le_uint()); f(self, len) }
    fn read_map_elt_key<T>(&mut self, _idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }
    fn read_map_elt_val<T>(&mut self, _idx: uint, f: |&mut BinaryDecoder<R>| -> IoResult<T>) -> IoResult<T> { f(self) }

    fn error(&mut self, _err: &str) -> io::IoError { IoError::last_error() }
}

pub struct BinaryEncoder<R: Writer> { writer: R }

impl<R: Writer> serialize::Encoder<IoError> for BinaryEncoder<R>
{
    fn emit_nil(&mut self)            -> IoResult<()> { Ok(()) }

    fn emit_uint(&mut self, v: uint)  -> IoResult<()> { self.writer.write_le_uint(v) }
    fn emit_u64(&mut self, v: u64)    -> IoResult<()> { self.writer.write_le_u64(v) }
    fn emit_u32(&mut self, v: u32)    -> IoResult<()> { self.writer.write_le_u32(v) }
    fn emit_u16(&mut self, v: u16)    -> IoResult<()> { self.writer.write_le_u16(v) }
    fn emit_u8(&mut self, v: u8)      -> IoResult<()> { self.writer.write_u8(v) }

    fn emit_int(&mut self, v: int)    -> IoResult<()> { self.writer.write_le_int(v) }
    fn emit_i64(&mut self, v: i64)    -> IoResult<()> { self.writer.write_le_i64(v) }
    fn emit_i32(&mut self, v: i32)    -> IoResult<()> { self.writer.write_le_i32(v) }
    fn emit_i16(&mut self, v: i16)    -> IoResult<()> { self.writer.write_le_i16(v) }
    fn emit_i8(&mut self, v: i8)      -> IoResult<()> { self.writer.write_i8(v) }

    fn emit_bool(&mut self, v: bool)  -> IoResult<()> { self.writer.write_u8(if v { 1 } else { 0 }) }

    fn emit_f64(&mut self, v: f64)    -> IoResult<()> { self.writer.write_le_f64(v) }
    fn emit_f32(&mut self, v: f32)    -> IoResult<()> { self.writer.write_le_f32(v) }

    fn emit_char(&mut self, _v: char) -> IoResult<()> { self.writer.write_le_u32(_v as u32) }
    fn emit_str(&mut self, v: &str)   -> IoResult<()> { self.writer.write_str(v) }

    fn emit_enum(&mut self, _name: &str, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
    fn emit_enum_variant(&mut self, _v_name: &str, v_id: uint, _len: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()>
        { try!(self.writer.write_le_uint(v_id)); f(self) }
    fn emit_enum_variant_arg(&mut self, _a_idx: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
    fn emit_enum_struct_variant(&mut self, _v_name: &str, v_id: uint, _len: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()>
        { try!(self.writer.write_le_uint(v_id)); f(self) }
    fn emit_enum_struct_variant_field(&mut self, _f_name: &str, _f_idx: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()>
        { f(self) }

    fn emit_struct(&mut self, _name: &str, _len: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
    fn emit_struct_field(&mut self, _f_name: &str, _f_idx: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }

    fn emit_tuple(&mut self, _len: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
    fn emit_tuple_arg(&mut self, _idx: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
    fn emit_tuple_struct(&mut self, _name: &str, _len: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
    fn emit_tuple_struct_arg(&mut self, _f_idx: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }

    fn emit_option(&mut self, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()>  { f(self) }
    fn emit_option_none(&mut self) -> IoResult<()> { try!(self.writer.write_u8(0)); Ok(()) }
    fn emit_option_some(&mut self, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { try!(self.writer.write_u8(1)); f(self) }

    fn emit_seq(&mut self,     _len: uint, f: |this: &mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()>
        { try!(self.writer.write_le_uint(_len)); f(self) }
    fn emit_seq_elt(&mut self, _idx: uint, f: |this: &mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }

    fn emit_map(&mut self,         _len: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()>
        { try!(self.writer.write_le_uint(_len)); f(self) }
    fn emit_map_elt_key(&mut self, _idx: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
    fn emit_map_elt_val(&mut self, _idx: uint, f: |&mut BinaryEncoder<R>| -> IoResult<()>) -> IoResult<()> { f(self) }
}
