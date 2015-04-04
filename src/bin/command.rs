// #![feature(old_io)]
// #![feature(libc)]
//
// extern crate libc;
//
// use std::old_io::stdio::{stdin_raw, stdout_raw};
// use std::old_io::{MemReader, MemWriter, IoErrorKind};
// use std::thread;

fn main() {
    // unsafe { libc::fcntl(0, libc::F_SETFL, libc::O_NONBLOCK); }
    // unsafe { libc::fcntl(1, libc::F_SETFL, libc::O_NONBLOCK); }
    //
    // let mut reader = stdin_raw();
    // let mut writer = stdout_raw();
    //
    // let mut memwriter = MemWriter::new();
    //
    // memwriter.write_le_u64(1).ok().expect("");
    // memwriter.write_le_u64(0).ok().expect("");
    // memwriter.write_le_u64(1).ok().expect("");
    //
    // let bytes = memwriter.into_inner();
    // writer.write_le_u64(bytes.len() as u64).ok().expect("");
    // writer.write_all(&bytes[..]).ok().expect("");
    // writer.flush().ok().expect("");
    //
    // let mut buffer = Vec::new();
    //
    // loop {
    //     // push some amount, then try decoding...
    //     match reader.push(1024, &mut buffer) {
    //         Ok(_)  => { },
    //         Err(e) => { if e.kind != IoErrorKind::ResourceUnavailable { panic!("Pull error: {}", e) }
    //                     else { thread::yield_now(); }},
    //     }
    //
    //     let available = buffer.len() as u64;
    //     let mut reader = MemReader::new(buffer);
    //     let mut cursor = 0;
    //
    //     let mut done = false;
    //
    //     while !done {
    //         let read = match reader.read_le_u64() {
    //             Ok(x) => x,
    //             Err(_) => { done = true; 0 }
    //         };
    //
    //         if done || read + 8 + cursor > available {
    //             done = true;
    //         }
    //         else {
    //             let mut updates = Vec::new();
    //             let externals = reader.read_le_u64().ok().expect("");
    //             for _ in (0..externals) {
    //                 let time = reader.read_le_u64().ok().expect("");
    //                 let delta= reader.read_le_i64().ok().expect("");
    //
    //                 if time < 1000000 {
    //                     updates.push((time, delta));
    //                 }
    //             }
    //
    //             if updates.len() > 0 {
    //                 let mut memwriter = MemWriter::new();
    //                 memwriter.write_le_u64(updates.len() as u64).ok().expect("");
    //                 while let Some((time, delta)) = updates.pop() {
    //                     memwriter.write_le_u64(time).ok().expect("");
    //                     memwriter.write_le_i64(delta).ok().expect("");
    //                 }
    //
    //                 memwriter.write_le_u64(0).ok().expect("");
    //                 memwriter.write_le_u64(0).ok().expect("");
    //
    //                 let bytes = memwriter.into_inner();
    //                 writer.write_le_u64(bytes.len() as u64).ok().expect("");
    //                 writer.write_all(&bytes[..]).ok().expect("");
    //                 writer.flush().ok().expect("");
    //             }
    //
    //             cursor += 8 + read;
    //         }
    //     }
    //
    //     buffer = reader.into_inner()[cursor as usize ..available as usize].to_vec();
    // }
}
