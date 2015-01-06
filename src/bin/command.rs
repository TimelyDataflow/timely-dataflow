extern crate libc;

use std::io::stdio::{stdin_raw, stdout_raw};
use std::io::{MemReader, MemWriter, IoErrorKind};
use std::thread::Thread;

fn main()
{

    unsafe { libc::fcntl(0, libc::F_SETFL, libc::O_NONBLOCK); }
    unsafe { libc::fcntl(1, libc::F_SETFL, libc::O_NONBLOCK); }


    let mut reader = stdin_raw();
    let mut writer = stdout_raw();

    let mut memwriter = MemWriter::new();

    memwriter.write_le_uint(1).ok().expect("");
    memwriter.write_le_uint(0).ok().expect("");
    memwriter.write_le_uint(1).ok().expect("");

    let bytes = memwriter.into_inner();
    writer.write_le_uint(bytes.len()).ok().expect("");
    writer.write(bytes.as_slice()).ok().expect("");
    writer.flush().ok().expect("");

    let mut buffer = Vec::new();

    loop
    {
        // push some amount, then try decoding...
        match reader.push(1024, &mut buffer)
        {
            Ok(_)  => { },
            Err(e) => { if e.kind != IoErrorKind::ResourceUnavailable { panic!("Pull error: {}", e) }
                        else { Thread::yield_now(); }},
        }

        let available = buffer.len();
        let mut reader = MemReader::new(buffer);
        let mut cursor = 0;

        let mut done = false;

        while !done
        {
            let read = match reader.read_le_uint()
            {
                Ok(x) => x,
                Err(_) => { done = true; 0 }
            };

            if done || read + 8 + cursor > available
            {
                done = true;
            }
            else
            {
                let mut updates = Vec::new();

                let externals = reader.read_le_uint().ok().expect("");
                for _ in range(0, externals)
                {
                    let time = reader.read_le_uint().ok().expect("");
                    let delta= reader.read_le_i64().ok().expect("");

                    if time < 1000000
                    {
                        updates.push((time, delta));
                    }
                }

                if updates.len() > 0
                {
                    let mut memwriter = MemWriter::new();
                    memwriter.write_le_uint(updates.len()).ok().expect("");
                    while let Some((time, delta)) = updates.pop()
                    {
                        memwriter.write_le_uint(time).ok().expect("");
                        memwriter.write_le_i64(delta).ok().expect("");
                    }

                    memwriter.write_le_uint(0).ok().expect("");
                    memwriter.write_le_uint(0).ok().expect("");

                    let bytes = memwriter.into_inner();
                    writer.write_le_uint(bytes.len()).ok().expect("");
                    writer.write(bytes.as_slice()).ok().expect("");
                    writer.flush().ok().expect("");
                }

                cursor += 8 + read;
            }
        }

        buffer = reader.into_inner().slice(cursor, available).to_vec();
    }
}
