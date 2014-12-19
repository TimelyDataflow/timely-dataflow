use std::io::{TcpListener, TcpStream};
use std::io::{Acceptor, Listener, IoResult};

use std::sync::{Arc, Future};

use std::time::duration::Duration;

pub fn initialize_networking(addresses: Vec<String>, my_index: uint) -> IoResult<Vec<Option<TcpStream>>>
{
    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let mut start_task = Future::spawn(move || start_connections(hosts1, my_index));
    let mut await_task = Future::spawn(move || await_connections(hosts2, my_index));

    let mut results = try!(await_task.get());

    results.push(None);
    results.push_all(try!(start_task.get()).as_slice());

    println!("worker {}:\tinitialization complete", my_index);

    return Ok(results);
}

pub fn start_connections(addresses: Arc<Vec<String>>, my_index: uint) -> IoResult<Vec<Option<TcpStream>>>
{
    // contains connections [0, my_index - 1].
    let mut results = Vec::from_elem(my_index, None);

    // connect to each worker in turn.
    for index in range(0, my_index)
    {
        let mut connected = false;

        while !connected
        {
            match TcpStream::connect_timeout(addresses[index].as_slice(), Duration::minutes(1))
            {
                Ok(mut stream) =>
                {
                    try!(stream.write_le_uint(my_index));

                    results[index] = Some(stream);

                    println!("worker {}:\tconnection to worker {}", my_index, index);
                    connected = true;
                },
                Err(error) =>
                {
                    println!("worker {}:\terror connecting to worker {}: {}; retrying", my_index, index, error);
                },
            }
        }
    }

    return Ok(results);
}

pub fn await_connections(addresses: Arc<Vec<String>>, my_index: uint) -> IoResult<Vec<Option<TcpStream>>>
{
    // contains connections [my_index + 1, addresses.len() - 1].
    let mut results = Vec::from_elem(addresses.len() - my_index, None);

    // listen for incoming connections
    let listener = TcpListener::bind(addresses[my_index].as_slice());

    let mut acceptor = try!(listener.listen());
    for _ in range(my_index + 1, addresses.len())
    {
        let mut stream = try!(acceptor.accept());
        let identifier = try!(stream.read_le_uint());

        results[identifier - my_index - 1] = Some(stream);

        println!("worker {}:\tconnection from worker {}", my_index, identifier);
    }

    return Ok(results);
}
