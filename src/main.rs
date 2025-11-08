use std::net::TcpListener;
use std::io::Write;

fn main() {

    let mut listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for opt_stream in listener.incoming() {
        match opt_stream {
            Ok(mut stream) => {
                stream.write_all(&[0, 0, 0, 5, 0, 0, 0, 7]).unwrap();
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
