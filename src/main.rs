mod kafka;

use std::net::TcpListener;
use std::io::{Read, Write};
use bytes::BytesMut;
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::{decode_request_header_from_buffer, encode_request_header_into_buffer, types};
use kafka_protocol::protocol::buf::ByteBuf;
use crate::kafka::parse_kafka_request;
// use kafka_protocol::protocol::buf::ByteBuf;

fn main() {

    let mut listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    //let mut bufz = [0; 8192];

    for opt_stream in listener.incoming() {
        match opt_stream {
            Ok(mut stream) => {
                let mut buf = BytesMut::zeroed(1024);
                let size = stream.read(&mut buf).unwrap();
                buf.truncate(size);
                let req =parse_kafka_request(&mut buf).unwrap();
                println!("{:#?}", req);

                let mut buf = BytesMut::new();
                let mut req_header = RequestHeader::default();
                // req_header.request_api_version = 3;
                // req_header.request_api_key = ApiKey::ApiVersions as i16;
                req_header.correlation_id = req.correlation_id;
                encode_request_header_into_buffer(&mut buf, &req_header).unwrap();

                stream.write_all(&buf).unwrap();

                // println!(" ++++++++ {:?}", size);
                // println!(" ++++++++ {:?}", buf);
                // decode_request_header_from_buffer(&mut buf);
                // let message_size = u32::from_be_bytes(buffer[0..4].try_into().unwrap());
                // let request_api_key = u16::from_be_bytes(buffer[4..6].try_into().unwrap());
                // let request_api_version = u16::from_be_bytes(buffer[6..8].try_into().unwrap());
                // let correlation_id = u32::from_be_bytes(buffer[8..12].try_into().unwrap());
                //println!(" ++++++++ {:?}", header);
                // stream.write_all(&req.correlation_id.to_be_bytes()).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

pub fn decode_request_header_from_bufferz<B: ByteBuf>(buf: &mut B) {
    let api_key = ApiKey::try_from(bytes::Buf::get_i16(&mut buf.peek_bytes(0..2)))
        .map_err(|_| anyhow::Error::msg("Unknown API key")).unwrap();
    let api_version = bytes::Buf::get_i16(&mut buf.peek_bytes(2..4));
    let header_version = api_key.request_header_version(api_version);
    println!(" ++++++++ Header {:?}", header_version);
    println!(" ++++++++ Api {:?}", api_version);
    // let request_api_key = types::Int16.decode(buf)?;
    // let request_api_version = types::Int16.decode(buf)?;
    // let correlation_id = types::Int32.decode(buf)?;
    // let client_id = types::String.decode(buf)?;

    //RequestHeader::decode(buf, header_version)
}