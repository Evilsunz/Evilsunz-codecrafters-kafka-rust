mod kafka;

use std::net::TcpListener;
use std::io::{Read, Write};
use bytes::BytesMut;
use kafka_protocol::messages::{request_header, ApiKey, ApiVersionsResponse, RequestHeader};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::protocol::{decode_request_header_from_buffer, encode_request_header_into_buffer, types, Encodable};
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
                //println!("{:#?}", req);
                let mut buf = BytesMut::new();
                // Reserve space for the message length (will be written later)
                buf.extend_from_slice(&[0u8; 4]);

                // Encode correlation_id for the response
                let correlation_id = req.correlation_id;
                buf.extend_from_slice(&correlation_id.to_be_bytes());

                // let request_header = RequestHeader::default().with_correlation_id(req.correlation_id);
                // //request_header.encode(&mut buf,0);
                // encode_request_header_into_buffer(&mut buf,&request_header);
                // Create and encode the response - MUST use version 0 for ApiVersionsResponse
                let api_version_resp = ApiVersionsResponse::default()
                    .with_error_code(35);
                api_version_resp.encode(&mut buf, 0).unwrap();  // Use version 0, not the request version

                // Write the message length at the beginning (total size - 4 bytes for the length field itself)
                let message_length = (buf.len() - 4) as i32;
                buf[0..4].copy_from_slice(&message_length.to_be_bytes());

                stream.write_all(&buf).unwrap();

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