mod kafka;

use crate::kafka::parse_kafka_request;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{request_header, ApiKey, ApiVersionsResponse, RequestHeader};
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{
    decode_request_header_from_buffer, encode_request_header_into_buffer, types, Encodable,
};
use std::io::{Read, Write};
use std::net::TcpListener;
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
                let req = parse_kafka_request(&mut buf).unwrap();
                println!("{:#?}", req);
                // First, encode the response body WITHOUT the message length prefix
                let mut response_buf = BytesMut::new();

                // Encode correlation_id
                let correlation_id = req.correlation_id;
                response_buf.extend_from_slice(&correlation_id.to_be_bytes());

                let api_version_resp = ApiVersionsResponse::default()
                    .with_api_keys(vec!(
                        ApiVersion::default()
                            .with_api_key(18)
                            .with_min_version(0)
                            .with_max_version(4)
                    ));

                // Encode the response
                let api_response = match api_version_resp.encode(&mut response_buf, req.api_version) {
                    Ok(_) => {}
                    Err(_) => {
                        ApiVersionsResponse::default()
                            .with_error_code(35)
                            .encode(&mut response_buf, 0).unwrap();
                    }
                };

                // NOW create the final buffer with the message length prefix
                let mut buf = BytesMut::new();
                let message_length = response_buf.len() as i32;
                buf.extend_from_slice(&message_length.to_be_bytes());
                buf.extend_from_slice(&response_buf);

                // Send the complete response
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
        .map_err(|_| anyhow::Error::msg("Unknown API key"))
        .unwrap();
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
