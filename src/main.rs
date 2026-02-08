mod handlers;
mod kraft_parser;

use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, DescribeTopicPartitionsRequest, RequestHeader, RequestKind};
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use anyhow::bail;
use bytes::{Buf, BytesMut};
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, StrBytes};
use crate::handlers::{process_api_version, process_describe_topic_partitions};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").expect("Failed to bind to port 9092");
    BytesMut::new();
    println!("Kafka broker listening on 127.0.0.1:9092");
    ApiVersionsRequest::default().with_client_software_name(StrBytes::from(""));
    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream) {
                        eprintln!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) -> io::Result<()> {
    loop {
        let response_buf = match parse_kafka_request(&mut stream) {
            Ok((api_key, header, request)) => {
                handle_request(api_key, header, request)
            }
            Err(e) => {
                eprintln!("Failed to parse Kafka request: {}", e);
                break;
            }
        };
        let mut buf = BytesMut::new();
        let message_length = response_buf.len() as i32;
        buf.extend_from_slice(&message_length.to_be_bytes());
        buf.extend_from_slice(&response_buf);
        stream.write_all(&buf)?;
    }
    Ok(())
}

fn handle_request(api_key: ApiKey, header: RequestHeader ,request: RequestKind) -> BytesMut {
    println!("Request: {:?}", request);
    match request {
        RequestKind::ApiVersions(req) => process_api_version(header, req),
        RequestKind::DescribeTopicPartitions(req) => process_describe_topic_partitions(api_key, header,req),
        _ => {
            panic!("Unsupported request kind");
        }
    }
}

fn parse_kafka_request(stream: &mut TcpStream) -> anyhow::Result<(ApiKey, RequestHeader, RequestKind)> {
    let mut buf = BytesMut::zeroed(1024);
    let bytes_read = stream.read(&mut buf)?;
    buf.truncate(bytes_read);

    let message_length = buf.get_i32();

    let api_key_value = bytes::Buf::get_i16(&mut buf.peek_bytes(0..2));
    let api_key =
        ApiKey::try_from(api_key_value).map_err(|_| anyhow::Error::msg("Unknown API key"))?;

    let api_version = bytes::Buf::get_i16(&mut buf.peek_bytes(2..4));
    let header_version = api_key.request_header_version(api_version);
    let header = RequestHeader::decode(&mut buf, header_version)?;
    let request = match api_key {
        ApiKey::ApiVersions => {
            let api_versions_request = ApiVersionsRequest::decode(&mut buf, header.request_api_version).unwrap_or_else(|_| {
                //Will send ApiVersionsRequest with error in handlers
                ApiVersionsRequest::default()
            });
            RequestKind::ApiVersions(api_versions_request)
        }
        ApiKey::DescribeTopicPartitions => {
            let describe_request =
                DescribeTopicPartitionsRequest::decode(&mut buf, header.request_api_version)?;
            RequestKind::DescribeTopicPartitions(describe_request)
        }
        _ => bail!("Unsupported API key: {:?}", api_key),
    };

    Ok((api_key, header, request))
}