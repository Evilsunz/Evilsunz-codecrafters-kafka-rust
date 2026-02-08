use std::collections::BTreeMap;
use std::fs;
use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, RequestHeader, RequestKind, ResponseHeader, TopicName};
use kafka_protocol::messages::describe_topic_partitions_response::{Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::types::Uuid;
use kafka_protocol::ResponseError;
use crate::kraft_parser::{decode, RecordType, Topic};

pub fn process_api_version(header: RequestHeader, req: ApiVersionsRequest) -> BytesMut{
    // First, encode the response body WITHOUT the message length prefix
    let mut response_buf = BytesMut::new();

    // Encode correlation_id
    let correlation_id = header.correlation_id;
    response_buf.extend_from_slice(&correlation_id.to_be_bytes());

    let api_version_resp = ApiVersionsResponse::default()
        .with_api_keys(vec!(
            ApiVersion::default()
                .with_api_key(18)
                .with_min_version(0)
                .with_max_version(4),
            ApiVersion::default()
                .with_api_key(75)
                .with_min_version(0)
                .with_max_version(0)
        ));

    // Encode the response
    let api_response = match api_version_resp.encode(&mut response_buf, header.request_api_version) {
        Ok(_) => {}
        Err(_) => {
            ApiVersionsResponse::default()
                .with_error_code(35)
                .encode(&mut response_buf, 0).unwrap();
        }
    };

    response_buf
}

pub fn process_describe_topic_partitions(api_key : ApiKey, header: RequestHeader, req: DescribeTopicPartitionsRequest) -> BytesMut {

    let res = decode().unwrap();
    println!(" +++++ {:#?}", res);

    let topics: Vec<&Topic> = res
        .iter()
        .filter_map(|rt| match rt {
            RecordType::TopicValue(t) => Some(t),
            _ => None,
        })
        .collect();

    //Temp
    let parsed_topic = topics.get(0).unwrap().to_owned();

    let mut response_buf = BytesMut::new();

    let _ = ResponseHeader::default()
        .with_correlation_id(header.correlation_id)
        .with_unknown_tagged_fields(BTreeMap::new())
        .encode(
            &mut response_buf,
            api_key.response_header_version(header.request_api_version),
        );

    let mut response_topics = Vec::with_capacity(req.topics.len());
    for topic in req.topics {
        if (topic.name.to_string() == parsed_topic.name) {
            //todo
            let partition = DescribeTopicPartitionsResponsePartition::default();
            response_topics.push(
                DescribeTopicPartitionsResponseTopic::default()
                    //.with_error_code(ResponseError::UnknownTopicOrPartition.code())
                    .with_name(Some(TopicName::from(kafka_protocol::protocol::StrBytes::from(parsed_topic.name.clone()))))
                    .with_is_internal(false)
                    .with_topic_id(parsed_topic.uuid)
                    .with_partitions(vec![partition])
            )
        } else {
            response_topics.push(
                DescribeTopicPartitionsResponseTopic::default()
                    .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                    .with_name(Some(topic.name.clone()))
                    .with_is_internal(false)
                    .with_topic_id("00000000-0000-0000-0000-000000000000".parse().unwrap())
            )
        }
    }


    let _ = DescribeTopicPartitionsResponse::default()
        .with_unknown_tagged_fields(BTreeMap::new())
        .with_topics(response_topics)
        .encode(&mut response_buf, header.request_api_version);

    //println!("ver {:#?}", header.request_api_version);
    // println!("buf {:#?}", response_buf);
    response_buf
}