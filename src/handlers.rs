use std::collections::BTreeMap;
use std::fs;
use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, FetchRequest, FetchResponse, RequestHeader, RequestKind, ResponseHeader, TopicName};
use kafka_protocol::messages::describe_topic_partitions_response::{Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::types::Uuid;
use kafka_protocol::ResponseError;
use crate::meta_parser::{decode, Partition, RecordType, Topic};
use crate::utils::group_topics;

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
                .with_max_version(0),
            ApiVersion::default()
                .with_api_key(1)
                .with_min_version(0)
                .with_max_version(16)
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

pub fn process_fetch(api_key : ApiKey, header: RequestHeader, req: FetchRequest) -> BytesMut {
    let res = decode().unwrap_or_else(|_| Vec::new());
    println!(" +++++ {:#?}", res);

    let grouped = group_topics(res);

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

        let requested_name = topic.topic_id.to_string();
        let matched_topic = grouped.iter().find(|tp| tp.topic.uuid.to_string() == requested_name);

        let response_topic = if let Some(tp) = matched_topic {
            FetchableTopicResponse::default()
                .with_topic(topic.topic)
                .with_topic_id(topic.topic_id)
                .with_partitions(vec![PartitionData::default()
                    //.with_error_code(ResponseError::UnknownTopicId.code())
                    .with_partition_index(0)
                ])
        } else {
            FetchableTopicResponse::default()
                .with_topic(topic.topic)
                .with_topic_id(topic.topic_id)
                .with_partitions(vec![PartitionData::default()
                    .with_error_code(ResponseError::UnknownTopicId.code())
                    .with_partition_index(0)
                ])
        };
        response_topics.push(response_topic);
    }

    let _ = FetchResponse::default()
        //.with_error_code(ResponseError::UnknownTopicId.code())
        .with_responses(response_topics)
        .encode(&mut response_buf, header.request_api_version);
    response_buf
}

pub fn process_describe_topic_partitions(api_key : ApiKey, header: RequestHeader, req: DescribeTopicPartitionsRequest) -> BytesMut {

    let res = decode().unwrap();
    println!(" +++++ {:#?}", res);

    let grouped = group_topics(res);

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
        let requested_name = topic.name.to_string();
        let matched_topic = grouped.iter().find(|tp| tp.topic.name == requested_name);

        let response_topic = if let Some(tp) = matched_topic {
            let partitions_response = build_partiotions_response(tp.partitions.clone());
            DescribeTopicPartitionsResponseTopic::default()
                .with_name(Some(TopicName::from(kafka_protocol::protocol::StrBytes::from(
                    tp.topic.name.clone(),
                ))))
                .with_is_internal(false)
                .with_topic_id(tp.topic.uuid)
                .with_partitions(partitions_response)
        } else {
            DescribeTopicPartitionsResponseTopic::default()
                .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                .with_name(Some(topic.name.clone()))
                .with_is_internal(false)
                .with_topic_id(uuid::Uuid::nil())
        };

        response_topics.push(response_topic);
    }

    //sort
    response_topics.sort_by_key(|r| r.name.clone());
    let _ = DescribeTopicPartitionsResponse::default()
        .with_unknown_tagged_fields(BTreeMap::new())
        .with_topics(response_topics)
        .encode(&mut response_buf, header.request_api_version);

    //println!("ver {:#?}", header.request_api_version);
    // println!("buf {:#?}", response_buf);
    response_buf
}

fn build_partiotions_response (mut partitions: Vec<Partition>) -> Vec<DescribeTopicPartitionsResponsePartition>{
    partitions.sort_by_key(|p| p.partition_id);
    partitions.into_iter().map(|p | {

        let replica_nodes = match p.rep_array_length {
            0 => Vec::new(),
            _ => vec![BrokerId::from(p.rep_array)],
        };

        let isr_nodes = match p.in_sync_rep_arr_length {
            0 => Vec::new(),
            _ => vec![BrokerId::from(p.in_sync_rep_arr)],
        };

        DescribeTopicPartitionsResponsePartition::default()
            //.with_error_code(ResponseError::None.code())
            .with_partition_index(p.partition_id as i32)
            .with_leader_id(BrokerId::from(p.leader))
            .with_leader_epoch(p.leader_eponch)
            .with_replica_nodes(replica_nodes)
            .with_isr_nodes(isr_nodes)
            .with_eligible_leader_replicas(Some(Vec::new()))
            .with_last_known_elr(Some(Vec::new()))
            .with_offline_replicas(Vec::new())
            .with_unknown_tagged_fields(BTreeMap::new())
    }).collect()
}