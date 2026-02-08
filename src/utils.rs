use std::fs;
use bytes::{Buf, Bytes, BytesMut};
use indexmap::IndexMap;
use uuid::Uuid;
use crate::meta_parser::{Partition, RecordType, Topic};

#[derive(Debug)]
pub struct TopicWithPartitions {
    pub topic: Topic,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Default)]
struct TopicAcc {
    topic: Option<Topic>,
    partitions: Vec<Partition>,
}

pub fn group_topics(records: Vec<RecordType>) -> Vec<TopicWithPartitions> {
    let mut by_topic: IndexMap<Uuid, TopicAcc> = IndexMap::new();

    for r in records {
        match r {
            RecordType::TopicValue(t) => {
                let uuid = t.uuid;
                by_topic.entry(uuid).or_default().topic = Some(t);
            }
            RecordType::PartitionValue(p) => {
                by_topic
                    .entry(p.topic_uuid)
                    .or_default()
                    .partitions
                    .push(p);
            }
            _ => {}
        }
    }

    by_topic
        .into_iter()
        .filter_map(|(_uuid, acc)| {
            let topic = acc.topic?;
            Some(TopicWithPartitions {
                topic,
                partitions: acc.partitions,
            })
        })
        .collect()
}

pub fn read_records(topic_name: &str, partition_id : u32) -> Bytes{
    let path = format!("/tmp/kraft-combined-logs/{}-{}/00000000000000000000.log", topic_name, partition_id);
    let file = fs::read(path).unwrap();
    let mut buf = BytesMut::from(&file[..]);
    buf.copy_to_bytes(buf.len())
}