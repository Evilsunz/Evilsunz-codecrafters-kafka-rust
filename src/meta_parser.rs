use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::records::{Record, RecordBatchDecoder, RecordBatchEncoder};
use std::{fs, thread,time};
use std::process::Command;

pub fn decode() -> anyhow::Result<Vec<RecordType>> {
    let path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    let file = fs::read(path)?;
    let mut buf = BytesMut::from(&file[..]);
    let res = RecordBatchDecoder::decode_all(&mut buf)?;
    // println!("{:?}", res);
    let result : Vec<RecordType> = res
        .iter()
        .flat_map(|rec| {
            rec.records.iter().map(|rec| {
                //eprintln!("{:?}", rec);
                let mut data = rec.value.clone().unwrap();
                //eprintln!("REM-> {:?}", data.remaining());
                let header = get_header(&mut data);
                //eprintln!("{:?}", header);
                //eprintln!("REM-> {:?}", data.remaining());

                match header.record_type {
                    2 => {
                        // Topic
                        let name_length = data.get_u8() - 1;
                        let name =
                            String::from_utf8(data.copy_to_bytes(name_length as usize).to_vec())
                                .unwrap();
                        let uuid = uuid::Uuid::from_u128(data.get_u128());
                        RecordType::TopicValue(Topic { header, name, uuid })
                    }
                    3 => {
                        // Partition
                        let partition_id = data.get_u32();
                        let topic_uuid = uuid::Uuid::from_u128(data.get_u128());
                        let rep_array_length = data.get_u8();
                        let rep_array = data.get_i32();
                        let in_sync_rep_arr_length = data.get_u8();
                        let in_sync_rep_arr = data.get_i32();
                        let rmv_rep_arr_length = data.get_u8();
                        let adding_rep_arr_length = data.get_u8();
                        let leader = data.get_i32();
                        let leader_eponch = data.get_i32();
                        let partition_eponch = data.get_u32();
                        let directories_arr_length = data.get_u8();
                        let directories_arr = uuid::Uuid::from_u128(data.get_u128());
                        let tagged_fields_count = data.get_u8();
                        RecordType::PartitionValue(Partition {
                            header,
                            partition_id,
                            topic_uuid,
                            rep_array_length,
                            rep_array,
                            in_sync_rep_arr_length,
                            in_sync_rep_arr,
                            rmv_rep_arr_length,
                            adding_rep_arr_length,
                            leader,
                            leader_eponch,
                            partition_eponch,
                            directories_arr_length,
                            directories_arr,
                            tagged_fields_count,
                        })
                    }
                    12 => { // Feature
                        let name_length = data.get_u8() - 1;
                        let name =
                            String::from_utf8(data.copy_to_bytes(name_length as usize).to_vec()).unwrap();
                        let feature_level = data.get_u16();
                        let _tagged_fields_count = data.get_u8();
                        RecordType::FeatureValue(Feature {
                            header,
                            name,
                            feature_level,
                        })
                    }
                    _ => {
                        RecordType::None
                    }
                }
            }).collect::<Vec<_>>().into_iter()
        }).collect();
    Ok(result)
}

fn get_header(data: &mut Bytes) -> Header {
    let frame_version = data.get_u8();
    let record_type = data.get_u8();
    let version = data.get_u8();
    Header {
        frame_version,
        record_type,
        version,
    }
}

#[derive(Debug)]
pub enum RecordType {
    FeatureValue(Feature),
    TopicValue(Topic),
    PartitionValue(Partition),
    None
}

#[derive(Debug,Clone)]
pub struct Header {
    pub frame_version: u8,
    pub record_type: u8,
    pub version: u8,
}

#[derive(Debug)]
pub struct Feature {
    pub header: Header,
    pub name: String,
    pub feature_level: u16,
}

#[derive(Debug)]
pub struct Topic {
    pub header: Header,
    pub name: String,
    pub uuid: uuid::Uuid,
}

#[derive(Debug,Clone)]
pub struct Partition {
    pub header: Header,
    pub partition_id: u32,
    pub topic_uuid: uuid::Uuid,
    pub rep_array_length: u8,
    pub rep_array: i32,
    pub in_sync_rep_arr_length: u8,
    pub in_sync_rep_arr: i32,
    pub rmv_rep_arr_length: u8,
    pub adding_rep_arr_length: u8,
    pub leader: i32,
    pub leader_eponch: i32,
    pub partition_eponch: u32,
    pub directories_arr_length: u8,
    pub directories_arr: uuid::Uuid,
    pub tagged_fields_count: u8,
}

// impl TryFrom<&mut BytesMut> for RecordValue {
//     type Error = String;
//
//     fn try_from(buf: &mut BytesMut) -> Result<Self, Self::Error> {
//         let header = RecordHeader::try_from(&mut *buf).unwrap();
//         match header.record_type {
//             RecordType::FeatureLevel => {
//                 let name_length = buf.get_u8() - 1;
//                 let name =
//                     String::from_utf8(buf.copy_to_bytes(name_length as usize).to_vec()).unwrap();
//                 let feature_level = buf.get_u16();
//                 let _tagged_fields_count = buf.get_u8();
//                 Ok(RecordValue::FeatureLevel(FeatureLevelRecord {
//                     header,
//                     name,
//                     feature_level,
//                 }))
//             }
//             RecordType::Topic => {
//                 let name_length = buf.get_u8() - 1;
//                 let name =
//                     String::from_utf8(buf.copy_to_bytes(name_length as usize).to_vec()).unwrap();
//                 let uuid = uuid::Uuid::from_u128(buf.get_u128());
//                 let _tagged_fields_count = buf.get_u8();
//                 Ok(RecordValue::Topic(TopicRecord { header, name, uuid }))
//             }
//             RecordType::Partition => {
//                 let partition_id = buf.get_u32();
//                 let topic_uuid = uuid::Uuid::from_u128(buf.get_u128());
//                 let rep_array_length = buf.get_u8();
//                 let rep_array = buf.get_i32();
//                 let in_sync_rep_arr_length = buf.get_u8();
//                 let in_sync_rep_arr = buf.get_i32();
//                 let rmv_rep_arr_length = buf.get_u8();
//                 let adding_rep_arr_length = buf.get_u8();
//                 let leader = buf.get_i32();
//                 let leader_eponch = buf.get_i32();
//                 let partition_eponch = buf.get_u32();
//                 let directories_arr_length = buf.get_u8();
//                 let directories_arr = uuid::Uuid::from_u128(buf.get_u128());
//                 let tagged_fields_count = buf.get_u8();
//                 Ok(RecordValue::Partition(PartitionRecord {
//                     header,
//                     partition_id,
//                     topic_uuid,
//                     rep_array_length,
//                     rep_array,
//                     in_sync_rep_arr_length,
//                     in_sync_rep_arr,
//                     rmv_rep_arr_length,
//                     adding_rep_arr_length,
//                     leader,
//                     leader_eponch,
//                     partition_eponch,
//                     directories_arr_length,
//                     directories_arr,
//                     tagged_fields_count,
//                 }))
//             }
//         }
//     }
// }
