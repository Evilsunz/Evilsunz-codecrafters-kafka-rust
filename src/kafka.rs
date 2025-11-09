
use bytes::{BytesMut, Buf};
use anyhow::Result;

#[derive(Debug)]
pub struct KafkaRequest {
    pub message_size: i32,
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

pub fn parse_kafka_request(buf: &mut BytesMut) -> Result<KafkaRequest> {
    // Parse message size (4 bytes)
    if buf.remaining() < 4 {
        anyhow::bail!("Buffer too small for message size");
    }
    let message_size = buf.get_i32();

    // Parse API key (2 bytes)
    if buf.remaining() < 2 {
        anyhow::bail!("Buffer too small for API key");
    }
    let api_key = buf.get_i16();

    // Parse API version (2 bytes)
    if buf.remaining() < 2 {
        anyhow::bail!("Buffer too small for API version");
    }
    let api_version = buf.get_i16();

    // Parse correlation ID (4 bytes)
    if buf.remaining() < 4 {
        anyhow::bail!("Buffer too small for correlation ID");
    }
    let correlation_id = buf.get_i32();

    // Parse client_id (nullable string with i16 length prefix)
    let client_id = parse_nullable_string(buf)?;

    // For API version >= 3, parse tag buffer and additional fields
    let mut client_software_name = None;
    let mut client_software_version = None;

    if api_version >= 3 {
        // In flexible versions (v3+), there's a tag buffer
        // First, read any remaining compact strings
        if buf.remaining() > 0 {
            // Parse client_software_name (compact string for v3+)
            client_software_name = parse_compact_string(buf)?;

            if buf.remaining() > 0 {
                // Parse client_software_version (compact string for v3+)
                client_software_version = parse_compact_string(buf)?;
            }
        }
    }

    Ok(KafkaRequest {
        message_size,
        api_key,
        api_version,
        correlation_id,
        client_id,
        client_software_name,
        client_software_version,
    })
}

/// Parse a nullable string (length-prefixed with i16, -1 means null)
fn parse_nullable_string(buf: &mut BytesMut) -> Result<Option<String>> {
    if buf.remaining() < 2 {
        anyhow::bail!("Buffer too small for string length");
    }

    let length = buf.get_i16();

    if length == -1 {
        return Ok(None);
    }

    if length < 0 {
        anyhow::bail!("Invalid string length: {}", length);
    }

    let length = length as usize;
    if buf.remaining() < length {
        anyhow::bail!("Buffer too small for string data");
    }

    let bytes = buf.copy_to_bytes(length);
    let string = String::from_utf8(bytes.to_vec())?;

    Ok(Some(string))
}

/// Parse a compact string (unsigned varint length prefix)
fn parse_compact_string(buf: &mut BytesMut) -> Result<Option<String>> {
    if buf.remaining() < 1 {
        anyhow::bail!("Buffer too small for compact string");
    }

    let length = parse_unsigned_varint(buf)? as i32 - 1;

    if length == -1 {
        return Ok(None);
    }

    if length < 0 {
        anyhow::bail!("Invalid compact string length");
    }

    let length = length as usize;
    if buf.remaining() < length {
        anyhow::bail!("Buffer too small for compact string data");
    }

    let bytes = buf.copy_to_bytes(length);
    let string = String::from_utf8(bytes.to_vec())?;

    Ok(Some(string))
}

/// Parse an unsigned varint
fn parse_unsigned_varint(buf: &mut BytesMut) -> Result<u32> {
    let mut result = 0u32;
    let mut shift = 0;

    loop {
        if buf.remaining() < 1 {
            anyhow::bail!("Buffer too small for varint");
        }

        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 35 {
            anyhow::bail!("Varint too large");
        }
    }

    Ok(result)
}