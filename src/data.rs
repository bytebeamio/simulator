use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};

pub trait Data {
    fn topic(&self) -> &str;
    fn serialized(&self) -> Vec<u8>;
}

#[derive(Debug, Serialize)]
pub struct Payload {
    sequence: u32,
    #[serde(with = "ts_milliseconds")]
    timestamp: DateTime<Utc>,
    #[serde(flatten)]
    payload: Value,
}

pub struct PayloadArray {
    pub topic: String,
    pub points: Vec<Payload>,
}

impl Data for PayloadArray {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn serialized(&self) -> Vec<u8> {
        serde_json::to_vec(&self.points).unwrap()
    }
}

pub struct Gps;
impl Gps {
    pub fn new(sequence: u32, longitude: f64, latitude: f64) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "longitude": longitude,
                "latitude": latitude,
            }),
        }
    }
}

pub struct Can;
impl Can {
    pub fn new(
        sequence: u32,
        can_id: u32,
        byte1: u8,
        byte2: u8,
        byte3: u8,
        byte4: u8,
        byte5: u8,
        byte6: u8,
        byte7: u8,
        byte8: u8,
    ) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "can_id": can_id,
                "byte1": byte1,
                "byte2": byte2,
                "byte3": byte3,
                "byte4": byte4,
                "byte5": byte5,
                "byte6": byte6,
                "byte7": byte7,
                "byte8": byte8,
            }),
        }
    }
}
