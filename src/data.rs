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
