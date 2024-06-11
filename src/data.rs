use std::io::Write;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use lz4_flex::frame::FrameEncoder;
use serde::{Deserialize, Serialize};
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
    pub compression: bool,
}

impl Data for PayloadArray {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn serialized(&self) -> Vec<u8> {
        let serialized = serde_json::to_vec(&self.points).unwrap();
        if self.compression {
            let mut compressor = FrameEncoder::new(vec![]);
            compressor.write_all(&serialized).unwrap();
            return compressor.finish().unwrap();
        }

        serialized
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gps {
    longitude: f64,
    latitude: f64,
}

impl Gps {
    pub fn payload(&self, sequence: u32, bearing_angle: f64, gps_distance: f64) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "longitude": self.longitude,
                "latitude": self.latitude,
                "bearing_angle": bearing_angle,
                "gps_distance": gps_distance,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Can {
    can_id: u32,
    byte1: u8,
    byte2: u8,
    byte3: u8,
    byte4: u8,
    byte5: u8,
    byte6: u8,
    byte7: u8,
    byte8: u8,
    dbc_ver: u16,
    pub timestamp: u64,
}

impl Can {
    pub fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "can_id": self.can_id,
                "byte1": self.byte1,
                "byte2": self.byte2,
                "byte3": self.byte3,
                "byte4": self.byte4,
                "byte5": self.byte5,
                "byte6": self.byte6,
                "byte7": self.byte7,
                "byte8": self.byte8,
                "dbc_ver": self.dbc_ver,
            }),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Imu {
    pub ax: f32,
    pub ay: f32,
    pub az: f32,
    pub gx: f32,
    pub gy: f32,
    pub gz: f32,
    pub mx: f32,
    pub my: f32,
    pub mz: f32,
}

impl Imu {
    pub fn as_payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!(self),
        }
    }
}

pub struct Heartbeat;
impl Heartbeat {
    pub fn as_payload(sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "Status": "Running"
            }),
        }
    }
}

pub struct ActionResponse;
impl ActionResponse {
    pub fn as_payload(sequence: u32, action_id: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "action_id": action_id,
                "state": match sequence {
                    0 => "Started",
                    100 => "Completed",
                    _ => "Running",
                },
                "progress": sequence * 10,
                "errors": [],
            }),
        }
    }
}
