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
    pub fn new(&self, sequence: u32, bearing_angle: f64, gps_distance: f64) -> Payload {
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
        dbc_ver: u16,
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
                "dbc_ver": dbc_ver,
            }),
        }
    }
}

pub struct Imu;
impl Imu {
    pub fn new(
        sequence: u32,
        ax: f32,
        ay: f32,
        az: f32,
        gx: f32,
        gy: f32,
        gz: f32,
        mx: f32,
        my: f32,
        mz: f32,
    ) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "ax": ax,
                "ay": ay,
                "az": az,
                "gx": gx,
                "gy": gy,
                "gz": gz,
                "mx": mx,
                "my": my,
                "mz": mz,
            }),
        }
    }
}

pub struct Heartbeat;
impl Heartbeat {
    pub fn new(sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({}),
        }
    }
}

pub struct ActionResponse;
impl ActionResponse {
    pub fn new(sequence: u32, action_id: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({"action_id": action_id, "state": "Started", "progress": 0, "errors": []}),
        }
    }
}
