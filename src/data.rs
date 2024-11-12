use std::io::Write;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use lz4_flex::frame::FrameEncoder;
use rand::{rngs::StdRng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sysinfo::System;

pub trait Type: std::fmt::Debug + Send + Sync + 'static {
    fn generate(rng: &mut StdRng) -> Self;
    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload;
}

pub trait Data {
    fn serialized(&self) -> Vec<u8>;
}

#[derive(Debug, Serialize)]
pub struct Payload {
    pub sequence: u32,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(Debug)]
pub struct PayloadArray {
    pub points: Vec<Payload>,
    pub compression: bool,
}

impl PayloadArray {
    pub fn new(cap: usize, compression: bool) -> Self {
        Self {
            points: Vec::with_capacity(cap), // PERF: ensures lesser allocs
            compression,
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            points: self.points.drain(..).collect(), // PERF: drain ensures we don't lose out alloc
            compression: self.compression,
        }
    }
}

impl Data for PayloadArray {
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

pub struct ActionResponse {
    pub sequence: u32,
    pub action_id: String,
    pub progress: u32,
    pub state: String,
    pub errors: Vec<String>,
}

impl ActionResponse {
    pub fn as_payload(self) -> Payload {
        Payload {
            sequence: self.sequence,
            timestamp: Utc::now(),
            payload: json!({
                "action_id": self.action_id,
                "state":  self.state,
                "progress": self.progress,
                "errors": self.errors,
            }),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct DeviceShadow {
    battery: u32,
    gsm: u32,
    wifi: u32,
}

impl Type for DeviceShadow {
    fn generate(rng: &mut StdRng) -> Self {
        Self {
            battery: rng.gen_range(0..100),
            gsm: rng.gen_range(0..100),
            wifi: rng.gen_range(0..100),
        }
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp,
            payload: json!({
                "Battery": self.battery,
                "GSMStrength": self.gsm,
                "WifiStrength": self.wifi,
                "SoftwareVersion": "v0.1.2",
                "Status": "Active"
            }),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct Transaction {
    amt: u32,
    customer: String,
}

impl Type for Transaction {
    fn generate(rng: &mut StdRng) -> Self {
        Self {
            amt: rng.gen_range(0..1000),
            customer: format!("User{}", rng.gen_range(0..10000000)),
        }
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp,
            payload: json!({
                "Amount": self.amt,
                "Currency": "Rupees",
                "CustomerName": self.customer,
                "MerchantName": "merchantA",
                "PaymentInstrument": "UPI",
                "Status": "Successful",
                "TransactionId": "",
            }),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct Resource {
    cpu: u32,
    memory: u32,
    upload: u32,
    download: u32,
}

impl Type for Resource {
    fn generate(rng: &mut StdRng) -> Self {
        let system = System::new_all();
        let total_memory = system.total_memory();
        let used_memory = system.used_memory();

        Self {
            cpu: system.global_cpu_info().cpu_usage() as u32,
            memory: (used_memory * 100 / total_memory) as u32,
            upload: rng.gen_range(0..100),
            download: rng.gen_range(0..100),
        }
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp,
            payload: json!({
                "CPU": self.cpu,
                "Memory": self.memory,
                "NetworkUplink": self.upload,
                "NetworkDownlink": self.download,
            }),
        }
    }
}
