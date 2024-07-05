use std::io::Write;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use lz4_flex::frame::FrameEncoder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::Type;

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

impl Type for Can {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Imu {
    gz: f64,
    az: f64,
    ax: f64,
    ay: f64,
    gy: f64,
    mz: i32,
    mx: i32,
    gx: f64,
    my: i32,
    pub timestamp: u64,
}

impl Type for Imu {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "gz": self.gz,
                "az": self.az,
                "ax": self.ax,
                "ay": self.ay,
                "gy": self.gy,
                "mz": self.mz,
                "mx": self.mx,
                "gx": self.gx,
                "my": self.my,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VicRequest {
    pub timestamp: u64,
    action_request: String,
    r#type: String,
    user_id: u32,
    request_id: u32,
}

impl Type for VicRequest {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "action_request": self.action_request,
                "type": self.r#type,
                "user_id": self.user_id,
                "request_id": self.request_id,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    pub timestamp: u64,
    action_response: String,
    r#type: String,
    user_id: u32,
    request_id: u32,
}

impl Type for ActionResult {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "action_response": self.action_response,
                "type": self.r#type,
                "user_id": self.user_id,
                "request_id": self.request_id,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VehicleLocation {
    pub timestamp: u64,
    longitude: f64,
    gps_speed: f64,
    bearing_angle: f64,
    latitude: f64,
}

impl Type for VehicleLocation {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "longitude": self.longitude,
                "gps_speed": self.gps_speed,
                "bearing_angle": self.bearing_angle,
                "latitude": self.latitude,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VehicleState {
    pub timestamp: u64,
    range_3: u32,
    vehicle_mode: u32,
    handle_lock_status: bool,
    range_1: u32,
    user_id: u32,
    range_2: u32,
    battery_soc: u32,
    extra: String,
}

impl Type for VehicleState {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "range_3": self.range_3,
                "vehicle_mode": self.vehicle_mode,
                "handle_lock_status": self.handle_lock_status,
                "range_1": self.range_1,
                "user_id": self.user_id,
                "range_2": self.range_2,
                "battery_soc": self.battery_soc,
                "extra": self.extra,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stop {
    pub timestamp: u64,
    location_name: String,
    longitude: f64,
    stop_id: u32,
    ride_distance: u32,
    latitude: f64,
    user_id: u32,
    ride_id: u32,
}

impl Type for Stop {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "location_name": self.location_name,
                "longitude": self.longitude,
                "stop_id": self.stop_id,
                "ride_distance": self.ride_distance,
                "latitude": self.latitude,
                "user_id": self.user_id,
                "ride_id": self.ride_id,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RideStatistics {
    pub timestamp: u64,
    zero_to_sixty: f64,
    max_left_lean_angle: u32,
    ride_efficiency: f64,
    ride_duration: u32,
    energy_consumed: f64,
    ride_stat_type: u32,
    ride_distance: f64,
    avg_speed: u32,
    user_id: u32,
    max_speed: u32,
    max_right_lean_angle: u32,
    co2_savings: u32,
}

impl Type for RideStatistics {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "zero_to_sixty": self.zero_to_sixty,
                "max_left_lean_angle": self.max_left_lean_angle,
                "ride_efficiency": self.ride_efficiency,
                "ride_duration": self.ride_duration,
                "energy_consumed": self.energy_consumed,
                "ride_stat_type": self.ride_stat_type,
                "ride_distance": self.ride_distance,
                "avg_speed": self.avg_speed,
                "user_id": self.user_id,
                "max_speed": self.max_speed,
                "max_right_lean_angle": self.max_right_lean_angle,
                "co2_savings": self.co2_savings,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RideSummary {
    pub timestamp: u64,
    update_time: String,
    dist_eco: f64,
    zero_to_sixty: f64,
    max_left_lean_angle: u32,
    fuel_cost_save: f64,
    ride_efficiency: f64,
    start_time: String,
    dist_city: f64,
    ride_duration: u32,
    energy_consumed: f64,
    dist_sports: f64,
    co2: f64,
    ride_distance: f64,
    avg_speed: u32,
    user_id: u32,
    max_speed: u32,
    ride_id: u32,
    max_right_lean_angle: u32,
}

impl Type for RideSummary {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }
    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "update_time": self.update_time,
                "dist_eco": self.dist_eco,
                "zero_to_sixty": self.zero_to_sixty,
                "max_left_lean_angle": self.max_left_lean_angle,
                "fuel_cost_save": self.fuel_cost_save,
                "ride_efficiency": self.ride_efficiency,
                "start_time": self.start_time,
                "dist_city": self.dist_city,
                "ride_duration": self.ride_duration,
                "energy_consumed": self.energy_consumed,
                "dist_sports": self.dist_sports,
                "co2": self.co2,
                "ride_distance": self.ride_distance,
                "avg_speed": self.avg_speed,
                "user_id": self.user_id,
                "max_speed": self.max_speed,
                "ride_id": self.ride_id,
                "max_right_lean_angle": self.max_right_lean_angle,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RideDetail {
    pub timestamp: u64,
    zero_to_sixty: f64,
    max_left_lean_angle: u32,
    ride_efficiency: f64,
    start_time: u64,
    ride_duration: u32,
    energy_consumed: f64,
    ride_distance: f64,
    avg_speed: u32,
    user_id: u32,
    max_speed: u32,
    ride_id: u32,
    max_right_lean_angle: u32,
    co2_savings: u32,
}

impl Type for RideDetail {
    fn timestamp(&self) -> u64 {
        self.timestamp
    }

    fn payload(&self, sequence: u32) -> Payload {
        Payload {
            sequence,
            timestamp: Utc::now(),
            payload: json!({
                "timestamp": self.timestamp,
                "zero_to_sixty": self.zero_to_sixty,
                "max_left_lean_angle": self.max_left_lean_angle,
                "ride_efficiency": self.ride_efficiency,
                "start_time": self.start_time,
                "ride_duration": self.ride_duration,
                "energy_consumed": self.energy_consumed,
                "ride_distance": self.ride_distance,
                "avg_speed": self.avg_speed,
                "user_id": self.user_id,
                "max_speed": self.max_speed,
                "ride_id": self.ride_id,
                "max_right_lean_angle": self.max_right_lean_angle,
                "co2_savings": self.co2_savings,
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
