use std::{
    env::{current_dir, var},
    fs::{read_to_string, File},
    io::BufReader,
    sync::Arc,
};

use csv::Reader;
use log::{debug, error};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rumqttc::{AsyncClient, MqttOptions};
use serde::Deserialize;
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinSet,
    time::{interval, sleep, Duration, Instant},
};
use tracing_subscriber::EnvFilter;

mod data;
mod mqtt;
mod serializer;

use data::{Data, Gps, PayloadArray};
use mqtt::Mqtt;
use serializer::Serializer;

use crate::data::{Can, Heartbeat, Imu};

#[derive(Debug, Deserialize)]
struct Auth {
    ca_certificate: String,
    device_certificate: String,
    device_private_key: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    project_id: String,
    broker: String,
    port: u16,
    authentication: Auth,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_env("RUST_LOG"))
        .try_init()
        .expect("initialized subscriber succesfully");
    let path = var("CONFIG_FILE").expect("Missing env variable");
    let rdr = BufReader::new(File::open(path).unwrap());
    let config: Config = serde_json::from_reader(rdr).unwrap();
    if config.project_id != "demo" {
        panic!("Non-demo tenant: {}", config.project_id);
    }
    let config = Arc::new(config);

    let start_id = var("START")
        .expect("Missing env variable 'START'")
        .parse()
        .unwrap();
    let end_id = var("END")
        .expect("Missing env variable 'END'")
        .parse()
        .unwrap();

    let mut tasks: JoinSet<()> = JoinSet::new();
    for i in start_id..=end_id {
        let config = config.clone();
        tasks.spawn(async move { single_device(i, config).await });
    }

    loop {
        if let Some(Err(e)) = tasks.join_next().await {
            error!("{e}");
        }
    }
}

struct GpsTrack {
    map: Vec<Gps>,
    trace_i: usize,
}

impl GpsTrack {
    fn new(mut trace_list: Vec<Gps>) -> Self {
        let mut traces = trace_list.iter();
        let Some(last) = traces.next() else {
            panic!("Not enough traces!");
        };

        let mut map = vec![last.clone()];
        for trace in traces {
            map.push(trace.clone());
        }
        trace_list.reverse();
        let mut traces = trace_list.iter();
        let Some(_) = traces.next() else {
            panic!("Not enough traces!");
        };
        for trace in traces {
            map.push(trace.clone());
        }

        Self { map, trace_i: 0 }
    }

    fn next(&mut self) -> Gps {
        // push trace 0 only for first set of point
        if self.trace_i == 0 {
            self.trace_i += 1;
            return self.map[0].clone();
        }
        loop {
            let trace_i = self.trace_i;
            self.trace_i = trace_i % self.map.len();
            return self.map[trace_i].clone();
        }
    }
}

// const GPS_RATE: usize = 1; // messages/sec i.e. 60 messages in ~60s
async fn push_gps(tx: Sender<PayloadArray>, client_id: u32) {
    let trace_list = {
        let mut gps_path = current_dir().unwrap();
        gps_path.push("paths");
        let i = rand::thread_rng().gen_range(0..10);
        let file_name: String = format!("path{}.json", i);
        gps_path.push(file_name);

        let contents = read_to_string(gps_path).expect("Oops, failed to read path");

        let parsed: Vec<Gps> = serde_json::from_str(&contents).unwrap();

        parsed
    };
    let mut path = GpsTrack::new(trace_list);

    let mut sequence = 0;
    let mut total_time = 0.0;
    let mut clock = interval(Duration::from_secs(60));
    let mut rng = StdRng::from_entropy();
    loop {
        clock.tick().await;
        let start = Instant::now();
        let mut gps_array = PayloadArray {
            topic: format!("/tenants/demo/devices/{client_id}/events/vehicle_location/jsonarray"),
            points: vec![],
            compression: false,
        };
        for _ in 0..60 {
            sequence %= u32::MAX;
            sequence += 1;
            gps_array
                .points
                .push(path.next().payload(sequence, rng.gen(), rng.gen()));
        }
        if let Err(e) = tx.send(gps_array).await {
            error!("{e}");
        }
        total_time += start.elapsed().as_secs_f64();
        debug!(
            "client_id: {client_id}; Messages: {sequence}; Avg time: {}",
            total_time / sequence as f64
        );
    }
}

// const CAN_RATE: usize = 700; // messages/sec i.e. 100 messages in ~140ms
async fn push_can(tx: Sender<PayloadArray>, client_id: u32) {
    let get_reader = || {
        let mut can_path = current_dir().unwrap();
        can_path.push("can");
        let i = rand::thread_rng().gen_range(0..10);
        let file_name: String = format!("{i}.csv");
        can_path.push(file_name);

        BufReader::new(File::open(can_path).unwrap())
    };

    let mut last_time = None;
    let mut sequence = 0;
    let mut total_time = 0.0;

    let mut rdr = Reader::from_reader(get_reader());
    let mut iter = rdr.deserialize::<Can>();
    let mut points = vec![];
    let mut start = Instant::now();

    loop {
        let rec = match iter.next() {
            Some(Ok(r)) => r,
            Some(e) => {
                error!("{e:?}");
                continue;
            }
            _ => {
                rdr = Reader::from_reader(get_reader());
                iter = rdr.deserialize::<Can>();
                continue;
            }
        };
        if let Some(start) = last_time {
            let diff = match rec.timestamp.checked_sub(start) {
                Some(d) => d,
                _ => start - rec.timestamp
            };
            let duration = Duration::from_millis(diff);
            sleep(duration).await
        }
        last_time = Some(rec.timestamp);

        if points.len() >= 100 {
            let points = points.drain(0..).collect();
            let gps_array = PayloadArray {
                topic: format!("/tenants/demo/devices/{client_id}/events/can_raw/jsonarray/lz4"),
                points,
                compression: true,
            };
            if let Err(e) = tx.send(gps_array).await {
                error!("{e}");
            }
            total_time += start.elapsed().as_secs_f64();
            debug!(
                "client_id: {client_id}; Messages: {sequence}; Avg time: {}",
                total_time / sequence as f64
            );
            start = Instant::now();
        }

        sequence %= u32::MAX;
        sequence += 1;
        points.push(rec.payload(sequence));
    }
}

// const CAN_RATE: usize = 10; // messages/sec i.e. 100 messages in ~140ms
async fn push_imu(tx: Sender<PayloadArray>, client_id: u32) {
    let mut sequence = 0;
    let mut total_time = 0.0;
    let mut clock = interval(Duration::from_millis(140));
    let mut rng = StdRng::from_entropy();
    loop {
        clock.tick().await;
        let start = Instant::now();
        let mut gps_array = PayloadArray {
            topic: format!("/tenants/demo/devices/{client_id}/events/imu_sensor/jsonarray/lz4"),
            points: vec![],
            compression: true,
        };
        for _ in 0..100 {
            sequence %= u32::MAX;
            sequence += 1;
            gps_array.points.push(Imu::new(
                sequence,
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
                rng.gen(),
            ));
        }
        if let Err(e) = tx.send(gps_array).await {
            error!("{e}");
        }
        total_time += start.elapsed().as_secs_f64();
        debug!(
            "client_id: {client_id}; Messages: {sequence}; Avg time: {}",
            total_time / sequence as f64
        );
    }
}

// const CAN_RATE: usize = 0.016; // messages/sec i.e. 1 message in 60s
async fn push_heartbeat(tx: Sender<PayloadArray>, client_id: u32) {
    let mut sequence = 0;
    let mut total_time = 0.0;
    let mut clock = interval(Duration::from_millis(140));
    loop {
        clock.tick().await;
        let start = Instant::now();
        let mut gps_array = PayloadArray {
            topic: format!("/tenants/demo/devices/{client_id}/events/device_shadow/jsonarray"),
            points: vec![],
            compression: false,
        };
        sequence %= u32::MAX;
        sequence += 1;
        gps_array.points.push(Heartbeat::new(sequence));

        if let Err(e) = tx.send(gps_array).await {
            error!("{e}");
        }
        total_time += start.elapsed().as_secs_f64();
        debug!(
            "client_id: {client_id}; Messages: {sequence}; Avg time: {}",
            total_time / sequence as f64
        );
    }
}

async fn single_device(client_id: u32, config: Arc<Config>) {
    let (tx, rx) = channel(1);
    let mut opt = MqttOptions::new(client_id.to_string(), &config.broker, config.port);
    opt.set_transport(rumqttc::Transport::tls_with_config(
        rumqttc::TlsConfiguration::Simple {
            ca: config.authentication.ca_certificate.as_bytes().to_vec(),
            alpn: None,
            client_auth: Some((
                config.authentication.device_certificate.as_bytes().to_vec(),
                config.authentication.device_private_key.as_bytes().to_vec(),
            )),
        },
    ));
    opt.set_max_packet_size(1024 * 1024, 1024 * 1024);
    let (client, mut eventloop) = AsyncClient::new(opt, 1);
    eventloop.network_options.set_connection_timeout(30);

    let mut handle = JoinSet::new();
    let mut serializer = Serializer {
        rx,
        client: client.clone(),
    };
    handle.spawn(async move { serializer.start(client_id).await });
    handle.spawn(async move { Mqtt { eventloop, client }.start(client_id).await });
    handle.spawn(push_gps(tx.clone(), client_id));
    handle.spawn(push_can(tx.clone(), client_id));
    handle.spawn(push_imu(tx.clone(), client_id));
    handle.spawn(push_heartbeat(tx.clone(), client_id));

    while let Some(o) = handle.join_next().await {
        if let Err(e) = o {
            error!("{e}");
        }
    }
}
