use std::{
    env::{current_dir, var},
    fs::{read_dir, File},
    io::BufReader,
    mem,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use csv::Reader;
use log::{debug, error};
use rand::seq::SliceRandom;
use rumqttc::{AsyncClient, MqttOptions};
use serde::{de::DeserializeOwned, Deserialize};
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinSet,
    time::{sleep, Instant},
};
use tracing_subscriber::EnvFilter;

mod data;
mod mqtt;
mod serializer;

use data::{
    ActionResult, Can, Data, DeviceShadow, Imu, Payload, PayloadArray, RideDetail, RideStatistics,
    RideSummary, Stop, VehicleLocation, VehicleState, VicRequest,
};
use mqtt::Mqtt;
use serializer::Serializer;

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
    authentication: Option<Auth>,
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

fn get_reader(path: &PathBuf) -> BufReader<File> {
    let paths: Vec<_> = read_dir(path)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_file())
        .map(|e| e.path())
        .collect();
    if paths.is_empty() {
        println!("No files found in the directory.");
    }

    // Choose a random file
    let mut rng = rand::thread_rng();
    let path = paths.choose(&mut rng).unwrap();

    BufReader::new(File::open(path).unwrap())
}

trait Type: DeserializeOwned + std::fmt::Debug {
    fn timestamp(&self) -> DateTime<Utc>;
    fn payload(&self, sequence: u32) -> Payload;
}

async fn push_data<T: Type>(
    tx: Sender<PayloadArray>,
    project_id: String,
    client_id: u32,
    stream: &str,
    max_buf_size: usize,
    timeout: Duration,
    compression: bool,
) {
    let mut last_time = None;
    let mut sequence = 0;
    let mut total_time = 0.0;

    let mut data_path = current_dir().unwrap();
    data_path.push(format!("data/{stream}"));
    let mut rdr = Reader::from_reader(get_reader(&data_path));
    let mut iter = rdr.deserialize::<T>();
    let mut points = vec![];
    let mut start = Instant::now();
    let mut push = Instant::now() + timeout;

    loop {
        if points.is_empty() && max_buf_size > 1 {
            push = Instant::now() + timeout
        }
        let rec = match iter.next() {
            Some(Ok(r)) => r,
            Some(e) => {
                error!("{e:?}");
                continue;
            }
            _ => {
                rdr = Reader::from_reader(get_reader(&data_path));
                iter = rdr.deserialize::<T>();
                continue;
            }
        };
        if let Some(start) = last_time {
            let diff: TimeDelta = rec.timestamp() - start;
            let Ok(duration) = diff.to_std() else {
                continue;
            };
            sleep(duration).await
        }
        last_time = Some(rec.timestamp());

        sequence %= u32::MAX;
        sequence += 1;
        points.push(rec.payload(sequence));

        if points.len() >= max_buf_size || push.elapsed() > Duration::ZERO {
            let points = mem::take(&mut points);
            let mut topic =
                format!("/tenants/{project_id}/devices/{client_id}/events/{stream}/jsonarray");
            if compression {
                topic.push_str("/lz4")
            }
            let data_array = PayloadArray {
                topic,
                points,
                compression,
            };
            if let Err(e) = tx.send(data_array).await {
                error!("{e}");
            }
            total_time += start.elapsed().as_secs_f64();
            debug!(
                "client_id: {client_id}; Messages: {sequence}; Avg time: {}",
                total_time / sequence as f64
            );
            start = Instant::now();
        }
    }
}

async fn single_device(client_id: u32, config: Arc<Config>) {
    let (tx, rx) = channel(1);
    let mut opt = MqttOptions::new(client_id.to_string(), &config.broker, config.port);

    if let Some(authentication) = &config.authentication {
        opt.set_transport(rumqttc::Transport::tls_with_config(
            rumqttc::TlsConfiguration::Simple {
                ca: authentication.ca_certificate.as_bytes().to_vec(),
                alpn: None,
                client_auth: Some((
                    authentication.device_certificate.as_bytes().to_vec(),
                    authentication.device_private_key.as_bytes().to_vec(),
                )),
            },
        ));
    }

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

    handle.spawn(push_data::<Can>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "C2C_CAN",
        100,
        Duration::from_secs(60),
        true,
    ));
    handle.spawn(push_data::<Imu>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "imu_sensor",
        100,
        Duration::from_secs(60),
        true,
    ));
    handle.spawn(push_data::<ActionResult>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "action_result",
        1,
        Duration::from_secs(1),
        false,
    ));
    handle.spawn(push_data::<RideDetail>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "ride_detail",
        1,
        Duration::from_secs(1),
        false,
    ));
    handle.spawn(push_data::<RideSummary>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "ride_summary",
        1,
        Duration::from_secs(1),
        false,
    ));
    handle.spawn(push_data::<RideStatistics>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "ride_statistics",
        1,
        Duration::from_secs(1),
        false,
    ));
    handle.spawn(push_data::<Stop>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "stop",
        10,
        Duration::from_secs(10),
        false,
    ));
    handle.spawn(push_data::<VehicleLocation>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "vehicle_location",
        10,
        Duration::from_secs(10),
        false,
    ));
    handle.spawn(push_data::<VehicleState>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "vehicle_state",
        1,
        Duration::from_secs(1),
        false,
    ));
    handle.spawn(push_data::<VicRequest>(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "vic_request",
        1,
        Duration::from_secs(1),
        false,
    ));
    handle.spawn(async move {
        let mut sequence = 0;
        loop {
            let data_array = PayloadArray {
                topic: format!("/tenants/demo/devices/{client_id}/events/device_shadow/jsonarray"),
                points: vec![DeviceShadow::default().payload(sequence)],
                compression: true,
            };
            sequence += 1;
            if let Err(e) = tx.send(data_array).await {
                error!("{e}");
            }
            sleep(Duration::from_secs(1)).await;
        }
    });

    while let Some(o) = handle.join_next().await {
        if let Err(e) = o {
            error!("{e}");
        }
    }
}
