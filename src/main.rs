use std::{
    env::{current_dir, var},
    fs::File,
    io::BufReader,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use chrono::TimeDelta;
use log::{debug, error, info, warn};
use rand::{rngs::StdRng, SeedableRng};
use rumqttc::{AsyncClient, MqttOptions};
use serde::Deserialize;
use tokio::{
    runtime::Builder,
    select, spawn,
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinSet,
    time::{interval, sleep, sleep_until, Instant, Sleep},
};
use tracing_subscriber::EnvFilter;

mod data;
mod mqtt;
mod serializer;

use data::{
    ActionResult, Can, Data, DeviceShadow, Historical, Imu, Payload, PayloadArray, RideDetail,
    RideStatistics, RideSummary, Stop, Type, VehicleLocation, VehicleState, VicRequest,
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

fn main() {
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

    let mut historical = Historical::new();
    historical.load::<Can>("C2C_CAN");
    historical.load::<Imu>("imu_sensor");
    historical.load::<ActionResult>("action_result");
    historical.load::<RideDetail>("ride_detail");
    historical.load::<RideSummary>("ride_summary");
    historical.load::<RideStatistics>("ride_statistics");
    historical.load::<Stop>("stop");
    historical.load::<VehicleLocation>("vehicle_location");
    historical.load::<VehicleState>("vehicle_state");
    historical.load::<VicRequest>("vic_request");
    let data = Arc::new(historical);

    info!("Data loaded into memory");

    let cpu_count = num_cpus::get();
    info!("Starting simulator on {cpu_count} cpus");
    Builder::new_multi_thread()
        .worker_threads(cpu_count)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut tasks: JoinSet<()> = JoinSet::new();
            for i in start_id..=end_id {
                let config = config.clone();
                let data = data.clone();
                tasks.spawn(async move { single_device(i, config, data).await });
            }

            loop {
                if let Some(Err(e)) = tasks.join_next().await {
                    error!("{e}");
                }
            }
        });
}

async fn batch_data(
    mut rx: Receiver<Payload>,
    tx: Sender<PayloadArray>,
    topic: String,
    max_buf_size: usize,
    timeout: Duration,
    compression: bool,
) {
    let mut data_array = PayloadArray {
        topic,
        points: vec![],
        compression,
    };
    let mut end: Pin<Box<Sleep>> = Box::pin(sleep(Duration::from_secs(u64::MAX)));
    let mut push = None;
    loop {
        select! {
            Some(payload) = rx.recv() => {
                if data_array.points.is_empty() {
                    push = Some( Box::pin(sleep(timeout)))
                }
                data_array.points.push(payload);
                if data_array.points.len() < max_buf_size {
                    continue
                }
            }
            _ = &mut push.as_mut().map(|a| a).unwrap_or(&mut end) => {
                push.take();
            }
        }

        if let Err(e) = tx.send(data_array.take()).await {
            error!("{e}");
        }
        data_array.points.clear();
    }
}

async fn push_data(
    batch_tx: Sender<PayloadArray>,
    project_id: String,
    client_id: u32,
    stream: &str,
    max_buf_size: usize,
    timeout: Duration,
    compression: bool,
    data: Arc<Historical>,
) {
    let mut last_time = None;
    let mut sequence = 0;
    let mut total_time = 0.0;

    let mut data_path = current_dir().unwrap();
    data_path.push(format!("data/{stream}"));

    let mut rng = StdRng::from_entropy();
    let mut iter = data.get_random(stream, &mut rng).iter();
    let mut start = Instant::now();

    let (tx, rx) = channel(1);
    let mut topic = format!("/tenants/{project_id}/devices/{client_id}/events/{stream}/jsonarray");
    if compression {
        topic.push_str("/lz4")
    }
    spawn(batch_data(
        rx,
        batch_tx,
        topic,
        max_buf_size,
        timeout,
        compression,
    ));

    loop {
        let rec: &Box<dyn Type> = match iter.next() {
            Some(r) => r,
            _ => {
                iter = data.get_random(stream, &mut rng).iter();
                continue;
            }
        };
        if let Some(start) = last_time {
            let diff: TimeDelta = rec.timestamp() - start;
            let duration = diff.abs().to_std().unwrap();
            let deadline = Instant::now() + duration;
            sleep_until(deadline).await;

            let elapsed = deadline.elapsed();
            if elapsed > Duration::from_secs(1) {
                warn!("Time waited beyond elapsed: {}s", elapsed.as_secs())
            }
        }
        last_time = Some(rec.timestamp());

        sequence %= u32::MAX;
        sequence += 1;
        if let Err(e) = tx.try_send(rec.payload(sequence)) {
            error!("{e}")
        }

        total_time += start.elapsed().as_secs_f64();
        debug!(
            "client_id: {client_id}; Messages: {sequence}; Avg time: {}",
            total_time / sequence as f64
        );
        start = Instant::now();
    }
}

async fn single_device(client_id: u32, config: Arc<Config>, data: Arc<Historical>) {
    info!("Starting device {client_id}");
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

    let mut serializer = Serializer {
        rx,
        client: client.clone(),
    };
    spawn(async move { serializer.start(client_id).await });
    let project_id = config.project_id.clone();
    spawn(async move {
        Mqtt { eventloop, client }
            .start(project_id, client_id)
            .await
    });

    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "C2C_CAN",
        100,
        Duration::from_secs(60),
        true,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "imu_sensor",
        100,
        Duration::from_secs(60),
        true,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "action_result",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "ride_detail",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "ride_summary",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "ride_statistics",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "stop",
        10,
        Duration::from_secs(10),
        false,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "vehicle_location",
        10,
        Duration::from_secs(10),
        false,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "vehicle_state",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
    ));
    spawn(push_data(
        tx.clone(),
        config.project_id.clone(),
        client_id,
        "vic_request",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
    ));

    let mut sequence = 0;
    let mut interval = interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        let data_array = PayloadArray {
            topic: format!(
                "/tenants/{}/devices/{client_id}/events/device_shadow/jsonarray",
                config.project_id
            ),
            points: vec![DeviceShadow::default().payload(sequence)],
            compression: false,
        };
        sequence += 1;
        if let Err(e) = tx.send(data_array).await {
            error!("{e}");
        }
    }
}
