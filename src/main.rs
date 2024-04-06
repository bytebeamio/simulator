use std::env::var;

use log::{debug, error};
use rumqttc::{AsyncClient, MqttOptions};
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinSet,
    time::{interval, Duration, Instant},
};
use tracing_subscriber::EnvFilter;

mod data;
mod mqtt;
mod serializer;

use data::{Data, Gps, PayloadArray};
use mqtt::Mqtt;
use serializer::Serializer;

use crate::data::Can;

const DEVICE_COUNT: u32 = 500;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_env("RUST_LOG"))
        .try_init()
        .expect("initialized subscriber succesfully");

    let mut tasks: JoinSet<()> = JoinSet::new();
    for i in 0..DEVICE_COUNT {
        tasks.spawn(async move { single_device(i).await });
    }

    loop {
        if let Some(Err(e)) = tasks.join_next().await {
            error!("{e}");
        }
    }
}

// const GPS_RATE: usize = 1; // messages/sec i.e. 60 messages in ~60s
async fn push_gps(tx: Sender<PayloadArray>, client_id: u32) {
    let mut sequence = 0;
    let mut total_time = 0.0;
    let mut clock = interval(Duration::from_secs(60));
    loop {
        clock.tick().await;
        let start = Instant::now();
        let mut gps_array = PayloadArray {
            topic: format!("/tenants/demo/devices/{client_id}/events/gps/jsonarray"),
            points: vec![],
        };
        for _ in 0..60 {
            sequence %= u32::MAX;
            sequence += 1;
            gps_array.points.push(Gps::new(sequence, 0.0, 0.0));
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
    let mut sequence = 0;
    let mut total_time = 0.0;
    let mut clock = interval(Duration::from_millis(140));
    loop {
        clock.tick().await;
        let start = Instant::now();
        let mut gps_array = PayloadArray {
            topic: format!("/tenants/demo/devices/{client_id}/events/can_raw/jsonarray"),
            points: vec![],
        };
        for _ in 0..100 {
            sequence %= u32::MAX;
            sequence += 1;
            gps_array
                .points
                .push(Can::new(sequence, 0, 0, 0, 0, 0, 0, 0, 0, 0));
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

async fn single_device(client_id: u32) {
    let (tx, rx) = channel(1);
    let broker = var("BROKER").expect("Missing env variable");
    let port = var("PORT").expect("Missing env variable").parse().unwrap();
    let mut opt = MqttOptions::new(client_id.to_string(), broker, port);
    opt.set_max_packet_size(1024 * 1024, 1024 * 1024);
    let (client, mut eventloop) = AsyncClient::new(opt, 1);
    eventloop.network_options.set_connection_timeout(30);

    let mut handle = JoinSet::new();
    handle.spawn(async move { Serializer { rx, client }.start(client_id).await });
    handle.spawn(async move { Mqtt { eventloop }.start(client_id).await });
    handle.spawn(push_gps(tx.clone(), client_id));
    handle.spawn(push_can(tx.clone(), client_id));

    while let Some(o) = handle.join_next().await {
        if let Err(e) = o {
            error!("{e}");
        }
    }
}
