use std::env::var;

use log::{debug, error};
use rumqttc::{AsyncClient, MqttOptions};
use tokio::{
    spawn,
    sync::mpsc::channel,
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_env("RUST_LOG"))
        .try_init()
        .expect("initialized subscriber succesfully");

    let mut tasks: JoinSet<()> = JoinSet::new();
    for i in 0..500 {
        tasks.spawn(async move { single_device(i).await });
    }

    loop {
        if let Some(Err(e)) = tasks.join_next().await {
            error!("{e}");
        }
    }
}

async fn single_device(client_id: u32) {
    let (tx, rx) = channel(1);
    let broker = var("BROKER").expect("Missing env variable");
    let port = var("PORT").expect("Missing env variable").parse().unwrap();
    let opt = MqttOptions::new(client_id.to_string(), broker, port);
    let (client, eventloop) = AsyncClient::new(opt, 1);
    spawn(async move { Serializer { rx, client }.start().await });
    spawn(async move { Mqtt { eventloop }.start().await });

    let mut sequence = 0;
    let mut total_time = 0.0;
    let mut clock = interval(Duration::from_millis(100));
    loop {
        clock.tick().await;
        let start = Instant::now();
        let mut gps_array = PayloadArray {
            topic: format!("/tenants/demo/devices/{client_id}/events/data/jsonarray"),
            points: vec![],
        };
        for _ in 0..10 {
            sequence %= u32::MAX;
            sequence += 1;
            gps_array.points.push(Gps::new(sequence, 0.0, 0.0));
        }
        if let Err(e) = tx.send(gps_array).await {
            error!("{e}");
        }
        total_time += start.elapsed().as_secs_f64();
        debug!(
            "Messages: {sequence}; Avg time: {}",
            total_time / sequence as f64
        );
    }
}
