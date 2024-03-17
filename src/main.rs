use std::env::var;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use log::{debug, error};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use serde::Serialize;
use tokio::{
    spawn,
    sync::mpsc::{channel, Receiver},
    task::JoinSet,
    time::{interval, Duration},
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_env("RUST_LOG"))
        .try_init()
        .expect("initialized subscriber succesfully");

    let mut tasks: JoinSet<()> = JoinSet::new();
    for i in 0..1000 {
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
    let mut clock = interval(Duration::from_millis(100));
    loop {
        clock.tick().await;
        let mut data = vec![];
        for i in 0..10 {
            sequence %= u32::MAX;
            sequence += 1;
            data.push(Payload {
                sequence,
                timestamp: Utc::now(),
                data: i,
            });
        }
        if let Err(e) = tx
            .send((
                format!("/tenants/demo/devices/{client_id}/events/data/jsonarray"),
                data,
            ))
            .await
        {
            error!("{e}");
        }
    }
}

#[derive(Debug, Serialize)]
struct Payload {
    sequence: u32,
    #[serde(with = "ts_milliseconds")]
    timestamp: DateTime<Utc>,
    data: u32,
}

struct Serializer {
    rx: Receiver<(String, Vec<Payload>)>,
    client: AsyncClient,
}

impl Serializer {
    async fn start(&mut self) {
        while let Some((topic, data)) = self.rx.recv().await {
            let data = serde_json::to_vec(&data).unwrap();
            if let Err(e) = self
                .client
                .publish(topic, QoS::AtLeastOnce, false, data)
                .await
            {
                error!("{e}");
            }
        }
    }
}

struct Mqtt {
    eventloop: EventLoop,
}

impl Mqtt {
    async fn start(&mut self) {
        loop {
            debug!("{:?}", self.eventloop.poll().await);
        }
    }
}
