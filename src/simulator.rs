use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant as StdInstant},
};

use chrono::{DateTime, Utc};
use flume::{bounded, Sender};
use log::{debug, error, info};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rumqttc::{mqttbytes::QoS, AsyncClient};
use serde::Serialize;
use serde_json::json;
use tokio::{
    spawn,
    time::{interval, sleep},
};

use crate::{
    data::{Data, Resource},
    Config,
};

use super::data::{DeviceShadow, Payload, PayloadArray, Type};

static mut DELAYED_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut MAX_DELAY: AtomicUsize = AtomicUsize::new(0);
static mut FAILURE_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Serialize, Clone)]
pub struct StreamMetrics {
    #[serde(skip_serializing)]
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing)]
    pub sequence: u32,
    #[serde(skip_serializing)]
    metrics_tx: Sender<Payload>,
    pub stream: String,
    pub points: usize,
    pub batches: u64,
    pub max_batch_points: usize,
    #[serde(skip_serializing)]
    pub batch_start_time: StdInstant,
    #[serde(skip_serializing)]
    pub total_latency: u64,
    #[serde(skip_serializing)]
    last_flush: StdInstant,
    pub min_batch_latency: u64,
    pub max_batch_latency: u64,
    pub average_batch_latency: u64,
}

impl StreamMetrics {
    pub fn new(stream: &str, max_batch_points: usize, metrics_tx: Sender<Payload>) -> Self {
        StreamMetrics {
            metrics_tx,
            stream: stream.to_owned(),
            timestamp: Utc::now(),
            sequence: 1,
            points: 0,
            batches: 0,
            max_batch_points,
            batch_start_time: StdInstant::now(),
            last_flush: StdInstant::now(),
            total_latency: 0,
            average_batch_latency: 0,
            min_batch_latency: 0,
            max_batch_latency: 0,
        }
    }

    pub fn add_point(&mut self) {
        self.points += 1;
        if self.points == 1 {
            self.timestamp = Utc::now();
        }
    }

    pub fn add_batch(&mut self) {
        self.batches += 1;

        let latency = self.batch_start_time.elapsed().as_millis() as u64;
        self.max_batch_latency = self.max_batch_latency.max(latency);
        self.min_batch_latency = self.min_batch_latency.min(latency);
        self.total_latency += latency;
        self.average_batch_latency = self.total_latency / self.batches;
        self.batch_start_time = StdInstant::now();
    }

    pub fn try_send(&mut self) {
        if self.last_flush.elapsed() < Duration::from_secs(30) {
            return; // Don't push stats before 30s
        }
        self.timestamp = Utc::now();
        self.sequence += 1;
        let captured = self.clone();
        self.batches = 0;
        self.points = 0;
        self.batches = 0;
        self.batch_start_time = StdInstant::now();
        self.last_flush = StdInstant::now();
        self.total_latency = 0;
        self.min_batch_latency = 0;
        self.max_batch_latency = 0;
        self.average_batch_latency = 0;
        let metrics = captured.payload(Utc::now(), 0);
        if let Err(e) = self.metrics_tx.try_send(metrics) {
            unsafe {
                FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            error!("{e}; stream={}", self.stream);
        }
    }
}

impl Type for StreamMetrics {
    fn generate(_: &mut StdRng) -> Self {
        todo!()
    }

    fn payload(&self, _: DateTime<Utc>, _: u32) -> Payload {
        Payload {
            sequence: self.sequence,
            timestamp: self.timestamp,
            payload: json!(self),
        }
    }
}

async fn push_data<T: Type>(
    client: AsyncClient,
    project_id: String,
    client_id: u32,
    stream: &str,
    timeout: u64,
    randomness: bool,
    mut rng: StdRng,
    metrics_tx: Sender<Payload>,
) {
    let mut sequence = 0;
    let mut metrics = StreamMetrics::new(stream, 1, metrics_tx);
    let default_diff = Duration::from_secs(timeout);

    let topic = format!("/tenants/{project_id}/devices/{client_id}/events/{stream}/jsonarray");

    loop {
        sleep(if randomness {
            Duration::from_secs(rng.gen_range(0..timeout))
        } else {
            default_diff
        })
        .await;
        let mut push = PayloadArray::new(1, false);
        let generated = T::generate(&mut rng);
        sequence += 1;
        push.points.push(generated.payload(Utc::now(), sequence));

        metrics.add_point();
        metrics.add_batch();
        metrics.try_send();

        if let Err(e) = client.try_publish(&topic, QoS::AtMostOnce, false, push.serialized()) {
            unsafe {
                FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            error!("{e}; topic={topic}");
        }
        info!("refreshing {client_id}/{stream}");
    }
}

pub async fn single_device(client_id: u32, config: Arc<Config>, client: AsyncClient) {
    let mut rng = StdRng::from_entropy();

    // Wait a few seconds at random to deter waves
    sleep(Duration::from_secs(rng.gen::<u8>() as u64)).await;
    info!("Simulating device {client_id}");
    // PERF sending to a channel should ideally be like pushing into a buf
    let (metrics_tx, metrics_rx) = bounded(1000);

    spawn(push_data::<DeviceShadow>(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "device_shadow",
        60,
        false,
        rng.clone(),
        metrics_tx.clone(),
    ));
    spawn(push_data::<Resource>(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "resource_usage",
        60,
        true,
        rng.clone(),
        metrics_tx.clone(),
    ));

    let topic = format!(
        "/tenants/{}/devices/{client_id}/events/uplink_stream_metrics/jsonarray",
        config.project_id
    );
    let mut interval = interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        let mut array = PayloadArray {
            points: metrics_rx.drain().collect(),
            compression: false,
        };
        let client = client.clone();
        let topic = topic.clone();

        if let Err(e) =
            client.try_publish(&topic, QoS::AtLeastOnce, false, array.take().serialized())
        {
            error!("{e}; topic={topic}")
        }
    }
}

pub async fn push_simulator_metrics(topic: String, client: AsyncClient) {
    let mut interval = interval(Duration::from_secs(10));
    let mut sequence = 0;
    loop {
        interval.tick().await;
        let delayed = unsafe { DELAYED_COUNT.swap(0, Ordering::Acquire) };
        let max_delay = unsafe { MAX_DELAY.swap(0, Ordering::Acquire) };
        let failure = unsafe { FAILURE_COUNT.swap(0, Ordering::Acquire) };
        debug!("delayed: {delayed}");
        sequence += 1;
        let payload = PayloadArray {
            points: vec![Payload {
                sequence,
                timestamp: Utc::now(),
                payload: json!({
                    "delayed": delayed,
                    "max_delay": max_delay,
                    "publish_failure": failure
                }),
            }],
            compression: false,
        };

        if let Err(e) = client
            .publish(&topic, QoS::AtLeastOnce, false, payload.serialized())
            .await
        {
            error!("{e}; topic={topic}")
        };
    }
}
