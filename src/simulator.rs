use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant as StdInstant},
};

use chrono::{DateTime, TimeDelta, Utc};
use flume::{bounded, Sender};
use log::{debug, error, info, warn};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rumqttc::{mqttbytes::QoS, AsyncClient};
use serde::Serialize;
use serde_json::json;
use tokio::{
    spawn,
    time::{interval, sleep, sleep_until, Instant},
};

use crate::{data::Data, Config};

use super::data::{DeviceShadow, Historical, Payload, PayloadArray, Type};

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
    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    fn payload(&self, _: DateTime<Utc>, _: u32) -> Payload {
        Payload {
            sequence: self.sequence,
            timestamp: self.timestamp,
            payload: json!(self),
        }
    }
}

async fn push_data(
    client: AsyncClient,
    project_id: String,
    client_id: u32,
    stream: &str,
    max_buf_size: usize,
    timeout: Duration,
    compression: bool,
    data: Arc<Historical>,
    mut rng: StdRng,
    (refresh_low, refresh_high): (u64, u64),
    metrics_tx: Sender<Payload>,
) {
    let mut sequence = 0;
    let mut data_array = PayloadArray::new(max_buf_size, compression);
    let mut metrics = StreamMetrics::new(stream, max_buf_size, metrics_tx);

    let mut topic = format!("/tenants/{project_id}/devices/{client_id}/events/{stream}/jsonarray");
    if compression {
        topic.push_str("/lz4")
    }

    loop {
        // Some data streams need not see much data
        let refresh_time = Duration::from_secs(rng.gen_range(refresh_low..refresh_high));
        sleep(refresh_time).await;
        let mut iter = data.get_random(stream, &mut rng).iter();
        let mut first_time = Utc::now();
        let mut start = None;
        'refresh: loop {
            let mut last_time = Duration::ZERO;

            let (push, till) = loop {
                if data_array.points.len() >= max_buf_size {
                    break (data_array.take(), start.map(|(init, _)| init + last_time));
                }

                let Some(rec) = iter.next() else {
                    if data_array.points.is_empty() {
                        break 'refresh;
                    }
                    break (data_array.take(), start.map(|(init, _)| init + last_time));
                };

                sequence %= u32::MAX;
                sequence += 1;
                if let Some((init, ts)) = start {
                    let diff: TimeDelta = rec.timestamp() - ts;
                    let duration = diff.abs().to_std().unwrap();
                    let mut push = None;
                    if duration > timeout {
                        push = Some((data_array.take(), Some(init + duration)));
                    } else {
                        last_time = duration;
                    }
                    data_array
                        .points
                        .push(rec.payload(first_time + diff, sequence));
                    if let Some(push) = push {
                        break push;
                    }
                } else {
                    first_time = Utc::now();
                    data_array.points.push(rec.payload(first_time, sequence));
                    start = Some((Instant::now(), rec.timestamp()));
                    last_time = Duration::ZERO;
                }

                metrics.add_point();
            };

            if let Some(till) = till {
                sleep_until(till).await;
                let elapsed = Instant::now() - till;
                if elapsed > Duration::from_millis(10) {
                    let delay = elapsed.as_millis() as usize;
                    warn!("Slow batching: {stream} for {client_id} by {}ms", delay);
                    unsafe {
                        DELAYED_COUNT.fetch_add(1, Ordering::SeqCst);
                        MAX_DELAY.fetch_max(delay, Ordering::SeqCst);
                    }
                }
            }

            metrics.add_batch();
            metrics.try_send();

            if let Err(e) = client.try_publish(&topic, QoS::AtMostOnce, false, push.serialized()) {
                unsafe {
                    FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
                }
                error!("{e}; topic={topic}");
            }
        }
        info!("refreshing {client_id}/{stream}");
    }
}

pub async fn single_device(
    client_id: u32,
    config: Arc<Config>,
    client: AsyncClient,
    data: Arc<Historical>,
) {
    let mut rng = StdRng::from_entropy();

    // Wait a few seconds at random to deter waves
    sleep(Duration::from_secs(rng.gen::<u8>() as u64)).await;
    info!("Simulating device {client_id}");
    // PERF sending to a channel should ideally be like pushing into a buf
    let (metrics_tx, metrics_rx) = bounded(1000);

    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "C2C_CAN",
        100,
        Duration::from_secs(60),
        true,
        data.clone(),
        rng.clone(),
        (0, 1),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "imu_sensor",
        100,
        Duration::from_secs(60),
        true,
        data.clone(),
        rng.clone(),
        (0, 10),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "ride_detail",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
        rng.clone(),
        (10_000, 10_000_000),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "ride_summary",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
        rng.clone(),
        (10_000, 10_000_000),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "ride_statistics",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
        rng.clone(),
        (10_000, 10_000_000),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "stop",
        10,
        Duration::from_secs(10),
        false,
        data.clone(),
        rng.clone(),
        (100, 10_000),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "vehicle_location",
        10,
        Duration::from_secs(10),
        false,
        data.clone(),
        rng.clone(),
        (100, 10_000),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "vehicle_state",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
        rng.clone(),
        (100, 10_000),
        metrics_tx.clone(),
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "vic_request",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
        rng.clone(),
        (100, 10_000),
        metrics_tx.clone(),
    ));
    spawn(push_device_shadow(
        client.clone(),
        config.project_id.clone(),
        client_id,
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

async fn push_device_shadow(
    client: AsyncClient,
    project_id: String,
    client_id: u32,
    metrics_tx: Sender<Payload>,
) {
    let mut sequence = 0;
    let timeout = Duration::from_secs(10);
    let mut interval = interval(timeout);
    let topic = format!("/tenants/{project_id}/devices/{client_id}/events/device_shadow/jsonarray");
    let mut metrics = StreamMetrics::new("device_shadow", 1, metrics_tx);

    loop {
        let start = Instant::now();
        interval.tick().await;
        let elapsed = start.elapsed().saturating_sub(timeout);
        if elapsed > Duration::from_millis(10) {
            let delay = elapsed.as_millis() as usize;
            warn!(
                "Slow batching: device_shadow for {client_id} by {}ms",
                delay
            );
            unsafe {
                DELAYED_COUNT.fetch_add(1, Ordering::SeqCst);
                MAX_DELAY.fetch_max(delay, Ordering::SeqCst);
            }
        }

        sequence += 1;
        let data_array = PayloadArray {
            points: vec![DeviceShadow.payload(Utc::now(), sequence)],
            compression: false,
        };

        let client = client.clone();
        let topic = topic.clone();
        metrics.add_point();
        metrics.add_batch();
        metrics.try_send();
        if let Err(e) = client.try_publish(&topic, QoS::AtMostOnce, false, data_array.serialized())
        {
            unsafe {
                FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            error!("{client_id}/device_shadow: {e}");
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
