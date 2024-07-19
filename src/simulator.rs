use std::{
    env::current_dir,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{TimeDelta, Utc};
use flume::{bounded, Receiver};
use log::{debug, error, info, warn};
use rand::{rngs::StdRng, SeedableRng};
use rumqttc::{mqttbytes::QoS, AsyncClient};
use serde_json::json;
use tokio::{
    select, spawn,
    time::{interval, sleep, sleep_until, Instant, Sleep},
};

use crate::{data::Data, Config};

use super::data::{DeviceShadow, Historical, Payload, PayloadArray, Type};

static mut DELAYED_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut FAILURE_COUNT: AtomicUsize = AtomicUsize::new(0);

async fn batch_data(
    rx: Receiver<Payload>,
    client: AsyncClient,
    topic: String,
    max_buf_size: usize,
    timeout: Duration,
    compression: bool,
) {
    let mut data_array = PayloadArray {
        points: vec![],
        compression,
    };
    let mut end: Pin<Box<Sleep>> = Box::pin(sleep(Duration::from_secs(u64::MAX)));
    let mut push = None;
    loop {
        select! {
            Ok(payload) = rx.recv_async() => {
                if data_array.points.is_empty() {
                    push = Some( Box::pin(sleep(timeout)))
                }
                data_array.points.push(payload);
                if data_array.points.len() < max_buf_size {
                    continue
                }
            }
            _ = &mut push.as_mut().unwrap_or(&mut end) => {
                let wait = push.take().unwrap();
                let elapsed = wait.deadline().elapsed();
                if elapsed > Duration::from_millis(500) {
                    warn!("Waited {}s longer than expected to batch for {topic}", elapsed.as_secs_f32());
                    unsafe {
                        DELAYED_COUNT.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }

        if let Err(e) = client.try_publish(
            &topic,
            QoS::AtMostOnce,
            false,
            data_array.take().serialized(),
        ) {
            unsafe {
                FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            error!("{e}; topic={topic}");
        }
        data_array.points.clear();
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
) {
    let mut last_time = None;
    let mut sequence = 0;
    let mut total_time = 0.0;

    let mut data_path = current_dir().unwrap();
    data_path.push(format!("data/{stream}"));

    let mut rng = StdRng::from_entropy();
    let mut iter = data.get_random(stream, &mut rng).iter();
    let mut start = Instant::now();

    let (tx, rx) = bounded(0);
    let mut topic = format!("/tenants/{project_id}/devices/{client_id}/events/{stream}/jsonarray");
    if compression {
        topic.push_str("/lz4")
    }
    spawn(batch_data(
        rx,
        client,
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
            if elapsed > Duration::from_millis(500) {
                warn!(
                    "Waited longer than expected to generate {stream} for {client_id}by {}s",
                    elapsed.as_secs_f32()
                );
                unsafe {
                    DELAYED_COUNT.fetch_add(1, Ordering::SeqCst);
                }
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

pub async fn single_device(
    client_id: u32,
    config: Arc<Config>,
    client: AsyncClient,
    data: Arc<Historical>,
) {
    info!("Simulating device {client_id}");

    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "C2C_CAN",
        100,
        Duration::from_secs(60),
        true,
        data.clone(),
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
    ));
    spawn(push_data(
        client.clone(),
        config.project_id.clone(),
        client_id,
        "action_result",
        1,
        Duration::from_secs(1),
        false,
        data.clone(),
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
    ));

    let mut sequence = 0;
    let mut interval = interval(Duration::from_secs(10));
    let topic = format!(
        "/tenants/{}/devices/{client_id}/events/device_shadow/jsonarray",
        config.project_id
    );

    loop {
        let start = Instant::now();
        interval.tick().await;
        let elapsed = start.elapsed();
        if elapsed > Duration::from_millis(1500) {
            warn!(
                "Waited longer than expected to generate device shadow for {client_id} by {}s",
                elapsed.as_secs_f32()
            );
            unsafe {
                DELAYED_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        let data_array = PayloadArray {
            points: vec![DeviceShadow::default().payload(sequence)],
            compression: false,
        };
        sequence += 1;
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
        let failure = unsafe { FAILURE_COUNT.swap(0, Ordering::Acquire) };
        debug!("delayed: {delayed}");
        sequence += 1;
        let payload = PayloadArray {
            points: vec![Payload {
                sequence,
                timestamp: Utc::now(),
                payload: json!({
                    "delayed": delayed,
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
