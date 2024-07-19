use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{TimeDelta, Utc};
use log::{debug, error, info, warn};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rumqttc::{mqttbytes::QoS, AsyncClient};
use serde_json::json;
use tokio::{
    spawn,
    time::{interval, sleep, sleep_until, Instant},
};

use crate::{data::Data, Config};

use super::data::{DeviceShadow, Historical, Payload, PayloadArray, Type};

static mut DELAYED_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut FAILURE_COUNT: AtomicUsize = AtomicUsize::new(0);

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
) {
    let mut sequence = 0;
    let mut data_array = PayloadArray {
        points: vec![],
        compression,
    };
    let mut iter = data.get_random(stream, &mut rng).iter();

    let mut topic = format!("/tenants/{project_id}/devices/{client_id}/events/{stream}/jsonarray");
    if compression {
        topic.push_str("/lz4")
    }

    loop {
        let mut start = None;
        let push = loop {
            if data_array.points.len() > max_buf_size {
                break data_array.take();
            }

            if data_array.points.is_empty() {
                start.take();
            }

            let rec: &Box<dyn Type> = match iter.next() {
                Some(r) => r,
                _ => {
                    iter = data.get_random(stream, &mut rng).iter();
                    continue;
                }
            };

            if let Some((_, ts)) = start {
                let diff: TimeDelta = rec.timestamp() - ts;
                let duration = diff.abs().to_std().unwrap();
                if duration > timeout {
                    let push = data_array.take();
                    data_array.points.push(rec.payload(sequence));
                    break push;
                }
            }

            if start.is_none() {
                start = Some((Instant::now(), rec.timestamp()));
            }

            sequence %= u32::MAX;
            sequence += 1;
            data_array.points.push(rec.payload(sequence));
        };

        if let Some((init, _)) = start {
            let till = init + timeout;
            sleep_until(till).await;
            let elapsed = Instant::now() - till;
            if elapsed > Duration::from_millis(10) {
                warn!(
                    "Slow batching: {stream} for {client_id}by {}ms",
                    elapsed.as_millis()
                );
                unsafe {
                    DELAYED_COUNT.fetch_add(1, Ordering::SeqCst);
                }
            }
        }

        if let Err(e) = client.try_publish(&topic, QoS::AtMostOnce, false, push.serialized()) {
            unsafe {
                FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            error!("{e}; topic={topic}");
        }
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
        rng.clone(),
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
    ));

    let mut sequence = 0;
    let timeout = Duration::from_secs(10);
    let mut interval = interval(timeout);
    let topic = format!(
        "/tenants/{}/devices/{client_id}/events/device_shadow/jsonarray",
        config.project_id
    );

    loop {
        let start = Instant::now();
        interval.tick().await;
        let elapsed = start.elapsed().saturating_sub(timeout);
        if elapsed > Duration::from_millis(10) {
            warn!(
                "Slow data generation: device shadow for {client_id} by {}ms",
                elapsed.as_millis()
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
