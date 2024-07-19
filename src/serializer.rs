use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use chrono::Utc;
use flume::Receiver;
use log::{debug, error};
use rumqttc::{AsyncClient, QoS};
use serde_json::json;
use tokio::time::interval;

use crate::data::{Payload, PayloadArray};

use super::Data;

pub struct Serializer {
    pub rx: Receiver<PayloadArray>,
    pub client: AsyncClient,
}

static mut SUCCESS_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut FAILURE_COUNT: AtomicUsize = AtomicUsize::new(0);

impl Serializer {
    pub async fn start(&mut self, client_id: u32) {
        while let Ok(d) = self.rx.recv_async().await {
            match self
                .client
                .try_publish(d.topic(), QoS::AtLeastOnce, false, d.serialized())
            {
                Ok(_) => unsafe {
                    SUCCESS_COUNT.fetch_add(1, Ordering::SeqCst);
                },
                Err(e) => {
                    error!("{client_id}: {e}; topic={}", d.topic());
                    unsafe {
                        FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }
    }
}

pub async fn push_serializer_metrics(topic: String, client: AsyncClient) {
    let mut interval = interval(Duration::from_secs(10));
    let mut sequence = 0;
    loop {
        interval.tick().await;
        let failure = unsafe { FAILURE_COUNT.swap(0, Ordering::Acquire) };
        let success = unsafe { SUCCESS_COUNT.swap(0, Ordering::Acquire) };
        debug!("failure: {}, success: {}", failure, success);
        sequence += 1;
        let payload = PayloadArray {
            topic: topic.clone(),
            points: vec![Payload {
                sequence,
                timestamp: Utc::now(),
                payload: json!({
                    "success": success,
                    "failure": failure
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
