use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use chrono::Utc;
use log::{debug, error};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, Publish, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    spawn,
    time::{interval, sleep},
};

static mut SUCCESS_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut FAILURE_COUNT: AtomicUsize = AtomicUsize::new(0);

use crate::data::{ActionResponse, Data, Payload, PayloadArray};

#[derive(Debug, Deserialize)]
pub struct Action {
    // action id
    #[serde(alias = "id")]
    pub action_id: String,
}

pub struct Mqtt {
    pub eventloop: EventLoop,
    pub client: AsyncClient,
}

impl Mqtt {
    pub async fn start(&mut self, project_id: String, client_id: u32) {
        self.client
            .subscribe(
                format!("/tenants/demo/devices/{client_id}/actions"),
                QoS::AtMostOnce,
            )
            .await
            .unwrap();

        loop {
            match self.eventloop.poll().await {
                Ok(m) => {
                    if let Event::Incoming(Incoming::Publish(Publish { payload, .. })) = &m {
                        let client = self.client.clone();
                        let action: Action = serde_json::from_slice(payload).unwrap();
                        let action_id = action.action_id.parse().unwrap();
                        let topic =
                            format!("/tenants/{project_id}/devices/{client_id}/action/status");
                        spawn(async move {
                            for sequence in 1..=10 {
                                let response_array = PayloadArray {
                                    topic: topic.clone(),
                                    points: vec![ActionResponse::as_payload(sequence, action_id)],
                                    compression: true,
                                };
                                let payload = response_array.serialized();
                                if let Err(e) = client.try_publish(
                                    &response_array.topic,
                                    QoS::AtLeastOnce,
                                    false,
                                    payload,
                                ) {
                                    error!("{client_id}: {e}")
                                }
                                sleep(Duration::from_secs(1)).await;
                            }
                        });
                    }
                    debug!("client_id: {client_id}; {m:?}");
                    unsafe {
                        SUCCESS_COUNT.fetch_add(1, Ordering::SeqCst);
                    }
                }
                Err(e) => {
                    error!("client_id: {client_id}; {e}");
                    unsafe {
                        FAILURE_COUNT.fetch_add(1, Ordering::SeqCst);
                    }
                    sleep(Duration::from_secs(10)).await;
                }
            };
        }
    }
}

pub async fn push_mqtt_metrics(topic: String, client: AsyncClient) {
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
