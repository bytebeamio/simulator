use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Utc;
use log::{debug, error};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Outgoing, Publish, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    spawn,
    time::{interval, sleep},
};

static mut PUBLISH_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut PUBACK_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut ERROR_COUNT: AtomicUsize = AtomicUsize::new(0);

use crate::{
    data::{ActionResponse, Data, Payload, PayloadArray},
    Config,
};

#[derive(Debug, Deserialize)]
pub struct Action {
    // action id
    #[serde(alias = "id")]
    pub action_id: String,
}

// #[derive(Debug, Clone)]
// pub struct ActionResult {
//     action_response: String,
//     r#type: String,
//     user_id: u32,
//     request_id: String,
// }

pub struct Mqtt {
    pub eventloop: EventLoop,
    pub client: AsyncClient,
}

impl Mqtt {
    pub fn new(client_id: u32, config: Arc<Config>) -> Self {
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
        let (client, mut eventloop) = AsyncClient::new(opt, 100);
        eventloop.network_options.set_connection_timeout(30);

        Self { client, eventloop }
    }

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
                    match &m {
                        Event::Incoming(Incoming::Publish(Publish { payload, .. })) => {
                            let client = self.client.clone();
                            let action: Action = serde_json::from_slice(payload).unwrap();
                            let action_id = action.action_id.parse().unwrap();
                            let topic =
                                format!("/tenants/{project_id}/devices/{client_id}/action/status");
                            spawn(async move {
                                for sequence in 1..=10 {
                                    let response_array = PayloadArray {
                                        points: vec![ActionResponse::as_payload(
                                            sequence, action_id,
                                        )],
                                        compression: true,
                                    };
                                    let payload = response_array.serialized();
                                    if let Err(e) =
                                        client.try_publish(&topic, QoS::AtLeastOnce, false, payload)
                                    {
                                        error!("{client_id}: {e}")
                                    }
                                    sleep(Duration::from_secs(1)).await;
                                }
                            });
                        }
                        Event::Outgoing(Outgoing::Publish(_)) => unsafe {
                            PUBLISH_COUNT.fetch_add(1, Ordering::SeqCst);
                        },
                        Event::Incoming(Incoming::PubAck(_)) => unsafe {
                            PUBACK_COUNT.fetch_add(1, Ordering::SeqCst);
                        },
                        _ => {}
                    }
                    debug!("client_id: {client_id}; {m:?}");
                }
                Err(e) => {
                    error!("client_id: {client_id}; {e}");
                    unsafe {
                        ERROR_COUNT.fetch_add(1, Ordering::SeqCst);
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
        let errors = unsafe { ERROR_COUNT.swap(0, Ordering::Acquire) };
        let publishes = unsafe { PUBLISH_COUNT.swap(0, Ordering::Acquire) };
        let pubacks = unsafe { PUBACK_COUNT.swap(0, Ordering::Acquire) };
        debug!("errors: {errors}, publishes: {publishes}, pubacks: {pubacks}");
        sequence += 1;
        let payload = PayloadArray {
            points: vec![Payload {
                sequence,
                timestamp: Utc::now(),
                payload: json!({
                    "errors": errors,
                    "publishes": publishes,
                    "pubacks": pubacks,
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
