use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Utc;
use log::{debug, error};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Publish, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    spawn,
    time::{interval, sleep},
};

static mut SUCCESS_COUNT: AtomicUsize = AtomicUsize::new(0);
static mut FAILURE_COUNT: AtomicUsize = AtomicUsize::new(0);

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
        let (client, mut eventloop) = AsyncClient::new(opt, 1);
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
                    if let Event::Incoming(Incoming::Publish(Publish { payload, .. })) = &m {
                        let client = self.client.clone();
                        let action: Action = serde_json::from_slice(payload).unwrap();
                        let action_id = action.action_id.parse().unwrap();
                        let topic =
                            format!("/tenants/{project_id}/devices/{client_id}/action/status");
                        spawn(async move {
                            for sequence in 1..=10 {
                                let response_array = PayloadArray {
                                    points: vec![ActionResponse::as_payload(sequence, action_id)],
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
