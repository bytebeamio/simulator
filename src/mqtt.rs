use std::time::Duration;

use log::{debug, error};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, Publish, QoS};
use serde::Deserialize;
use tokio::{
    spawn,
    time::{sleep, Instant},
};

use crate::data::{ActionResponse, Data, PayloadArray};

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
    pub async fn start(&mut self, client_id: u32) {
        let mut success = 0;
        let mut failure = 0;
        loop {
            let start = Instant::now();
            match self.eventloop.poll().await {
                Ok(m) => {
                    if let Event::Incoming(Incoming::Publish(Publish { payload, .. })) = &m {
                        let client = self.client.clone();
                        let action: Action = serde_json::from_slice(&payload).unwrap();
                        let action_id = action.action_id.parse().unwrap();
                        spawn(async move {
                            for sequence in 0..10 {
                                let response_array = PayloadArray {
                                    topic: format!(
                                        "/tenants/demo/devices/{client_id}/action_status"
                                    ),
                                    points: vec![ActionResponse::new(sequence, action_id)],
                                    compression: true,
                                };
                                let payload = response_array.serialized();
                                client
                                    .publish(
                                        &response_array.topic,
                                        QoS::AtLeastOnce,
                                        false,
                                        payload,
                                    )
                                    .await
                                    .unwrap();
                                sleep(Duration::from_secs(1)).await;
                            }
                        });
                    }
                    debug!("client_id: {client_id}; {m:?}");
                    success += 1;
                }
                Err(e) => {
                    error!("client_id: {client_id}; {e}");
                    failure += 1;
                }
            };
            debug!(
                "client_id: {client_id}; timespent: {}; success = {success}; failure = {failure}",
                start.elapsed().as_secs_f64()
            );
        }
    }
}
