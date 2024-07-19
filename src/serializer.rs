use log::error;
use rumqttc::{AsyncClient, QoS};
use tokio::sync::mpsc::Receiver;

use crate::data::PayloadArray;

use super::Data;

pub struct Serializer {
    pub rx: Receiver<PayloadArray>,
    pub client: AsyncClient,
}

impl Serializer {
    pub async fn start(&mut self, client_id: u32) {
        while let Some(d) = self.rx.recv().await {
            if let Err(e) =
                self.client
                    .try_publish(d.topic(), QoS::AtLeastOnce, false, d.serialized())
            {
                error!("{client_id}: {e}; topic={}", d.topic());
            }
        }
    }
}
