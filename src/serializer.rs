use log::{debug, error};
use rumqttc::{AsyncClient, QoS};
use tokio::{sync::mpsc::Receiver, time::Instant};

use super::Data;

pub struct Serializer<D: Data> {
    pub rx: Receiver<D>,
    pub client: AsyncClient,
}

impl<D: Data> Serializer<D> {
    pub async fn start(&mut self) {
        while let Some(d) = self.rx.recv().await {
            let start = Instant::now();
            if let Err(e) = self
                .client
                .publish(d.topic(), QoS::AtLeastOnce, false, d.serialized())
                .await
            {
                error!("{e}");
            }
            debug!("timespent: {}", start.elapsed().as_secs_f64());
        }
    }
}
