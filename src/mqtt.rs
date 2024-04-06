use log::{debug, error};
use rumqttc::EventLoop;
use tokio::time::Instant;

pub struct Mqtt {
    pub eventloop: EventLoop,
}

impl Mqtt {
    pub async fn start(&mut self, client_id: u32) {
        let mut success = 0;
        let mut failure = 0;
        loop {
            let start = Instant::now();
            match self.eventloop.poll().await {
                Ok(m) => {
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
