use log::{debug, error};
use rumqttc::EventLoop;
use tokio::time::Instant;

pub struct Mqtt {
    pub eventloop: EventLoop,
}

impl Mqtt {
    pub async fn start(&mut self) {
        let mut success = 0;
        let mut failure = 0;
        loop {
            let start = Instant::now();
            match self.eventloop.poll().await {
                Ok(m) => {
                    debug!("{m:?}");
                    success += 1;
                }
                Err(e) => {
                    error!("{e}");
                    failure += 1;
                }
            };
            debug!(
                "timespent: {}; success = {success}; failure = {failure}",
                start.elapsed().as_secs_f64()
            );
        }
    }
}
