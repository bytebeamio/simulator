use std::{collections::HashMap, env::var, fs::File, io::BufReader, sync::Arc, thread};

use log::{error, info};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions};
use serde::Deserialize;
use simulator::{push_simulator_metrics, single_device};
use tokio::{runtime::Builder, task::JoinSet};
use tracing_subscriber::EnvFilter;

mod data;
mod mqtt;
mod simulator;

use data::{
    ActionResult, Can, Historical, Imu, RideDetail, RideStatistics, RideSummary, Stop,
    VehicleLocation, VehicleState, VicRequest,
};
use mqtt::{push_mqtt_metrics, Mqtt};

#[derive(Debug, Deserialize)]
struct Auth {
    ca_certificate: String,
    device_certificate: String,
    device_private_key: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    project_id: String,
    broker: String,
    port: u16,
    authentication: Option<Auth>,
}

fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_env("RUST_LOG"))
        .try_init()
        .expect("initialized subscriber succesfully");
    let path = var("CONFIG_FILE").expect("Missing env variable");
    let rdr = BufReader::new(File::open(path).unwrap());
    let config: Config = serde_json::from_reader(rdr).unwrap();

    let config = Arc::new(config);

    let start_id = var("START")
        .expect("Missing env variable 'START'")
        .parse()
        .unwrap();
    let end_id = var("END")
        .expect("Missing env variable 'END'")
        .parse()
        .unwrap();

    let mut device_client_mapping = HashMap::new();
    let mut device_handler_mapping = HashMap::new();
    for id in start_id..=end_id {
        let mqtt = Mqtt::new(id, config.clone());
        device_client_mapping.insert(id, mqtt.client.clone());
        device_handler_mapping.insert(id, mqtt);
    }

    let mqtt_config = config.clone();
    thread::spawn(move || {
        Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("MQTT Handlers")
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut tasks = JoinSet::new();
                for (client_id, mqtt) in device_handler_mapping {
                    start_mqtt_connection(&mut tasks, client_id, mqtt_config.clone(), mqtt).await
                }

                let mut opt = MqttOptions::new("simulator", &mqtt_config.broker, mqtt_config.port);

                if let Some(authentication) = &mqtt_config.authentication {
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
                // Don't start simulation till first connack
                loop {
                    if let Ok(Event::Incoming(Incoming::ConnAck(_))) = eventloop.poll().await {
                        break;
                    }
                }

                let project_id = mqtt_config.project_id.clone();
                let mut mqtt = Mqtt {
                    eventloop,
                    client: client.clone(),
                };
                tasks.spawn(async move { mqtt.start(project_id, 1).await });
                let topic = format!(
                    "/tenants/{}/devices/1/events/simulator_mqtt_metrics/jsonarray",
                    mqtt_config.project_id
                );
                tasks.spawn(push_mqtt_metrics(topic, client.clone()));
                let topic = format!(
                    "/tenants/{}/devices/1/events/simulator_data_metrics/jsonarray",
                    mqtt_config.project_id
                );
                tasks.spawn(push_simulator_metrics(topic, client));

                while let Some(Err(e)) = tasks.join_next().await {
                    error!("{e}")
                }
            })
    });

    let mut historical = Historical::new();
    historical.load::<Can>("C2C_CAN");
    historical.load::<Imu>("imu_sensor");
    historical.load::<ActionResult>("action_result");
    historical.load::<RideDetail>("ride_detail");
    historical.load::<RideSummary>("ride_summary");
    historical.load::<RideStatistics>("ride_statistics");
    historical.load::<Stop>("stop");
    historical.load::<VehicleLocation>("vehicle_location");
    historical.load::<VehicleState>("vehicle_state");
    historical.load::<VicRequest>("vic_request");
    let data = Arc::new(historical);

    info!("Data loaded into memory");

    let simulator_cpu_count = num_cpus::get() - 4; // reserve 4 cores for mqtt
    info!("Starting simulator on {simulator_cpu_count} cpus");
    Builder::new_multi_thread()
        .worker_threads(simulator_cpu_count)
        .thread_name("Data Generators")
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut tasks: JoinSet<()> = JoinSet::new();
            for (i, client) in device_client_mapping {
                let config = config.clone();
                let data = data.clone();
                tasks.spawn(async move { single_device(i, config, client, data).await });
            }

            loop {
                if let Some(Err(e)) = tasks.join_next().await {
                    error!("{e}");
                }
            }
        });
}

async fn start_mqtt_connection(
    tasks: &mut JoinSet<()>,
    client_id: u32,
    config: Arc<Config>,
    mut mqtt: Mqtt,
) {
    let project_id = config.project_id.clone();

    // Don't start simulation till first connack
    loop {
        if let Ok(Event::Incoming(Incoming::ConnAck(_))) = mqtt.eventloop.poll().await {
            break;
        }
    }

    tasks.spawn(async move { mqtt.start(project_id, client_id).await });
}
