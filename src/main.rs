use std::{collections::HashMap, env::var, fs::File, io::BufReader, sync::Arc, thread};

use log::{error, info};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions};
use serde::Deserialize;
use simulator::single_device;
use tokio::{
    runtime::Builder,
    sync::mpsc::{channel, Receiver},
    task::JoinSet,
};
use tracing_subscriber::EnvFilter;

mod data;
mod mqtt;
mod serializer;
mod simulator;

use data::{
    ActionResult, Can, Data, Historical, Imu, PayloadArray, RideDetail, RideStatistics,
    RideSummary, Stop, VehicleLocation, VehicleState, VicRequest,
};
use mqtt::{push_mqtt_metrics, Mqtt};
use serializer::{push_serializer_metrics, Serializer};

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

    let mut device_rx_mapping = HashMap::new();
    let mut device_tx_mapping = HashMap::new();
    for id in start_id..=end_id {
        let (tx, rx) = channel(1);
        device_tx_mapping.insert(id, tx);
        device_rx_mapping.insert(id, rx);
    }

    let mqtt_config = config.clone();
    thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut tasks = JoinSet::new();
                for (client_id, rx) in device_rx_mapping {
                    start_mqtt_connection(&mut tasks, client_id, mqtt_config.clone(), rx).await
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
                    "/tenants/{}/devices/1/events/simulator_serializer_metrics/jsonarray",
                    mqtt_config.project_id
                );
                tasks.spawn(push_serializer_metrics(topic, client.clone()));
                let topic = format!(
                    "/tenants/{}/devices/1/events/simulator_mqtt_metrics/jsonarray",
                    mqtt_config.project_id
                );
                tasks.spawn(push_mqtt_metrics(topic, client.clone()));
                let topic = format!(
                    "/tenants/{}/devices/1/events/simulator_data_metrics/jsonarray",
                    mqtt_config.project_id
                );
                tasks.spawn(push_mqtt_metrics(topic, client));

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

    let simulator_cpu_count = num_cpus::get() - 1; // reserve one core for mqtt
    info!("Starting simulator on {simulator_cpu_count} cpus");
    Builder::new_multi_thread()
        .worker_threads(simulator_cpu_count)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut tasks: JoinSet<()> = JoinSet::new();
            for (i, tx) in device_tx_mapping {
                let config = config.clone();
                let data = data.clone();
                tasks.spawn(async move { single_device(i, config, tx, data).await });
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
    rx: Receiver<PayloadArray>,
) {
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
    // Don't start simulation till first connack
    loop {
        if let Ok(Event::Incoming(Incoming::ConnAck(_))) = eventloop.poll().await {
            break;
        }
    }

    let project_id = config.project_id.clone();
    let mut mqtt = Mqtt {
        eventloop,
        client: client.clone(),
    };
    tasks.spawn(async move { mqtt.start(project_id, client_id).await });

    let mut serializer = Serializer { rx, client };
    tasks.spawn(async move { serializer.start(client_id).await });
}
