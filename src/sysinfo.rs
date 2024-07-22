use chrono::{DateTime, Utc};
use log::error;
use rumqttc::{mqttbytes::QoS, AsyncClient};
use serde::Serialize;
use serde_json::json;
use sysinfo::{Components, Disks, NetworkData, Networks, System};
use tokio::time::{interval, Instant};

use std::{collections::HashMap, time::Duration};

use crate::data::{Data, Payload, PayloadArray, Type};

type Pid = u32;

#[derive(Debug, Default, Clone)]
pub struct SysInfo {
    kernel_version: String,
    uptime: u64,
    no_processes: usize,
    /// Average load within one minute.
    load_avg_one: f64,
    /// Average load within five minutes.
    load_avg_five: f64,
    /// Average load within fifteen minutes.
    load_avg_fifteen: f64,
    total_memory: u64,
    available_memory: u64,
    used_memory: u64,
}

impl SysInfo {
    fn init(sys: &System) -> SysInfo {
        SysInfo {
            kernel_version: System::kernel_version().unwrap_or_default(),
            total_memory: sys.total_memory(),
            ..Default::default()
        }
    }

    fn update(&mut self, sys: &sysinfo::System) {
        self.uptime = System::uptime();
        self.no_processes = sys.processes().len();
        let sysinfo::LoadAvg { one, five, fifteen } = System::load_average();
        self.load_avg_one = one;
        self.load_avg_five = five;
        self.load_avg_fifteen = fifteen;
        self.available_memory = sys.available_memory();
        self.used_memory = self.total_memory - self.available_memory;
    }
}

impl Type for SysInfo {
    fn timestamp(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        let SysInfo {
            kernel_version,
            uptime,
            no_processes,
            load_avg_one,
            load_avg_five,
            load_avg_fifteen,
            total_memory,
            available_memory,
            used_memory,
            ..
        } = self;

        Payload {
            sequence,
            timestamp,
            payload: json!({
                "kernel_version": kernel_version,
                "uptime": uptime,
                "no_processes": no_processes,
                "load_avg_one": load_avg_one,
                "load_avg_five": load_avg_five,
                "load_avg_fifteen": load_avg_fifteen,
                "total_memory": total_memory,
                "available_memory": available_memory,
                "used_memory": used_memory,
            }),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
struct Network {
    name: String,
    incoming_data_rate: f64,
    outgoing_data_rate: f64,
    #[serde(skip_serializing)]
    timer: Instant,
}

impl Network {
    fn init(name: String) -> Self {
        Network {
            name,
            incoming_data_rate: 0.0,
            outgoing_data_rate: 0.0,
            timer: Instant::now(),
        }
    }

    /// Update metrics values for network usage over time
    fn update(&mut self, data: &NetworkData) {
        let update_period = self.timer.elapsed().as_secs_f64();
        self.timer = Instant::now();
        self.incoming_data_rate = data.received() as f64 / update_period;
        self.outgoing_data_rate = data.transmitted() as f64 / update_period;
    }
}

impl Type for Network {
    fn timestamp(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        let Network {
            name,
            incoming_data_rate,
            outgoing_data_rate,
            ..
        } = self;

        Payload {
            sequence,
            timestamp,
            payload: json!({
                "name": name,
                "incoming_data_rate": incoming_data_rate,
                "outgoing_data_rate": outgoing_data_rate,
            }),
        }
    }
}

struct NetworkStats {
    sequence: u32,
    networks: Networks,
    map: HashMap<String, Network>,
}

impl NetworkStats {
    fn capture(&mut self, timestamp: DateTime<Utc>) -> PayloadArray {
        let mut array = PayloadArray::new(10, false);
        for (net_name, net_data) in self.networks.list() {
            self.sequence += 1;
            let net = self
                .map
                .entry(net_name.clone())
                .or_insert_with(|| Network::init(net_name.to_owned()));
            net.update(net_data);

            self.sequence += 1;
            let network = net.payload(timestamp, self.sequence);
            array.points.push(network);
        }

        array
    }
}

#[derive(Debug, Serialize, Default, Clone)]
struct Disk {
    name: String,
    total: u64,
    available: u64,
    used: u64,
}

impl Disk {
    fn init(name: String, disk: &sysinfo::Disk) -> Self {
        Disk {
            name,
            total: disk.total_space(),
            ..Default::default()
        }
    }

    fn update(&mut self, disk: &sysinfo::Disk) {
        self.total = disk.total_space();
        self.available = disk.available_space();
        self.used = self.total - self.available;
    }
}

impl Type for Disk {
    fn timestamp(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        let Disk {
            name,
            total,
            available,
            used,
        } = self;

        Payload {
            sequence,
            timestamp,
            payload: json!({
                "name": name,
                "total": total,
                "available": available,
                "used": used,
            }),
        }
    }
}

struct DiskStats {
    sequence: u32,
    disks: Disks,
    map: HashMap<String, Disk>,
}

impl DiskStats {
    fn capture(&mut self, timestamp: DateTime<Utc>) -> PayloadArray {
        let mut array = PayloadArray::new(10, false);
        for disk_data in self.disks.list() {
            self.sequence += 1;
            let disk_name = disk_data.name().to_string_lossy().to_string();
            let disk = self
                .map
                .entry(disk_name.clone())
                .or_insert_with(|| Disk::init(disk_name, disk_data));
            disk.update(disk_data);
            let disk = disk.payload(timestamp, self.sequence);
            array.points.push(disk);
        }

        array
    }
}

#[derive(Debug, Default, Clone)]
struct Processor {
    name: String,
    frequency: u64,
    usage: f32,
}

impl Processor {
    fn init(name: String) -> Self {
        Processor {
            name,
            ..Default::default()
        }
    }

    fn update(&mut self, proc: &sysinfo::Cpu) {
        self.frequency = proc.frequency();
        self.usage = proc.cpu_usage();
    }
}

impl Type for Processor {
    fn timestamp(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        let Processor {
            name,
            frequency,
            usage,
        } = self;

        Payload {
            sequence,
            timestamp,
            payload: json!({
                "name": name,
                "frequency": frequency,
                "usage": usage,
            }),
        }
    }
}

struct ProcessorStats {
    sequence: u32,
    map: HashMap<String, Processor>,
}

impl ProcessorStats {
    fn capture(&mut self, proc_data: &sysinfo::Cpu, timestamp: DateTime<Utc>) -> Payload {
        let proc_name = proc_data.name().to_string();
        self.sequence += 1;
        let proc = self
            .map
            .entry(proc_name.clone())
            .or_insert_with(|| Processor::init(proc_name));
        proc.update(proc_data);
        proc.payload(timestamp, self.sequence)
    }
}

#[derive(Debug, Default, Clone)]
struct Component {
    label: String,
    temperature: f32,
}

impl Component {
    fn init(label: String) -> Self {
        Component {
            label,
            ..Default::default()
        }
    }

    fn update(&mut self, comp: &sysinfo::Component) {
        self.temperature = comp.temperature();
    }
}

impl Type for Component {
    fn timestamp(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        let Component { label, temperature } = self;

        Payload {
            sequence,
            timestamp,
            payload: json!({
                "label": label, "temperature": temperature
            }),
        }
    }
}

struct ComponentStats {
    sequence: u32,
    components: Components,
    map: HashMap<String, Component>,
}

impl ComponentStats {
    fn capture(&mut self, timestamp: DateTime<Utc>) -> PayloadArray {
        let cap = self.components.len();
        let mut array = PayloadArray::new(cap, false);
        for component in self.components.list() {
            let comp_label = component.label().to_string();
            self.sequence += 1;
            let comp = self
                .map
                .entry(comp_label.clone())
                .or_insert_with(|| Component::init(comp_label));
            comp.update(component);

            let component = comp.payload(timestamp, self.sequence);
            array.points.push(component);
        }

        array
    }
}

#[derive(Debug, Default, Clone)]
struct Process {
    pid: Pid,
    name: String,
    cpu_usage: f32,
    mem_usage: u64,
    disk_total_written_bytes: u64,
    disk_written_bytes: u64,
    disk_total_read_bytes: u64,
    disk_read_bytes: u64,
    start_time: u64,
}

impl Process {
    fn init(pid: Pid, name: String, start_time: u64) -> Self {
        Process {
            pid,
            name,
            start_time,
            ..Default::default()
        }
    }

    fn update(&mut self, proc: &sysinfo::Process) {
        let sysinfo::DiskUsage {
            total_written_bytes,
            written_bytes,
            total_read_bytes,
            read_bytes,
        } = proc.disk_usage();
        self.disk_total_written_bytes = total_written_bytes;
        self.disk_written_bytes = written_bytes;
        self.disk_total_read_bytes = total_read_bytes;
        self.disk_read_bytes = read_bytes;
        self.cpu_usage = proc.cpu_usage();
        self.mem_usage = proc.memory();
    }
}

impl Type for Process {
    fn timestamp(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn payload(&self, timestamp: DateTime<Utc>, sequence: u32) -> Payload {
        let Process {
            pid,
            name,
            cpu_usage,
            mem_usage,
            disk_total_written_bytes,
            disk_written_bytes,
            disk_total_read_bytes,
            disk_read_bytes,
            start_time,
        } = self;

        Payload {
            sequence,
            timestamp,
            payload: json!({
                "pid": pid,
                "name": name,
                "cpu_usage": cpu_usage,
                "mem_usage": mem_usage,
                "disk_total_written_bytes": disk_total_written_bytes,
                "disk_written_bytes": disk_written_bytes,
                "disk_total_read_bytes": disk_total_read_bytes,
                "disk_read_bytes": disk_read_bytes,
                "start_time": start_time,
            }),
        }
    }
}

struct ProcessStats {
    sequence: u32,
    map: HashMap<Pid, Process>,
}

impl ProcessStats {
    fn push(
        &mut self,
        id: Pid,
        proc_data: &sysinfo::Process,
        name: String,
        timestamp: DateTime<Utc>,
    ) -> Payload {
        self.sequence += 1;
        let proc = self
            .map
            .entry(id)
            .or_insert_with(|| Process::init(id, name, proc_data.start_time()));
        proc.update(proc_data);

        proc.payload(timestamp, self.sequence)
    }
}

/// Collects and forward system information such as kernel version, memory and disk space usage,
/// information regarding running processes, network and processor usage, etc to an IoT platform.
pub struct StatCollector {
    /// Handle to sysinfo struct containing system information.
    sys: sysinfo::System,
    system_stats_topic: String,
    /// System information values to be serialized.
    system: SysInfo,
    /// Information about running processes.
    processes: ProcessStats,
    process_stats_topic: String,
    /// Individual Processor information.
    processors: ProcessorStats,
    processor_stats_topic: String,
    /// Information regarding individual Network interfaces.
    networks: NetworkStats,
    network_stats_topic: String,
    /// Information regarding individual Disks.
    disks: DiskStats,
    disk_stats_topic: String,
    /// Temperature information from individual components.
    components: ComponentStats,
    component_stats_topic: String,
}

impl StatCollector {
    /// Create and initialize a stat collector
    pub fn new(project_id: &str, client_id: u32) -> Self {
        let mut sys = System::new();
        sys.refresh_all();

        let system = SysInfo::init(&sys);
        let system_stats_topic = format!(
            "/tenants/{project_id}/devices/{client_id}/events/uplink_system_stats/jsonarray"
        );

        let mut map = HashMap::new();
        let disks = Disks::new_with_refreshed_list();
        for disk_data in disks.list() {
            let disk_name = disk_data.name().to_string_lossy().to_string();
            map.insert(disk_name.clone(), Disk::init(disk_name, disk_data));
        }
        let disks = DiskStats {
            sequence: 0,
            disks,
            map,
        };
        let disk_stats_topic =
            format!("/tenants/{project_id}/devices/{client_id}/events/uplink_disk_stats/jsonarray");

        let mut map = HashMap::new();
        let networks = Networks::new_with_refreshed_list();
        for net_name in networks.list().keys() {
            map.insert(net_name.to_owned(), Network::init(net_name.to_owned()));
        }
        let networks = NetworkStats {
            networks,
            sequence: 0,
            map,
        };
        let network_stats_topic = format!(
            "/tenants/{project_id}/devices/{client_id}/events/uplink_network_stats/jsonarray"
        );

        let mut map = HashMap::new();
        for proc in sys.cpus().iter() {
            let proc_name = proc.name().to_owned();
            map.insert(proc_name.clone(), Processor::init(proc_name));
        }
        let processors = ProcessorStats { sequence: 0, map };
        let processor_stats_topic = format!(
            "/tenants/{project_id}/devices/{client_id}/events/uplink_processor_stats/jsonarray"
        );

        let components = ComponentStats {
            sequence: 0,
            components: Components::new_with_refreshed_list(),
            map: HashMap::new(),
        };
        let component_stats_topic = format!(
            "/tenants/{project_id}/devices/{client_id}/events/uplink_component_stats/jsonarray"
        );

        let processes = ProcessStats {
            sequence: 0,
            map: HashMap::new(),
        };
        let process_stats_topic = format!(
            "/tenants/{project_id}/devices/{client_id}/events/uplink_process_stats/jsonarray"
        );

        StatCollector {
            sys,
            system_stats_topic,
            system,
            processes,
            process_stats_topic,
            disks,
            disk_stats_topic,
            networks,
            network_stats_topic,
            processors,
            processor_stats_topic,
            components,
            component_stats_topic,
        }
    }

    /// Update system information values and increment sequence numbers, while sending to specific data streams.
    pub async fn start(mut self, client: AsyncClient) {
        let mut clock = interval(Duration::from_secs(60));
        let mut sequence = 0;

        loop {
            clock.tick().await;
            let timestamp = Utc::now();
            self.system.update(&self.sys);
            sequence += 1;
            let array = PayloadArray {
                points: vec![self.system.payload(timestamp, sequence)],
                compression: false,
            };
            if let Err(e) = client.try_publish(
                &self.system_stats_topic,
                QoS::AtMostOnce,
                false,
                array.serialized(),
            ) {
                error!("uplink_system_stats: {e}");
            }

            let disks = self.disks.capture(timestamp);
            if let Err(e) = client.try_publish(
                &self.disk_stats_topic,
                QoS::AtMostOnce,
                false,
                disks.serialized(),
            ) {
                error!("uplink_disk_stats: {e}");
            }

            let networks = self.networks.capture(timestamp);
            if let Err(e) = client.try_publish(
                &self.network_stats_topic,
                QoS::AtMostOnce,
                false,
                networks.serialized(),
            ) {
                error!("uplink_network_stats: {e}");
            }

            let processors = self.sys.cpus();
            let mut array = PayloadArray::new(processors.len(), false);
            for proc_data in processors {
                let payload = self.processors.capture(proc_data, timestamp);
                array.points.push(payload);
            }
            if let Err(e) = client.try_publish(
                &self.processor_stats_topic,
                QoS::AtMostOnce,
                false,
                array.serialized(),
            ) {
                error!("uplink_processor_stats: {e}");
            }

            let components = self.components.capture(timestamp);
            if let Err(e) = client.try_publish(
                &self.component_stats_topic,
                QoS::AtMostOnce,
                false,
                components.serialized(),
            ) {
                error!("uplink_component_stats: {e}");
            }

            let processes = self.sys.processes();
            let mut array = PayloadArray::new(processes.len(), false);
            for (&id, p) in processes {
                let name = p
                    .cmd()
                    .first()
                    .map(|s| s.to_string())
                    .unwrap_or(p.name().to_string());

                let payload = self.processes.push(id.as_u32(), p, name, timestamp);
                array.points.push(payload);
            }
            if let Err(e) = client.try_publish(
                &self.process_stats_topic,
                QoS::AtMostOnce,
                false,
                array.serialized(),
            ) {
                error!("uplink_process_stats: {e}");
            }
        }
    }
}
