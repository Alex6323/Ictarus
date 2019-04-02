use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, RwLock};

use crate::constants::*;
use log::*;

/// A config that can be shared across tasks.
pub type SharedConfig = Arc<RwLock<Config>>;

/// Node configuration.
///
/// You can use the `ConfigBuilder` to conveniently create one.
#[derive(Clone, Debug)]
pub struct Config {
    pub min_forward_delay: u64,
    pub max_forward_delay: u64,
    pub host: String,
    pub port: u16,
    pub round_duration: u64,
    pub neighbors: Vec<SocketAddr>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            min_forward_delay: 0,
            max_forward_delay: 3,
            host: String::from("127.0.0.1"),
            port: 1337,
            round_duration: 60000,
            neighbors: vec![],
        }
    }
}

impl Config {
    /// Sets the port of the node.
    ///
    /// NOTE: This method doesn't check the validity of the port provided yet.
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    /// Sets the host address of the node.
    ///
    /// NOTE: This method doesn't check the validity of the host provided yet.
    pub fn set_host(&mut self, host: &str) {
        //value.to_socket_addrs().unwrap().nth(0).unwrap().to_string();
        self.host = host.into();
    }

    /// Adds a neighbor to the node.
    ///
    /// This method will panic, if one attempts to add more neighbors than allowed,
    /// or if the provided address doesn't resolve to a valid socket address.
    pub fn add_neighbor(&mut self, address: &str) {
        if self.neighbors.len() >= MAX_NEIGHBOR_COUNT {
            panic!("error: cannot add more neighbors");
        }

        self.neighbors.push(
            address
                .to_socket_addrs()
                .expect("Couldn't resolve provided address")
                .nth(0)
                .unwrap(),
        );

        #[cfg(debug_assertions)]
        println!(
            "{} > Added {} as neighbor to config ({}).",
            self.port,
            address,
            self.neighbors.len()
        );
    }

    /// Returns the socket address of the node.
    ///
    /// This method will panic, if the host and port fields don't resolve
    /// to a valid socket address.
    pub fn get_socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .to_socket_addrs()
            .unwrap()
            .nth(0)
            .unwrap()
    }

    /// Creates a config from the specified file.
    pub fn from_file(file: &str) -> Self {
        let buffered = BufReader::new(File::open(file).expect("File does not exist."));
        let mut config = Config::default();

        buffered
            .lines()
            .filter_map(|line| line.ok())
            .for_each(|line| {
                let parts = line.split('=').collect::<Vec<&str>>();
                match parts[0] {
                    MIN_FORWARD_DELAY_PARAM => {
                        config.min_forward_delay = parts[1]
                            .parse::<u64>()
                            .expect("error: couldn't parse min-forward-delay");
                    }
                    MAX_FORWARD_DELAY_PARAM => {
                        config.max_forward_delay = parts[1]
                            .parse::<u64>()
                            .expect("error: couldn't parse max-forward-delay");
                    }
                    PORT_PARAM => {
                        config.port = parts[1].parse::<u16>().expect("error: couldn't parse port");
                    }
                    HOST_PARAM => {
                        config.host = parts[1].into();
                    }
                    ROUND_DURATION_PARAM => {
                        config.round_duration = parts[1]
                            .parse::<u64>()
                            .expect("error: couldn't parse round-duration-ms");
                    }
                    NEIGHBORS_PARAM => {
                        let addresses = parts[1].split(',').collect::<Vec<&str>>();
                        if addresses.is_empty() {
                            panic!("error: no neighbors specified");
                        } else if addresses.len() > MAX_NEIGHBOR_COUNT {
                            panic!("error: too many neighbors specified");
                        }
                        for address in addresses.iter() {
                            config
                                .neighbors
                                .push(address.to_socket_addrs().unwrap().nth(0).unwrap());
                            /*
                            config.neighbors.push(
                                address
                                    .parse::<SocketAddr>()
                                    .expect("couldn't parse socket address"),
                            );
                            */
                        }
                    }
                    _ => {
                        warn!("unknown property '{}'", &parts[0]);
                    }
                }
            });
        config
    }
}

/// A convenience type for creating node configs via method chaining.
///
/// Example:
/// `let config = ConfigBuilder:default().host("localhost").port("1337").build();`
pub struct ConfigBuilder {
    config: Config,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        //let config = Config::default();
        ConfigBuilder {
            config: Config::default(),
        }
    }
}

impl ConfigBuilder {
    pub fn min_forward_delay(mut self, min_forward_delay: u64) -> Self {
        self.config.min_forward_delay = min_forward_delay;
        self
    }

    pub fn max_forward_delay(mut self, max_forward_delay: u64) -> Self {
        self.config.max_forward_delay = max_forward_delay;
        self
    }
    pub fn host(mut self, host: &str) -> Self {
        self.config.host = host.to_string();
        self
    }
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }
    pub fn round_duration(mut self, round_duration: u64) -> Self {
        self.config.round_duration = round_duration;
        self
    }
    pub fn neighbor(mut self, address: &str) -> Self {
        self.config.neighbors.push(
            address
                .to_socket_addrs()
                .expect("couldn't parse given value to a socket address")
                .nth(0)
                .unwrap(),
        );
        self
    }
    pub fn build(self) -> Config {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::*;
    use std::io::Write;

    /// This test creates a default config.
    ///
    /// The test is passed, if all fields hold their expected default values.
    #[test]
    fn test_create_default_config() {
        let config = Config::default();

        assert_eq!(0, config.min_forward_delay);
        assert_eq!(3, config.max_forward_delay);
        assert_eq!(0, config.neighbors.len());
        assert_eq!(1337, config.port);
        assert_eq!("127.0.0.1", config.host);
        assert_eq!(60000, config.round_duration);
    }

    /// This test creates a Config from a test file.
    ///
    /// The test is passed, if all fields hold the values as specified in the file.
    #[test]
    fn test_config_from_file() {
        let mut file = File::create("ictarus.cfg").expect("couldn't create file");
        write!(file, "min_forward_delay=0\nmax_forward_delay=10\nport=14265\nhost=127.0.0.1\nround_duration=60000\nneighbors=127.0.0.1:14266,127.0.0.1:14267,127.0.0.1:14268").expect("couldn't write file");

        let config = Config::from_file("./ictarus.cfg");

        assert_eq!(0, config.min_forward_delay);
        assert_eq!(10, config.max_forward_delay);
        assert_eq!(3, config.neighbors.len());
        assert_eq!(14265, config.port);
        assert_eq!("127.0.0.1", config.host);
        assert_eq!(60000, config.round_duration);

        remove_file("./ictarus.cfg").expect("couldn't delete file");
    }

    /// This test creates a Config by using the ConfigBuilder type.
    ///
    /// The test is passed, if all fields hold the values as specified by the builder.
    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::default()
            .host("127.0.0.2")
            .port(6666)
            .neighbor("127.0.0.3:1337")
            .build();

        assert_eq!(config.host, "127.0.0.2");
        assert_eq!(config.port, 6666);
        assert_eq!(1, config.neighbors.len());

        println!("{}", config.neighbors.get(0).unwrap());
    }

    /// This test creates a Config by using the ConfigBuilder type.
    ///
    /// The test is passed, if the domain name 'localhost' is resolved to
    /// the corresponding ip address '127.0.0.1'.
    #[test]
    fn test_domain_name_resolution() {
        let config = ConfigBuilder::default()
            .host("localhost")
            .port(1337)
            .build();

        assert_eq!(
            "127.0.0.1:1337".to_string(),
            config.get_socket_addr().to_string()
        );
    }
}
