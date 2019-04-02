//#![deny(warnings)]

#[macro_use]
mod time;

mod config;
mod constants;
mod convert;
mod crypto;
mod ictarus;
mod ixi;
mod model;
mod network;

use pretty_env_logger;
use std::env;
use std::sync::{Arc, RwLock};

use crate::config::Config;
use crate::constants::*;
use crate::ictarus::Ictarus;

fn main() -> Result<(), Box<std::error::Error>> {
    env::set_var(APP_ENV_VAR, DEBUG_LEVEL);
    pretty_env_logger::init_custom_env(APP_ENV_VAR);

    let mut ictarus = Ictarus::new(Arc::new(RwLock::new(Config::from_file(CONFIG_FILEPATH))));

    ictarus.run()?;
    ictarus.wait_for_kill_signal();

    env::remove_var(APP_ENV_VAR);

    Ok(())
}
