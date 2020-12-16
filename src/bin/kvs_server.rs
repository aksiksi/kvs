/// KVS Server
use clap::{App, AppSettings, Arg};

use kvs::Result;

fn main() -> Result<()> {
    env_logger::init();
    log::info!("Hello world!");

    let matches = App::new("kvs-server")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("KVS Server")
        .arg(Arg::with_name("V").help("Print version info"))
        .arg(
            Arg::with_name("addr")
                .long("addr")
                .value_name("IP-PORT")
                .help("IPv4/IPv6 in address:port format")
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::with_name("engine")
                .long("engine")
                .value_name("ENGINE-NAME")
                .possible_values(&["kvs", "sled"])
                .help("KV engine name"),
        )
        .get_matches();

    // If version was requested, print it and return
    if matches.is_present("V") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    Ok(())
}
