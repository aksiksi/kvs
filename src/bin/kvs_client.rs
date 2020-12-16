/// KVS Client
use clap::{App, AppSettings, Arg, SubCommand};

use kvs::Result;

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    env_logger::init();
    log::info!("Hello world!");

    let matches = App::new("kvs-client")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("KVS Client")
        .arg(Arg::with_name("V").help("Print version info"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set a key and value")
                .arg(Arg::with_name("key"))
                .arg(Arg::with_name("value"))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .default_value(DEFAULT_SERVER_ADDR)
                        .help("Server address")
                )
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get value with specified key")
                .arg(Arg::with_name("key"))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .default_value(DEFAULT_SERVER_ADDR)
                        .help("Server address")
                )
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove the the specified key")
                .arg(Arg::with_name("key"))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .default_value(DEFAULT_SERVER_ADDR)
                        .help("Server address")
                )
        )
        .get_matches();

    // If version was requested, print it and return
    if matches.is_present("V") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    Ok(())
}
