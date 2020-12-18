/// KVS Client
use clap::{App, AppSettings, Arg, SubCommand};

use kvs::{client::KvsClient, Error, Result};

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    env_logger::init();

    let matches = App::new("kvs-client")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::GlobalVersion)
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("KVS Client")
        .arg(Arg::with_name("V").short("V").help("Print version info"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set a key and value")
                .arg(Arg::with_name("key"))
                .arg(Arg::with_name("value"))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .help("Server address"),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get value with specified key")
                .arg(Arg::with_name("key"))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .help("Server address"),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove the the specified key")
                .arg(Arg::with_name("key"))
                .arg(
                    Arg::with_name("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .help("Server address"),
                ),
        )
        .get_matches();

    // If version was requested, print it and return
    if matches.is_present("V") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    match matches.subcommand() {
        ("set", sub_match) => {
            let addr = sub_match
                .unwrap()
                .value_of("addr")
                .unwrap_or(DEFAULT_SERVER_ADDR);
            let mut client = KvsClient::connect(addr)?;
            let key = sub_match.unwrap().value_of("key").unwrap().to_owned();
            let value = sub_match.unwrap().value_of("value").unwrap().to_owned();
            client.set(key, value)?;
        }
        ("get", sub_match) => {
            let addr = sub_match
                .unwrap()
                .value_of("addr")
                .unwrap_or(DEFAULT_SERVER_ADDR);
            let mut client = KvsClient::connect(addr)?;
            let key = sub_match.unwrap().value_of("key").unwrap().to_owned();
            let value = client.get(key);

            match value {
                Ok(value) => {
                    if let Some(value) = value {
                        println!("{}", value);
                    } else {
                        println!("Key not found");
                    }
                }
                Err(Error::KeyNotFound) => {
                    println!("Key not found");
                    std::process::exit(1);
                }
                Err(e) => return Err(e),
            }
        }
        ("rm", sub_match) => {
            let addr = sub_match
                .unwrap()
                .value_of("addr")
                .unwrap_or(DEFAULT_SERVER_ADDR);
            let mut client = KvsClient::connect(addr)?;
            let key = sub_match.unwrap().value_of("key").unwrap().to_owned();
            match client.remove(key) {
                Ok(_) => (),
                Err(Error::KeyNotFound) => {
                    // NOTE: tests check for stderr
                    eprintln!("Key not found");
                    std::process::exit(1);
                }
                Err(e) => return Err(e),
            }
        }
        (s, _) => {
            panic!("Unexpected subcommand: \"{}\"", s);
        }
    }

    Ok(())
}
