/// KVS Server
use clap::{App, AppSettings, Arg};

use kvs::{KvStore, KvsServer, Result};

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
                .default_value("kvs")
                .help("KV engine name"),
        )
        .get_matches();

    // If version was requested, print it and return
    if matches.is_present("V") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let current_dir = std::env::current_dir()?;

    let current_engine = if KvStore::is_log_present(&current_dir) {
        Some("kvs")
    } else {
        None
    };

    let engine = matches.value_of("engine").unwrap();

    if let Some(curr) = current_engine {
        if curr != engine {
            println!(
                "Current engine {} does not match selected engine {}",
                curr, engine
            );
            std::process::exit(1)
        }
    }

    let engine = match engine {
        "kvs" => {
            let engine = KvStore::open(current_dir)?;
            Box::new(engine)
        }
        "sled" => {
            unimplemented!()
        }
        _ => panic!("Unexpected engine!"),
    };

    let addr = matches.value_of("addr").unwrap();

    let mut server = KvsServer::new(engine, addr.to_string())?;
    server.start()?;

    Ok(())
}
