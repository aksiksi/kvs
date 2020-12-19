/// KVS Server
use clap::{App, AppSettings, Arg};

use kvs::engine::{KvsEngine, SledKvsEngine};
use kvs::{server::KvsServer, KvStore, Result};

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let matches = App::new("kvs-server")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::GlobalVersion)
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("KVS Server")
        .arg(Arg::with_name("V").short("V").help("Print version info"))
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

    log::info!("Version: {}", env!("CARGO_PKG_VERSION"));

    let current_dir = std::env::current_dir()?;

    // Figure out which engine is currently in used based on presence of relevant
    // log in the current directory
    let current_engine = if KvStore::is_log_present(&current_dir) {
        Some("kvs")
    } else if SledKvsEngine::is_log_present(&current_dir) {
        Some("sled")
    } else {
        None
    };

    let engine = match matches.value_of("engine") {
        None => {
            // If no engine was provided, use the detected engine, or default to "kvs"
            current_engine.unwrap_or("kvs")
        }
        Some(engine) => engine,
    };

    // If the user provided an engine that does not match current engine, error out
    if let Some(curr) = current_engine {
        if curr != engine {
            println!(
                "Current engine {} does not match selected engine {}",
                curr, engine
            );
            std::process::exit(1)
        }
    }

    log::info!("Engine: {}", engine);

    // Setup the appropriate engine
    let engine: Box<dyn KvsEngine> = match engine {
        "kvs" => {
            let engine = KvStore::open(current_dir)?;
            Box::new(engine)
        }
        "sled" => {
            let engine = SledKvsEngine::open(current_dir)?;
            Box::new(engine)
        }
        _ => panic!("Unexpected engine!"),
    };

    let addr = matches.value_of("addr").unwrap();

    log::info!("Address: {}", addr);

    let mut server = KvsServer::new(engine, addr.to_string())?;
    server.start()?;

    Ok(())
}
