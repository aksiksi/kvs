/// KVS CLI
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvStore, KvsEngine, Result};

fn main() -> Result<()> {
    let matches = App::new("KVS")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("KVS CLI")
        .arg(Arg::with_name("V").help("Print version info"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set a key and value")
                .arg(Arg::with_name("key"))
                .arg(Arg::with_name("value")),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get value with specified key")
                .arg(Arg::with_name("key")),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove the the specified key")
                .arg(Arg::with_name("key")),
        )
        .get_matches();

    // If version was requested, print it and return
    if matches.is_present("V") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let mut store = KvStore::open(std::env::current_dir()?)?;

    match matches.subcommand() {
        ("set", sub_match) => {
            let key = sub_match.unwrap().value_of("key").unwrap().to_owned();
            let value = sub_match.unwrap().value_of("value").unwrap().to_owned();
            store.set(key, value)?;
        }
        ("get", sub_match) => {
            let key = sub_match.unwrap().value_of("key").unwrap().to_owned();
            match store.get(key)? {
                None => println!("Key not found"),
                Some(value) => println!("{}", value),
            }
        }
        ("rm", sub_match) => {
            let key = sub_match.unwrap().value_of("key").unwrap().to_owned();
            match store.remove(key) {
                Err(kvs::Error::KeyNotFound) => {
                    println!("Key not found");
                    std::process::exit(1);
                }
                Err(e) => {
                    // Abort on any other error
                    return Err(e);
                }
                Ok(_) => (),
            }
        }
        (_, _) => {
            panic!("Unexpected subcommand");
        }
    }

    Ok(())
}
