use clap::{App, AppSettings, Arg, SubCommand};

fn main() {
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
        return;
    }

    match matches.subcommand() {
        ("set", sub_match) => {
            let _key = sub_match.unwrap().value_of("key").unwrap();
            let _value = sub_match.unwrap().value_of("value").unwrap();
            unimplemented!()
        }
        ("get", sub_match) => {
            let _key = sub_match.unwrap().value_of("key").unwrap();
            unimplemented!()
        }
        ("rm", sub_match) => {
            let _key = sub_match.unwrap().value_of("key").unwrap();
            unimplemented!()
        }
        (_, _) => {
            panic!("Unexpected subcommand");
        }
    }
}
