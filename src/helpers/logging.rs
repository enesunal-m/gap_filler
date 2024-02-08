use std::path::Path;

use log::LevelFilter;
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
    Config,
};
use std::fs;

pub fn setup_logging() -> Result<(), Box<dyn std::error::Error>> {
    let pattern = "{d(%Y-%m-%d %H:%M:%S)} [{t}] {l} - {m}\n";

    ensure_log_directory_exists("log")?;

    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build("log/output.log")?;

    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(LevelFilter::Info))?;

    log4rs::init_config(config)?;

    Ok(())
}

pub fn ensure_log_directory_exists(log_directory: &str) -> Result<(), std::io::Error> {
    let path = Path::new(log_directory);
    if !path.exists() {
        // Create the directory and any necessary parent directories.
        fs::create_dir_all(path)?;
    }
    Ok(())
}
