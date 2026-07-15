use std::env;
use log::{Log, Metadata, Record};
use env_filter::{Builder, Filter};
use tokio::sync::broadcast;

pub struct WsLogger {
    pub tx: broadcast::Sender<String>,
    pub filter: Filter,
}

impl WsLogger {
    pub fn new(tx: tokio::sync::broadcast::Sender<String>) -> Self {
        let mut builder = Builder::new();

        // Parse the RUST_LOG environment variable using the new parser
        if let Ok(rust_log) = env::var("RUST_LOG") {
            builder.parse(&rust_log);
        } else {
            builder.parse("info"); // Default
        }

        Self {
            tx,
            filter: builder.build(),
        }
    }
}


impl Log for WsLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.filter.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if self.filter.matches(record) {
            // 1. Format the core message
            let msg = format!("[{}] {}", record.level(), record.args());

            // 2. Print to the Linux Terminal (so you can see it locally)
            println!("{}", msg);

            // 3. Send to the Web UI (with the \r\n needed for the web terminal)
            let _ = self.tx.send(format!("{}\r\n", msg));
        }
    }

    fn flush(&self) {}
}