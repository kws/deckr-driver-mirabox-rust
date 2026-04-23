use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Parser)]
#[command(name = "deckr-mirabox-manager")]
#[command(about = "MiraBox hardware bridge client")]
pub struct Args {
    #[arg(
        long,
        env = "DECKR_BRIDGE_URL",
        help = "Bridge URI, for example ws://127.0.0.1:9876/hardware"
    )]
    pub bridge_url: String,
    #[arg(long, env = "DEVICE_MANAGER_ID")]
    pub manager_id: String,
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

impl Args {
    pub fn parse_and_init() -> Result<Self> {
        let args = Self::parse();
        let filter = EnvFilter::try_new(&args.log_level)
            .with_context(|| format!("invalid log filter {}", args.log_level))?;
        tracing_subscriber::fmt().with_env_filter(filter).init();
        Ok(args)
    }
}
