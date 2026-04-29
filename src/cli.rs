use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Parser)]
#[command(name = "deckr-mirabox-manager")]
#[command(about = "MiraBox hardware manager for Deckr over NATS")]
pub struct Args {
    #[arg(
        long,
        env = "DECKR_NATS_URL",
        default_value = "nats://127.0.0.1:4222",
        help = "NATS server URL"
    )]
    pub nats_url: String,
    #[arg(
        long,
        env = "DECKR_STATE_BUCKET",
        default_value = "deckr_state_v1",
        help = "Deckr JetStream KV current-state bucket"
    )]
    pub state_bucket: String,
    #[arg(long, env = "DECKR_MANAGER_ID")]
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
