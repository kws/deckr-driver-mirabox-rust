use deckr_mirabox_manager::app;
use deckr_mirabox_manager::cli::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse_and_init()?;
    app::run(args).await
}
