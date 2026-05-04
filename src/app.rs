use anyhow::Result;

use crate::cli::Args;
use crate::manager::MiraBoxRemoteManager;

pub async fn run(args: Args) -> Result<()> {
    let manager = MiraBoxRemoteManager::new(
        args.nats_url,
        args.lease_state_bucket,
        args.discovery_state_bucket,
        args.manager_id,
    )?;
    manager.run().await
}
