// Copyright 2017-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

//! Work Generated and gathered from the PostgreSQL Database
//! IE: Missing Blocks/Storage/Inherents/Transactions
//! Gathers Missing blocks -> passes to metadata -> passes to extractors -> passes to decode -> passes to insert

use crate::actors::{
    scheduler::{Algorithm, Scheduler},
    workers,
};
use crate::{
    backend::ChainAccess,
    error::Error as ArchiveError,
    queries,
    types::{NotSignedBlock, Substrate, System},
};
use bastion::prelude::*;
use sp_runtime::generic::BlockId;
use sqlx::PgConnection;
use std::sync::Arc;

pub fn actor<T, C>(
    client: Arc<C>,
    pool: sqlx::Pool<PgConnection>,
    url: String,
) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock<T>> + 'static,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Header: serde::de::DeserializeOwned,
{
    let meta_workers = workers::metadata::<T>(url, pool.clone())?;
    // generate work from missing blocks
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let client = client.clone();
            let pool = pool.clone();
            let workers = meta_workers.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                sched.add_worker("meta", &workers);
                loop {
                    if handle_shutdown(&ctx).await {
                        break;
                    }
                    match entry::<T, _>(&client, &pool, &mut sched).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                }
                Bastion::stop();
                Ok(())
            }
        })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate database generator"))
}

async fn entry<T, C>(
    client: &Arc<C>,
    pool: &sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock<T>> + 'static,
{
    let block_nums = queries::missing_blocks(&pool).await?;
    log::info!("missing {} blocks", block_nums.len());
    if !(block_nums.len() > 0) {
        timer::Delay::new(std::time::Duration::from_secs(5)).await;
        return Ok(());
    }
    log::info!(
        "Starting to crawl for {} missing blocks, from {} to {} ...",
        block_nums.len(),
        block_nums[0].generate_series,
        block_nums[block_nums.len() - 1].generate_series
    );
    let mut blocks = Vec::new();
    for block_num in block_nums.iter() {
        let num = block_num.generate_series as u32;
        let b = client.block(&BlockId::Number(T::BlockNumber::from(num)))?;
        if b.is_none() {
            log::warn!("Block does not exist!")
        } else {
            blocks.push(b.expect("Checked for none; qed"));
        }
    }
    log::info!("Got {} blocks", blocks.len());
    let answer = sched.ask_next("meta", blocks)?.await;
    log::debug!("{:?}", answer);
    Ok(())
}

// Handle a shutdown
async fn handle_shutdown(ctx: &BastionContext) -> bool {
    if let Some(msg) = ctx.try_recv().await {
        msg! {
            msg,
            broadcast: super::Broadcast => {
                match broadcast {
                    super::Broadcast::Shutdown => {
                        return true;
                    }
                }
            };
            e: _ => log::warn!("Received unknown message: {:?}", e);
        };
    }
    false
}