// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use arc_swap::ArcSwap;
use async_std::task;
use itertools::Itertools;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use xtra::prelude::*;

use desub::Decoder;

use crate::{
	actors::{
		workers::database::{DatabaseActor, GetState},
		SystemConfig,
	},
	database::{queries, DecodedStorageModel},
	error::{ArchiveError, Result},
	types::BatchDecodedStorage,
};

/// Actor which crawls missing encoded extrinsics and
/// sends decoded JSON to the database.
/// Crawls missing extrinsics upon receiving an `Index` message.
pub struct StorageDecoder {
	/// Pool of Postgres Connections.
	pool: PgPool,
	/// Address of the database actor.
	addr: Address<DatabaseActor>,
	/// Max amount of extrinsics to load at any one time.i.
	max_block_load: u32,
	/// Desub Legacy + current decoder.
	decoder: Arc<Decoder>,
	/// Cache of blocks where runtime upgrades occurred.
	/// number -> spec
	upgrades: ArcSwap<HashMap<u32, u32>>,
}

impl StorageDecoder {
	#[allow(unused)]
	pub async fn new<B: Send + Sync, Db: Send + Sync>(
		config: &SystemConfig<B, Db>,
		addr: Address<DatabaseActor>,
	) -> Result<Self> {
		let max_block_load = config.control.max_block_load;
		let chain = config.persistent_config.chain();
		let pool = addr.send(GetState::Pool).await??.pool();
		let decoder = Arc::new(Decoder::new(chain));
		let mut conn = pool.acquire().await?;
		let upgrades = ArcSwap::from_pointee(queries::upgrade_blocks_from_spec(&mut conn, 0).await?);
		log::info!("Started storage decoder");
		Ok(Self { pool, addr, max_block_load, decoder, upgrades })
	}

	async fn crawl_missing_decoded_storage(&mut self) -> Result<()> {
		let mut conn = self.pool.acquire().await?;
		let storage_entries = queries::storage_missing_decoded_storage(&mut conn, self.max_block_load).await?;
		log::debug!("Crawling {} missing decoded storage entries", storage_entries.len());
		let versions: Vec<u32> = storage_entries
			.iter()
			.filter(|b| !self.decoder.has_version(&b.5))
			.map(|(_, _, _, _, _, v)| *v)
			.unique()
			.collect();
		// above and below line are separate to let immutable ref to `self.decoder` to go out of scope.
		for version in versions.iter() {
			let metadata = queries::metadata(&mut conn, *version as i32).await?;
			log::debug!("Registering version {}", version);
			Arc::get_mut(&mut self.decoder)
				.ok_or_else(|| ArchiveError::Msg("Reference to decoder is not safe to access".into()))?
				.register_version(*version, &metadata)?;
		}

		if let Some(first) = versions.first() {
			if let (Some(past), _, Some(past_metadata), _) =
				queries::past_and_present_version(&mut conn, *first as i32).await?
			{
				Arc::get_mut(&mut self.decoder)
					.ok_or_else(|| ArchiveError::Msg("Reference to decoder is not safe to access".into()))?
					.register_version(past, &past_metadata)?;
				log::debug!("Registered previous version {}", past);
			}
		}

		if self.upgrades.load().iter().max_by(|a, b| a.1.cmp(b.1)).map(|(_, v)| v)
			< storage_entries.iter().map(|&(_, _, _, _, _, v)| v).max().as_ref()
		{
			self.update_upgrade_blocks().await?;
		}
		let decoder = self.decoder.clone();
		let upgrades = self.upgrades.load().clone();
		let decoded_entries =
			task::spawn_blocking(move || Ok::<_, ArchiveError>(Self::decode(&decoder, storage_entries, &upgrades)))
				.await??;
		if decoded_entries.is_empty() {
			return Ok(());
		}
		log::info!("Decoded {} entries", decoded_entries.len());
		self.addr.send(BatchDecodedStorage::new(decoded_entries)).await?;
		Ok(())
	}

	fn decode(
		decoder: &Decoder,
		storage_entries: Vec<(i32, Vec<u8>, u32, Vec<u8>, Option<Vec<u8>>, u32)>,
		upgrades: &HashMap<u32, u32>,
	) -> Result<Vec<DecodedStorageModel>> {
		let mut decoded_entries = Vec::new();
		if storage_entries.len() > 2 {
			let first = storage_entries.first().expect("Checked len; qed");
			let last = storage_entries.last().expect("Checked len; qed");
			log::info!(
				"Decoding {} storage entries in blocks {}..{} versions {}..{}",
				storage_entries.len(),
				first.2,
				last.2,
				first.5,
				last.5
			);
		}
		for (raw_storage_id, hash, number, key, value, spec) in storage_entries.into_iter() {
			let version = if let Some(version) = upgrades.get(&number) {
				let previous = upgrades
					.values()
					.sorted()
					.tuple_windows()
					.find(|(_curr, next)| *next >= version)
					.map(|(c, _)| c)
					.ok_or(ArchiveError::PrevSpecNotFound(*version))?;
				*previous
			} else {
				spec
			};
			let storage = decoder.decode_storage(version, key.as_slice(), value.as_deref())?;
			decoded_entries.push(DecodedStorageModel::new(raw_storage_id, hash, number, storage));
		}
		Ok(decoded_entries)
	}

	async fn update_upgrade_blocks(&self) -> Result<()> {
		let max_spec = *self.upgrades.load().iter().max_by(|a, b| a.1.cmp(b.1)).map(|(k, _)| k).unwrap_or(&0);
		let mut conn = self.pool.acquire().await?;
		let upgrade_blocks = queries::upgrade_blocks_from_spec(&mut conn, max_spec).await?;
		self.upgrades.rcu(move |upgrades| {
			let mut upgrades = HashMap::clone(upgrades);
			upgrades.extend(upgrade_blocks.iter());
			upgrades
		});
		Ok(())
	}
}

#[async_trait::async_trait]
impl Actor for StorageDecoder {}

pub struct Index;
impl Message for Index {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<Index> for StorageDecoder {
	async fn handle(&mut self, _: Index, ctx: &mut Context<Self>) {
		match self.crawl_missing_decoded_storage().await {
			Err(ArchiveError::Disconnected) => ctx.stop(),
			Ok(_) => {}
			Err(e) => log::error!("{:?}", e),
		}
	}
}
