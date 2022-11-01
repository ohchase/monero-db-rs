// Copyright (c) 2022 Boog900

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

use lmdb::{Cursor, Database, Environment, EnvironmentFlags, Transaction};
use monero::consensus::{deserialize, serialize, Decodable, Encodable};
use monero::database_types::block::{AltBlock, BlockHeight, BlockInfo};
use monero::database_types::transaction::{
    OutTx, PreRctOutkey, RctOutkey, TransactionPruned, TxIndex, TxOutputIdx, TxPoolMeta,
};
use monero::{Block, Hash};
use std::fmt::Debug;
use std::path::Path;

use super::sub_db::MoneroSubDB;
use super::{Error, ZERO_KEY};

/// Output of a retrieval from a database
///
#[derive(Debug)]
pub enum Parse<T: Decodable + Encodable + Debug> {
    No(Vec<u8>),
    Yes(T),
}

impl<T: Decodable + Encodable + Debug> Parse<T> {
    pub fn deserialize(self) -> Result<T, Error> {
        match self {
            Parse::No(data) => Ok(deserialize(&data)?),
            Parse::Yes(data) => Ok(data),
        }
    }

    pub fn serialize(self) -> Vec<u8> {
        match self {
            Parse::No(data) => data,
            Parse::Yes(data) => serialize(&data),
        }
    }
}

/// Struct containing the data needed to interact with a
/// Monero database
///
pub struct MoneroDB {
    env: Environment,
    sub_dbs: MoneroSubDB,
    read_only: bool,
}

impl MoneroDB {
    /// Opens the Monero database
    ///
    pub fn open(dir: &Path, read_only: bool) -> Result<Self, Error> {
        let mut env = Environment::new();
        let mut flags = EnvironmentFlags::NO_READAHEAD;
        if read_only {
            flags |= EnvironmentFlags::READ_ONLY
        }
        env.set_max_dbs(32)
            .set_map_size(1 << 30)
            .set_max_readers(126)
            .set_flags(flags);
        let env = env.open(dir)?;
        env.check_do_resize()?;
        let sub_dbs = MoneroSubDB::new(&env)?;
        Ok(MoneroDB {
            env,
            sub_dbs,
            read_only,
        })
    }

    /// Gets alternative block from database.
    ///
    pub fn get_alt_block(&self, block_hash: &[u8], parse: bool) -> Result<Parse<AltBlock>, Error> {
        if block_hash.len() != 32 {
            return Err(Error::ValueError(
                "Block hash should be 32 bytes long".to_string(),
            ));
        }
        get_item(
            &self.env,
            self.sub_dbs.alt_blocks,
            block_hash,
            &[0],
            15,
            parse,
        )
    }

    /// Gets block from database.
    ///
    pub fn get_block(&self, block_height: u64, parse: bool) -> Result<Parse<Block>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.blocks,
            &block_height.to_le_bytes(),
            &[0],
            15,
            parse,
        )
    }

    /// Gets block info from database
    ///
    pub fn get_block_info(
        &self,
        block_height: u64,
        parse: bool,
    ) -> Result<Parse<BlockInfo>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.block_info,
            &ZERO_KEY,
            &block_height.to_be_bytes(),
            2,
            parse,
        )
    }

    /// Gets block height from database
    ///
    pub fn get_block_height(
        &self,
        block_hash: &[u8],
        parse: bool,
    ) -> Result<Parse<BlockHeight>, Error> {
        if block_hash.len() != 32 {
            return Err(Error::ValueError(
                "Block hash should be 32 bytes long".to_string(),
            ));
        }
        get_item(
            &self.env,
            self.sub_dbs.block_heights,
            &ZERO_KEY,
            block_hash,
            2,
            parse,
        )
    }

    /// Get the height of the blockchain (1 + height of max block)
    ///
    pub fn get_blockchain_height(&self) -> Result<u64, Error> {
        let transaction = self.env.begin_ro_txn()?;
        let stats = transaction.stat(self.sub_dbs.block_heights)?;
        Ok(stats.entries() as u64)
    }

    /// Gets the blocks hard fork version
    ///
    pub fn get_hf_version(&self, block_height: u64) -> Result<u8, Error> {
        get_item::<u8>(
            &self.env,
            self.sub_dbs.hf_versions,
            &block_height.to_le_bytes(),
            &[0],
            15,
            true,
        )?
        .deserialize()
    }

    /// Gets the pruned part of the transaction
    ///
    pub fn get_tx_pruned(
        &self,
        txn_id: u64,
        parse: bool,
    ) -> Result<Parse<TransactionPruned>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.txs_pruned,
            &txn_id.to_le_bytes(),
            &[0],
            15,
            parse,
        )
    }

    /// Gets the prunable part of the transaction
    ///
    pub fn get_tx_prunable(&self, txn_id: u64) -> Result<Vec<u8>, Error> {
        // This will always return bytes and won't attempt to de-serialize
        // that is why it has a random type(Block)
        Ok(get_item::<Block>(
            &self.env,
            self.sub_dbs.txs_prunable,
            &txn_id.to_le_bytes(),
            &[0],
            15,
            false,
        )?
        .serialize())
    }

    /// Gets the [`Outkey`] of a transactions output
    ///
    pub fn get_output_rct_outkey(
        &self,
        amount: u64,
        amount_output_index: u64,
        parse: bool,
    ) -> Result<Parse<RctOutkey>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.output_amounts,
            &amount.to_le_bytes(),
            &amount_output_index.to_le_bytes(),
            2,
            parse,
        )
    }

    /// Gets the [`PreRctOutkey`] of a transactions output
    ///
    pub fn get_output_pre_rct_outkey(
        &self,
        amount: u64,
        amount_output_index: u64,
        parse: bool,
    ) -> Result<Parse<PreRctOutkey>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.output_amounts,
            &amount.to_le_bytes(),
            &amount_output_index.to_le_bytes(),
            2,
            parse,
        )
    }

    /// Gets amount output indices of the transaction outputs
    ///
    pub fn get_tx_output_idx(&self, txn_id: u64, parse: bool) -> Result<Parse<TxOutputIdx>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.tx_outputs,
            &txn_id.to_le_bytes(),
            &[0],
            15,
            parse,
        )
    }

    /// Gets the hash of the prunable part of the transaction
    ///
    pub fn get_txs_prunable_hash(&self, txn_id: u64, parse: bool) -> Result<Parse<Hash>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.txs_prunable_hash,
            &txn_id.to_le_bytes(),
            &[0],
            15,
            parse,
        )
    }

    /// Gets the height of the transaction if that transactions block height + 5500 is > the blockchain height
    ///
    pub fn get_txs_prunable_tip(&self, txn_id: u64, parse: bool) -> Result<Parse<u64>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.txs_prunable_tip,
            &txn_id.to_be_bytes(),
            &[0],
            15,
            parse,
        )
    }

    /// Gets the height of the first block where the blocks height + 5500 is > than the blockchain height
    ///
    pub fn get_prunable_tip(&self) -> Result<u64, Error> {
        get_item::<u64>(
            &self.env,
            self.sub_dbs.txs_prunable_tip,
            &[0],
            &[0],
            0,
            true,
        )?
        .deserialize()
    }

    /// Gets the [`OutTx`] of an output
    ///
    pub fn get_output_tx(&self, output_id: u64, parse: bool) -> Result<Parse<OutTx>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.output_txs,
            &ZERO_KEY,
            &output_id.to_le_bytes(),
            2,
            parse,
        )
    }

    /// Get the [`TxIndex`] from a transaction  
    ///
    pub fn get_tx_indices(&self, txn_hash: &[u8], parse: bool) -> Result<Parse<TxIndex>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.tx_indices,
            &ZERO_KEY,
            txn_hash,
            2,
            parse,
        )
    }

    /// Returns if a key image has already been spent
    ///
    pub fn is_key_image_spent(&self, spent_key: &[u8]) -> Result<bool, Error> {
        // This will always return nothing and won't attempt to de-serialize
        // that is why it has a random type(Block)
        let data = get_item::<Block>(
            &self.env,
            self.sub_dbs.spent_keys,
            &ZERO_KEY,
            spent_key,
            2,
            false,
        );
        if let Err(Error::DatabaseError(e)) = data {
            if e.to_err_code() == -30798 {
                return Ok(false);
            }
            return Err(Error::DatabaseError(e));
        }
        Ok(true)
    }

    /// Get the transaction from transaction pool
    ///
    pub fn get_txpool_tx(
        &self,
        txn_hash: &[u8],
        parse: bool,
    ) -> Result<Parse<monero::Transaction>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.txpool_blob,
            txn_hash,
            &[0],
            15,
            parse,
        )
    }

    /// Get the TxPoolMeta from transaction pool
    ///
    pub fn get_txpool_meta(
        &self,
        txn_hash: &[u8],
        parse: bool,
    ) -> Result<Parse<TxPoolMeta>, Error> {
        get_item(
            &self.env,
            self.sub_dbs.txpool_meta,
            txn_hash,
            &[0],
            15,
            parse,
        )
    }

    /// Gets the version of the database, the current version is 5
    ///
    pub fn get_db_version(&self) -> Result<u32, Error> {
        let key = b"version\0";
        get_item::<u32>(&self.env, self.sub_dbs.properties, key, &[0], 15, true)?.deserialize()
    }

    /// Gets the pruning seed of the database
    ///
    pub fn get_db_pruning_seed(&self) -> Result<u32, Error> {
        let key = b"pruning_seed\0";
        get_item::<u32>(&self.env, self.sub_dbs.properties, key, &[0], 15, true)?.deserialize()
    }

    /// Returns if the database is readonly
    /// 
    pub fn is_readonly(&self) -> bool {
        self.read_only
    }
}

fn get_item<T: Decodable + Encodable + Debug>(
    env: &Environment,
    db: Database,
    key: &[u8],
    data: &[u8],
    op: u32,
    parse: bool,
) -> Result<Parse<T>, Error> {
    let transaction = env.begin_ro_txn()?;
    let curser = transaction.open_ro_cursor(db)?;
    let value = curser.get(Some(key), Some(data), op)?;
    if parse {
        return Ok(Parse::Yes(deserialize(value.1)?));
    }
    Ok(Parse::No(value.1.to_vec()))
}
