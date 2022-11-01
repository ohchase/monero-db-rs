// Copyright (c) 2022 Boog900

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

use crate::Error;
use lmdb::{Database, DatabaseFlags, Environment, Transaction};

pub(crate) struct MoneroSubDB {
    pub(crate) blocks: Database,
    pub(crate) block_heights: Database,
    pub(crate) block_info: Database,
    pub(crate) txs_pruned: Database,
    pub(crate) txs_prunable: Database,
    pub(crate) txs_prunable_hash: Database,
    pub(crate) txs_prunable_tip: Database,
    pub(crate) tx_indices: Database,
    pub(crate) tx_outputs: Database,
    pub(crate) output_txs: Database,
    pub(crate) output_amounts: Database,
    pub(crate) spent_keys: Database,
    pub(crate) txpool_meta: Database,
    pub(crate) txpool_blob: Database,
    pub(crate) alt_blocks: Database,
    pub(crate) hf_versions: Database,
    pub(crate) properties: Database,
}

impl MoneroSubDB {
    fn open_sub_dbs(env: &Environment) -> Result<Self, Error> {
        Ok(MoneroSubDB {
            blocks: open_subdb(env, "blocks", DatabaseFlags::INTEGER_KEY)?,
            block_info: open_subdb(
                env,
                "block_info",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            block_heights: open_subdb(
                env,
                "block_heights",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            txs_pruned: open_subdb(env, "txs_pruned", DatabaseFlags::INTEGER_KEY)?,
            txs_prunable: open_subdb(env, "txs_prunable", DatabaseFlags::INTEGER_KEY)?,
            txs_prunable_hash: open_subdb(
                env,
                "txs_prunable_hash",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            txs_prunable_tip: open_subdb(
                env,
                "txs_prunable_tip",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            tx_indices: open_subdb(
                env,
                "tx_indices",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            tx_outputs: open_subdb(
                env,
                "tx_outputs",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            output_txs: open_subdb(
                env,
                "output_txs",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            output_amounts: open_subdb(
                env,
                "output_amounts",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            spent_keys: open_subdb(
                env,
                "spent_keys",
                DatabaseFlags::INTEGER_KEY | DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
            )?,
            txpool_meta: open_subdb(env, "txpool_meta", DatabaseFlags::empty())?,
            txpool_blob: open_subdb(env, "txpool_blob", DatabaseFlags::empty())?,
            alt_blocks: open_subdb(env, "alt_blocks", DatabaseFlags::empty())?,
            hf_versions: open_subdb(env, "hf_versions", DatabaseFlags::INTEGER_KEY)?,
            properties: open_subdb(env, "properties", DatabaseFlags::empty())?,
        })
    }

    fn set_sort(&self, env: &Environment) -> Result<(), Error> {
        let transaction = env.begin_ro_txn()?;
        transaction.set_dupsort_hash32(self.spent_keys);
        transaction.set_dupsort_hash32(self.block_heights);
        transaction.set_dupsort_hash32(self.tx_indices);
        transaction.set_dupsort_uint64(self.output_amounts);
        transaction.set_dupsort_uint64(self.output_txs);
        transaction.set_dupsort_uint64(self.block_info);
        transaction.set_dupsort_uint64(self.txs_prunable_tip);
        transaction.set_compare_uint64(self.txs_prunable);
        transaction.set_dupsort_uint64(self.txs_prunable_hash);
        transaction.set_compare_hash32(self.txpool_meta);
        transaction.set_compare_hash32(self.txpool_blob);
        transaction.set_compare_hash32(self.alt_blocks);
        transaction.set_compare_string(self.properties);
        transaction.commit()?;
        Ok(())
    }

    pub fn new(env: &Environment) -> Result<Self, Error> {
        let sub_dbs = MoneroSubDB::open_sub_dbs(env)?;
        sub_dbs.set_sort(env)?;
        Ok(sub_dbs)
    }
}

fn open_subdb(env: &Environment, name: &str, flags: DatabaseFlags) -> Result<Database, Error> {
    Ok(env.open_db_with_flags(Some(name), flags.bits())?)
}
