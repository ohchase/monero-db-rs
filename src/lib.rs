// Copyright (c) 2022 Boog900

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

//! # Rust Monero Database Library
//!
//! This is a library for interacting with a Monero database. Currently only reading
//! from the database is supported. This library should support all current and
//! previous Monero types, however only the current database version is supported (5).
//!


// Coding conventions
#![forbid(unsafe_code)]
#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(unused_mut)]
#![deny(missing_docs)]

use thiserror::Error;

mod monero_db;
pub use monero_db::MoneroDB;
mod sub_db;

const ZERO_KEY: [u8; 8] = [0; 8];

/// Potential errors
///
#[derive(Error, Debug)]
pub enum Error {
    /// Errors relating to the database eg: retrieving value from database
    #[error("Retrieval error: {0}")]
    DatabaseError(#[from] lmdb::Error),
    /// Input for a retrieval is incorrect eg: hash is not 32 bytes long
    #[error("Value cannot be searched for: {0}")]
    ValueError(String),
    /// Error deserializing the retrieved data
    #[error("Failed to decode value from database: {0}")]
    MoneroDecodingError(#[from] monero::consensus::encode::Error),
}
