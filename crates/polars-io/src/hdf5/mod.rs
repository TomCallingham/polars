//! # Reading Hdf5 files.
// use std::path::PathBuf;

// use polars_core::prelude::*;

mod read;
mod read_impl;
mod write;

use polars_hdf5::metadata::vaex_hdf5::Hdf5Schema;
// use std::borrow::Cow;

// pub use polars_hdf5::write::FileMetaData;
pub use read::*;
