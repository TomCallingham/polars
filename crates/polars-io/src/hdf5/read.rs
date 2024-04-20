use std::io::{Read, Seek};

use arrow::datatypes::ArrowSchemaRef;
use polars_core::prelude::*;
use polars_hdf5::metadata::vaex_hdf5::Hdf5Metadata;

use super::read_impl::read_hdf5;
use crate::mmap::MmapBytesReader;
use crate::predicates::PhysicalIoExpr;
use crate::prelude::*;
use crate::RowIndex;
/*
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use super::read_impl::FetchRowGroupsFromMmapReader;

pub use crate::hdf5::read_impl::BatchedHdf5Reader;
 */
/* #[derive(Copy, Clone, Debug, Eq, PartialEq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ParallelStrategy {
    /// Don't parallelize
    None,
    /// Parallelize over the columns
    Columns,
    /// Parallelize over the row groups
    RowGroups,
    /// Automatically determine over which unit to parallelize
    /// This will choose the most occurring unit.
    #[default]
    Auto,
} */

#[must_use]
pub struct Hdf5Reader<R: Read + Seek> {
    reader: R,
    n_rows: Option<usize>,
    columns: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
    row_index: Option<RowIndex>,
    schema: Option<ArrowSchemaRef>,
    //Needed?
    predicate: Option<Arc<dyn PhysicalIoExpr>>,
    rechunk: bool,
    // parallel: ParallelStrategy,
    // low_memory: bool,
    hdf5_metadata: Option<Arc<Hdf5Metadata>>,
    // use_statistics: bool,
}

impl<R: MmapBytesReader> Hdf5Reader<R> {
    /// Read the hdf5 file in parallel (default). The single threaded reader consumes less memory.
    /* pub fn read_parallel(mut self, parallel: ParallelStrategy) -> Self {
        self.parallel = parallel;
        self
    } */

    /// Stop parsing when `n` rows are parsed. By settings this parameter the csv will be parsed
    /// sequentially.
    pub fn with_n_rows(mut self, num_rows: Option<usize>) -> Self {
        self.n_rows = num_rows;
        self
    }

    /// Columns to select/ project
    pub fn with_columns(mut self, columns: Option<Vec<String>>) -> Self {
        self.columns = columns;
        self
    }

    /// Set the reader's column projection. This counts from 0, meaning that
    /// `vec![0, 4]` would select the 1st and 5th column.
    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    /// Add a row index column.
    pub fn with_row_index(mut self, row_index: Option<RowIndex>) -> Self {
        self.row_index = row_index;
        self
    }

    /// Set the [`Schema`] if already known. This must be exactly the same as
    /// the schema in the file itself.
    pub fn with_schema(mut self, schema: Option<ArrowSchemaRef>) -> Self {
        self.schema = schema;
        self
    }

    /// [`Schema`] of the file.
    pub fn schema(&mut self) -> PolarsResult<ArrowSchemaRef> {
        match &self.schema {
            Some(schema) => Ok(schema.clone()),
            None => {
                let metadata = self.get_metadata()?;
                Ok(Arc::new(read::infer_schema(metadata)?))
            },
        }
    }

    /// Use statistics in the hdf5 to determine if pages
    /// can be skipped from reading.
    /* pub fn use_statistics(mut self, toggle: bool) -> Self {
        self.use_statistics = toggle;
        self
    } */

    /// Number of rows in the hdf5 file.
    pub fn num_rows(&mut self) -> PolarsResult<usize> {
        let metadata = self.get_metadata()?;
        Ok(metadata.n_rows)
    }

    pub fn get_metadata(&mut self) -> PolarsResult<&Hdf5Metadata> {
        if self.hdf5_metadata.is_none() {
            self.hdf5_metadata = Some(Arc::new(read::read_metadata(&mut self.reader)?));
        }
        Ok(self.hdf5_metadata.as_ref().unwrap())
    }

    pub fn with_predicate(mut self, predicate: Option<Arc<dyn PhysicalIoExpr>>) -> Self {
        self.predicate = predicate;
        self
    }
}

/* impl<R: MmapBytesReader + 'static> Hdf5Reader<R> {
    pub fn batched(mut self, chunk_size: usize) -> PolarsResult<BatchedHdf5Reader> {
        let metadata = self.get_metadata()?.clone();
        let schema = self.schema()?;

        let row_group_fetcher = FetchRowGroupsFromMmapReader::new(Box::new(self.reader))?.into();
        BatchedHdf5Reader::new(
            row_group_fetcher,
            metadata,
            schema,
            self.n_rows.unwrap_or(usize::MAX),
            self.projection,
            self.predicate.clone(),
            self.row_index,
            chunk_size,
            self.use_statistics,
            self.hive_partition_columns,
            self.parallel,
        )
    }
} */

impl<R: MmapBytesReader> SerReader<R> for Hdf5Reader<R> {
    /// Create a new [`Hdf5Reader`] from an existing `Reader`.
    fn new(reader: R) -> Self {
        Hdf5Reader {
            reader,
            n_rows: None,
            columns: None,
            projection: None,
            row_index: None,
            schema: None,
            predicate: None,
            rechunk: false,
            /* parallel: Default::default(),
            low_memory: false,
            metadata: None,*/
        }
    }

    /* fn set_rechunk(mut self, rechunk: bool) -> Self {
        self.rechunk = rechunk;
        self
    } */

    fn finish(mut self) -> PolarsResult<DataFrame> {
        let schema = self.schema()?;
        // let metadata = self.get_metadata()?.clone();

        if let Some(cols) = &self.columns {
            self.projection = Some(columns_to_projection(cols, schema.as_ref())?);
        }

        read_hdf5(
            self.reader,
            self.n_rows.unwrap_or(usize::MAX),
            self.projection.as_deref(),
            &schema,
            // Some(metadata),
            self.predicate.as_deref(),
            // self.parallel,
            self.row_index,
        )
        .map(|mut df| {
            if self.rechunk {
                df.as_single_chunk_par();
            }
            df
        })
    }
}
