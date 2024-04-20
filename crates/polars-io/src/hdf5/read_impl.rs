use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Deref, Range};

use arrow::array::new_empty_array;
use arrow::datatypes::ArrowSchemaRef;
use polars_core::prelude::*;
use polars_core::utils::{accumulate_dataframes_vertical, split_df};
use polars_core::POOL;
use polars_hdf5::read;
use polars_hdf5::read::{ArrayIter, FileMetaData, RowGroupMetaData};
use rayon::prelude::*;

use super::materialize_empty_df;
use super::mmap::ColumnStore;
#[cfg(feature = "cloud")]
use crate::hdf5::async_impl::FetchRowGroupsFromObjectStore;
use crate::hdf5::mmap::mmap_columns;
use crate::hdf5::predicates::read_this_row_group;
use crate::hdf5::{mmap, FileMetaDataRef, ParallelStrategy};
use crate::mmap::{MmapBytesReader, ReaderBytes};
use crate::predicates::{apply_predicate, PhysicalIoExpr};
use crate::utils::get_reader_bytes;
use crate::RowIndex;
#[allow(clippy::too_many_arguments)]

pub fn read_hdf5<R: MmapBytesReader>(
    mut reader: R,
    mut limit: usize,
    projection: Option<&[usize]>,
    reader_schema: &ArrowSchemaRef,
    // metadata: Option<FileMetaDataRef>,
    predicate: Option<&dyn PhysicalIoExpr>,
    // mut parallel: ParallelStrategy,
    row_index: Option<RowIndex>,
) -> PolarsResult<DataFrame> {
    // Fast path.
    if limit == 0 {
        return Ok(materialize_empty_df(
            projection,
            reader_schema,
            hive_partition_columns,
            row_index.as_ref(),
        ));
    }

    let file_metadata = metadata
        .map(Ok)
        .unwrap_or_else(|| read::read_metadata(&mut reader).map(Arc::new))?;
    let n_row_groups = file_metadata.row_groups.len();

    // if there are multiple row groups and categorical data
    // we need a string cache
    // we keep it alive until the end of the function
    let _sc = if n_row_groups > 1 {
        #[cfg(feature = "dtype-categorical")]
        {
            Some(polars_core::StringCacheHolder::hold())
        }
        #[cfg(not(feature = "dtype-categorical"))]
        {
            Some(0u8)
        }
    } else {
        None
    };

    let materialized_projection = projection
        .map(Cow::Borrowed)
        .unwrap_or_else(|| Cow::Owned((0usize..reader_schema.len()).collect::<Vec<_>>()));

    if let ParallelStrategy::Auto = parallel {
        if n_row_groups > materialized_projection.len() || n_row_groups > POOL.current_num_threads()
        {
            parallel = ParallelStrategy::RowGroups;
        } else {
            parallel = ParallelStrategy::Columns;
        }
    }

    if let (ParallelStrategy::Columns, true) = (parallel, materialized_projection.len() == 1) {
        parallel = ParallelStrategy::None;
    }

    let reader = ReaderBytes::from(&reader);
    let bytes = reader.deref();
    let store = mmap::ColumnStore::Local(bytes);

    let dfs = rg_to_dfs(
        &store,
        &mut 0,
        0,
        n_row_groups,
        &mut limit,
        &file_metadata,
        reader_schema,
        predicate,
        row_index.clone(),
        parallel,
        &materialized_projection,
        use_statistics,
        hive_partition_columns,
    )?;

    if dfs.is_empty() {
        Ok(materialize_empty_df(
            projection,
            reader_schema,
            hive_partition_columns,
            row_index.as_ref(),
        ))
    } else {
        accumulate_dataframes_vertical(dfs)
    }
}
