use arrow::datatypes::ArrowSchema;
use polars_error::PolarsResult;

use self::vaex_hdf5::Hdf5Metadata;

// mod hdf5_file_metadata;
pub mod vaex_hdf5;

pub fn infer_schema_hdf5(hdf5_metadata: &Hdf5Metadata) -> PolarsResult<ArrowSchema> {
    // let fields = hdf5_metadata.columns;
    let fields: Vec<Field>;
    let metadata: Metadata;

    Ok(ArrowSchema { fields, metadata })
}
