from __future__ import annotations

import contextlib

# from io import BytesIO, StringIO
from pathlib import Path

# from typing import IO, TYPE_CHECKING, Any, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Sequence

# from polars.datatypes import N_INFER_DEFAULT, String
# from polars.datatypes.convert import py_type_to_dtype
# from polars.io._utils import (
#     is_glob_pattern,
#     parse_columns_arg,
#     parse_row_index_args,
#     prepare_file_arg,
# )
from polars._utils.various import (
    is_int_sequence,
    normalize_filepath,
)

# import polars._reexport as pl
# from polars._utils.deprecation import deprecate_renamed_parameter
# from polars._utils.various import (
#     _process_null_values,
#     is_str_sequence,
#     normalize_filepath,
# )
# from polars._utils.wrap import wrap_df, wrap_ldf
from polars._utils.wrap import wrap_ldf

# from polars.io.csv._utils import _check_arg_is_1byte, _update_columns
# from polars.io.csv.batched_reader import BatchedCsvReader

with contextlib.suppress(ImportError):  # Module not available when building docs
    # from polars.polars import PyDataFrame, PyLazyFrame
    from polars.polars import PyLazyFrame

if TYPE_CHECKING:
    from polars import DataFrame, LazyFrame
    # from polars.type_aliases import CsvEncoding, PolarsDataType, SchemaDict

def read_hdf5(
    source: str | Path, #| IO[str] | IO[bytes] | bytes,
    *,
    subgroup: str|None = None,  # The subgroup which to look under
    format: str|None = None, # pytable, vaex, nothing ect
    ###
    columns: Sequence[int] | Sequence[str] | None = None,
) -> DataFrame:
    r"""
    Read a HDF5 file into a DataFrame.

    Parameters
    ----------

    Examples
    --------
    >>> pl.read_hdf5("data.hdf5")  # doctest: +SKIP
    └─────┴─────────┴────────────┘
    """
    lf = scan_hdf5(
        source,  # type: ignore[arg-type]
        subgroup=subgroup,
        format=format,
        # n_rows=n_rows,
        # row_index_name=row_index_name,
        # row_index_offset=row_index_offset,
        # parallel=parallel,
        # use_statistics=use_statistics,
        # hive_partitioning=hive_partitioning,
        # hive_schema=hive_schema,
        # rechunk=rechunk,
        # low_memory=low_memory,
        # cache=False,
        # storage_options=storage_options,
        # retries=retries,
    )

    if columns is not None:
        if is_int_sequence(columns):
            columns = [lf.columns[i] for i in columns]
        lf = lf.select(columns)

    return lf.collect()

def scan_hdf5(
    source: str | Path, #| IO[str] | IO[bytes] | bytes,
    *,
    subgroup: str|None = None,  # The subgroup which to look under
    format: str|None = None, # pytable, vaex, nothing ect
) -> LazyFrame:
    r"""
    Lazily read a HDF5 file.

    Parameters
    ----------

    Examples
    --------
    └─────┴─────────┴────────────┘
    """
    if isinstance(source, (str, Path)):
        source = normalize_filepath(source)
    else:
        # source = [normalize_filepath(source) for source in source]
        raise AttributeError("MyErr: path not str or path?")


    return _scan_hdf5_impl(
        source,
        subgroup=subgroup,
        format=format
    )


def _scan_hdf5_impl(
    source: str | list[str] | list[Path],
    *,
    subgroup: str|None = None,  # The subgroup which to look under
    format: str|None = None, # pytable, vaex, nothing ect
    # n_rows: int | None = None,
    # cache: bool = True,
    # parallel: ParallelStrategy = "auto",
    # rechunk: bool = True,
) -> LazyFrame:
    if isinstance(source, list):
        sources = source
        source = None  # type: ignore[assignment]
    else:
        sources = []

    pylf = PyLazyFrame.new_from_hdf5(
        source,
        sources,
        subgroup,
        format

    )
    return wrap_ldf(pylf)
