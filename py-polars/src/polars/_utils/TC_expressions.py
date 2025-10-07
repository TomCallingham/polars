import polars as pl
import re


def trans_func(df: pl.DataFrame, name: str, value: str) -> pl.DataFrame:
    # Convert any instance of {col} into pl.col("col")
    expr_str = re.sub(r"\{(\w+)\}", r"pl.col('\1')", value)
    # Evaluate the expression in a controlled namespace (only pl is available)
    expr: pl.Expr = eval(expr_str, {"pl": pl})
    # Add the new column with the given alias
    return df.with_columns(expr.alias(name))
