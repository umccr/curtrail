import polars as pl


def assert_all_values_are_the_same(series: pl.Series, value: str):
    """Asserts that all values in a Polars Series are the same as the given value."""
    if series.n_unique() == 1:
        unique_value = series.unique().to_list()

        if unique_value[0] != value:
            raise AssertionError(
                f"Series '{series.name}' has a single value: {unique_value[0]}: but != expected {value}"
            )
    elif series.n_unique() > 1:
        unique_values = series.unique().to_list()
        raise AssertionError(
            f"Series '{series.name}' has multiple unique values: {unique_values}"
        )
    elif series.is_empty():
        # Handle empty series if needed, depending on desired behavior
        pass

def assert_all_values_are_null(series: pl.Series):
    """Asserts that all values in a Polars Series are null."""
    if not series.null_count() != series.len():
        raise AssertionError(
            f"Series '{series.name}' should be all null"
        )
