from typing import Any, Literal

def write_to_hyper(
    dict_of_capsules: dict[tuple[str, str], Any],
    path: str,
    table_mode: Literal["w", "a"],
    not_null_columns: set[str],
    json_columns: set[str],
    geo_columns: set[str],
) -> None: ...
def read_from_hyper_query(path: str, query: str): ...
