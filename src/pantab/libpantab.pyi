from typing import Any, Literal, Optional

def write_to_hyper(
    dict_of_capsules: dict[tuple[str, str], Any],
    path: str,
    table_mode: Literal["w", "a"],
    not_null_columns: set[str],
    json_columns: set[str],
    geo_columns: set[str],
    process_params: Optional[dict[str, str]],
) -> None: ...
def read_from_hyper_query(
    path: str,
    query: str,
    process_params: Optional[dict[str, str]],
) -> Any: ...
def escape_sql_identifier(str: str) -> str: ...
def get_table_names(path: str) -> list[str]: ...
