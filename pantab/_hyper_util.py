from contextlib import contextmanager, nullcontext
from typing import Optional

import tableauhyperapi as tab_api


def ensure_hyper_process(hyper_process: Optional[tab_api.HyperProcess]):
    """
    Spawns an adhoc HyperProcess if needed, i.e. if no existing HyperProcess is provided

    Usage:
    ```
    with ensure_hyper_process(<HyperProcess or None>) as h:
        h.execute_query(...)
    ```
    """
    if hyper_process is None:
        return tab_api.HyperProcess(tab_api.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
    else:
        # Wrap the HyperProcess into a nullcontext such that the `with` doesn't close
        # the HyperProcess
        return nullcontext(hyper_process)


def forbid_hyper_process(hyper_process: Optional[tab_api.HyperProcess]):
    if hyper_process is not None:
        raise ValueError(
            "hyper_process parameter is useless because `Connection` is provided"
        )
