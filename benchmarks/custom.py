import time

import numpy as np
import pandas as pd

import pantab

if __name__ == '__main__':
    df = pd.DataFrame(np.ones((100_000_000, 1)), columns=["a"])
    start = time.time()
    pantab.frame_to_hyper(df, "test.hyper", table="test")
    end = time.time()
    print(f"Execution took: {end - start}")
