from functools import wraps
from time import perf_counter


def time_this(f):
    """
    Simple decorator for timing a function.
    Prints total runtime after the function is done.
    """

    @wraps(f)
    def decorated(*args, **kwargs):
        start = perf_counter()
        result = f(*args, **kwargs)
        end = perf_counter()
        elapsed = round(end - start, 3)
        print(f"Run time for {f.__name__}: {elapsed} seconds")
        return result

    return decorated
