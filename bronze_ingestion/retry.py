import time
from functools import wraps
from config import MAX_RETRY, RETRY_WAIT

def retry():

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            last_exception = None

            for attempt in range(1, MAX_RETRY + 1):

                try:

                    return func(*args, **kwargs)

                except Exception as e:

                    last_exception = e

                    print(
                        f"Attempt {attempt}/{MAX_RETRY} failed : {e}"
                    )

                    if attempt < MAX_RETRY:

                        time.sleep(RETRY_WAIT)

            raise last_exception

        return wrapper

    return decorator