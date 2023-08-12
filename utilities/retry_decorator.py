import time
from functools import wraps


class Decorator:    

    def __init__(self):
        self.max_retry = 3

    def retry(self,tries=3, delay=1, backoff=2):
        """
        Decorator for retrying a function if any exception occurs

        :param tries: number of times to try
        :param delay: initial delay between retries
        :param backoff: backoff multiplier
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                mtries, mdelay = tries, delay
                while mtries > 1:
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        msg = f"{str(e)}, Retrying in {mdelay} seconds..."
                        print(msg)
                        time.sleep(mdelay)
                        mtries -= 1
                        mdelay *= backoff
                return func(*args, **kwargs)  # last try
            return wrapper
        return decorator




    def retry_on_none(self,func):
        """
        Decorator to retry function when the result is None.
        """
        def wrapper(*args, **kwargs):
            max_retries = 3
            delay = 30  # Delay in seconds
            
            for _ in range(max_retries):
                result = func(*args, **kwargs)
                if result is not None:
                    return result
                print(f"Function returned None. Retrying in {delay} seconds...")
                time.sleep(delay)
            
            print(f"Function returned None after {max_retries} attempts.")
            return None
        return wrapper