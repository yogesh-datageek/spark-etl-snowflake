"""
Simple retry decorator with exponential back-off.
"""

from __future__ import annotations

import time
import functools
from typing import Any, Callable

from src.utils.logger import get_logger

logger = get_logger(__name__)


def retry(
    max_attempts: int = 3,
    backoff_seconds: float = 5.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable:
    """
    Decorator that retries a function up to *max_attempts* times on failure.

    Parameters
    ----------
    max_attempts     : total number of tries (including the first one).
    backoff_seconds  : initial wait between retries (doubled each attempt).
    exceptions       : tuple of exception types that trigger a retry.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = backoff_seconds
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__, max_attempts, exc,
                        )
                        raise
                    logger.warning(
                        "%s attempt %d/%d failed (%s). Retrying in %.1fs …",
                        func.__name__, attempt, max_attempts, exc, delay,
                    )
                    time.sleep(delay)
                    delay *= 2
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator
