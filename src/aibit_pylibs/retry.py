"""
Retry utilities for external service operations.
Provides exponential backoff and circuit breaker patterns.
"""

import asyncio
import time
from functools import wraps
from typing import Callable, Optional, TypeVar

from .logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: tuple = (Exception,),
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions


def retry_with_backoff(config: Optional[RetryConfig] = None):
    """
    Decorator for retry with exponential backoff.

    Args:
        config: Retry configuration, uses default if None
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(config.max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e

                    if attempt == config.max_retries:
                        logger.error(
                            "Max retries exceeded",
                            function=func.__name__,
                            attempts=attempt + 1,
                            error=str(e),
                            exc_info=True,
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = config.base_delay * (config.exponential_base**attempt)
                    delay = min(delay, config.max_delay)

                    # Add jitter to prevent thundering herd
                    if config.jitter:
                        delay = delay * (
                            0.5 + 0.5 * asyncio.get_event_loop().time() % 1
                        )

                    logger.warning(
                        "Retry attempt failed",
                        function=func.__name__,
                        attempt=attempt + 1,
                        max_attempts=config.max_retries + 1,
                        delay_seconds=delay,
                        error=str(e),
                    )

                    await asyncio.sleep(delay)

            raise last_exception

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e

                    if attempt == config.max_retries:
                        logger.error(
                            "Max retries exceeded",
                            function=func.__name__,
                            attempts=attempt + 1,
                            error=str(e),
                            exc_info=True,
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = config.base_delay * (config.exponential_base**attempt)
                    delay = min(delay, config.max_delay)

                    # Add jitter to prevent thundering herd
                    if config.jitter:
                        delay = delay * (0.5 + 0.5 * time.time() % 1)

                    logger.warning(
                        "Retry attempt failed",
                        function=func.__name__,
                        attempt=attempt + 1,
                        max_attempts=config.max_retries + 1,
                        delay_seconds=delay,
                        error=str(e),
                    )

                    time.sleep(delay)

            raise last_exception

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


class CircuitBreaker:
    """Circuit breaker pattern for external service calls."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half_open
        self.lock = asyncio.Lock()

    async def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            Expected exception if circuit is open
        """
        async with self.lock:
            if self.state == "open":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    # Try to recover
                    self.state = "half_open"
                    logger.info("Circuit breaker attempting recovery", state=self.state)
                else:
                    # Circuit is open, fail fast
                    raise self.expected_exception("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)

            # Reset on success
            async with self.lock:
                if self.state == "half_open":
                    logger.info("Circuit breaker recovered", state="closed")
                self.state = "closed"
                self.failure_count = 0

            return result

        except self.expected_exception as e:
            async with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.failure_count >= self.failure_threshold:
                    old_state = self.state
                    self.state = "open"
                    logger.warning(
                        "Circuit breaker opened",
                        failure_count=self.failure_count,
                        threshold=self.failure_threshold,
                        old_state=old_state,
                        new_state=self.state,
                    )

            raise e


def with_circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    expected_exception: type = Exception,
):
    """
    Decorator for circuit breaker pattern.

    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Time to wait before attempting recovery
        expected_exception: Exception type that triggers circuit breaker
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        circuit_breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
        )

        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await circuit_breaker.call(func, *args, **kwargs)

        return wrapper

    return decorator


def create_http_retry_config() -> RetryConfig:
    """Create retry configuration optimized for HTTP operations."""
    return RetryConfig(
        max_retries=3,
        base_delay=1.0,
        max_delay=30.0,
        exponential_base=2.0,
        jitter=True,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
        ),
    )


def create_git_retry_config() -> RetryConfig:
    """Create retry configuration optimized for Git operations."""
    return RetryConfig(
        max_retries=3,
        base_delay=2.0,
        max_delay=60.0,
        exponential_base=2.0,
        jitter=True,
        retryable_exceptions=(
            Exception,  # Git operations can fail for various reasons
        ),
    )
