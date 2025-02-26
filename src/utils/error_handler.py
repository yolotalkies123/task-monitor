from functools import wraps
import logging
import traceback
from datetime import datetime
from typing import Optional, Dict, Any


class TaskError(Exception):
    """Custom exception for task-related errors with context information"""

    def __init__(
            self,
            message: str,
            task_id: Optional[str] = None,
            tenant_id: Optional[str] = None,
            details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.task_id = task_id
        self.tenant_id = tenant_id
        self.details = details or {}
        self.timestamp = datetime.utcnow()
        # Include timestamp in string representation
        self.details['error_timestamp'] = self.timestamp.isoformat()
        super().__init__(self.message)


def error_handler(logger: logging.Logger):
    """Decorator for handling errors in async functions with structured logging"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                # Execute the wrapped function
                return await func(*args, **kwargs)

            except TaskError as e:
                # Handle known task-related errors
                logger.error(
                    f"Task error: {str(e)}",
                    extra={
                        'task_id': e.task_id,
                        'tenant_id': e.tenant_id,
                        'error_details': e.details,
                        'error_type': 'TaskError',
                        'timestamp': e.timestamp.isoformat(),
                        'stack_trace': traceback.format_exc()
                    }
                )
                raise

            except Exception as e:
                # Handle unexpected errors
                error_context = {
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'timestamp': datetime.utcnow().isoformat(),
                    'stack_trace': traceback.format_exc(),
                    'function_name': func.__name__,
                    'args': str(args),
                    'kwargs': str(kwargs)
                }

                logger.error(
                    f"Unexpected error: {str(e)}",
                    extra=error_context
                )

                # Convert to TaskError for consistent error handling
                raise TaskError(
                    message=f"Unexpected error: {str(e)}",
                    details=error_context
                )

        return wrapper

    return decorator


class RetryableError(TaskError):
    """Error type for operations that can be retried"""

    def __init__(
            self,
            message: str,
            retry_count: int = 0,
            max_retries: int = 3,
            **kwargs
    ):
        super().__init__(message, **kwargs)
        self.retry_count = retry_count
        self.max_retries = max_retries
        self.can_retry = retry_count < max_retries


class DatabaseError(TaskError):
    """Error type for database-related issues"""

    def __init__(
            self,
            message: str,
            operation: str,
            collection: str,
            **kwargs
    ):
        super().__init__(message, **kwargs)
        self.operation = operation
        self.collection = collection
        self.details.update({
            'operation': operation,
            'collection': collection
        })


class KafkaError(TaskError):
    """Error type for Kafka-related issues"""

    def __init__(
            self,
            message: str,
            topic: str,
            action: str,
            **kwargs
    ):
        super().__init__(message, **kwargs)
        self.topic = topic
        self.action = action
        self.details.update({
            'topic': topic,
            'action': action
        })