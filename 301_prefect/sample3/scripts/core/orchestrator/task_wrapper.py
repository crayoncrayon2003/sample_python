# scripts/core/orchestrator/task_wrapper.py

from prefect import task
from typing import Callable, Any, Dict
import functools

def wrap_as_task(
    func: Callable,
    name: str = None,
    retries: int = 0,
    retry_delay_seconds: int = 10,
    **task_kwargs: Dict[str, Any]
) -> Callable:
    """
    Wraps a given function to be executed as a Prefect task.

    This utility function applies the @task decorator to any callable,
    allowing for dynamic task creation with specified parameters like
    retries and task name.

    Args:
        func (Callable): The function to be wrapped as a Prefect task.
        name (str, optional): The name to assign to the task. If None,
            the function's name is used. Defaults to None.
        retries (int, optional): The number of times to retry the task
            on failure. Defaults to 0.
        retry_delay_seconds (int, optional): The number of seconds to
            wait between retries. Defaults to 10.
        **task_kwargs (Dict[str, Any]): Additional keyword arguments to
            pass to the Prefect @task decorator.

    Returns:
        Callable: A Prefect task-wrapped version of the input function.
    """
    if not callable(func):
        raise TypeError("The object to wrap must be a callable function.")

    # Determine the task name. Use the provided name or default to the function's name.
    task_name = name or getattr(func, '__name__', 'unnamed_task')

    # Create a decorator with the specified parameters
    task_decorator = task(
        name=task_name,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        **task_kwargs
    )

    # Apply the decorator to the original function and return the new task
    return task_decorator(func)