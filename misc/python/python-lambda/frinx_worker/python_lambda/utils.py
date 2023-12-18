import inspect
from collections.abc import Callable

executed_func_template: str = """
def execute(worker_input: dict) -> None:
{custom_execution_commands}

worker_input = {custom_worker_inputs}
result = execute(worker_input)
"""


def get_indentation(line: str) -> int:
    """
    Get the number of leading spaces in a string.

    Parameters:
    - line: The input string.

    Returns:
    The number of leading spaces.
    """
    leading_spaces = len(line) - len(line.lstrip())
    return leading_spaces


def sanitize_lines(func: str, indent: int) -> str:
    """
    Remove decorators and adjust indentation from the given function string.

    Parameters:
    - func: The function as string.
    - indent: The number of spaces to remove from the beginning of each line.

    Returns:
    The modified function as string.
    """
    new_lines: list[str] = []
    for line in func.splitlines():
        new_line = line[indent:]
        if new_line.startswith('@'):
            continue
        if new_line.startswith('def '):
            continue
        new_lines.append(new_line)
    return '\n'.join(new_lines)


def python_lambda_stringify(func: Callable[[], str]) -> Callable[[], str]:
    """
    Convert a function to a string and remove decorators, adjust indentation.

    Parameters:
    - func: The input function.

    Returns:
    A string representation of the modified function.
    """
    def wrap() -> str:
        # Get function in string format
        func_as_string = inspect.getsource(func)
        # Get indentation of the first line; then, all lines will be stripped with this indentation
        indent = get_indentation(func_as_string.split('\n')[1])
        # Return function without decorators, with decreased indentation, and without custom defined def
        return sanitize_lines(func_as_string, indent)
    return wrap
