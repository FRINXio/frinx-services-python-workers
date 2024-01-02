import ast
from typing import Any

import pytest
from frinx.common.conductor_enums import TaskResultStatus

from frinx_worker.python_lambda import PythonLambda
from frinx_worker.python_lambda.code_visitor import FunctionCallVisitor
from frinx_worker.python_lambda.code_wrapper import get_indentation
from frinx_worker.python_lambda.code_wrapper import python_lambda_stringify
from frinx_worker.python_lambda.code_wrapper import sanitize_lines

VISITOR = FunctionCallVisitor()


class TestCodeVisitor:
    def test_import_exception(self) -> None:
        code = """
        import json
        json.dumps({"a":"b"})
        """

        sanitized_code = sanitize_lines(code, get_indentation(code))
        with pytest.raises(ValueError) as error:
            VISITOR.visit(ast.parse(sanitized_code))
        assert str(error.value) == 'Import statements are not allowed.'

    def test_from_import_exception(self) -> None:
        code = """
        from json import dumps
        dumps({"a":"b"})
        """

        sanitized_code = sanitize_lines(code, get_indentation(code))
        with pytest.raises(ValueError) as error:
            VISITOR.visit(ast.parse(sanitized_code))
        assert str(error.value) == 'Import from statements are not allowed.'

    def test_call_exception_print(self) -> None:
        code = """
        print("hello world")
        """

        sanitized_code = sanitize_lines(code, get_indentation(code))
        with pytest.raises(ValueError) as error:
            VISITOR.visit(ast.parse(sanitized_code))
        assert str(error.value) == 'Function print is not allowed.'

    def test_call_exception_open(self) -> None:
        code = """
        my_file = open("hello.txt", "r")
        print(my_file.read())
        """

        sanitized_code = sanitize_lines(code, get_indentation(code))
        with pytest.raises(ValueError) as error:
            VISITOR.visit(ast.parse(sanitized_code))
        assert str(error.value) == 'Function open is not allowed.'

    def test_attribute_exception(self) -> None:
        code = """
        variable = 5
        var_name = variable.__class__.__name__
        """

        sanitized_code = sanitize_lines(code, get_indentation(code))
        with pytest.raises(ValueError) as error:
            VISITOR.visit(ast.parse(sanitized_code))
        assert str(error.value) == 'Dunder operation __class__ is not allowed.'

    def test_name_exception(self) -> None:
        code = """
        __builtin__ = "not allowed"
        """

        sanitized_code = sanitize_lines(code, get_indentation(code))
        with pytest.raises(ValueError) as error:
            VISITOR.visit(ast.parse(sanitized_code))
        assert str(error.value) == 'Usage of __builtin__ is not allowed.'


class TestCodeWrapper:
    def test_python_lambda_execution(self) -> None:
        @python_lambda_stringify
        def execute(worker_input: dict[str, Any]) -> Any:
            return worker_input['username']

        python_lambda = PythonLambda()
        worker_inputs = python_lambda.WorkerInput(
            lambda_function=execute(),
            worker_inputs={'username': 'cisco'}
        )
        result = python_lambda.execute(worker_input=worker_inputs)

        assert result.status == TaskResultStatus.COMPLETED
        assert result.output == python_lambda.WorkerOutput(result='cisco')
        assert result.logs == []

    def test_python_lambda_execution_exception(self) -> None:
        @python_lambda_stringify
        def execute(worker_input: dict[str, Any]) -> Any:
            return worker_input['password']

        python_lambda = PythonLambda()
        worker_inputs = python_lambda.WorkerInput(
            lambda_function=execute(),
            worker_inputs={'username': 'cisco'}
        )

        with pytest.raises(KeyError) as error:
            python_lambda.execute(worker_input=worker_inputs)
        assert str(error.type) == "<class 'KeyError'>"
        assert str(error.value) == "'password'"
