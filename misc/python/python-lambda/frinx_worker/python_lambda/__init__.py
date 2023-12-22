import ast
from typing import Any

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from timeout_decorator import timeout

from frinx_worker.python_lambda.code_visitor import FunctionCallVisitor
from frinx_worker.python_lambda.code_wrapper import executed_func_template


class PythonLambda(WorkerImpl):

    VISITOR = FunctionCallVisitor()

    class ExecutionProperties(TaskExecutionProperties):
        exclude_empty_inputs: bool = True
        transform_string_to_json_valid: bool = True
        pass_worker_input_exception_to_task_output: bool = True

    class WorkerDefinition(TaskDefinition):
        name: str = 'PYTHON_lambda'
        description: str = 'Execute simple python code'
        labels: list[str] = ['PYTHON']
        timeout_seconds: int = 60
        response_timeout_seconds: int = 59

    class WorkerInput(TaskInput):
        lambda_function: str
        worker_inputs: DictAny

    class WorkerOutput(TaskOutput):
        result: Any

    @staticmethod
    def _wrap_lambda_func(worker_input: WorkerInput) -> str:
        return executed_func_template.format(
            custom_execution_commands=worker_input.lambda_function
        )

    @classmethod
    def _validate_code(cls, code: str) -> None:
        cls.VISITOR.visit(ast.parse(code))

    @staticmethod
    @timeout(5)  # type: ignore[misc]
    def _process_code(code: str, worker_inputs: DictAny) -> DictAny:
        ex_locals: dict[str, Any] = {'worker_input': worker_inputs}
        exec(code, None, ex_locals)
        return ex_locals.get('result', {})

    def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
        code = self._wrap_lambda_func(worker_input)
        self._validate_code(code)

        result = self._process_code(code, worker_input.worker_inputs)
        return TaskResult(
            status=TaskResultStatus.COMPLETED,
            output=self.WorkerOutput(
                result=result
            )
        )
