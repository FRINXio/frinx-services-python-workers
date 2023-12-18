import ast
import os
import subprocess
import sys
import typing
from typing import Any

from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.type_aliases import DictAny
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl

from frinx_worker.python_lambda import execute
from frinx_worker.python_lambda.utils import executed_func_template


class PythonLambda(WorkerImpl):

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
            custom_execution_commands=worker_input.lambda_function,
            custom_worker_inputs={**worker_input.worker_inputs}
        )

    @staticmethod
    def _create_subprocess(code: str) -> subprocess.Popen[bytes]:

        env: typing.Mapping[str, str] = {'PATH': os.environ.get('PATH', '/usr/local/lib/python3')}

        return subprocess.Popen(
            [sys.executable, execute.__file__, code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )

    def execute(self, worker_input: WorkerInput) -> TaskResult[Any]:
        code = self._wrap_lambda_func(worker_input)
        proc = self._create_subprocess(code)

        try:
            stdout, stderr = proc.communicate(timeout=50)
            if stderr:
                return TaskResult(
                    status=TaskResultStatus.FAILED,
                    logs=stderr.decode('ascii')
                )

            result = ast.literal_eval(str(stdout.decode('ascii')))
            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    result=result
                )
            )

        except subprocess.TimeoutExpired:
            proc.kill()
            raise Exception('Process timed out')
