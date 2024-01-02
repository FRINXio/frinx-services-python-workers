# Conductor Python Lambda worker for Frinx Machine

The python-lambda task helps define necessary logic in the workflow definition using a sandbox python executor.
- This task can be used as alternative for conductor inline task.
- Execute in controlled environment with strict restrictions and defined resource limits.
- Ideally to run as isolated worker.

## Worker Input Parameters
 
```python
SimpleTask(
    name=PythonLambda,
    task_reference_name='python_lambda',
    input_parameters=SimpleTaskInputParameters(
        root=dict(
            lambda_function=custom_python_code(),
            worker_inputs=workflow_inputs.custom_input.wf_input
        )
    )
)
```

#### lambda_function: 
- Must be defined as a **def**:
- Must have worker_input only as input parameter
- Must return value from def 
- Use **python_lambda_stringify** decorator

Example: 
 ```python
@python_lambda_stringify
def custom_python_code(worker_input: dict) -> Any:
    worker_input['IOS01'] = {"username": "cisco", "password": "cisco"}
    return worker_input



@python_lambda_stringify
def get_ids_from_dict(worker_input: dict) -> Any:
    ids = []
    for k in worker_input:
        ids.append(k.get('_id'))
    return ids
```

#### worker_inputs
- Dict[str, Any] type data 

Example: 
 ```json
 {
  "input": {
    "num": 10
  }
}
```

## Workflow example

```python
from typing import Any

from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField
from frinx.common.workflow.workflow import FrontendWFInputFieldType

from frinx_worker.python_lambda import PythonLambda
from frinx_worker.python_lambda.code_wrapper import python_lambda_stringify


class PythonLambdaWF(WorkflowImpl):
    name: str = 'Python_lambda_example'
    version: int = 1
    description: str = 'Python lambda task usage example'
    labels: list[str] = ['PYTHON']

    class WorkflowInput(WorkflowImpl.WorkflowInput):
        custom_input: WorkflowInputField = WorkflowInputField(
            name='custom_input',
            description='Custom input in dict format',
            frontend_default_value=None,
            type=FrontendWFInputFieldType.STRING
        )

    class WorkflowOutput(WorkflowImpl.WorkflowOutput):
        ...

    @staticmethod
    @python_lambda_stringify
    def custom_python_code_a(worker_input: dict) -> Any:
        worker_input['IOS01'] = {"username": "cisco", "password": "cisco"}
        return worker_input

    @staticmethod
    @python_lambda_stringify
    def custom_python_code_b(worker_input: dict) -> Any:
        return worker_input['IOS01']['username']

    def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
        task_a = SimpleTask(
            name=PythonLambda,
            task_reference_name='python_lambda_a',
            input_parameters=SimpleTaskInputParameters(
                root=dict(
                    lambda_function=self.custom_python_code_a(),
                    worker_inputs=workflow_inputs.custom_input.wf_input
                )
            )
        )

        task_b = SimpleTask(
            name=PythonLambda,
            task_reference_name='python_lambda_b',
            input_parameters=SimpleTaskInputParameters(
                root=dict(
                    lambda_function=self.custom_python_code_b(),
                    worker_inputs=task_a.output_ref('result')
                )
            )
        )

        self.tasks = [
            task_a,
            task_b
        ]
```

