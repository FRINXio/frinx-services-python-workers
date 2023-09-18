from frinx.common.conductor_enums import TimeoutPolicy
from frinx.common.type_aliases import ListStr
from frinx.common.workflow.service import ServiceWorkflowsImpl
from frinx.common.workflow.task import DynamicForkTask
from frinx.common.workflow.task import DynamicForkTaskInputParameters
from frinx.common.workflow.task import JoinTask
from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import FrontendWFInputFieldType
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField

from .. import TestWorker


class TestWorkflows(ServiceWorkflowsImpl):
    class TestWorkflow(WorkflowImpl):
        name: str = 'Test_workflow'
        version: int = 1
        description: str = 'Test workflow built from test workers'
        labels: ListStr = ['TEST']
        timeout_seconds: int = 60 * 5
        timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            num_paragraphs: WorkflowInputField = WorkflowInputField(
                name='num_paragraphs',
                frontend_default_value=10,
                description='Paragraphs to generate',
                type=FrontendWFInputFieldType.INT,
            )

            num_sentences: WorkflowInputField = WorkflowInputField(
                name='num_sentences',
                frontend_default_value=10,
                description='Sentences to generate per paragraph',
                type=FrontendWFInputFieldType.INT,
            )

            num_words: WorkflowInputField = WorkflowInputField(
                name='num_words',
                frontend_default_value=10,
                description='Words to generate per sentence',
                type=FrontendWFInputFieldType.INT,
            )

            sleep_time: WorkflowInputField = WorkflowInputField(
                name='sleep_time',
                frontend_default_value=10,
                description='How many seconds to sleep during the workflow',
                type=FrontendWFInputFieldType.INT,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            text: str
            bytes: int

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            generate_task = SimpleTask(
                name=TestWorker.LoremIpsum,
                task_reference_name='generate',
                input_parameters=SimpleTaskInputParameters(
                    num_paragraphs=workflow_inputs.num_paragraphs.wf_input,
                    num_sentences=workflow_inputs.num_sentences.wf_input,
                    num_words=workflow_inputs.num_words.wf_input,
                ),
            )
            self.tasks.append(generate_task)

            self.tasks.append(
                SimpleTask(
                    name=TestWorker.Sleep,
                    task_reference_name='sleep',
                    input_parameters=SimpleTaskInputParameters(
                        time=workflow_inputs.sleep_time.wf_input
                    ),
                )
            )

            echo_task = SimpleTask(
                name=TestWorker.Echo,
                task_reference_name='echo',
                input_parameters=SimpleTaskInputParameters(input=generate_task.output_ref('text')),
            )
            self.tasks.append(echo_task)

            self.output_parameters['text'] = echo_task.output_ref('output')
            self.output_parameters['bytes'] = generate_task.output_ref('bytes')

    class TestForkWorkflow(WorkflowImpl):
        name: str = 'Test_fork_workflow'
        version: int = 1
        description: str = 'Test workflows executed in a parallel, dynamic fork'
        labels: ListStr = ['TEST']
        timeout_seconds: int = 60 * 5
        timeout_policy: TimeoutPolicy = TimeoutPolicy.TIME_OUT_WORKFLOW

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            fork_count: WorkflowInputField = WorkflowInputField(
                name='fork_count',
                frontend_default_value=10,
                description='How many forks to execute in parallel',
                type=FrontendWFInputFieldType.INT,
            )

            num_paragraphs: WorkflowInputField = WorkflowInputField(
                name='num_paragraphs',
                frontend_default_value=10,
                description='Paragraphs to generate',
                type=FrontendWFInputFieldType.INT,
            )

            num_sentences: WorkflowInputField = WorkflowInputField(
                name='num_sentences',
                frontend_default_value=10,
                description='Sentences to generate per paragraph',
                type=FrontendWFInputFieldType.INT,
            )

            num_words: WorkflowInputField = WorkflowInputField(
                name='num_words',
                frontend_default_value=10,
                description='Words to generate per sentence',
                type=FrontendWFInputFieldType.INT,
            )

            sleep_time: WorkflowInputField = WorkflowInputField(
                name='sleep_time',
                frontend_default_value=10,
                description='How many seconds to sleep during the workflow',
                type=FrontendWFInputFieldType.INT,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            pass

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            self.tasks.append(
                SimpleTask(
                    name=TestWorker.DynamicForkGenerator,
                    task_reference_name='fork_generator',
                    input_parameters=SimpleTaskInputParameters(
                        wf_count=workflow_inputs.fork_count.wf_input,
                        wf_name='Test_workflow',
                        wf_inputs={
                            'num_words': workflow_inputs.num_words.wf_input,
                            'num_sentences': workflow_inputs.num_sentences.wf_input,
                            'num_paragraphs': workflow_inputs.num_paragraphs.wf_input,
                            'sleep_time': workflow_inputs.sleep_time.wf_input,
                        },
                    ),
                )
            )

            self.tasks.append(
                DynamicForkTask(
                    name='dyn_fork',
                    task_reference_name='dyn_fork',
                    dynamic_fork_tasks_param='dynamic_tasks',
                    dynamic_fork_tasks_input_param_name='dynamic_tasks_input',
                    input_parameters=DynamicForkTaskInputParameters(
                        dynamic_tasks='${fork_generator.output.dynamic_tasks}',
                        dynamic_tasks_input='${fork_generator.output.dynamic_tasks_i}',
                    ),
                )
            )

            self.tasks.append(JoinTask(name='dyn_fork_join', task_reference_name='dyn_fork_join'))
