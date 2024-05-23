from frinx.common.workflow.service import ServiceWorkflowsImpl
from frinx.common.workflow.task import SimpleTask
from frinx.common.workflow.task import SimpleTaskInputParameters
from frinx.common.workflow.workflow import FrontendWFInputFieldType
from frinx.common.workflow.workflow import WorkflowImpl
from frinx.common.workflow.workflow import WorkflowInputField
from pydantic import Field

from frinx_worker.uniconfig.transaction_helpers import TransactionHelpers


class TransactionWorkflows(ServiceWorkflowsImpl):
    class CloseTransactionsWorkflow(WorkflowImpl):
        name: str = "close_transactions"
        version: int = 1
        description: str = "Close UniConfig transactions that left open after end of the input workflow."
        labels: list[str] = ["UNICONFIG"]
        timeout_seconds: int = 300

        class WorkflowInput(WorkflowImpl.WorkflowInput):
            workflow_id: WorkflowInputField = WorkflowInputField(
                name="workflowId",
                description="ID of the workflow to rollback.",
                type=FrontendWFInputFieldType.STRING,
            )

        class WorkflowOutput(WorkflowImpl.WorkflowOutput):
            closed_transactions: str = Field(
                description="List of successfully closed UniConfig transactions in this workflow."
            )

        def workflow_builder(self, workflow_inputs: WorkflowInput) -> None:
            find_open_transactions = SimpleTask(
                name=TransactionHelpers.FindOpenTransactions,
                task_reference_name="find_open_transactions",
                description="Find transactions that have been created in the workflow (excl. sub-workflows) "
                            "but are still open.",
                input_parameters=SimpleTaskInputParameters(
                    root=dict(
                        workflow_id=workflow_inputs.workflow_id.wf_input,
                    )
                )
            )

            close_transactions = SimpleTask(
                name=TransactionHelpers.CloseTransactions,
                task_reference_name="close_transactions",
                description="Try to close all input UniConfig transactions.",
                optional=True,
                input_parameters=SimpleTaskInputParameters(
                    root=dict(
                        uniconfig_transactions=find_open_transactions.output_ref("uniconfig_transactions"),
                    )
                )
            )

            self.tasks = [
                find_open_transactions,
                close_transactions
            ]
            self.output_parameters = self.WorkflowOutput(
                closed_transactions=close_transactions.output_ref("closed_transactions")
            ).model_dump()
