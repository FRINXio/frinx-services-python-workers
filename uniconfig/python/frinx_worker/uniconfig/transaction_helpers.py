import logging

import requests
from frinx.common.conductor_enums import TaskResultStatus
from frinx.common.frinx_rest import CONDUCTOR_HEADERS
from frinx.common.frinx_rest import CONDUCTOR_URL_BASE
from frinx.common.frinx_rest import UNICONFIG_HEADERS
from frinx.common.type_aliases import DictAny
from frinx.common.type_aliases import ListStr
from frinx.common.worker.service import ServiceWorkersImpl
from frinx.common.worker.task_def import TaskDefinition
from frinx.common.worker.task_def import TaskExecutionProperties
from frinx.common.worker.task_def import TaskInput
from frinx.common.worker.task_def import TaskOutput
from frinx.common.worker.task_result import TaskResult
from frinx.common.worker.worker import WorkerImpl
from frinx.common.workflow.task import HttpMethod
from pydantic import BaseModel
from pydantic import Field

from frinx_worker.uniconfig import uniconfig_zone_to_cookie
from frinx_worker.uniconfig.uniconfig_manager import UniconfigManager

logger = logging.getLogger(__name__)


class UniconfigTransactionContext(BaseModel):
    transaction_id: str = Field(description="UniConfig transaction ID in form of UUID.")
    uniconfig_url_base: str = Field(description="Base URL of the UniConfig service in the zone.")
    uniconfig_server_id: str | None = Field(description="Identifier of the UniConfig instance.", default=None)


class TransactionHelpers(ServiceWorkersImpl):
    class FindOpenTransactions(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_find_open_transactions"
            description: str = "Find open transactions that have been created in the workflow (excl. sub-workflows)."
            limit_to_thread_count: int = 1
            labels: ListStr = ["UNICONFIG"]

        class WorkerInput(TaskInput):
            workflow_id: str = Field(description="Identifier of the executed workflow.")
            conductor_url_base: str = Field(
                description="Base URL of the Conductor service.",
                default=CONDUCTOR_URL_BASE
            )

        class WorkerOutput(TaskOutput):
            uniconfig_transactions: list[UniconfigTransactionContext] = Field(
                description="List of open UniConfig transactions."
            )

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            response = requests.request(
                url=f"{worker_input.conductor_url_base}/workflow/{worker_input.workflow_id}",
                method=HttpMethod.GET,
                headers=dict(CONDUCTOR_HEADERS),
            )
            tasks = response.json()["tasks"]

            create_transaction_tasks = filter(
                lambda task: self._is_create_transaction_task(task),
                tasks
            )
            uniconfig_transactions = map(
                lambda task: self._create_transaction_context(task["outputData"]),
                create_transaction_tasks
            )
            open_uniconfig_transactions = filter(
                lambda tx: self._is_open_transaction(tx),
                uniconfig_transactions
            )

            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    uniconfig_transactions=list(open_uniconfig_transactions),
                )
            )

        @staticmethod
        def _is_create_transaction_task(task: DictAny) -> bool:
            return bool(task.get("taskType") == "UNICONFIG_Create_transaction_RPC"
                        and task.get("status") == "COMPLETED")

        @staticmethod
        def _create_transaction_context(create_transaction_output: DictAny) -> UniconfigTransactionContext:
            return UniconfigTransactionContext(
                transaction_id=create_transaction_output["transaction_id"],
                uniconfig_url_base=create_transaction_output["uniconfig_url_base"],
                uniconfig_server_id=create_transaction_output.get("uniconfig_server_id", None),
            )

        @staticmethod
        def _is_open_transaction(transaction: UniconfigTransactionContext) -> bool:
            cookies = uniconfig_zone_to_cookie(
                # we must send the request to the same uniconfig instance where the transaction was created
                uniconfig_server_id=transaction.uniconfig_server_id,
            )
            uri = (f"{transaction.uniconfig_url_base}/data/transactions-data/"
                   f"transaction-data={transaction.transaction_id}?content=nonconfig")
            response = requests.request(
                url=uri,
                method=HttpMethod.GET,
                headers=dict(UNICONFIG_HEADERS),
                cookies=cookies
            )
            return True if response.ok else False

    class CloseTransactions(WorkerImpl):
        class ExecutionProperties(TaskExecutionProperties):
            exclude_empty_inputs: bool = True
            transform_string_to_json_valid: bool = True

        class WorkerDefinition(TaskDefinition):
            name: str = "UNICONFIG_close_transactions"
            description: str = "Try to close all input UniConfig transactions."
            limit_to_thread_count: int = 1
            labels: ListStr = ["UNICONFIG"]

        class WorkerInput(TaskInput):
            uniconfig_transactions: list[UniconfigTransactionContext] = Field(
                description="List of UniConfig transactions to be closed by this worker.",
                default=[]
            )

        class WorkerOutput(TaskOutput):
            closed_transactions: list[UniconfigTransactionContext] = Field(
                description="List of successfully closed UniConfig transactions."
            )

        def execute(self, worker_input: WorkerInput) -> TaskResult[WorkerOutput]:
            failed_operations_logs: list[str] = []
            closed_transactions: list[UniconfigTransactionContext] = []

            for transaction in worker_input.uniconfig_transactions:
                close_transaction_input = UniconfigManager.CloseTransaction.WorkerInput(
                    uniconfig_url_base=transaction.uniconfig_url_base,
                    transaction_id=transaction.transaction_id,
                    uniconfig_server_id=transaction.uniconfig_server_id
                )
                result = UniconfigManager.CloseTransaction().execute(close_transaction_input)

                if result.status == TaskResultStatus.COMPLETED:
                    logger.info(f"Successfully closed transaction {transaction.transaction_id}")
                    closed_transactions.append(transaction)
                else:
                    logger.warning(f"Failed to close transaction {transaction.transaction_id}")
                    failed_operations_logs.append(result.logs)

            if failed_operations_logs:
                return TaskResult(
                    status=TaskResultStatus.FAILED,
                    logs=failed_operations_logs,
                    output=self.WorkerOutput(
                        closed_transactions=closed_transactions
                    )
                )
            return TaskResult(
                status=TaskResultStatus.COMPLETED,
                output=self.WorkerOutput(
                    closed_transactions=closed_transactions
                )
            )
