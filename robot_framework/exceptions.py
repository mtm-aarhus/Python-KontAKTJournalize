"""This module contains various functions and classes to handle errors in the framework."""

import traceback

from OpenOrchestrator.database.queues import QueueElement, QueueStatus
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from robot_framework import config
from robot_framework import error_screenshot


class BusinessError(Exception):
    """An empty exception used to identify errors caused by breaking business rules"""


class CaseDeleted(Exception):
    """What this queue element was about no longer exists in KontAKT.

    KontAKT answers HTTP 410 with ``{"case_deleted": true}`` (or
    ``{"document_deleted": true}``) when the caseworker deleted the case, mappe
    or document while the element waited in the queue. There is nothing to do
    and nothing to fix, so the queue framework marks the element DONE instead of
    FAILED: no retry, no error screenshot.
    """


def handle_error(message: str, error: Exception, queue_element: QueueElement | None, orchestrator_connection: OrchestratorConnection) -> None:
    """Handles an error caught during the process.
    Logs an error to OpenOrchestrator.
    Marks the queue element (if any) as failed.
    Sends an error screenshot by email.

    Args:
        message: A message to prepend to the error message.
        error: The exception that should be handled.
        queue_element: The queue element to fail, if any.
        orchestrator_connection: A connection to OpenOrchestrator.
    """
    error_msg = f"{message}: {repr(error)}\n\nTrace:\n{traceback.format_exc()}"
    error_msg = error_msg[:490]+error_msg[-500:]
    error_email = orchestrator_connection.get_constant(config.ERROR_EMAIL).value

    orchestrator_connection.log_error(error_msg)
    if queue_element:
        orchestrator_connection.set_queue_element_status(queue_element.id, QueueStatus.FAILED, error_msg[:999])
    error_screenshot.send_error_screenshot(error_email, error, orchestrator_connection.process_name)


def log_exception(orchestrator_connection: OrchestratorConnection) -> callable:
    """Creates a function to be used as an exception hook that logs any uncaught exception in OpenOrchestrator.

    Args:
        orchestrator_connection: The connection to OpenOrchestrator.

    Returns:
        callable: A function that can be assigned to sys.excepthook.
    """
    def inner(exception_type, value, traceback_string):
        orchestrator_connection.log_error(f"Uncaught Exception:\nType: {exception_type}\nValue: {value}\nTrace: {traceback_string}")
    return inner
