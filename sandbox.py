"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from robot_framework.process import process
import os

# pylint: disable-next=unused-argum
orchestrator_connection = OrchestratorConnection(
    "TilsynsAppToKassen",
    os.getenv("OpenOrchestratorSQL"),
    os.getenv("OpenOrchestratorKey"),
    None,
    None
)

process(orchestrator_connection)