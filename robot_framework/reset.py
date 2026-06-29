"""This module handles resetting the state of the computer so the robot can work with a clean slate.

For this robot the "state" is the set of remote connections (GO + SharePoint)
and the cached OO credentials. ``open_all`` opens them and returns a
:class:`Client`; ``reset`` re-opens them, so the queue framework can reconnect
on a retry instead of reconnecting for every single document.
"""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from oomtm import go as oomtm_go
from oomtm import sharepoint as sp


class Client:
    """Live GO + SharePoint connections + cached KontAKT credentials.

    Opened once per run by ``open_all`` and reused across every queue element,
    so a 2000-document case doesn't re-authenticate to SharePoint 2000 times.
    """

    def __init__(self, orchestrator_connection: OrchestratorConnection):
        go_cred = orchestrator_connection.get_credential("GOAktApiUser")
        self.go_url = orchestrator_connection.get_constant("GOApiURL").value
        self.go_user = go_cred.username
        self.go_pass = go_cred.password
        self.go_session = oomtm_go.session(go_cred.username, go_cred.password)
        self.sp_ctx, self.sp_site_url = _build_sp_context(orchestrator_connection)
        kontakt = orchestrator_connection.get_credential("KontAKTAPI")
        self.kontakt_base = kontakt.username
        self.kontakt_key = kontakt.password


def reset(orchestrator_connection: OrchestratorConnection) -> Client:
    """Clean up, close/kill all programs, then (re)open the connections.

    Returns the freshly-opened :class:`Client` so the queue framework can reuse
    it across queue elements (and reconnect by calling ``reset`` again)."""
    orchestrator_connection.log_trace("Resetting.")
    clean_up(orchestrator_connection)
    close_all(orchestrator_connection)
    kill_all(orchestrator_connection)
    return open_all(orchestrator_connection)


def clean_up(orchestrator_connection: OrchestratorConnection) -> None:
    """Do any cleanup needed to leave a blank slate."""
    orchestrator_connection.log_trace("Doing cleanup.")


def close_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Gracefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Closing all applications.")


def kill_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Forcefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Killing all applications.")


def open_all(orchestrator_connection: OrchestratorConnection) -> Client:
    """Open all connections used by the robot and return them as a :class:`Client`."""
    orchestrator_connection.log_trace("Opening GO + SharePoint connections.")
    return Client(orchestrator_connection)


# ----- SharePoint context ----------------------------------------------------


def _build_sp_context(orchestrator_connection):
    cert = orchestrator_connection.get_credential("SharePointCert")  # user=thumbprint, pwd=cert_path
    api = orchestrator_connection.get_credential("SharePointAPI")     # user=tenant,    pwd=client_id
    raw = (orchestrator_connection.get_constant("KontAKTSharePoint").value or "").strip().rstrip("/")
    # The constant may include the document library; ClientContext needs the
    # site root only. Strip a trailing "/Delte dokumenter" (any encoding).
    for suffix in ("/Delte dokumenter", "/Delte%20dokumenter"):
        if raw.lower().endswith(suffix.lower()):
            raw = raw[: -len(suffix)]
    site_url = raw.rstrip("/")
    ctx = sp.connect(
        site_url=site_url, tenant=api.username, client_id=api.password,
        thumbprint=cert.username, cert_path=cert.password,
    )
    return ctx, site_url
