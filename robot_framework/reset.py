"""This module handles resetting the state of the computer so the robot can work with a clean slate.

For this robot the "state" is the F2 connection and the cached KontAKT
credentials. ``open_all`` opens them and returns a :class:`Client`; ``reset``
re-opens them, so the queue framework can reconnect on a retry instead of
reconnecting for every single document.

Der er ikke laengere en privilegeret konto. GO havde GOAdminUser, fordi sletning
af et journaliseret dokument kraevede at det foerst blev afmarkeret som sagsakt.
I F2 findes det problem ikke - et dokument kan slet ikke slettes, uanset hvem man
er (det HAR ingen slette-relation), saa der er ingenting for en privilegeret
konto at lave. Se ``process._udgaa_dokument``.
"""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from oomtm import f2 as oomtm_f2


class Client:
    """Live F2 connection + cached KontAKT credentials.

    Opened once per run by ``open_all`` and reused across every queue element:
    F2-klienten holder sit bearer-token og sit service index, saa en sag med
    2000 dokumenter logger ind én gang og ikke 2000 gange.
    """

    def __init__(self, orchestrator_connection: OrchestratorConnection):
        self._oc = orchestrator_connection
        # Hele F2-opsaetningen er ét sted, og et skifte til produktion er én
        # constant (F2Miljoe). Se oomtm.f2.Config.from_orchestrator.
        self.f2 = oomtm_f2.F2(
            oomtm_f2.Config.from_orchestrator(orchestrator_connection),
            log=orchestrator_connection.log_info)
        kontakt = orchestrator_connection.get_credential("KontAKTAPI")
        self.kontakt_base = kontakt.username
        self.kontakt_key = kontakt.password

    def default_unit_party_no(self) -> int | None:
        """Afdelingen, en sag journaliseres paa, naar KontAKT ikke sender en.

        MIDLERTIDIGT. Den rigtige kilde er sagens team i KontAKT, men
        koblingen team -> F2-afdeling findes ikke i databasen endnu (den venter
        paa afdelingstabellen og admin-siden). Indtil da: OO-constanten
        ``F2AfdelingStandard`` med afdelingens partynummer.

        Hvorfor afdelingen og ikke sagsbehandleren: "Ansvarlig enhed" i F2 er
        udledt af den ansvarliges organisation, saa en sag med en person som
        ansvarlig flytter afdeling, naar en vikar tager over - og robotten
        mister sin skriveret til akterne. Maalt: enhed som ansvarlig lykkedes
        15 af 15, en person 2 af 4.
        """
        try:
            raw = (self._oc.get_constant("F2AfdelingStandard").value or "").strip()
        except Exception:  # pylint: disable=broad-except
            return None
        try:
            return int(raw)
        except ValueError:
            self._oc.log_info(
                f"F2AfdelingStandard er {raw!r} og ikke et partynummer - "
                f"ignoreres.")
            return None


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
    orchestrator_connection.log_trace("Opening F2 connection.")
    return Client(orchestrator_connection)
