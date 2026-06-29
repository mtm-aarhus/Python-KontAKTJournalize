"""KontAKT → GetOrganized journalisation robot.

Queue-driven. Keeps a GO "AKT" journaliseringssag in sync with a KontAKT case and
journalises the correspondence + the final delivery onto it. Mode is set by
``mode`` in the payload:

* ``create_case``     — create the AKT case in GO with the case profile metadata
                        (Sagsprofil = MTM Aktindsigt, Facet = A53, Modtaget,
                        Title = Emne) and set the caseworker as CaseOwner. Calls
                        back with the new GO case number.
* ``update_metadata`` — update Title / Modtaget / CaseOwner when they change in
                        KontAKT.
* ``journalize_email``— fetch a sent e-mail from KontAKT, render it to PDF and
                        add + journalise it on the GO case.
* ``finalize``        — upload every delivered SharePoint file to the GO case,
                        journalise them, and close the GO case. Calls back so
                        KontAKT closes the case too.

The GO + SharePoint connections and the cached KontAKT credentials live on the
``Client`` opened in ``reset.open_all`` and are reused across queue elements.

OO config (same as the other KontAKT GO robots):
    Constant   GOApiURL          — GO base URL (e.g. https://ad.go.aarhuskommune.dk)
    Credential GOAktApiUser      — GO NTLM username + password
    Constant   KontAKTSharePoint — SharePoint site URL (delivery library)
    Credential SharePointCert    — username = thumbprint, password = cert path
    Credential SharePointAPI     — username = tenant,     password = client id
    Credential KontAKTAPI        — username = base URL,    password = X-API-Key
"""
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
from urllib.parse import unquote, urlparse
import json
import os
import tempfile

import requests

from robot_framework import reset
from oomtm import go as oomtm_go
from oomtm import pdf as oomtm_pdf
from oomtm import sharepoint as sp

# The AKT cases live under the /aktindsigt web (the caseworker's proven create
# endpoint is …/aktindsigt/_goapi/Cases). Metadata/upload/close route by CaseId,
# so they work from the root web.
GO_AKT_WEB = "/aktindsigt"

# Fixed case-profile metadata for an MTM aktindsigt journaliseringssag (the
# GUID-paired term-store values the caseworker supplied).
SAGSPROFIL_AKT = "165;#MTM Aktindsigt"
SAGSPROFIL_AKT_TERM = "MTM Aktindsigt|b0976f16-ee0b-4d5f-8755-1048d4b796ba"
SAGSPROFIL_AKT_FIELD = "pc53b1eb189a451bbe8688ffaa073059"
FACET = "4;#A53 Aktindsigtsanmodning mv."
FACET_TERM = "A53 Aktindsigtsanmodning mv.|db5714c1-9346-47e6-b7a7-2230bf997699"
FACET_FIELD = "hd725939cd4d495483312d36ba720a4d"


def process(
    orchestrator_connection: OrchestratorConnection,
    queue_element: QueueElement | None = None,
    client: "reset.Client | None" = None,
) -> None:
    orchestrator_connection.log_trace("Running process.")
    if queue_element is None:
        raise RuntimeError("KontAKTJournalize is queue-driven; no queue_element given.")
    if client is None:  # manual run outside the queue framework
        client = reset.open_all(orchestrator_connection)

    payload = json.loads(queue_element.data or "{}")
    mode = (payload.get("mode") or "").strip()
    case_id = int(payload["kontakt_case_id"])

    if mode == "create_case":
        _create_case(orchestrator_connection, client, case_id, payload)
    elif mode == "update_metadata":
        _update_metadata(orchestrator_connection, client, case_id, payload)
    elif mode == "journalize_email":
        _journalize_email(orchestrator_connection, client, case_id, payload)
    elif mode == "finalize":
        _finalize(orchestrator_connection, client, case_id, payload)
    else:
        raise RuntimeError(f"Unknown KontAKTJournalize mode: {mode!r}")


# ----- create_case -----------------------------------------------------------


def _create_case(oc, client, case_id, payload):
    path = f"/api/v1/cases/{case_id}/go-journal/created"
    title = str(payload.get("title") or "").strip() or f"Aktindsigt (KontAKT-sag {case_id})"
    modtaget = str(payload.get("modtaget") or "").strip()
    caseworker_email = str(payload.get("caseworker_email") or "").strip()
    oc.log_info(f"GO create_case for KontAKT case {case_id}: {title!r}")
    try:
        metadata_xml = _create_metadata_xml(title, modtaget)
        created = oomtm_go.create_case(
            client.go_session, base_url=client.go_url,
            metadata_xml=metadata_xml, case_type_prefix="AKT", web=GO_AKT_WEB,
        )
        go_case_no = created.get("CaseID")
        relative_url = created.get("CaseRelativeUrl")
        oc.log_info(f"GO case created: {go_case_no} ({relative_url})")
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO create_case failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "note": "GO returnerede intet CaseID."})
        return

    owner_note = _try_set_owner(oc, client, go_case_no, caseworker_email, relative_url)
    _callback(oc, client, path, {"ok": True, "go_case_no": go_case_no, "note": owner_note})
    oc.log_info(f"GO create_case done: {go_case_no}")


# ----- update_metadata -------------------------------------------------------


def _update_metadata(oc, client, case_id, payload):
    path = f"/api/v1/cases/{case_id}/go-journal/updated"
    go_case_no = str(payload.get("go_case_no") or "").strip()
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "note": "Mangler GO-sagsnummer."})
        return
    title = payload.get("title")
    modtaget = payload.get("modtaget")
    caseworker_email = str(payload.get("caseworker_email") or "").strip()
    oc.log_info(f"GO update_metadata for {go_case_no} (case {case_id})")
    try:
        xml = _update_metadata_xml(title, modtaget)
        if xml:
            oomtm_go.set_case_metadata(client.go_session, base_url=client.go_url,
                                       case_id=go_case_no, metadata_xml=xml)
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO update_metadata failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    owner_note = _try_set_owner(oc, client, go_case_no, caseworker_email) if caseworker_email else None
    _callback(oc, client, path, {"ok": True, "note": owner_note})
    oc.log_info(f"GO update_metadata done: {go_case_no}")


# ----- journalize_email ------------------------------------------------------


def _journalize_email(oc, client, case_id, payload):
    email_id = int(payload["email_id"])
    go_case_no = str(payload.get("go_case_no") or "").strip()
    path = f"/api/v1/cases/{case_id}/emails/{email_id}/journalized"
    oc.log_info(f"GO journalize_email email={email_id} -> {go_case_no}")
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "note": "Sagen er endnu ikke oprettet i GO."})
        return
    try:
        email = _kontakt_get(client, f"/api/v1/cases/{case_id}/emails/{email_id}")
        pdf_bytes, file_name = _email_to_pdf(oc, email)
        meta = _doc_metadata_xml(
            title=(email.get("subject") or f"E-mail {email_id}"),
            date=_date_only(email.get("sent_at")),
            korrespondance="Udgående" if email.get("direction") == "outbound" else "Indgående",
        )
        doc_id = oomtm_go.upload_document(
            client.go_session, base_url=client.go_url, case_id=go_case_no,
            file_bytes=pdf_bytes, file_name=file_name, metadata_xml=meta,
        )
        if doc_id:
            oomtm_go.mark_as_case_record(client.go_session, base_url=client.go_url, doc_ids=[doc_id])
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO journalize_email failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    _callback(oc, client, path, {"ok": True, "doc_id": doc_id})
    oc.log_info(f"GO journalize_email done email={email_id} doc={doc_id}")


# ----- finalize --------------------------------------------------------------


def _finalize(oc, client, case_id, payload):
    path = f"/api/v1/cases/{case_id}/go-journal/finalized"
    go_case_no = str(payload.get("go_case_no") or "").strip()
    oc.log_info(f"GO finalize case {case_id} -> {go_case_no}")
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "note": "Sagen er endnu ikke oprettet i GO."})
        return
    try:
        data = _kontakt_get(client, f"/api/v1/cases/{case_id}/delivery-files")
        files = data.get("files") or []
        doc_ids = []
        with tempfile.TemporaryDirectory() as tmp:
            for f in files:
                url = (f.get("sharepoint_url") or "").strip()
                name = (f.get("file_name") or "").strip() or os.path.basename(urlparse(url).path)
                if not url:
                    continue
                local = os.path.join(tmp, _safe_name(name))
                server_rel = unquote(urlparse(url).path)
                sp.download_file(client.sp_ctx, file_path=server_rel, local_path=local)
                with open(local, "rb") as fh:
                    file_bytes = fh.read()
                meta = _doc_metadata_xml(title=os.path.splitext(name)[0], korrespondance="Udgående")
                doc_id = oomtm_go.upload_document(
                    client.go_session, base_url=client.go_url, case_id=go_case_no,
                    file_bytes=file_bytes, file_name=name, metadata_xml=meta,
                )
                if doc_id:
                    doc_ids.append(doc_id)
        if doc_ids:
            oomtm_go.mark_as_case_record(client.go_session, base_url=client.go_url, doc_ids=doc_ids)
        oomtm_go.close_case(client.go_session, base_url=client.go_url, case_id=go_case_no)
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO finalize failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    _callback(oc, client, path, {"ok": True, "doc_count": len(doc_ids)})
    oc.log_info(f"GO finalize done: {len(doc_ids)} dokument(er) journaliseret, sag lukket.")


# ----- metadata XML builders -------------------------------------------------


def _xml_attr(value) -> str:
    """Escape a value for use inside a double-quoted XML attribute."""
    return (str(value or "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def _create_metadata_xml(title: str, modtaget: str) -> str:
    parts = [
        f'ows_Title="{_xml_attr(title)}"',
        'ows_CaseStatus="Åben"',
        f'ows_Sagsprofil_AKT="{_xml_attr(SAGSPROFIL_AKT)}"',
        f'ows_{SAGSPROFIL_AKT_FIELD}="{_xml_attr(SAGSPROFIL_AKT_TERM)}"',
        f'ows_Facet="{_xml_attr(FACET)}"',
        f'ows_{FACET_FIELD}="{_xml_attr(FACET_TERM)}"',
    ]
    if modtaget:
        parts.append(f'ows_Modtaget="{_xml_attr(modtaget)}"')
    return '<z:row xmlns:z="#RowsetSchema" ' + " ".join(parts) + " />"


def _update_metadata_xml(title, modtaget) -> str | None:
    parts = []
    if title:
        parts.append(f'ows_Title="{_xml_attr(title)}"')
    if modtaget:
        parts.append(f'ows_Modtaget="{_xml_attr(modtaget)}"')
    if not parts:
        return None
    return '<z:row xmlns:z="#RowsetSchema" ' + " ".join(parts) + " />"


def _doc_metadata_xml(*, title: str, date: str = "", korrespondance: str = "") -> str:
    parts = [f'ows_Title="{_xml_attr(title)}"']
    if date:
        parts.append(f'ows_Dato="{_xml_attr(date)}"')
    if korrespondance:
        parts.append(f'ows_Korrespondance="{_xml_attr(korrespondance)}"')
    parts.append('ows_CCMMustBeOnPostList="0"')
    return '<z:row xmlns:z="#RowsetSchema" ' + " ".join(parts) + " />"


# ----- e-mail rendering ------------------------------------------------------


def _email_to_pdf(oc, email: dict):
    """Render a KontAKT e-mail to PDF bytes via LibreOffice (html → pdf).
    Returns (pdf_bytes, file_name)."""
    soffice = oomtm_pdf.ensure_libreoffice(log=oc.log_info)
    subject = email.get("subject") or "E-mail"
    sent = _date_only(email.get("sent_at"))
    header = (
        f"<p style='color:#555;font-size:12px'>"
        f"<b>Fra:</b> {_xml_attr(email.get('from_address') or '')}<br>"
        f"<b>Til:</b> {_xml_attr(email.get('to_addresses') or '')}<br>"
        f"<b>Sendt:</b> {_xml_attr(email.get('sent_at') or '')}<br>"
        f"<b>Emne:</b> {_xml_attr(subject)}</p><hr>"
    )
    body = email.get("body_html") or (
        "<pre>" + _xml_attr(email.get("body_text") or "") + "</pre>"
    )
    html = (f"<!doctype html><html><head><meta charset='utf-8'></head>"
            f"<body>{header}{body}</body></html>")
    with tempfile.TemporaryDirectory() as tmp:
        html_path = os.path.join(tmp, "email.html")
        with open(html_path, "w", encoding="utf-8") as fh:
            fh.write(html)
        pdf_path = oomtm_pdf.office_to_pdf(html_path, tmp, soffice_path=soffice)
        if pdf_path is None:
            raise RuntimeError("LibreOffice kunne ikke konvertere e-mailen til PDF.")
        with open(pdf_path, "rb") as fh:
            pdf_bytes = fh.read()
    file_name = _safe_name(f"E-mail {sent} - {subject}")[:120] + ".pdf"
    return pdf_bytes, file_name


# ----- caseowner (best-effort) -----------------------------------------------


def _try_set_owner(oc, client, go_case_no, caseworker_email, case_relative_url=None) -> str | None:
    """Set CaseOwner from the caseworker e-mail. Best-effort: never fails the
    job. ``case_relative_url`` is the create response's CaseRelativeUrl (on a
    fresh create); on a later update it's resolved from ows_CaseUrl. Returns a
    short note when it couldn't be set, else None."""
    if not caseworker_email:
        return None
    try:
        ok = oomtm_go.set_case_owner(
            client.go_session, base_url=client.go_url, case_id=go_case_no,
            caseworker_email=caseworker_email, case_relative_url=case_relative_url,
        )
        if not ok:
            return f"Sagsbehandler {caseworker_email} blev ikke fundet i GO."
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"set_case_owner failed (non-fatal): {exc!r}")
        return f"CaseOwner kunne ikke sættes automatisk ({str(exc)[:120]})."
    return None


# ----- helpers ---------------------------------------------------------------


def _date_only(value) -> str:
    """'2026-06-25T10:58:00' / '2026-06-25 10:58:00' -> '25-06-2026' for GO."""
    s = str(value or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return f"{s[8:10]}-{s[5:7]}-{s[0:4]}"
    return s[:10]


def _safe_name(name: str) -> str:
    keep = []
    for ch in str(name or ""):
        keep.append(ch if ch not in '\\/:*?"<>|' else " ")
    return " ".join("".join(keep).split()) or "dokument"


def _kontakt_get(client, path: str) -> dict:
    r = requests.get(
        f"{client.kontakt_base}{path}",
        headers={"X-API-Key": client.kontakt_key, "Accept": "application/json"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _callback(oc, client, path: str, body: dict) -> None:
    try:
        requests.post(
            f"{client.kontakt_base}{path}",
            headers={"X-API-Key": client.kontakt_key, "Content-Type": "application/json"},
            json=body, timeout=30,
        )
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"Callback to KontAKT failed: {exc!r}")
