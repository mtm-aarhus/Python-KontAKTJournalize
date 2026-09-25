"""KontAKT -> F2 journalisation robot.

Queue-driven. Keeps an aktindsigtssag in F2 in sync with a KontAKT case and
journalises the correspondence + the final delivery onto it. Mode is set by
``mode`` in the payload:

* ``create_case``     - create the case in F2 (KLE 00.07.03, handlingsfacet A53,
                        undtaget fra postliste) with the team's department as
                        responsible. Calls back with the case number AND the
                        address F2 itself gives the case (``f2t://case/<id>``).
* ``update_metadata`` - Emne / frist / afdeling changed in KontAKT.
* ``journalize_email``- one message becomes one akt. The e-mail body IS the
                        akt's text; no PDF is rendered.
* ``sync_f2``         - the reconciliation: bring the delivered documents and
                        the aktliste in step with what KontAKT actually hands
                        over. KontAKT computes the plan; this robot executes it.
* ``delete_doc``      - a document was withdrawn in KontAKT.

HVAD DER ER ANDERLEDES END GO-UDGAVEN, OG HVORFOR

    INTET LIBREOFFICE. GO kunne kun tage filer, saa en mail skulle renderes til
        PDF foerst. I F2 ER en akt et stykke korrespondance: den har ``Text``
        (HTML), ``Sender``, ``Receivers`` og datoer. Mailen bliver derfor akten
        selv. Og skal et dokument ses som PDF, renderer F2 det -
        ``rel/pdf-content`` - saa robotmaskinen behoever ingen konvertering.

    INGEN MAPPER. GO havde en undermappe pr. kildesag i Dokumenter-biblioteket,
        og en halv snes linjer til at oprette dem idempotent. I F2 er akten
        mappen: én akt pr. kildesag, og dokumenterne ligger paa den.

    SLETNING UDEN EN PRIVILEGERET KONTO. Et dokument, der traekkes ud af
        udleveringen, bliver SLETTET: DELETE paa dokumentets egen URL svarer 204
        (maalt 2026-09-10). Det virker, fordi udleveringsakten er ULAAST - vi
        undlader SentDate med vilje. Paa en LAAST akt giver det 403, og saa
        maerkes dokumentet "UDGÅET - ..." i stedet; se _fjern_dokument.

        Det stod i en periode her, at INTET kunne slettes. Det var forkert. Jeg
        ledte efter en rel/...delete-relation, fandt ingen, og konkluderede at
        handlingen ikke fandtes - men link-relationerne annoncerer ikke
        almindelig HTTP DELETE. cBrain gjorde opmaerksom paa det.

        GOAdminUser er stadig vaek: i GO fandtes den for at kunne AFMARKERE et
        dokument som sagsakt foer sletning, og det trin findes ikke i F2.

DET DER STADIG GAELDER

    Afstemning frem for haendelser. KontAKT regner forskellen ud og giver
    robotten en plan (upload / replace / delete / keep). En tabt haendelse, en
    fejlet koersel eller en gentagelse kan derfor ikke efterlade F2 permanent
    forkert: naeste koersel regner det hele om fra aktuelle data. En koersel
    uden noget at goere er normal.

    Fejl pr. dokument rapporteres og kastes DEREFTER, saa elementet proeves igen
    - kvitteringen har allerede gemt det, der lykkedes, saa et nyt forsoeg kun
    gentager resten.

OO config:
    Constant   F2Miljoe            "test" eller "prod"
    Constant   F2RestTestURL       F2's vaert (med eller uden https://)
    Constant   F2RestProdURL       ditto
    Credential F2TESTRestAkt       F2REST-klientens id + hemmelighed
    Credential F2PRODRestAkt       ditto
    Constant   F2AfdelingStandard  MIDLERTIDIG: afdelingens partynummer, indtil
                                   team -> F2-afdeling staar i KontAKTs database
    Credential KontAKTAPI          username = base URL, password = X-API-Key
"""
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
from urllib.parse import quote
import json
import re

import requests

from robot_framework import reset
from robot_framework.exceptions import CaseDeleted
from oomtm import f2 as oomtm_f2

# Titlen paa den akt, udleveringen af én kildesag ligger paa. Robotten finder
# akten igen paa det id, KontAKT har gemt - ikke paa titlen - men titlen er hvad
# et menneske ser i F2, saa den skal baere sagsnummeret.
UDLEVERING_TITEL = "Udlevering - {sag}"

# Akten er Outbound, fordi det ER udgaaende post. Men SentDate saettes IKKE:
# maalt, at en Outbound-akt oprettet med SentDate kommer tilbage
# DocumentsLocked=true, og saa kan afstemningen aldrig laegge et dokument mere
# paa den - laasen kan ikke aabnes igen (set-documents-locked svarer 400/500).
UDLEVERING_TYPE = oomtm_f2.OUTBOUND


# ----- Deleted in KontAKT ----------------------------------------------------


def _check_gone(resp) -> None:
    """Stop cleanly if what this queue element is about was deleted in KontAKT.

    KontAKT answers HTTP 410 with ``{"deleted": "case"|"reference"|"document"}``
    when the caseworker deleted the KontAKT case, the sag/mappe or the document
    while this element waited in the queue. Not an error and not retryable, so
    the queue framework marks the element done and takes the next one.
    """
    if resp is None or resp.status_code != 410:
        return
    try:
        body = resp.json() or {}
    except ValueError:
        body = {}
    if body.get("deleted"):
        raise CaseDeleted(body.get("note") or f"{body['deleted']} deleted in KontAKT")


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
    elif mode == "sync_f2":
        _sync_f2(orchestrator_connection, client, case_id, payload)
    elif mode == "delete_doc":
        _delete_doc(orchestrator_connection, client, case_id, payload)
    else:
        raise RuntimeError(f"Unknown KontAKTJournalize mode: {mode!r}")


# ----- afdelingen og sagsbehandleren -----------------------------------------


def _unit(client, payload) -> dict:
    """Afdelingen, sagen og akterne skal staa paa.

    KontAKT sender den, naar den kender den; ellers OO-constanten. Uden en
    afdeling siges der fra frem for at gaette: en sag med den forkerte ansvarlige
    er usynlig for det team, den hoerer til, og robotten mister skriveretten til
    akterne.
    """
    nummer = payload.get("f2_unit_party_no") or client.default_unit_party_no()
    if not nummer:
        raise RuntimeError(
            "Ingen F2-afdeling til sagen. KontAKT sendte ingen "
            "f2_unit_party_no, og OO-constanten F2AfdelingStandard er ikke sat. "
            "Uden en afdeling som ansvarlig er sagen usynlig for teamet, og "
            "robotten kan ikke rette akterne bagefter.")
    return oomtm_f2.party(party_no=int(nummer))


def _people(payload) -> list:
    """Aktparterne: sagsbehandleren og en eventuel vikar.

    DET er det, der giver dem adgang - ikke sagens partsfelter. En mailadresse
    er nok; F2 opløser den til den interne bruger (selv om opslags-endpointet
    ``party-by-email`` svarer 404 for interne, er det to forskellige kodeveje).
    """
    mails = []
    for noegle in ("caseworker_email", "standin_email"):
        m = str(payload.get(noegle) or "").strip()
        if m and m not in mails:
            mails.append(m)
    return [oomtm_f2.party(email=m) for m in mails]


# ----- create_case -----------------------------------------------------------


def _create_case(oc, client, case_id, payload):
    """Opret aktindsigtssagen i F2.

    PEO gør trin 2 gentageligt, men det beskytter kun inden for ÉN engangs-URL.
    Køres hele køelementet igen - fordi kvitteringen til KontAKT ikke kom
    igennem, eller fordi rammeværket prøver igen - henter robotten en ny
    engangs-URL og ville lave en ANDEN sag. Derfor spørges der først, om
    ExternalId allerede sidder på en sag.

    Det er en best-effort-spærre og ikke en garanti: opslaget går gennem
    søgeindekset, som er forsinket med minutter, så et forsøg igen inden for det
    første minut kan stadig ramme forbi. Men den fanger det almindelige tilfælde
    (et forsøg igen senere), og alternativet er ingen spærre.
    """
    path = f"/api/v1/cases/{case_id}/f2-journal/created"
    ekstern = f"KontAKT-{case_id}"
    title = str(payload.get("title") or "").strip() or f"Aktindsigt (KontAKT-sag {case_id})"
    oc.log_info(f"F2 create_case for KontAKT case {case_id}: {title!r}")

    allerede = client.f2.find_case(ekstern)
    if allerede and (allerede.get("CaseNumber") or "").strip():
        nummer = allerede["CaseNumber"].strip()
        oc.log_info(f"F2 create_case: {ekstern} sidder allerede på sag "
                    f"{nummer} - opretter ikke en ny.")
        sag = client.f2.case_by_number(nummer)
        _callback(oc, client, path,
                  {"ok": True, "f2_case_number": nummer,
                   "f2_case_url": (oomtm_f2.links(sag).get("alternate", "")
                                   if sag is not None else "")})
        return

    try:
        sag, ny = client.f2.create_case(
            title=title,
            external_id=ekstern,
            responsible=_unit(client, payload),
            deadline=str(payload.get("deadline") or "").strip(),
            cpr=str(payload.get("cpr") or "").strip(),
            # Rent oplysende - feltet "Suppl. sagsbeh." et menneske kigger i.
            # Det giver INGEN adgang; det er maalt. Adgangen ligger paa akterne.
            supplementary=_people(payload) or None,
        )
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"F2 create_case failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise

    nummer = oomtm_f2.text(sag, "CaseNumber")
    # Sagens egen adresse, som F2 oplyser den. Ikke bygget af os: den kraever
    # F2's interne id, og skemaet er cBrains.
    adresse = oomtm_f2.links(sag).get("alternate", "")
    oc.log_info(f"F2 case {'created' if ny else 'already existed'}: "
                f"{nummer} ({adresse})")
    if not nummer:
        _callback(oc, client, path,
                  {"ok": False, "note": "F2 returnerede ingen CaseNumber."})
        return
    _callback(oc, client, path, {"ok": True, "f2_case_number": nummer,
                                 "f2_case_url": adresse})


# ----- update_metadata -------------------------------------------------------


def _update_metadata(oc, client, case_id, payload):
    path = f"/api/v1/cases/{case_id}/f2-journal/updated"
    sag = _case(oc, client, case_id, payload)
    if sag is None:
        _callback(oc, client, path,
                  {"ok": False, "note": "Sagen er endnu ikke oprettet i F2."})
        return
    oc.log_info(f"F2 update_metadata for {oomtm_f2.text(sag, 'CaseNumber')} "
                f"(case {case_id})")
    try:
        # Afdelingen sendes kun med, naar KontAKT faktisk har skiftet team -
        # ellers ville hver titelrettelse ogsaa skrive den ansvarlige, og en
        # skrivning, der ikke behoeves, er en skrivning der kan gaa galt.
        afdeling = (_unit(client, payload)
                    if payload.get("f2_unit_party_no") else None)
        client.f2.update_case(sag, title=str(payload.get("title") or "").strip(),
                              deadline=str(payload.get("deadline") or "").strip(),
                              responsible=afdeling,
                              supplementary=_people(payload) or None)
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"F2 update_metadata failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    _callback(oc, client, path, {"ok": True})


# ----- journalize_email ------------------------------------------------------


def _journalize_email(oc, client, case_id, payload):
    """Én besked bliver én akt - med alle sine vedhaeftninger.

    Mailen renderes IKKE til PDF. En akt i F2 er et stykke korrespondance: den
    har ``Text`` (HTML), afsender, modtagere og datoer, og det er praecis en
    mail. GO kunne kun tage filer, og derfor skulle mailen igennem LibreOffice
    foerst - den omvej er vaek.

    VEDHAEFTNINGERNE LAEGGES PAA AKTEN. Tidligere stod her, at de ikke fulgte
    med, og det betoed at et foto eller en PDF, ansoegeren sendte, kun fandtes i
    KontAKT. Maalt 2026-09-25: F2's laas er append-only - en laast akt tager
    imod nye dokumenter, den naegter bare at fjerne dem. Saa filerne kan lægges
    paa, efter akten er oprettet som altid.

    DEN HER FUNKTION SKAL KUNNE KOERE IGEN. Den bliver kaldt igen, hver gang en
    indgaaende mails filer lander i KontAKT (de kommer EFTER selve mailen), og
    koeelementer proeves igen ved fejl. Hver koersel skal derfor ende det samme
    sted: én akt, og hver vedhaeftning én gang.

    Det er svaerere end det lyder, fordi PEO kun beskytter et forsoeg igen INDE
    I ét kald. En ny koersel beder om en ny engangs-URL og faar en NY akt. Og en
    dublet kan aldrig fjernes igen, fordi akten er laast mod at fjerne noget.
    Saa der holdes styr paa, hvad der er lavet, to steder - se ``_mail_akt`` og
    ``_vedhaeftninger``.
    """
    email_id = int(payload["email_id"])
    path = f"/api/v1/cases/{case_id}/emails/{email_id}/journalized"
    sag = _case(oc, client, case_id, payload)
    if sag is None:
        _callback(oc, client, path,
                  {"ok": False, "note": "Sagen er endnu ikke oprettet i F2."})
        return
    oc.log_info(f"F2 journalize_email email={email_id} -> "
                f"{oomtm_f2.text(sag, 'CaseNumber')}")
    try:
        mail = _kontakt_get(client, f"/api/v1/cases/{case_id}/emails/{email_id}")
        akt, ny = _mail_akt(oc, client, sag, mail, payload)
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"F2 journalize_email failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    akt_id = oomtm_f2.text(akt, "Id")

    # Aktens id meldes STRAKS - foer vedhaeftningerne. Doer koerslen midt i en
    # stor fil, ved KontAKT i det mindste, hvilken akt mailen er, og den naeste
    # koersel genbruger den i stedet for at oprette en til.
    _callback(oc, client, path, {"ok": True, "doc_id": akt_id})

    meldt = _vedhaeftninger(oc, client, case_id, email_id, sag, akt, ny, mail)
    if meldt:
        _callback(oc, client, path, {"ok": True, "doc_id": akt_id,
                                     "attachments": meldt})
    oc.log_info(f"F2 journalize_email done email={email_id} akt={akt_id} "
                f"({'ny' if ny else 'fandtes'}), {len(meldt)} vedhæftning(er)")


# Maerket paa et dokument, robotten selv har lagt paa en mail-akt. Staar i
# ``Description``, fordi det er det eneste frie felt, F2 giver tilbage: strukturen
# viser hverken beskrivelse eller filnavn, kun ``Title``, ``Size`` og
# ``ContentType`` (maalt 2026-09-25). Titlen er det, sagsbehandleren laeser, og
# den skal vaere filens navn - ikke et id.
MAERKE = "Vedhæftet mailen (KontAKT-vedhæftning {id})"
MAERKE_RE = re.compile(r"KontAKT-vedhæftning (\d+)")


def _mail_akt(oc, client, sag, mail, payload):
    """Akten, mailen er - genbrugt, hvis den findes. Returnerer (akt, ny).

    Tre veje, i den raekkefoelge:

      1. KontAKT kender aktens id. Den slaas op. Kan den ikke hentes, siges der
         fra: at lave en ny ville vaere en dublet, der ikke kan fjernes igen.
      2. KontAKT kender den IKKE - men den kan alligevel findes. Det er vinduet,
         hvor F2 oprettede akten, og koerslen doede, foer den naaede at sige det.
         Saa ledes der paa sagen efter en akt med samme titel OG samme dato.
      3. Ellers oprettes den.
    """
    kendt = str(mail.get("f2_document_id") or "").strip()
    if kendt:
        try:
            return client.f2.get(client.f2.rel("matter-by-id").replace("{id}", kendt)), False
        except oomtm_f2.F2Error as exc:
            raise RuntimeError(
                f"Mailen er journaliseret som akt {kendt}, men den kan ikke hentes "
                f"({exc.status}). Der oprettes IKKE en ny: akten er låst mod at "
                f"fjerne noget, så en dublet kan aldrig ryddes op. Tjek akten i F2 - "
                f"er den slettet med vilje, så ryd case_emails.f2_document_id.") from exc

    udgaaende = (mail.get("direction") or "") == "outbound"
    emne = (mail.get("subject") or f"E-mail {mail.get('id')}").strip()[:255]
    # Den rigtige dato for hver retning. Her stod sent_at for BEGGE - og en
    # indgaaende mail har ingen sent_at, saa den kom i F2 uden dato, og F2
    # stemplede selv journaliseringstidspunktet som modtagelsesdato.
    tidspunkt = _f2_dato(mail.get("sent_at") if udgaaende else mail.get("received_at"))

    fundet = _find_mail_akt(client, sag, emne, tidspunkt,
                            "SentDate" if udgaaende else "ReceivedDate")
    if fundet is not None:
        oc.log_info(f"F2 journalize_email: fandt akt {oomtm_f2.text(fundet, 'Id')} "
                    f"fra en tidligere kørsel - genbruger den.")
        return fundet, False

    modtagere = [oomtm_f2.party(email=m) for m in _adresser(mail.get("to_addresses"))]
    afsender = (oomtm_f2.party(email=_adresser(mail.get("from_address"))[0])
                if _adresser(mail.get("from_address")) else None)
    return client.f2.create_matter(
        sag,
        kind=oomtm_f2.OUTBOUND if udgaaende else oomtm_f2.INBOUND,
        title=emne,
        text=mail.get("body_html") or _som_html(mail.get("body_text")),
        responsible=_unit(client, payload),
        involved=_people(payload) or None,
        sender=afsender,
        receivers=modtagere or None,
        # En mail vokser ikke med ny TEKST, saa datoen maa gerne laase akten.
        # Laasen forhindrer ikke vedhaeftningerne: den er append-only.
        sent=tidspunkt if udgaaende else "",
        received="" if udgaaende else tidspunkt,
        # ARKIVERET - altsaa "journaliseret" i F2's brugerflade, og det er
        # foerst DER akten faar sit aktnummer (maalt paa 42 akter: hver
        # arkiveret akt har et nummer, ingen uarkiveret har et). F2 arkiverer
        # selv Inbound-akter, men ikke Outbound - saa den udgaaende post laa som
        # kladde uden nummer, mens svaret ved siden af var journaliseret.
        # Bemaerket i brugerfladen 2026-09-11.
        archived=True,
    )


def _find_mail_akt(client, sag, titel: str, tidspunkt: str, datofelt: str):
    """En akt paa sagen med denne titel og denne dato - eller None.

    Titel ALENE ville ikke vaere nok: "Sv: giv mig aktindsigt" staar paa mange
    akter. Titel OG dato til sekundet er et menneske, der skriver to svar med
    samme emne i samme sekund - det sker ikke. Uden dato (ingen at sammenligne
    med) findes der intet, og saa oprettes akten som foer.
    """
    if not tidspunkt:
        return None
    # Strukturens akt-noder baerer baade Title, SentDate og ReceivedDate (maalt
    # 2026-09-25), saa der sammenlignes uden et GET pr. akt. Kun den ene, der
    # passer, hentes helt - add_document skal bruge dens egne links.
    for kandidat in oomtm_f2.matters(client.f2.structure(sag)):
        if (oomtm_f2.text(kandidat, "Title") or "").strip() != titel:
            continue
        if (oomtm_f2.text(kandidat, datofelt) or "")[:19] != tidspunkt[:19]:
            continue
        return client.f2.get(oomtm_f2.links(kandidat)["self"])
    return None


def _vedhaeftninger(oc, client, case_id, email_id, sag, akt, ny, mail) -> list:
    """Laeg mailens filer paa akten. Returnerer [{id, f2_document_id}] til KontAKT.

    Hver fil én gang. KontAKT ved, hvad der er meldt tilbage; men en koersel kan
    doe EFTER F2 tog imod filen og FOER den naaede at melde det, og saa ville
    naeste koersel lægge den paa igen - som en dublet, der aldrig kan fjernes.
    Derfor laeses maerkerne paa aktens dokumenter ogsaa, naar akten ikke er ny.
    En ny akt er tom, saa der er intet at laese.
    """
    mangler = [a for a in (mail.get("attachments") or []) if not a.get("f2_document_id")]
    if not mangler:
        return []
    allerede = {} if ny else _maerker_paa(client, sag, akt)

    meldt = []
    for a in mangler:
        aid = int(a["id"])
        if aid in allerede:
            meldt.append({"id": aid, "f2_document_id": allerede[aid]})
            continue
        if not a.get("has_file"):
            # Indgaaende mails filer kommer én ad gangen EFTER mailen. KontAKT
            # beder om en ny koersel, naar filen er landet.
            oc.log_info(f"F2 journalize_email: vedhæftning {aid} er ikke hentet "
                        f"endnu - kommer med, når den lander.")
            continue
        try:
            data = _kontakt_bytes(
                client, f"/api/v1/cases/{case_id}/emails/{email_id}/attachments/{aid}/file")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 410:
                oc.log_info(f"F2 journalize_email: vedhæftning {aid} har ingen fil "
                            f"i KontAKT endnu.")
                continue
            raise
        navn = (a.get("file_name") or f"vedhaeftning-{aid}").strip()
        dok, _ = client.f2.add_document(
            akt,
            # Titlen er den, sagsbehandleren ser, og uden endelse - cBrain
            # anbefaler det, og titlen maa baere æøå, hvilket et filnavn i en
            # multipart-header ikke maa.
            title=_uden_endelse(navn),
            filename=navn, data=data,
            content_type=a.get("content_type") or _content_type(navn),
            description=MAERKE.format(id=aid))
        meldt.append({"id": aid, "f2_document_id": oomtm_f2.text(dok, "Id")})
    return meldt


def _maerker_paa(client, sag, akt) -> dict:
    """{kontakt-vedhaeftnings-id: f2-dokument-id} for dokumenterne paa akten.

    Ét GET pr. dokument, fordi beskrivelsen kun staar paa dokumentet selv og
    ikke i strukturen. Koeres kun paa en akt, der fandtes i forvejen - altsaa
    paa et forsoeg igen, hvor det netop er vaerd at betale for.
    """
    akt_id = oomtm_f2.text(akt, "Id")
    ud = {}
    for kandidat in oomtm_f2.matters(client.f2.structure(sag)):
        if oomtm_f2.text(kandidat, "Id") != akt_id:
            continue
        for d in oomtm_f2.documents(kandidat):
            selv = oomtm_f2.links(d).get("self")
            if not selv:
                continue
            m = MAERKE_RE.search(oomtm_f2.text(client.f2.get(selv), "Description") or "")
            if m:
                ud[int(m.group(1))] = oomtm_f2.text(d, "Id")
    return ud


# ----- sync_f2: afstemningen --------------------------------------------------


def _sync_f2(oc, client, case_id, payload):
    """Bring F2-sagen i takt med det, KontAKT faktisk udleverer.

    KontAKT giver planen, robotten udfoerer den. Fejl pr. dokument samles og
    meldes tilbage, og fejlen kastes DEREFTER, saa elementet proeves igen uden
    at det, der lykkedes, gaar tabt.
    """
    path = f"/api/v1/cases/{case_id}/f2-journal/documents-journalized"
    src = str(payload.get("source_case_id") or "").strip()
    query = f"?source_case_id={quote(src)}" if src else ""
    try:
        plan = _kontakt_get(client, f"/api/v1/cases/{case_id}/f2-journal/plan{query}")
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"F2 sync: kunne ikke hente planen: {exc!r}")
        _callback(oc, client, path, {"ok": False, "source_case_id": src or None,
                                     "note": f"Kunne ikke hente planen: {exc}"[:400]})
        raise

    sag = _case(oc, client, case_id, payload, plan=plan)
    if sag is None:
        _callback(oc, client, path, {"ok": False, "source_case_id": src or None,
                                     "note": "Sagen er endnu ikke oprettet i F2."})
        return

    sager = plan.get("sager") or []
    if not sager:
        oc.log_info("F2 sync: ingen sager at holde i takt.")
        return

    resultater, fejlede = [], 0
    for del_plan in sager:
        r = _sync_en_sag(oc, client, case_id, sag, del_plan, payload)
        fejlede += len(r["failed"])
        resultater.append(r)
    _callback(oc, client, path, {"ok": True, "sager": resultater})
    if fejlede:
        raise RuntimeError(f"{fejlede} dokument(er) kunne ikke journaliseres i F2.")


def _sync_en_sag(oc, client, case_id, sag, del_plan, payload) -> dict:
    """Udfoer én kildesags del af planen. Kaster aldrig - hver fejl lander i
    ``failed``, saa resten af sagen stadig bliver gjort."""
    src = str(del_plan.get("source_case_id") or "").strip()
    ud = {"source_case_id": src, "uploaded": [], "replaced": [],
          "deleted": [], "kept": del_plan.get("keep") or [], "failed": []}
    oc.log_info(f"F2 sync {src}: {len(del_plan.get('upload') or [])} ny, "
                f"{len(del_plan.get('replace') or [])} erstat, "
                f"{len(del_plan.get('delete') or [])} udgår.")

    try:
        akt = _udleveringsakt(oc, client, sag, del_plan, payload)
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"F2 sync: kunne ikke få en akt til {src}: {exc!r}")
        ud["failed"].append({"doc_id": None, "note": f"Akten: {exc}"[:300]})
        return ud
    ud["f2_matter_id"] = oomtm_f2.text(akt, "Id")

    # Trukket ud af udleveringen -> SLETTET fra F2. Kun hvis akten er blevet
    # arkiveret, faldes der tilbage paa at maerke det UDGÅET (se
    # _fjern_dokument). Begge dele rapporteres som "deleted", fordi det er
    # praecis hvad KontAKT skal goere med sin egen henvisning: glemme den.
    for post in (del_plan.get("delete") or []):
        try:
            _fjern_dokument(oc, client, post.get("f2_document_id"))
            ud["deleted"].append(post.get("doc_id"))
        except Exception as exc:  # pylint: disable=broad-except
            oc.log_info(f"F2 sync: kunne ikke fjerne dokument "
                        f"{post.get('f2_document_id')}: {exc!r}")
            ud["failed"].append({"doc_id": post.get("doc_id"), "note": str(exc)[:300]})

    # Aendret siden det blev journaliseret: den nye laegges op, og den gamle
    # fjernes. Raekkefoelgen er med vilje - den nye FOERST, saa en fejl aldrig
    # efterlader sagen uden en gyldig kopi.
    for post in (del_plan.get("replace") or []):
        if not _laeg_op(oc, client, case_id, akt, post, ud, "replaced"):
            continue
        try:
            _fjern_dokument(oc, client, post.get("f2_document_id"))
        except Exception as exc:  # pylint: disable=broad-except
            oc.log_info(f"F2 sync: den nye kopi er lagt op, men den gamle "
                        f"({post.get('f2_document_id')}) blev ikke fjernet: {exc!r}")

    for post in (del_plan.get("upload") or []):
        _laeg_op(oc, client, case_id, akt, post, ud, "uploaded")

    # Dokumentlisten aendrede sig med dokumenterne, saa kopien i F2 skal vaere
    # den nye. Den gamle fjernes, praecis som et udtrukket dokument.
    if del_plan.get("aktliste"):
        try:
            for gammel in (del_plan.get("aktliste_f2_document_ids") or []):
                try:
                    _fjern_dokument(oc, client, gammel)
                except Exception as exc:  # pylint: disable=broad-except
                    oc.log_info(f"F2 sync: gammel aktliste {gammel} blev ikke "
                                f"fjernet: {exc!r}")
            nye = _journaliser_aktliste(oc, client, case_id, akt, src)
            if nye:
                ud["aktliste_f2_document_ids"] = nye
        except Exception as exc:  # pylint: disable=broad-except
            oc.log_info(f"F2 sync: aktlisten for {src} fejlede: {exc!r}")
            ud["failed"].append({"doc_id": None, "note": f"Aktliste: {exc}"[:300]})

    oc.log_info(f"F2 sync {src} færdig: {len(ud['uploaded'])} lagt op, "
                f"{len(ud['replaced'])} erstattet, {len(ud['deleted'])} udgået, "
                f"{len(ud['failed'])} fejlede.")
    return ud


def _udleveringsakt(oc, client, sag, del_plan, payload):
    """Akten, én kildesags udlevering ligger paa. Findes den, genbruges den.

    Tre veje ind, i den raekkefoelge:

      1. KontAKT har gemt aktens id fra sidste koersel - slaa den op.
      2. Er den akt LAAST, laegges en ny akt paa i stedet, og laasen kan ikke
         aabnes (``set-documents-locked`` svarer 400/500).

         Grunden er SLETNING, ikke tilfoejelse. Her stod foer, at en laast akt
         ikke kunne tage imod flere dokumenter - det er forkert. Maalt
         2026-09-25: laasen er append-only, og en laast akt tager fint imod nye.
         Men afstemningen skal ogsaa kunne FJERNE et dokument, der ikke laengere
         udleveres, og det naegter en laast akt (DELETE -> 403). En
         udleveringsakt, der kun kan vokse, ville vise mere, end der blev
         udleveret.
      3. Ellers oprettes den.

    Akten findes IKKE paa sin titel. Titlen er vores egen tekst, og et menneske
    maa gerne rette den i F2 uden at journaliseringen holder op med at virke.
    """
    src = str(del_plan.get("source_case_id") or "").strip()
    kendt = str(del_plan.get("f2_matter_id") or "").strip()
    if kendt:
        try:
            akt = client.f2.get(client.f2.rel("matter-by-id").replace("{id}", kendt))
            if not client.f2.locked(akt):
                return akt
            oc.log_info(f"F2 sync: akt {kendt} er låst, så dokumenter kan ikke "
                        f"fjernes fra den - lægger en ny akt på {src}.")
        except oomtm_f2.F2Error as exc:
            # 403 daekker baade "findes ikke" og "maa du ikke se" - F2 skelner
            # ikke. Begge betyder, at vi ikke kan bruge den akt.
            oc.log_info(f"F2 sync: akt {kendt} kunne ikke bruges "
                        f"({exc.status}) - lægger en ny på.")

    akt, ny = client.f2.create_matter(
        sag, kind=UDLEVERING_TYPE,
        title=UDLEVERING_TITEL.format(sag=src or "sagen")[:255],
        text=f"<p>Dokumenter udleveret som aktindsigt i {src}.</p>" if src else "",
        responsible=_unit(client, payload),
        involved=_people(payload) or None)
    oc.log_info(f"F2 sync: akt {oomtm_f2.text(akt, 'Id')} "
                f"{'oprettet' if ny else 'fandtes'} til {src}.")
    return akt


def _laeg_op(oc, client, case_id, akt, post, ud, noegle) -> bool:
    """Hent ét dokuments bytes fra KontAKT og laeg dem paa akten.

    True hvis det lykkedes. Alt andet lander i ``failed`` - ogsaa "dokumentet
    har ingen fil", som ikke er robottens at rette, men heller ikke maa ligne
    en succes.
    """
    doc_id = post.get("doc_id")
    navn = post.get("file_name") or f"dokument-{doc_id}"
    try:
        data = _kontakt_bytes(client, f"/api/v1/cases/{case_id}/documents/"
                                      f"{doc_id}/content")
    except FileNotFoundError:
        ud["failed"].append({"doc_id": doc_id,
                             "note": "Dokumentet har ingen fil i filstoret."})
        return False
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"F2 sync: doc={doc_id} kunne ikke hentes: {exc!r}")
        ud["failed"].append({"doc_id": doc_id, "note": str(exc)[:300]})
        return False
    try:
        dok, _ = client.f2.add_document(
            akt,
            # Titlen er sagsbehandlerens egen og skal vaere uden endelse -
            # cBrain anbefaler det, og titlen maa baere aeoeaa, hvilket et
            # filnavn i en multipart-header ikke maa.
            title=(post.get("title") or _uden_endelse(navn)),
            filename=navn, data=data, content_type=_content_type(navn),
            description="Udleveret som aktindsigt (KontAKT)")
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"F2 sync: doc={doc_id} kunne ikke lægges på: {exc!r}")
        ud["failed"].append({"doc_id": doc_id, "note": str(exc)[:300]})
        return False
    ud[noegle].append({"doc_id": doc_id,
                       "f2_document_id": oomtm_f2.text(dok, "Id"),
                       "token": post.get("token")})
    return True


def _fjern_dokument(oc, client, f2_document_id) -> str:
    """Fjern et dokument, der er trukket ud af udleveringen.

    Returnerer "deleted", "marked" eller "" (der var intet id).

    DET BLIVER SLETTET. Maalt 2026-09-10: DELETE paa dokumentets egen URL giver
    204, og det er vaek bagefter. Udleveringsakten er ULAAST - vi undlader
    SentDate med vilje - og dér virker sletningen. Arkivering alene blokerer
    ikke: en arkiveret, ulaast akt tillod ogsaa sletning.

    En tidligere udgave hed _udgaa_dokument og MAERKEDE dokumentet "UDGÅET - ..."
    i stedet, fordi jeg havde konkluderet at et journaliseret dokument ikke kunne
    fjernes. Det var forkert: jeg ledte efter en rel/...delete-relation, fandt
    ingen, og troede at handlingen ikke fandtes. Link-relationerne annoncerer
    bare ikke almindelig HTTP DELETE. cBrain gjorde opmaerksom paa det.

    Maerkningen er tilbage som RESERVEVEJ - se f2.remove_document. En arkiveret
    akt giver 403, og saa er et maerket dokument stadig bedre end en fil, der ser
    udleveret ud uden at vaere det.
    """
    raw = str(f2_document_id or "").strip()
    if not raw:
        return ""
    url = client.f2.rel("document-by-id").replace("{id}", quote(raw))
    dok = client.f2.get(url)
    udfald = client.f2.remove_document(dok)
    if udfald == "marked":
        # Vaerd at kunne se i loggen: sletningen er normalen, maerkningen er det
        # afvigende, og den fortaeller at akten er blevet arkiveret.
        oc.log_info(f"F2-dokument {raw} kunne ikke slettes - mærket UDGÅET "
                    f"i stedet (akten er formentlig blevet låst).")
    return udfald


def _journaliser_aktliste(oc, client, case_id, akt, src) -> list[str]:
    """Hent aktlisten (PDF + Excel) fra KontAKT og laeg begge paa akten.

    KontAKT renderer den; robotten journaliserer den kun. Én renderer for hele
    systemet betyder, at ansoegerens kopi, sagsbehandlerens forhaandsvisning og
    kopien i F2 ikke kan drive fra hinanden - layout, kolonner, logo og datoen
    "dokumentliste hentet" kommer alle fra KontAKT.
    """
    path = f"/api/v1/cases/{case_id}/aktliste/generated"
    oc.log_info(f"Aktliste case={case_id} sag={src}")
    nye: list[str] = []
    try:
        data = _kontakt_get(client, f"/api/v1/cases/{case_id}/aktliste"
                                    f"?source_case_id={quote(src)}")
        raekker = data.get("rows") or []
        sagsnummer = data.get("sagsnummer") or src
        token = data.get("content_token")
        if not raekker:
            oc.log_info("Aktliste: ingen dokumenter - springer over.")
            return nye

        filer = [
            (f"Aktliste - {sagsnummer}.xlsx",
             _kontakt_bytes(client, f"/api/v1/cases/{case_id}/aktliste.xlsx"
                                    f"?source_case_id={quote(src)}")),
            (f"Aktliste - {sagsnummer}.pdf",
             _kontakt_bytes(client, f"/api/v1/cases/{case_id}/aktliste.pdf"
                                    f"?source_case_id={quote(src)}")),
        ]
        for navn, blob in filer:
            dok, _ = client.f2.add_document(
                akt, title=_uden_endelse(navn), filename=navn, data=blob,
                content_type=_content_type(navn),
                description="Dokumentliste dannet af KontAKT")
            nye.append(oomtm_f2.text(dok, "Id"))
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"Aktliste failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "source_case_id": src,
                                     "note": str(exc)[:400]})
        raise
    # Fortael KontAKT hvilket indhold aktlisten nu afspejler, saa
    # sagsbehandlerens "aktlisten er aktuel" bliver ved med at vaere sandt.
    _callback(oc, client, path, {"ok": True, "source_case_id": src,
                                 "content_token": token,
                                 "f2_document_ids": nye})
    oc.log_info(f"Aktliste opdateret for {sagsnummer}: {len(raekker)} rækker, "
                f"2 filer.")
    return nye


# ----- delete_doc ------------------------------------------------------------


def _delete_doc(oc, client, case_id, payload):
    """Et dokument blev trukket ud i KontAKT. Best-effort; ingen kvittering
    (KontAKTs egen raekke er allerede vaek).

    Slettes hvis akten tillader det - se ``_fjern_dokument``.
    """
    raw = str(payload.get("f2_document_id") or "").strip()
    oc.log_info(f"F2 delete_doc case={case_id} f2_document_id={raw}")
    if not raw:
        return
    _fjern_dokument(oc, client, raw)
    oc.log_info(f"F2 delete_doc done: {raw} er mærket UDGÅET")


# ----- sagen -----------------------------------------------------------------


def _case(oc, client, case_id, payload, plan=None):
    """F2-sagen dette koeelement handler om, eller None.

    Sagsnummeret er den hurtige vej: ``case-by-case-number`` gaar ikke gennem
    soegeindekset og virker derfor med det samme efter en oprettelse. Mangler
    nummeret - eller er sagen ikke at finde paa det - proeves ``ExternalId``
    som reparationsvej. Den GAAR gennem indekset og er forsinket med minutter,
    saa den er sidste udkald og ikke foerste.
    """
    nummer = str((plan or {}).get("f2_case_number")
                 or payload.get("f2_case_number") or "").strip()
    if nummer:
        sag = client.f2.case_by_number(nummer)
        if sag is not None:
            return sag
        oc.log_info(f"F2: sagsnummer {nummer!r} gav intet - prøver ExternalId.")
    raekke = client.f2.find_case(f"KontAKT-{case_id}")
    if not raekke:
        return None
    fundet = (raekke.get("CaseNumber") or "").strip()
    oc.log_info(f"F2: fandt sagen på ExternalId (nummer {fundet!r}).")
    return client.f2.case_by_number(fundet) if fundet else None


# ----- helpers ---------------------------------------------------------------


def _uden_endelse(navn: str) -> str:
    """"Aktliste - 2026 - 559.pdf" -> "Aktliste - 2026 - 559"."""
    s = str(navn or "")
    return s.rsplit(".", 1)[0] if "." in s.rsplit("/", 1)[-1] else s


# F2 udleder filtypen af filnavnets endelse. Content-typen sender vi, fordi det
# er det rigtige for en HTTP-klient at goere, og fordi det intet koster.
#
# Men den er IKKE det, der bestemmer, hvad der staar paa dokumentet bagefter:
# maalt, at F2 selv finder content-typen ud af BYTES. En gyldig PDF kom ind som
# application/pdf, mens en byte-streng, der bare begyndte med "PK", kom ind som
# application/octetstream - selv om vi sendte regnearkets rigtige type i
# multipart-headeren. Foerst troede jeg, at det var headeren, der manglede; det
# var den ikke.
CONTENT_TYPER = {
    "pdf": "application/pdf",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc": "application/msword",
    "html": "text/html", "htm": "text/html", "txt": "text/plain",
    "csv": "text/csv", "xml": "application/xml", "json": "application/json",
    "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
    "gif": "image/gif", "tif": "image/tiff", "tiff": "image/tiff",
    "msg": "application/vnd.ms-outlook", "eml": "message/rfc822",
    "zip": "application/zip",
}


def _content_type(navn: str) -> str:
    """Content-typen ud fra filnavnets endelse. Tom, hvis vi ikke kender den -
    saa saetter klienten application/octet-stream, som er det aerlige svar."""
    endelse = str(navn or "").rsplit(".", 1)[-1].lower() if "." in str(navn or "") else ""
    return CONTENT_TYPER.get(endelse, "")


def _som_html(tekst) -> str:
    """Ren tekst pakket, saa aktens Text-felt kan vise den med linjeskift."""
    s = (str(tekst or "")
         .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
    return f"<pre>{s}</pre>" if s else ""


def _adresser(raw) -> list[str]:
    """"a@x.dk; b@y.dk" -> ["a@x.dk", "b@y.dk"]."""
    ud = []
    for stykke in str(raw or "").replace(",", ";").split(";"):
        m = stykke.strip()
        if "@" in m and m not in ud:
            ud.append(m)
    return ud


def _f2_dato(vaerdi) -> str:
    """KontAKTs "2026-06-25 10:58:00" -> F2's "2026-06-25T10:58:00".

    F2 tager ISO 8601. Kommer der ingen tid med, saettes midnat - et tomt
    tidsfelt ville F2 afvise.
    """
    s = str(vaerdi or "").strip()
    if not s:
        return ""
    s = s.replace(" ", "T")
    if len(s) == 10:
        s += "T00:00:00"
    return s


def _kontakt_get(client, path: str) -> dict:
    r = requests.get(
        f"{client.kontakt_base}{path}",
        headers={"X-API-Key": client.kontakt_key, "Accept": "application/json"},
        timeout=60,
    )
    _check_gone(r)
    r.raise_for_status()
    return r.json()


def _kontakt_bytes(client, path: str) -> bytes:
    """En fil fra KontAKT - et gemt dokument eller en renderet aktliste.

    Hele filen i hukommelsen, fordi F2's dokumentoprettelse er én multipart-POST
    og ikke tager en stroem. GO-udgaven streamede til en midlertidig fil, men det
    gav kun mening, fordi dens upload kunne laese fra disken.

    ``FileNotFoundError`` paa 404: et dokument uden fil i filstoret er ikke en
    netvaerksfejl, og kalderen skal kunne skelne.
    """
    r = requests.get(
        f"{client.kontakt_base}{path}",
        headers={"X-API-Key": client.kontakt_key}, timeout=300,
    )
    _check_gone(r)
    if r.status_code == 404:
        raise FileNotFoundError(path)
    r.raise_for_status()
    if not r.content:
        raise RuntimeError(f"KontAKT returned an empty file for {path}")
    return r.content


def _callback(oc, client, path: str, body: dict) -> None:
    try:
        resp = requests.post(
            f"{client.kontakt_base}{path}",
            headers={"X-API-Key": client.kontakt_key,
                     "Content-Type": "application/json"},
            json=body, timeout=30,
        )
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"Callback to KontAKT failed: {exc!r}")
        return
    # Outside the except: a network blip stays harmless, but "deleted in KontAKT"
    # must reach the framework instead of being swallowed as a broad Exception.
    _check_gone(resp)
