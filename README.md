# Python-KontAKTJournalize

Keeps a **GetOrganized (GO)** journaliseringssag (an `AKT` case) in sync with a
**KontAKT** aktindsigt case, and journalises the correspondence + the final
delivery onto it.

KontAKT triggers this whenever a case is created, when Emne / sagsbehandler /
modtaget changes, when a reply is sent, and when the case is finished.

## What it does

Queue-driven; the mode is set by `mode` in the input:

- **`create_case`** — creates the `AKT` case in GO with the MTM aktindsigt case
  profile (Sagsprofil = *MTM Aktindsigt*, Facet = *A53 Aktindsigtsanmodning mv.*,
  Modtaget, Title = Emne) and sets the KontAKT caseworker as **CaseOwner** (the
  PeoplePicker "hack" — best-effort). Calls back with the new GO case number.
- **`update_metadata`** — updates Title / Modtaget (via `/_goapi/Cases/Metadata`)
  and CaseOwner when they change in KontAKT.
- **`journalize_email`** — fetches a sent e-mail from KontAKT, renders it to PDF
  (LibreOffice), then adds + journalises it on the GO case.
- **`finalize`** — downloads every delivered SharePoint file, uploads each to the
  GO case, journalises them (MarkMultipleAsCaseRecord) and **closes** the GO
  case. Calls back so KontAKT closes the case too.

## Input

| Field | Modes | Meaning |
|-------|-------|---------|
| `mode` | all | `create_case` / `update_metadata` / `journalize_email` / `finalize` |
| `kontakt_case_id` | all | KontAKT case id (callback target) |
| `go_case_no` | update/email/finalize | the GO `AKT-…` case number |
| `title` | create/update | Emne → `ows_Title` |
| `modtaget` | create/update | `YYYY-MM-DD HH:MM:SS` → `ows_Modtaget` |
| `caseworker_email` | create/update | the caseworker to set as CaseOwner |
| `email_id` | journalize_email | KontAKT `case_emails` row to journalise |

Large data (e-mail bodies, the delivered-files list) is **fetched from KontAKT**
(`GET /api/v1/cases/{id}/emails/{email_id}`, `…/delivery-files`) rather than put
on the 2000-char queue payload.

## Output / callbacks

- create → `POST /api/v1/cases/{id}/go-journal/created` `{ok, go_case_no, note}`
- update → `POST /api/v1/cases/{id}/go-journal/updated` `{ok, note}`
- email → `POST /api/v1/cases/{id}/emails/{email_id}/journalized` `{ok, doc_id}`
- finalize → `POST /api/v1/cases/{id}/go-journal/finalized` `{ok, doc_count}`

(or `{ok: false, note}` on failure)

## Required configuration

- Constant `GOApiURL` — GO base URL (e.g. `https://ad.go.aarhuskommune.dk`)
- Credential `GOAktApiUser` — GO NTLM username + password
- Constant `KontAKTSharePoint` — SharePoint site URL (delivery library)
- Credential `SharePointCert` — username = thumbprint, password = certificate path
- Credential `SharePointAPI` — username = tenant, password = client id
- Credential `KontAKTAPI` — username = base URL, password = API key

## Dependencies

The shared [`oomtm`](https://github.com/mtm-aarhus/oomtm) library (`go`,
`sharepoint`, `pdf`). E-mail/HTML → PDF uses LibreOffice headless, auto-installed
on first use by `oomtm.pdf.ensure_libreoffice`.

## Caveats

- **CaseOwner** is set via a PeoplePicker + `ValidateUpdateListItem` hack
  (`oomtm.go.set_case_owner`). It resolves the case's ModernConfiguration path
  from the create response's `CaseRelativeUrl` (which equals `ows_CaseUrl`), so
  there's no tenant-specific guess. It's best-effort and never fails the job — if
  it can't be set, the case still exists and the owner can be set in GO.
- Large files (>10 MB) upload to GO via a chunked SharePoint upload
  (startUpload/continueUpload/finishUpload to the case's `Dokumenter` library,
  then locate the DocId + set metadata) instead of the AddToCase byte-array.
