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
- **`journalize_ref`** — fired when ONE GO/Nova case is shared: downloads its
  delivered files (`?source_case_id=…`) from SharePoint, adds + journalises them
  on the GO case, and reports `doc_id → go_doc_id` back.
- **`journalize_folder`** — fired when the WHOLE case is shared: journalises every
  file in the case's SharePoint folder (incl. files added manually in SharePoint),
  mapping the known ones to their `doc_id`.
- **`delete_doc`** — deletes a document from GO (by `go_doc_id`) after it was
  deleted/removed in KontAKT.

Documents are journalised into a sub-folder per GO/Nova case under the GO case's
`Dokumenter` library, mirroring the SharePoint layout (`{source_case_id}`; files
lying loose in the case folder go to the root). `AddToCase` creates the folder
itself; the large-file chunked path makes a placeholder folder first (oomtm.go).

## Input

| Field | Modes | Meaning |
|-------|-------|---------|
| `mode` | all | `create_case` / `update_metadata` / `journalize_email` / `journalize_ref` / `journalize_folder` / `delete_doc` |
| `kontakt_case_id` | all | KontAKT case id (callback target) |
| `go_case_no` | most | the GO `AKT-…` case number |
| `title` | create/update | Emne → `ows_Title` |
| `modtaget` | create/update | `YYYY-MM-DD HH:MM:SS` → `ows_Modtaget` |
| `caseworker_email` | create/update | the caseworker to set as CaseOwner |
| `email_id` | journalize_email | KontAKT `case_emails` row to journalise |
| `source_case_id` | journalize_ref | the GO/Nova case whose files to journalise |
| `case_title` | journalize_folder | builds the SharePoint folder path |
| `go_doc_id` | delete_doc | the GO DocId to delete |

Large data (e-mail bodies, the delivered-files list) is **fetched from KontAKT**
(`GET /api/v1/cases/{id}/emails/{email_id}`, `…/delivery-files[?source_case_id=]`).

## Output / callbacks

- create → `POST /api/v1/cases/{id}/go-journal/created` `{ok, go_case_no, note}`
- update → `POST /api/v1/cases/{id}/go-journal/updated` `{ok, note}`
- email → `POST /api/v1/cases/{id}/emails/{email_id}/journalized` `{ok, doc_id}`
- journalize_ref / journalize_folder → `POST /api/v1/cases/{id}/go-journal/documents-journalized`
  `{ok, mappings:[{doc_id, go_doc_id}], doc_count}`
- delete_doc → no callback (best-effort; the KontAKT row is already gone)

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
