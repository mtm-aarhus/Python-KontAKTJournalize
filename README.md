# Python-KontAKTJournalize

Holder en **aktindsigtssag i F2** (cBrains ESDH) i takt med en **KontAKT**-sag,
og journaliserer korrespondancen + den endelige udlevering på den.

KontAKT sætter arbejde i kø, når en sag oprettes, når Emne / sagsbehandler /
modtaget ændres, når en besked sendes, og hver gang noget ændrer sig i det, der
faktisk bliver udleveret.

> Robotten talte tidligere med **GetOrganized**. F2 erstatter GO på alle
> parametre, og GO er ude af KontAKTs kode og database. Se `MDFiles/F2-TESTLOG.md` i
> KontAKT-repoet for hver enkelt måling, denne robot er bygget på - intet
> herunder er læst i en dokumentation, det er alt sammen prøvet mod
> `aak-esdh-test-mob.f2-cloud.com`.

## Hvad der blev enklere, da GO blev F2

**Ingen LibreOffice.** GO kunne kun tage filer, så en mail skulle renderes til
PDF først. I F2 **er** en akt et stykke korrespondance: den har `Text` (HTML),
afsender, modtagere og datoer. Mailen bliver derfor akten selv. Og skal et
dokument ses som PDF, renderer F2 det (`rel/pdf-content`).

**Ingen mapper.** GO havde en undermappe pr. kildesag i `Dokumenter`-biblioteket
plus kode til at oprette dem idempotent. I F2 er akten mappen.

**Ingen privilegeret konto.** `GOAdminUser` fandtes for at kunne afmarkere et
dokument som sagsakt før sletning. Det trin findes ikke i F2 - robotten sletter
sine egne dokumenter med et almindeligt `DELETE`.

## Et dokument, der trækkes ud, bliver slettet

`DELETE` på dokumentets egen `self`-URL svarer **204**, og et GET bagefter giver
403. Målt 2026-09-10 på fem dokumenter, heraf tre som robotten selv havde
journaliseret.

Det virker, fordi udleveringsakten er **ulåst** - vi undlader `SentDate` med
vilje, se afsnittet om låsen. Arkivering alene blokerer ikke: en arkiveret, men
ulåst akt tillod også sletning. Er akten låst, svarer F2 403, og så falder
robotten tilbage på at **mærke** dokumentet `UDGÅET - <titel>`; titlen kan
PATCHes. Aktlisten - som journaliseres i samme kørsel - er under alle
omstændigheder det autoritative indeks over det udleverede.

> **Her stod det modsatte indtil 2026-09-10:** at et journaliseret dokument
> slet ikke kunne fjernes, fordi det ingen slette-relation har. Det var forkert.
> Jeg ledte efter en `rel/…delete`, fandt ingen, og konkluderede at handlingen
> ikke fandtes - men link-relationerne annoncerer ikke almindelig HTTP `DELETE`.
> **Fraværet af et link er ikke fraværet af en metode.** cBrain gjorde
> opmærksom på det.

En hel **sag** må API-brugeren derimod stadig ikke slette (403 på dry-run; selve
sletningen svarer 204 og gør ingenting). Det kræver privilegiet *"Kan slette
sager"* i en rolle.

## Adgang og skriveret - to betingelser, og de skal begge holde

Målt på 42 akter fordelt på 12 sager, med `PATCH /Title` som prøve:

| `AccessLevel` | ansvarlig | PATCH lykkedes |
|---|---|---|
| `Unit` | en **enhed** | **15 af 15** |
| `Unit` | en person | 2 af 4 |
| `All` | hvad som helst | **0 af 16** |
| `Involved` | hvad som helst | **0 af 7** |

Derfor: **`AccessLevel=Unit` og afdelingen som ansvarlig** på alt robotten
opretter. Klienten verificerer det efter oprettelsen og siger fra, hvis akten
fik en person - konsekvensen er ellers permanent, for afstemningen ville få 403
på den akt ved hver eneste kørsel fra nu af.

Menneskene kommer til via aktens `InvolvedParties`, som lægger sig **oven på**
enhedsadgangen. Det er også det, der løser ferien: Byggeris sag bliver på
Byggeri, og vikaren fra Digital Udvikling sættes som aktpart.

`InvolvedParties` bliver **tavst ignoreret**, hvis det sendes ved oprettelsen af
akten (201 Created, ingen parter). Klienten sætter det med PATCH bagefter og
læser akten tilbage for at se, at de sad fast - adgangen hænger på netop det
felt, så en sag ville ellers blive journaliseret "korrekt" og være usynlig for
sagsbehandleren.

## Låsen: hvorfor udleveringsakten ikke får en SentDate

En akt med `DocumentsLocked=true` afviser nye dokumenter (`400 Du har ikke
rettigheder til at oprette dokumentet`), og **låsen kan ikke åbnes**:
`rel/set-documents-locked` svarer 400 på `locked`/`Locked` og 500 `The operation
SetMatterDocumentsLocked for MatterHandler is not ready`.

Hvad der låser: alle `Inbound`-akter kommer tilbage låste, og en `Outbound`-akt
oprettet **med** `SentDate` blev også låst, mens `Outbound` uden ikke blev.
Udleveringsakten får derfor ingen `SentDate`. Og fordi den regel er målt og ikke
oplyst, spørger robotten alligevel før hvert dokument - er akten låst, lægger
den en **ny** akt på sagen i stedet for at fejle for altid.

## Hvad den gør

Køstyret; `mode` i nyttelasten bestemmer:

- **`create_case`** — opretter sagen i F2: KLE `00.07.03`, handlingsfacet `A53`,
  `ExternalAccessCode=Closed` (undtaget fra postliste), afdelingen som ansvarlig
  og sagsbehandleren som supplerende (rent oplysende - det giver ingen adgang).
  Melder sagsnummeret og **den adresse, F2 selv oplyser** (`f2t://case/<id>`,
  link-relationen `alternate`) tilbage. Koderne sendes som KLE-**numre** og ikke
  som GUID'er, netop for at test og drift kan bruge samme konfiguration.
- **`update_metadata`** — Emne / frist / afdeling ændret i KontAKT.
- **`journalize_email`** — én besked bliver én akt: emnet er titlen, mailkroppen
  er aktens `Text`, afsender/modtagere bliver parter, og datoen sættes som
  modtaget eller sendt. Ingen PDF-rendering.
- **`sync_f2`** — afstemningen. KontAKT regner forskellen ud mellem "hvad ligger
  der i F2" og "hvad bliver faktisk udleveret" og giver robotten en plan
  (upload / replace / delete / keep + om aktlisten skal dannes om). Robotten
  udfører den. En tabt hændelse, en fejlet kørsel eller en gentagelse kan derfor
  ikke efterlade F2 permanent forkert: næste kørsel regner det hele om fra
  aktuelle data. **En kørsel uden noget at gøre er normal.**
- **`delete_doc`** — et dokument blev trukket ud i KontAKT; det slettes i F2.

## Input

| Felt | Modes | Betydning |
|-------|-------|---------|
| `mode` | alle | `create_case` / `update_metadata` / `journalize_email` / `sync_f2` / `delete_doc` |
| `kontakt_case_id` | alle | KontAKT-sagens id (kvitteringens adresse) |
| `f2_case_number` | de fleste | F2-sagsnummeret, fx `2026 - 559` |
| `f2_unit_party_no` | create/update | afdelingens partynummer. Mangler den, bruges OO-constanten `F2AfdelingStandard` |
| `title` | create/update | Emne |
| `deadline` | create/update | ISO-dato, valgfri |
| `caseworker_email` | de fleste | sagsbehandleren; bliver aktpart |
| `standin_email` | de fleste | vikaren, hvis der er en; bliver også aktpart |
| `email_id` | journalize_email | KontAKTs `case_emails`-række |
| `source_case_id` | sync_f2 | kildesagen, der skal holdes i takt |
| `f2_document_id` | delete_doc | dokumentet, der skal fjernes |

Store data hentes **fra KontAKT** og står ikke i køelementet: mailkroppen
(`…/emails/{id}`), planen (`…/f2-journal/plan`), aktlistens rækker og filer
(`…/aktliste`, `…/aktliste.xlsx`, `…/aktliste.pdf`) og dokumenternes bytes
(`…/documents/{id}/content`).

## Kvitteringer

- create → `POST /api/v1/cases/{id}/f2-journal/created`
  `{ok, f2_case_number, f2_case_url}`
- update → `POST /api/v1/cases/{id}/f2-journal/updated` `{ok}`
- email → `POST /api/v1/cases/{id}/emails/{email_id}/journalized` `{ok, doc_id}`
- sync → `POST /api/v1/cases/{id}/f2-journal/documents-journalized`
  `{ok, sager:[{source_case_id, f2_matter_id, uploaded, replaced, deleted, kept,
  failed, aktliste_f2_document_ids}]}`
- aktliste → `POST /api/v1/cases/{id}/aktliste/generated`
  `{ok, source_case_id, content_token, f2_document_ids}`
- delete_doc → ingen kvittering (KontAKTs egen række er allerede væk)

(eller `{ok: false, note}` når det gik galt)

Fejl pr. dokument meldes tilbage og fejlen kastes **derefter**, så elementet
prøves igen uden at det, der lykkedes, går tabt.

## Konfiguration

| | |
|---|---|
| Constant `F2Miljoe` | `test` eller `prod`. Mangler den: test. En stavefejl fejler frem for at gætte |
| Constant `F2RestTestURL` / `F2RestProdURL` | F2's vært. `https://` må gerne stå der - klienten sætter det kun på, hvis det mangler |
| Credential `F2TESTRestAkt` / `F2PRODRestAkt` | F2REST-klientens id + hemmelighed |
| Constant `F2BrugerTest` / `F2BrugerProd` | valgfri: F2-brugerens navn, hvis den ikke hedder som klienten |
| Constant `F2AfdelingStandard` | **midlertidig**: afdelingens partynummer, indtil koblingen team → F2-afdeling står i KontAKTs database. **Teknik og Miljø = `38`** (synknøgle 1007, slået op i F2 2026-09-09). Til sammenligning: Byggeri = 450, Digital Udvikling = 2008 |
| Credential `KontAKTAPI` | username = base URL, password = X-API-Key |

Journalplan og handlingsfacet står **ikke** i konfigurationen: de er KLE-numre og
derfor de samme i test og drift.

## Afhængigheder

Det delte [`oomtm`](https://github.com/mtm-aarhus/oomtm)-bibliotek, modulet
`oomtm.f2`. Det er en **ordret kopi** af `app/integrations/f2_rest.py` i
KontAKT-repoet, hvor den udvikles og prøves (`tests/test_f2_rest.py`) - ret der,
og kopiér ud igen; `tests/test_copies.py` fejler, når de to er kommet ud af takt.

`oomtm[pdf]` er **ikke** længere nødvendig. F2 renderer selv til PDF.

## Uafklaret: rækker en FORÆLDER-enhed ned i sine underenheder?

`F2AfdelingStandard` sættes til **Teknik og Miljø (38)**, som er en rod i
enhedstræet - Byggeri (450) og Digital Udvikling (2008) ligger under den.

Det er **ikke målt**, om det virker, og den målte regel peger den forkerte vej:

> `Unit` betyder **kun den ansvarliges egen enhed**. Robotten var ansvarlig,
> akten stod på `Unit`, og Jakob kunne ikke se sagen. *(prøve D, 2026-09-07)*

Prøve E viste, at en kollega **i samme enhed** som ansvarlig gav adgang. Ingen
prøve har testet en forælder-enhed. Betyder `Unit` præcis den ene enhed og ikke
dens undertræ, bliver hver sag journaliseret "korrekt" og er **usynlig for
alle** - præcis fejlen fra prøve D, og den er tavs.

**Prøven, der afgør det** (ét menneske, to minutter): opret én sag med
`F2AfdelingStandard = 38`, lad robotten journalisere den, og bed en
sagsbehandler i Byggeri eller Digital Udvikling søge den frem i F2.

- **Ser de den:** enhedsadgang er hierarkisk, 38 er det rigtige valg, og alle i
  Teknik og Miljø kan finde alle aktindsigtssager.
- **Ser de den ikke:** brug den enhed, sagen faktisk hører til (450 for
  Byggeri), og få koblingen team → F2-afdeling ind i KontAKTs database, som er
  den rigtige løsning alligevel.

Bemærk konsekvensen, hvis den virker: `AccessLevel=Unit` + Teknik og Miljø som
ansvarlig giver **hele magistraten** adgang til hver aktindsigtssag. Det kan
være præcis det, der ønskes - aktindsigtsteamet betjener hele TM - men det er en
adgangsbeslutning, ikke en teknisk detalje.

## Uafklaret hos cBrain

Se `MDFiles/F2-SPOERGSMAAL-TIL-CBRAIN.md` i KontAKT-repoet. Det, der berører denne robot:

- ~~Hvad er den tilsigtede måde at trække et journaliseret dokument ud?~~
  **Besvaret 2026-09-10:** `DELETE` på dokumentet virker. Mærkningen er nu kun
  reservevej for en låst akt.
- Hvad skiller de to `Unit`-akter med en person som ansvarlig, hvor den ene
  virker og den anden ikke?
- Hvad er det rigtige kald til `set-documents-locked`?
