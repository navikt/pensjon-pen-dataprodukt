{% docs __overview__ %}

# dbt docs for [pensjon-pen-dataprodukt (GitHub-link)](https://github.com/navikt/pensjon-pen-dataprodukt "pensjon-pen-dataprodukt")

Dette er autogenerert dokumentasjon som er ***søkbar og interaktiv***.

Ved spørsmål ta kontakt på Slack i kanalen **`#pen-dataprodukt-github`**.

På venstresiden er en meny som viser **Sources** og **Projects**.
**Sources** er alle kildene som blir brukt i prosjektet, og viser alle tabeller med tilhørende kolonner som blir brukt.
**Projects**, nærmere bestemt `pen_dataprodukt` under **Projects**, viser alle modellene som blir brukt i prosjektet.
I **Projects** kan du navigere til modellene og finne metadata, SQL-kode og avhengigheter til andre modeller.


## Tips til bruk av denne siden

- **Søkefeltet øverst søker på alt** av tabeller, kolonner, modeller og annet. Feks er alle kolonner fra pen-tabeller søkbare.
- **Filtrer ut sources under resources i lineage**, fordi de vises dobbelt som kilde og som staging-modell. Tester kan også filtreres ut.
- **Høyreklikk på en modell** i lineagen for å feks kun vise opp- og nedstrøms modeller fra den modellen.
- **Kopier SQL-kode fra Code/Compiled** i en modell og kjør det rett i databasen.
- **Bruk CTEene som modellene er bygget opp med** for å forstå hvordan modellene henger sammen. Kjør en og en CTE for å se resultatet.

## Lineage

    Trykk på den blå knappen nederst til høyre for å se lineage.
    Lineagen viser dataflyten i prosjektet mellom modellene, som typisk er views. 

Hvis du er inne på en spesifikk modell, så starter lineagen der. 
For å se hele så trykk først på mappen `pen_dataprodukt` under **Projects**, og deretter lineage-knappen.

### Farger i lineage


![Kilder](https://dummyimage.com/150x30/60b825.png&text=Kilder)

![Seeds](https://dummyimage.com/150x30/065903.png&text=Seeds)

![Staging](https://dummyimage.com/150x30/a3c785.png&text=Staging)

![Intermediate](https://dummyimage.com/150x30/49a8b3.png&text=Intermediate)

![Marts](https://dummyimage.com/150x30/a176b3.png&text=Marts)


## Modeller i dbt

### Hovedmodellene ligger i mappen `models/`

**Kilder**
  - tabeller som er kilder til modellene, fordelt på ulike skjemaer
  - de fleste kildene er pen-skjematabeller
  - alle kilder er definert i mappen `models/sources.yml` på GitHub

**Staging**
  - modeller som speiler kilder, men kun med kolonner som blir brukt i prosjektet
  - eventuelt med enkle transformasjoner og filtrering

**Intermediate**
  - modeller som gjør mer komplekse transformasjoner, men som er interne
  - deles opp i flere modeller for å gjøre det lettere å forstå og vedlikeholde

**Marts**
  - modeller som er ferdige dataprodukter eller klare for analyse
  - disse skal deles ut, både internt i POet og på datamarkedsplassen

### Andre type modeller

**Tests**
  - tester for å sjekke at dataen er som forventet
  - tester ligger både i mappen `tests/` og er definert i yaml-configen til modeller

**Seeds**
  - tabeller fra csv-filer, hvor csv-filene ligger i mappen `seeds/`

**Analysis**
  - kompilert SQL-kode som kan bruke jinja-syntax fra dbt, men som ikke er noe i databasen
  - fint å ha her for å vise hvilke modeller de bygger på og å samle analysespørringer


{% enddocs %}
