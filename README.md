# pensjon-pen-dataprodukt
Dataanalyse i PEN for å lage dataprodukter med dbt


---

### Ambisjoner og beskrivelse

- **Kortsiktig ambisjon:** Lage et sett dataprodukter til datavarehuset, slik at de ikke trenger kopi av databasen.

- **Langsiktig ambisjon**: Lage analytiske dataprodukter som gir verdi til PO Pensjon.


Alle dbt-transformasjoner skjer i pen-databasen, altså flyttes ikke data ut for transformasjon.
Data leses fra skjemaet pen og transformeres i skjemaet pen_dataprodukt.

Idéen er å tilgjengeliggjøre dataproduktene på datamarkedsplassen.

Link til dbt-docs kommer etterhvert, https://dbt.ansatt.nav.no/


---

### Struktur i dbt-mappen

Kommer

---

### Nyttige lenker

- https://github.com/navikt/dv-team-pensjon/blob/main/SCRIPTS/Saker/STO-3175/SQL_pilot.sql 
- https://pensjon-dokumentasjon.ansatt.dev.nav.no/pen/Domenemodell/PEN_Domenemodell.html#_domenemodell
- https://confluence.adeo.no/display/PEN/Grov+datamodellskisse+dok 


---

### Lokal utvikling

Kommer

---

### Kjøre dbt på skarpe data

Trenger sørvisbruker for pen-Prod. 
Har sørvisbruker i pen-Q1, men ikke satt opp kjøring enda, venter på Knast fra Nada.
Enn så lenge er all utvikling i Q2.