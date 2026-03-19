# TransportDash

## Project Description

A Dashboard with various transport data

Kurze Beschreibung des Projekts. Was macht es? Welche APIs werden genutzt? Welche Datenbank wird verwendet? Wie sieht die Visualisierung aus?

## Teammitglieder

- Schwarz, Fabian
- Dralle, Julius
- Putz, Jonas

## Status

### Branch to grade

**main** (oder ein anderer Branch, der zur Bewertung vorgesehen ist)

### Status

*In Bearbeitung* / *Bereit zur Bewertung* (je nach aktuellem Stand des Projekts)

## C4 Diagramme

### Context Diagramm

![Context Diagramm](path/to/context_diagram.png)

### Container Diagramm

![Container Diagramm](path/to/container_diagram.png)

## Devcontainer Konfiguration

Die Devcontainer Konfiguration kann in dem Verzeichnis `.devcontainer` eingesehen werden.

Airflow verwendet ein lokal bauendes Dockerfile (nach Vorlage aus den Vorlesungen).
Streamlit als Dashboard verwendet einen von Microsoft bereitgestellten Pythoncontainer.
Dazu wird ein Postgres gestartet, welches als Datenplattform von beiden Services verwendet wird.

Die Auswahl des Entwicklungszenarios (Airflow Entwicklung / Streamlit Entwicklung) kann
über `devcontainer.json` getroffen werden. Hier bitte einfach das gewünschte Tool auskommentieren.

Innerhalb des Airflow Devcontainers kann airflow über `airflow standalone` gestartet werden.
Streamlit wird mit `streamlit run streamlit/app.py` gestartet.

## Kubernetes Deployment Manifest

Link zum Kubernetes Deployment Manifest im Repository.

## Image Liste

- Image 1: Beschreibung, Link zum Dockerfile im Repository
- Image 2: Beschreibung, Link zum Dockerfile im Repository
- ...

## Tests

- Test: Beschreibung, Link zum Testcode im Repository
