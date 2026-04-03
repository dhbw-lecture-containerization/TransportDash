# TransportDash

## Project Description

Dieses Projekt dient der Darstellung verschiedener Daten aus dem Luft-, Schiffs- und Straßenverkehr.
Hierbei werden durch automatisierte DAGs mehrere API-Endpunkte angefragt und die so erhaltenen Daten in einer
Postgres Datenbank persistent abgespeichert. Zur Visualisierung wird ein Streamlit Frontend angeboten, welches
aktuelle Daten der Verkehrsarten auf mehreren Seiten darstellt.

Die Daten werden von folgenden Diensten abgerufen:
- [https://opensky-network.org/](https://opensky-network.org/) für Luftverkehrsdaten
- [https://autobahn.api.bund.dev/](https://autobahn.api.bund.dev/) für Straßenverkehrsdaten
- [https://aisstream.io/](https://aisstream.io/) für Schiffsverkehrsdaten

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

In diesem Projekt wird ein durch Github Workflows automatisierter Test bei jedem push auf main oder PR durchgeführt.
Die Einstellungen der Testumgebung können in [/.github/workflows/tests.yml](/.github/workflows/tests.yml) betrachtet werden.
Die Tests werden durch das Python Modul `unittest` und der Datei [/tests/tests.py](/tests/tests.py) durchgeführt.

In einem Test wird hierbei die relativ einfache Bereinigung der von einer API bereitgestellten Daten getestet.
Genauer werden die Daten der AutobahnGMBH durch eine Methode geleitet und so auf das in der Datenbank verwendete
Schema angepasst.

Ein weiterer Test dient der Funktionlität der AutobahnAPI. Hierbei wird eine einfache Anfrage gestellt und die Antwort
korrektheit überprüft. Mögliche Fehlerquellen sind hierbei: die Erreichbarkeit der API, die nicht-Einhaltung des Schemas
und das fehlen von erwarteten Datenpunkten.

## Credits:


shapefile: 
airlines dataset: https://www.kaggle.com/datasets/elmoallistair/airlines-airport-and-routes

## Image-Helferskripte

Im Repository gibt es zwei Hilfsskripte für die Container-Images des Projekts:

- `build_images.sh` baut die lokalen Images `transportdash-airflow:latest` und `transportdash-streamlit:latest` aus den Dockerfiles in `dags/` und `streamlit/` und zieht zusätzlich das Basis-Image `postgres:16`.
- `pull_images.sh` lädt die für das Kubernetes-Deployment benötigten Images herunter. Dabei werden `postgres:16`, das Streamlit-Image und das Airflow-Image gezogen.

Für `pull_images.sh` wird eine funktionierende Anmeldung bei `ghcr.io` benötigt. Falls der Pull fehlschlägt, kann stattdessen `build_images.sh` verwendet werden, um die Images lokal zu bauen.

Außerdem müssen die Ports geforwarded werden durch z.B:

```bash
kubectl port-forward service/airflow-webserver 8080:8080 -n transportdash
kubectl port-forward service/streamlit 8501:8501 -n transportdash 
```

Beispielbefehle:

```bash
kubectl create namespace transportdash
./pull_images.sh
kubectl apply -n transportdash -f kubernetes/TransportDash.yaml
```


