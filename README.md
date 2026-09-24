# TD Apache Spark — INSA Lyon 4TC

Traiter la télémétrie d'un réseau mobile avec Apache Spark : cinq millions
d'événements, 87 antennes de la Métropole de Lyon, une journée de trafic.

**L'énoncé du TD est dans [`docs/enonce.md`](docs/enonce.md).**
Ce fichier-ci explique seulement comment installer et lancer l'environnement.

---

## ⚠ À faire AVANT la séance (chez vous, environ 20 minutes)

Le jour du TD, trente personnes qui téléchargent 1 Go en même temps sur le
Wi-Fi de la salle, ça ne marche pas. Faites cette installation chez vous et
vérifiez qu'elle fonctionne.

### 1. Installer Docker

Docker fait tourner sur votre portable une petite machine Linux déjà
configurée avec tout ce qu'il faut (Spark, Java, Python, Node.js, JupyterLab).
Vous n'avez rien d'autre à installer, et vous n'avez pas besoin de savoir vous
servir de Docker : trois commandes suffisent, elles sont toutes ci-dessous.

| Système | Quoi installer |
|---|---|
| **Windows 10/11** | [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/). Acceptez l'option **WSL 2** proposée à l'installation, puis redémarrez. |
| **macOS** | [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/). Prenez la version **Apple Silicon** (M1, M2…) ou **Intel** selon votre Mac ( → *À propos de ce Mac*). |
| **Linux** | [Docker Engine](https://docs.docker.com/engine/install/) et le plugin `docker compose`. Ajoutez-vous au groupe `docker` (`sudo usermod -aG docker $USER`, puis reconnectez-vous). |

Lancez Docker Desktop et attendez qu'il indique *Engine running*.

**Mémoire :** dans Docker Desktop, *Settings → Resources*, donnez au moins
**4 Go** de mémoire à Docker (6 Go si votre portable en a 16).

### 2. Récupérer le TD

```bash
git clone <URL du dépôt communiquée par l'enseignant> td-spark
cd td-spark
```

Toutes les commandes suivantes se tapent **dans ce dossier `td-spark`**, dans
un terminal : *PowerShell* sous Windows, *Terminal* sous macOS et Linux.

### 3. Construire l'environnement (une seule fois, 5 à 10 minutes)

```bash
docker compose build
```

### 4. Démarrer et vérifier

```bash
docker compose up
```

Laissez ce terminal ouvert (il affiche les messages du serveur) et ouvrez
**http://localhost:8888** dans votre navigateur : c'est JupyterLab.

Dans JupyterLab, ouvrez un terminal (*File → New → Terminal*) et tapez :

```bash
node producer/generate.js
```

Au bout de quelques secondes, vous devez lire `lignes : 5 000 000`. Les données
sont créées ; elles seront conservées jusqu'au TD.

Ouvrez ensuite le dossier `lab/`, double-cliquez sur `seq1_rdd.py` : il
s'ouvre comme un notebook. Exécutez la première cellule (`Maj+Entrée`). Si
elle affiche `Spark UI : http://localhost:4040`, **tout fonctionne**.
Ouvrez cette adresse dans un nouvel onglet pour voir la Spark UI.

### 5. Arrêter

Fermez l'onglet, puis dans le terminal où tourne `docker compose up` :
`Ctrl-C`. Vous pouvez aussi taper `docker compose down` depuis n'importe quel
terminal ouvert dans le dossier.

---

## Le jour du TD

```bash
cd td-spark
docker compose up
```

puis http://localhost:8888. C'est tout.

| Adresse | Quoi |
|---|---|
| http://localhost:8888 | JupyterLab : le code et les notebooks |
| http://localhost:4040 | Spark UI : ce que Spark fait réellement (visible seulement pendant qu'une séquence est ouverte) |
| http://localhost:3000 | Tableau de bord de supervision (fin de la séquence 4) |

Vos modifications dans `lab/` sont enregistrées directement dans le dossier
`td-spark` de votre portable : vous pouvez arrêter Docker sans rien perdre.

---

## Dépannage

| Problème | Solution |
|---|---|
| `Cannot connect to the Docker daemon` / `error during connect` | Docker Desktop n'est pas lancé. Lancez-le et attendez *Engine running*. |
| `port is already allocated` (8888, 4040 ou 3000) | Un autre programme utilise ce port — souvent un JupyterLab déjà ouvert, ou un ancien `docker compose up`. Fermez-le, ou tapez `docker compose down` puis relancez. |
| `localhost:4040` ne répond pas | Normal si aucun notebook n'a créé de session Spark : exécutez la première cellule d'une séquence. La Spark UI vit et meurt avec la session. |
| La Spark UI est sur 4041 au lieu de 4040 | Une séquence précédente est encore ouverte. Dans JupyterLab : *Kernel → Shut Down All Kernels*, puis relancez la première cellule. |
| `telemetrie.ndjson introuvable` | Les données n'ont pas été générées : `node producer/generate.js` dans un terminal JupyterLab. |
| Tout est très lent, ou le noyau meurt (`Kernel died`) | Pas assez de mémoire. Augmentez-la dans Docker Desktop (*Settings → Resources*), fermez les autres séquences, ou régénérez un jeu plus petit : `node producer/generate.js --rows 2000000`. |
| Le `.py` s'ouvre comme un fichier texte | Clic droit sur le fichier → *Open With* → *Jupytext Notebook*. |
| Windows : `docker compose build` échoue sur WSL | Dans un PowerShell administrateur : `wsl --update`, puis redémarrez. |

---

## Contenu du dépôt

```
docs/enonce.md             l'énoncé du TD
lab/seq1_rdd.py            séquence 1 — du JavaScript fonctionnel au RDD
lab/seq2_dag_shuffle.py    séquence 2 — paresse, DAG, shuffle
lab/seq3_dataframes.py     séquence 3 — DataFrames, Catalyst, Parquet
lab/seq4_cas_telecom.py    séquence 4 — cas télécom : jointure, skew, cache
lab/lib.py                 outils communs (session, chronomètre, métriques)
producer/generate.js       producteur de télémétrie (Node.js)
producer/bench-node.js     la même analyse en Node.js, pour comparer (séquence 1)
consumer/api.js            service Node.js qui publie les résultats de Spark
consumer/dashboard.html    tableau de bord de supervision
Dockerfile, docker-compose.yml   l'environnement
```
