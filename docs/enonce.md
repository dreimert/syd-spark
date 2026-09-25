# TD Apache Spark — Traiter la télémétrie d'un réseau mobile

**INSA Lyon · 4TC · Grands systèmes informatiques et réseaux**

Cinq millions d'événements réseau, quatre-vingt-sept antennes de la Métropole
de Lyon, une journée de trafic. Vous allez mesurer ce que coûte réellement un
calcul distribué — et découvrir que **la bonne mesure n'est presque jamais le
chronomètre**.

| | |
|---|---|
| **Durée** | 100 minutes |
| **Prérequis** | L'environnement installé et vérifié **avant** la séance : voir le [README](../README.md) |
| **Livrable** | Les 4 fichiers `lab/seq*.py` complétés (6 TODO + réponses) et le relevé de mesures ci-dessous |

---

## Apache Spark, en bref

**Apache Spark** est un moteur de calcul distribué open source, né à Berkeley
en 2009 et devenu l'outil standard pour traiter de gros volumes de données.
Il sert quand une seule machine ne suffit plus, parce que les données ne
tiennent pas en mémoire ou parce que le calcul serait trop long. Il répartit
alors le travail sur les cœurs d'une machine ou sur les machines d'un cluster.
Il succède à Hadoop MapReduce, dont il reprend le principe, mais il garde les
résultats intermédiaires en mémoire au lieu de les écrire sur disque entre
chaque étape.

L'idée est simple : vous écrivez votre programme comme s'il manipulait une
seule collection. Spark découpe cette collection en **partitions** et exécute
le même code sur chacune, en parallèle, là où elle se trouve.

```
                 driver  (votre notebook Python)
          construit le plan de calcul, le découpe en tâches
             /                  |                  \
       executor 1          executor 2          executor 3
    partitions 1, 4       partitions 2, 5      partitions 3, 6
```

Le vocabulaire dont vous aurez besoin pour lire la Spark UI :

| Terme | Ce que c'est |
|---|---|
| **Driver** | Le programme que vous écrivez (ici, le notebook). Il décrit le calcul, et Spark en déduit un plan qu'il distribue. |
| **Executor** | Un processus qui fait le travail sur les données. Un cluster en compte des dizaines. |
| **Partition** | Un morceau des données. C'est l'unité de parallélisme. |
| **Tâche** (*task*) | Le traitement d'une partition par un executor, sur un cœur. |
| **Stage** | Une suite de tâches qui s'enchaînent sans que les partitions aient besoin d'échanger des données. |
| **Job** | Tout le travail déclenché par une demande de résultat (`count`, `collect`, `write`…). |

Spark propose deux façons de programmer, que vous verrez dans cet ordre :

* **les RDD** (*Resilient Distributed Dataset*, séquences 1 et 2) : une
  collection d'objets découpée en partitions, à laquelle on
  applique des fonctions (`map`, `filter`, `reduceByKey`), comme un tableau
  JavaScript ;
* **les DataFrames et SQL** (séquences 3 et 4) : une table avec un schéma, sur
  laquelle on décrit une requête, et que Spark optimise lui-même.

On utilise ici **PySpark**, l'interface Python de Spark. Le moteur, lui, est
écrit en Scala et tourne dans une machine virtuelle Java : c'est pourquoi
l'environnement contient Java.

**Dans ce TD**, Spark tourne en mode `local[*]` : le driver et les executors
partagent un seul processus sur votre portable, avec une tâche par cœur. Le même
code tournerait sans modification sur un cluster ; seule l'adresse du *master*
changerait (la variable `TD_MASTER` dans `docker-compose.yml`). Les secondes
mesurées sur un portable ne se transposent pas à un cluster. Les octets
échangés entre partitions, eux, se transposent, et c'est pourquoi le TD vous
les fera relever.

---

## Déroulé

| Temps | Séquence | Fichier |
|---|---|---|
| 00:00 – 00:10 | Mise en route | — |
| 00:10 – 00:25 | 1. Du JavaScript fonctionnel au RDD | `lab/seq1_rdd.py` |
| 00:25 – 00:50 | 2. Paresse, DAG et coût du shuffle | `lab/seq2_dag_shuffle.py` |
| 00:50 – 01:15 | 3. DataFrames, Catalyst et format de stockage | `lab/seq3_dataframes.py` |
| 01:15 – 01:35 | 4. Cas télécom : jointure, déséquilibre, cache | `lab/seq4_cas_telecom.py` |
| 01:35 – 01:40 | Synthèse | — |

Les sections marquées *Facultatif* ne sont à faire que si vous êtes en avance.

---

## Mise en route (10 minutes)

1. Dans un terminal, dans le dossier du TD : `docker compose up`
2. Ouvrez **http://localhost:8888** (JupyterLab).
3. Si vous ne l'avez pas fait chez vous, générez les données dans un terminal
   JupyterLab (*File → New → Terminal*) : `node producer/generate.js`
4. Ouvrez `lab/seq1_rdd.py` (double-clic : il s'ouvre comme un notebook).

### L'architecture que vous allez manipuler

```
producer/generate.js  →  data/telemetrie.ndjson  →  [ Spark ]  →  data/out/  →  consumer/api.js
     Node.js                  fichier texte          Python          fichiers        Node.js
```

Node.js ne parle pas directement à Spark : il produit le flux de télémétrie
d'un côté, et publie les résultats de l'autre. Entre les deux, Spark
travaille. Les deux mondes communiquent par des fichiers : c'est ainsi que
sont construites la plupart des plateformes de données.

Un événement ressemble à ceci — sept champs, **pas de capacité d'antenne**
(elle est dans un référentiel séparé, il faudra une jointure) :

```json
{"ts":"2026-10-24T17:28:53.601Z","cell_id":"CELL_0001","niveau":"INFO",
 "event":"SESSION_START","connexions_actives":1557,"latence_ms":172,"octets":1281866}
```

Le générateur est déterministe : tout le monde obtient exactement le même jeu
de données, donc des mesures comparables.

### Deux onglets ouverts en permanence

* **JupyterLab** (http://localhost:8888) pour le code ;
* **la Spark UI** (http://localhost:4040) pour voir ce qui se passe réellement.
  La moitié du TD se joue dans cet onglet. Onglets utiles : *Jobs*, *Stages*
  (colonnes *Shuffle Read / Write*), *SQL / DataFrame*, *Storage*.

La Spark UI n'existe **que pendant qu'une session Spark est ouverte**, c'est-à-dire
entre la première et la dernière cellule d'une séquence. Chaque séquence se
termine par `spark.stop()` : exécutez cette dernière cellule avant de passer à
la séquence suivante.

### Comment on travaille

Chaque fichier `lab/seq*.py` est un notebook : texte, code et questions. On
exécute une cellule avec `Maj+Entrée`, on regarde le résultat **et la Spark
UI**, puis on passe à la suivante.

* **Les questions** (Q1.1, Q1.2…) sont dans les notebooks. Répondez dans les
  cellules *Réponse* prévues juste en dessous : double-cliquez dessus pour les
  modifier. Les questions marquées ⚑ dans le tableau ci-dessous portent
  l'essentiel du raisonnement.
* **Les TODO** sont les endroits où vous écrivez du code. Ils sont courts et
  progressifs : chacun prépare le suivant.

| Séquence | TODO | Questions ⚑ |
|---|---|---|
| 1 | TODO 1 : changer la clé d'un comptage | Q1.3, Q1.4 |
| 2 | TODO 2 : écrire la fonction d'un `reduceByKey` | Q2.3, Q2.4 |
| 3 | TODO 3 : votre première requête DataFrame | Q3.5 |
| 4 | TODO 4 : jointure et agrégation · TODO 5 : cache · TODO 6 : livraison | Q4.5, Q4.7 |

---

## Les quatre séquences en bref

**1. Du JavaScript fonctionnel au RDD.** `filter`, `map`, `reduce` : vous
connaissez déjà. Ce qui change, c'est que les données sont découpées en
partitions traitées en parallèle. On compare Spark à Node.js sur le même
fichier, avec le même algorithme — et le résultat vous surprendra peut-être.

**2. Paresse, DAG et shuffle.** Spark ne calcule rien avant qu'on lui demande
un résultat. Quand il calcule, il découpe le travail en *stages* séparés par
des *shuffles*, les moments où les données changent de machine. On mesure ce
que coûtent deux écritures différentes du même calcul — en octets, pas
seulement en secondes.

**3. DataFrames, Catalyst et format de stockage.** Au lieu de donner des
fonctions à Spark, on lui décrit ce qu'on veut, et un optimiseur choisit
comment le faire. On compare aussi le JSON et le format Parquet, sur disque et
à la lecture.

**4. Cas télécom.** La supervision veut savoir quelles antennes saturent, heure
par heure. Il faut joindre la télémétrie au référentiel des antennes, et une
antenne — celle de la gare de la Part-Dieu — concentre une part énorme du
trafic. On livre le résultat à un tableau de bord Node.js.

---

## Relevé de mesures

À compléter au fil de la séance. Vos valeurs dépendent de votre portable : ce
qui compte, ce sont les **rapports** entre les deux colonnes, et votre
explication.

| Mesure | Avant | Après | Rapport |
|---|---|---|---|
| Séq. 1 — Node 1 thread → Spark tous cœurs (durée) | | | |
| Séq. 2 — Shuffle write : `groupByKey` → `reduceByKey` | | | |
| Séq. 2 — Shuffle write : enregistrement entier → projeté | | | |
| Séq. 2 — Durée : enregistrement entier → projeté | | | |
| Séq. 3 — Lecture : inférence de schéma → schéma explicite | | | |
| Séq. 3 — Taille sur disque : NDJSON → Parquet | | | |
| Séq. 3 — Requête filtrée : JSON → Parquet | | | |
| Séq. 4 — Jointure : sort-merge → broadcast | | | |
| Séq. 4 — Jointure déséquilibrée : tâche médiane → tâche max | | | |
| Séq. 4 — Nombre de tâches : sans AQE → avec AQE | | | |
| Séq. 4 — 3 requêtes : sans cache → avec cache | | | |

> Une réponse qui cite un chiffre sans l'expliquer ne vaut rien ; une
> explication juste avec un chiffre approximatif vaut tout. On évalue votre
> compréhension du modèle d'exécution, pas la vitesse de votre portable.

---

## À retenir

- **Rien ne s'exécute avant une action.** Une chaîne de transformations n'est qu'un plan.
- **Le shuffle est l'opération la plus chère** : c'est là que les données quittent la machine qui les a lues. Projetez et agrégez avant, jamais après.
- **Déclarer vaut mieux que programmer.** Un optimiseur ne peut raisonner que sur ce qu'il comprend ; une lambda est une boîte noire.
- **Le format de stockage est une décision de dimensionnement réseau**, pas un détail d'implémentation.
- **Un stage se termine au retour de sa tâche la plus lente.** Passé un certain point, ajouter des machines ne sert à rien : c'est la répartition des clés qui compte.
- **Le chronomètre d'un portable est un mauvais juge du distribué.** Les volumes transférés, eux, se transposent à un vrai cluster.
- **Savoir ne pas sortir Spark** fait partie du métier.

---

*Environnement : PySpark 3.5 / Java 17 / Node.js 18 (scripts compatibles jusqu'à Node.js 26) sous Docker. Jeu de
données synthétique déterministe, 5 000 000 d'événements, 87 cellules.*
