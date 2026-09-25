# %% [markdown]
# # Séquence 4 — Cas télécom : jointure, déséquilibre et cache  (20 min)
#
# **Mission.** La supervision réseau de la Métropole de Lyon veut, pour chaque
# heure de la journée, la liste des cellules dont le taux de charge dépasse
# 70 % de leur capacité nominale. Le résultat doit être consommable par le
# tableau de bord Node.js de l'équipe.
#
# La capacité nominale n'est pas dans la télémétrie : elle est dans le
# référentiel des antennes (`data/cellules.json`, 87 lignes). Il faut donc une
# **jointure distribuée**.
#
# Prérequis : la séquence 3 a écrit le Parquet (section 3.3).

# %%
from lib import session, chrono, pause, metriques_stages, profil_taches, PARQUET, CELLULES, SORTIE
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
import os

spark = session("seq4-cas-telecom", aqe=False)

telemetrie = spark.read.parquet(PARQUET)
cellules = spark.read.json(CELLULES)

print(f"Télémétrie : {telemetrie.count():,} lignes".replace(",", " "))
print(f"Référentiel: {cellules.count()} cellules")
cellules.show(3, truncate=False)

# %% [markdown]
# ## 4.1 — Jointure : la petite table doit-elle voyager ?
#
# **Ce que fait la jointure.** `telemetrie.join(cellules, "cell_id")` colle à
# chaque événement la ligne du référentiel qui a le même `cell_id` : c'est le
# `JOIN` de SQL. En JS, sur un seul cœur, ce serait
# `evenements.map(e => ({...e, ...index[e.cell_id]}))`, avec `index` un objet
# construit à partir du référentiel.
#
# **Pourquoi c'est difficile en distribué.** Une tâche ne voit que sa
# partition de télémétrie. Pour joindre un événement de `CELL_0042`, elle doit
# avoir sous la main la ligne `CELL_0042` du référentiel. 87 lignes d'un côté,
# 5 millions de l'autre : deux façons de réunir les lignes qui vont ensemble.
#
# * **broadcast join** : on envoie une copie de la petite table à chaque
#   executor, la grande ne bouge pas ;
# * **sort-merge join** : on redistribue les **deux** tables par clé (shuffle),
#   on les trie, puis on les fusionne.
#
# ```
#   broadcast                          sort-merge
#
#   référentiel ──copie──► executor 1  télémétrie ──shuffle──┐
#               ──copie──► executor 2                        ├─► tri ─► fusion
#               ──copie──► executor 3  référentiel ─shuffle──┘
#   (la télémétrie reste en place)     (les deux tables bougent)
# ```
#
# Catalyst choisit seul : il diffuse automatiquement toute table estimée à
# moins de `spark.sql.autoBroadcastJoinThreshold` (10 Mo par défaut). La
# seconde cellule met ce seuil à `-1` pour interdire la diffusion.
#
# **Lire les plans.** Comme `toDebugString` en séquence 2, un plan se lit de
# bas en haut. Celui-ci a deux branches, une par table (`:-` pour la
# télémétrie, `+-` pour le référentiel), qui se rejoignent dans l'opérateur de
# jointure. Cherchez les **`Exchange`** : chacun est un shuffle, donc une
# frontière de stage. `BroadcastExchange` désigne, lui, l'envoi d'une copie à
# chaque executor.

# %%
with chrono("jointure — stratégie choisie par Catalyst"):
    telemetrie.join(cellules, "cell_id").count()
telemetrie.join(cellules, "cell_id").explain()
metriques_stages(spark, n=3)

# %%
# On interdit la diffusion pour forcer l'autre stratégie.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
with chrono("jointure — broadcast interdit (sort-merge)"):
    telemetrie.join(cellules, "cell_id").count()
telemetrie.join(cellules, "cell_id").explain()
metriques_stages(spark, n=3)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# %% [markdown]
# **Q4.1** Nommez les deux opérateurs de jointure lus dans les plans
# (`...Join`). Comparez les durées et les volumes de shuffle des deux
# exécutions. Qu'a-t-on évité de déplacer en diffusant 87 lignes ?
#
# **Q4.2** À partir de quelle taille de référentiel la diffusion
# devient-elle une mauvaise idée ? Où la table diffusée est-elle stockée, et en
# combien d'exemplaires ?

# %% [markdown]
# **Réponse Q4.1 :**
#
# **Réponse Q4.2 :**

# %% [markdown]
# ## 4.2 — Calculer le taux de charge
#
# **TODO 4** — Complétez `charge` : une ligne par cellule et par heure, avec
#
# | colonne | contenu |
# |---|---|
# | `cell_id`, `secteur`, `techno`, `heure` | les clés du regroupement (déjà écrites) |
# | `connexions_moy` | moyenne de `connexions_actives`, arrondie à 1 décimale |
# | `capacite_max` | la capacité de la cellule : une seule valeur par cellule, `F.first(...)` suffit |
# | `taux_charge` | `100 * connexions_moy / capacite_max`, arrondi à 2 décimales |
#
# C'est la même mécanique que le TODO 3 de la séquence 3. Pour la dernière
# colonne : `.withColumn("taux_charge", F.round(..., 2))`, où l'on désigne une
# colonne existante par `F.col("nom")`.
#
# Trois remarques pour comprendre le squelette :
#
# * **Pourquoi quatre clés dans le `groupBy` ?** Les groupes sont définis par
#   `cell_id` et `heure`. `secteur` et `techno` ne changent rien aux groupes
#   (une cellule n'a qu'un secteur et qu'une techno) ; on les met dans le
#   `groupBy` parce qu'un `groupBy` ne garde **que** ses clés et les
#   agrégats. Sans cela, elles disparaîtraient du résultat.
# * **Pourquoi `F.first` ?** `capacite_max` vaut la même chose sur toutes les
#   lignes d'une cellule : il suffit d'en prendre une. `max` donnerait le même
#   résultat ; `first` dit mieux l'intention.
# * **Pourquoi `withColumn` après `agg` ?** `taux_charge` se calcule à partir
#   de `connexions_moy`, qui n'existe qu'une fois l'agrégation faite.
#   `agg` produit une valeur par groupe ; `withColumn` ajoute ensuite une
#   colonne, ligne par ligne, au résultat.
#
# Un taux supérieur à 100 % n'est pas une erreur : la cellule sert plus de
# connexions que sa capacité nominale, elle est en surcharge.

# %%
charge = (
    telemetrie
    .join(F.broadcast(cellules), "cell_id")     # diffusion explicite : 87 lignes
    .groupBy("cell_id", "secteur", "techno", "heure")
    .agg(
        # TODO 4a : connexions_moy
        # TODO 4b : capacite_max
    )
    # TODO 4c : .withColumn("taux_charge", ...)
)

charge.orderBy(F.desc("taux_charge")).show(10)

# %% [markdown]
# ## 4.3 — Le déséquilibre (data skew)
#
# Le trafic mobile n'est jamais uniforme : une gare concentre à elle seule une
# part énorme du trafic.

# %%
volumetrie = telemetrie.groupBy("cell_id").count().orderBy(F.desc("count"))
volumetrie.show(5)

total = telemetrie.count()
tete = volumetrie.first()
print(f"\nCellule la plus chargée : {tete['cell_id']} = "
      f"{100 * tete['count'] / total:.1f} % du trafic total, à elle seule.")

# %% [markdown]
# On se place dans les conditions par défaut de Spark : 200 partitions de
# shuffle. Chaque clé (`cell_id`) est envoyée dans **une** partition, choisie
# par hachage : `partition = hash(cell_id) modulo 200`. Toutes les lignes d'une
# même cellule arrivent donc dans la **même** partition, traitée par une
# **seule** tâche, quelle que soit leur quantité. C'est indispensable (il faut
# réunir toutes les données d'une clé) et c'est la source du problème : si
# l'on expédie les lignes brutes, la tâche chargée de la cellule de la gare
# reçoit à elle seule la part de trafic affichée ci-dessus.
#
# **Lire `profil_taches`.** La fonction affiche, pour le stage qui a lu le
# shuffle, la durée **médiane** des tâches (la moitié ont été plus rapides),
# le **p90** (90 % ont été plus rapides) et le **MAX**. Si les données sont
# bien réparties, les trois sont proches. Un MAX très supérieur à la médiane
# signale une tâche surchargée : c'est elle qui fixe la durée du stage.
#
# Commençons par une simple agrégation par cellule.

# %%
spark.conf.set("spark.sql.shuffle.partitions", 200)

with chrono("agrégation par cellule, 200 partitions"):
    telemetrie.groupBy("cell_id").agg(F.avg("latence_ms")).count()
profil_taches(spark)

# %% [markdown]
# **Q4.3** Cette agrégation souffre-t-elle du déséquilibre ? Pourquoi ?
# *(Indice : que fait chaque tâche **avant** d'expédier ses données ? C'est le
# mécanisme de la question Q2.3.)*
#
# Le déséquilibre fait mal là où l'on ne peut pas pré-agréger : **les
# jointures**. Forçons une jointure par shuffle (diffusion interdite, comme en
# 4.1). La requête calcule la latence moyenne **par secteur** : elle joint
# chaque événement à sa cellule pour connaître son secteur, puis agrège.

# %%
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

with chrono("jointure sur clé déséquilibrée — SANS AQE"):
    (telemetrie.join(cellules, "cell_id")
               .groupBy("secteur").agg(F.avg("latence_ms")).count())
profil_taches(spark)

# %% [markdown]
# **Q4.4** Relevez le rapport entre la tâche la plus longue et la tâche
# médiane. Reliez-le au pourcentage de trafic de la cellule la plus chargée.
#
# **Q4.5** Vous ajoutez 20 machines au cluster. La tâche la plus lente
# va-t-elle plus vite ? Le stage se terminera-t-il plus tôt ? Quelle loi
# reconnaissez-vous ?

# %% [markdown]
# **Réponse Q4.3 :**
#
# **Réponse Q4.4 :**
#
# **Réponse Q4.5 :**

# %% [markdown]
# ### Adaptive Query Execution (AQE)
#
# Depuis Spark 3, le moteur peut observer les statistiques **réelles** du
# shuffle une fois écrit, et réécrire la suite du plan pendant l'exécution.
# Jusqu'ici, Catalyst décidait tout **avant** de lancer le calcul, à partir
# d'estimations. AQE (*Adaptive Query Execution*) corrige le plan **entre deux
# stages**, avec les vraies tailles. Il sait notamment :
#
# * **regrouper** des partitions de sortie trop petites en moins de tâches ;
# * **découper** une partition trop grosse en plusieurs tâches (*skew join*) ;
# * **changer de stratégie de jointure** s'il découvre qu'une table est en
#   fait assez petite pour être diffusée.
#
# À vous de trouver, dans la Spark UI, ce qu'il a réellement fait ici.
#
# **La règle du découpage.** AQE ne considère une partition comme déséquilibrée
# que si elle remplit **les deux** conditions : être plus grosse que
# `skewedPartitionFactor` fois la taille médiane des partitions, **et** plus
# grosse que `skewedPartitionThresholdInBytes`. La cellule affiche ces deux
# réglages.

# %%
spark.conf.set("spark.sql.adaptive.enabled", True)

with chrono("même jointure — AVEC AQE"):
    (telemetrie.join(cellules, "cell_id")
               .groupBy("secteur").agg(F.avg("latence_ms")).count())
profil_taches(spark)
metriques_stages(spark, n=4)

print("\nRéglages AQE pour les partitions déséquilibrées :")   # voir Q4.7
for cle in ("spark.sql.adaptive.skewJoin.enabled",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"):
    print(f"  {cle} = {spark.conf.get(cle)}")

# On remet les réglages du TD pour la suite de la séquence.
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
spark.conf.set("spark.sql.adaptive.enabled", False)

# %% [markdown]
# **Q4.6** Combien de tâches reste-t-il ? Que sont devenues les 200 ? (Onglet
# *SQL / DataFrame*, dernière requête : cherchez `AQEShuffleRead`.) La durée
# a baissé : est-ce parce que le déséquilibre a été corrigé ? Avec si peu de
# tâches, le rapport max/médiane a-t-il encore un sens ?
#
# **Q4.7** AQE sait aussi **découper** une partition trop lourde
# (`skewJoin.enabled = true` ci-dessus). Il ne l'a pas fait ici : d'après les
# deux seuils affichés et le volume de shuffle mesuré, expliquez pourquoi.
# Faut-il encore comprendre le déséquilibre quand on a AQE ? Que se passe-t-il
# si une seule clé produit plus de données que la mémoire d'un executor ?

# %% [markdown]
# **Réponse Q4.6 :**
#
# **Réponse Q4.7 :**

# %% [markdown]
# ## 4.4 — Le cache
#
# La supervision va interroger `charge` plusieurs fois de suite. Sans cache,
# **chaque** action relit le Parquet, refait la jointure et l'agrégation.
#
# Pourquoi ? Pour la même raison qu'en séquence 2 : `charge` n'est pas un
# tableau de résultats, c'est un **plan**. Chaque action (`count`, `show`,
# `write`…) exécute ce plan depuis le début. `persist` fonctionne sur un
# DataFrame comme sur un RDD : il demande à Spark de garder le résultat la
# première fois qu'il est calculé.
#
# La cellule ci-dessous lance trois requêtes typiques de la supervision sur
# `charge` et chronomètre chacune, puis le total. Dans l'onglet *Jobs*, chaque
# job porte le nom de sa requête.

# %%
requetes = [
    ("cellules saturées",      lambda d: d.filter(F.col("taux_charge") > 70).count()),
    ("pire heure par secteur", lambda d: d.groupBy("secteur").agg(F.max("taux_charge")).count()),
    ("charge moyenne 4G/5G",   lambda d: d.groupBy("techno").agg(F.avg("taux_charge")).count()),
]

def trois_requetes(d, titre):
    with chrono(f"3 requêtes {titre}"):
        for nom, f in requetes:
            with chrono("   " + nom):
                f(d)

trois_requetes(charge, "SANS cache")

# %% [markdown]
# **TODO 5** — Mettez `charge` en cache avec
# `charge.persist(StorageLevel.MEMORY_AND_DISK)`, puis déclenchez **une
# action** sur `charge` pour le remplir (rappel séquence 2 : `persist` seul
# ne calcule rien). Relancez ensuite les trois requêtes.

# %%
# TODO 5

trois_requetes(charge, "AVEC cache")

# %% [markdown]
# **Q4.8** Sur quelle partie du travail porte le gain ? Vérifiez l'onglet
# *Storage* de la Spark UI. Que se passerait-il si `charge` ne tenait pas en
# mémoire ?

# %% [markdown]
# **Réponse Q4.8 :**

# %% [markdown]
# ## 4.5 — Livrer le résultat au service Node.js
#
# **TODO 6** — Écrivez dans `os.path.join(SORTIE, "saturation")`, au format
# JSON et en mode `overwrite`, les lignes de `charge` dont `taux_charge > 70`,
# triées par taux décroissant, avec les colonnes attendues par le tableau de
# bord : `cell_id`, `secteur`, `techno`, `heure`, `connexions_moy`,
# `capacite_max`, `taux_charge`.
#
# Forme générale : `charge.filter(...).select(...).orderBy(...).write.mode("overwrite").json(...)`
#
# * Le `select` fixe le **contrat** avec le tableau de bord : les colonnes et
#   leurs noms sont ce que le service Node.js s'attend à lire.
# * Le mode `overwrite` remplace une sortie précédente. Sans lui, Spark refuse
#   d'écrire dans un dossier qui existe déjà : vous ne pourriez pas relancer
#   la cellule.
# * `.json(...)` écrit un objet JSON par ligne, le même format que
#   `telemetrie.ndjson`.

# %%
# TODO 6

# %% [markdown]
# Regardez le contenu de `data/out/saturation/` dans l'explorateur de
# JupyterLab : Spark n'écrit jamais **un** fichier, mais un dossier de
# fichiers `part-*` (un par partition), plus un marqueur `_SUCCESS`.
#
# Puis, dans un terminal JupyterLab :
#
# ```bash
# node consumer/api.js
# ```
#
# et ouvrez http://localhost:3000. Lisez `consumer/api.js` : comment le
# service recolle-t-il les fichiers `part-*` ?

# %% [markdown]
# ## Fin de séquence

# %%
pause()        # en mode script uniquement ; dans un notebook, ne fait rien
spark.stop()
