# %% [markdown]
# # Séquence 3 — DataFrames, Catalyst et format de stockage  (25 min)
#
# Jusqu'ici Spark exécutait vos lambdas sans rien comprendre à ce qu'elles
# faisaient. Avec les DataFrames, vous décrivez **l'intention**, et un
# optimiseur (Catalyst) décide du **plan**.
#
# ⚠ Cette séquence écrit aussi le fichier Parquet dont la séquence 4 a besoin :
# exécutez-la au moins jusqu'à la section 3.3 incluse.

# %%
from lib import session, chrono, pause, taille_disque, NDJSON, PARQUET
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = session("seq3-dataframes")

# %% [markdown]
# ## 3.1 — Le schéma n'est pas gratuit
#
# Sans schéma, Spark doit lire les données une première fois rien que pour
# deviner leur structure.

# %%
with chrono("read.json SANS schéma (inférence)"):
    df_infere = spark.read.json(NDJSON)

schema = StructType([
    StructField("ts", StringType()),
    StructField("cell_id", StringType()),
    StructField("niveau", StringType()),
    StructField("event", StringType()),
    StructField("connexions_actives", LongType()),
    StructField("latence_ms", LongType()),
    StructField("octets", LongType()),
])

with chrono("read.json AVEC schéma explicite"):
    df = spark.read.schema(schema).json(NDJSON)

df.printSchema()
df.show(3)

# %% [markdown]
# **Quoi regarder dans la Spark UI.** Ouvrez l'onglet *Jobs* (vérifiez en
# haut à droite que l'application s'appelle bien `4TC-seq3-dataframes` : sinon,
# une session d'une séquence précédente est encore ouverte). Dans le tableau
# *Completed Jobs*, chaque ligne est un job, c'est-à-dire un passage de Spark
# sur des données. Trois colonnes suffisent :
#
# * **Description** : le libellé du `chrono` qui a lancé le job. Un job
#   lancé hors `chrono` garde un nom interne, comme `showString` pour
#   `df.show(3)`.
# * **Duration** : le temps passé par ce job.
# * **Tasks** : le nombre de tâches, une par morceau de fichier lu. Le job
#   de `show(3)` n'en a qu'une : il ne lit que le début du fichier. Un job
#   qui en a une par morceau a parcouru **tout** le fichier.
#
# Cherchez une ligne par `chrono` de la cellule ci-dessus. Un `chrono` qui
# n'a lancé aucun job n'apparaît pas du tout : son absence est une réponse.
# Cliquez sur la description d'un job pour voir ses stages.
#
# **Q3.1** D'où vient l'écart de durée entre les deux lectures ? Combien de
# fois Spark lit-il le fichier dans chaque cas, en comptant le calcul que vous
# lancerez ensuite sur `df` ? Que se passerait-il sur 2 To de logs ?

# %% [markdown]
# **Réponse Q3.1 :**

# %% [markdown]
# ## 3.2 — *Facultatif, si vous êtes en avance* : même calcul, deux API

# %%
import json
rdd = spark.sparkContext.textFile(NDJSON)

with chrono("latence moyenne par cellule — API RDD"):
    (rdd.map(json.loads)
        .map(lambda r: (r["cell_id"], (r["latence_ms"], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda s: s[0] / s[1])
        .collect())

with chrono("latence moyenne par cellule — API DataFrame"):
    (df.groupBy("cell_id")
       .agg(F.avg("latence_ms").alias("latence_moyenne"))
       .collect())

# %% [markdown]
# **Q3.2** Le code DataFrame est plus court. Est-il plus rapide ? De combien ?
# Où s'exécute `json.loads` dans le premier cas ? Où se fait le parsing dans le
# second ?

# %% [markdown]
# **Réponse Q3.2 :**

# %% [markdown]
# ## 3.3 — Le format de stockage : JSON contre Parquet

# %%
with chrono("conversion NDJSON -> Parquet"):
    (df.withColumn("ts", F.to_timestamp("ts"))
       .withColumn("heure", F.hour("ts"))
       .write.mode("overwrite")
       .partitionBy("niveau")
       .parquet(PARQUET))

mo_json = taille_disque(NDJSON)
mo_parquet = taille_disque(PARQUET)
print(f"\n  NDJSON  : {mo_json:8.1f} Mo")
print(f"  Parquet : {mo_parquet:8.1f} Mo   (facteur {mo_json / mo_parquet:.1f}×)")

# %% [markdown]
# Parquet range les données **par colonne** : toutes les valeurs de `cell_id`
# côte à côte, puis toutes celles de `latence_ms`, etc. `partitionBy("niveau")`
# crée en plus un sous-dossier par niveau (`niveau=ERROR/`, `niveau=INFO/`…) :
# regardez dans `data/parquet/telemetrie/` avec l'explorateur de JupyterLab.
#
# **Q3.3** Donnez trois raisons à l'écart de taille. Pistes : ce qui se répète
# à chaque ligne d'un fichier JSON ; ce qui se ressemble dans une colonne ;
# comment un nombre est écrit en texte et en binaire.
#
# **Q3.4** Ce facteur s'applique à tout ce qui traverse un disque ou un
# réseau. Qu'est-ce que cela change pour une antenne qui remonte sa télémétrie
# par un lien de collecte (backhaul) ?

# %% [markdown]
# **Réponse Q3.3 :**
#
# **Réponse Q3.4 :**

# %% [markdown]
# ## 3.4 — Catalyst : lire un plan d'exécution

# %%
dfp = spark.read.parquet(PARQUET)

requete = (dfp.filter(F.col("niveau") == "ERROR")
              .filter(F.col("latence_ms") > 100)
              .select("cell_id", "latence_ms"))

requete.explain()

# %% [markdown]
# Dans le plan ci-dessus, cherchez trois choses :
#
# * `PartitionFilters: [..., (niveau = ERROR)]` — des dossiers entiers jamais ouverts
# * `PushedFilters: [..., GreaterThan(latence_ms,100)]` — le filtre descendu
#   dans le lecteur Parquet, pas appliqué après coup par Spark
# * `ReadSchema: struct<cell_id:string,latence_ms:bigint>` — deux colonnes
#   lues sur sept
#
# **Q3.5** Vous avez écrit `filter` puis `filter` puis `select`. Catalyst
# a-t-il exécuté dans cet ordre ? Aurait-il pu en faire autant avec vos lambdas
# Python de la séquence 1 ? **Pourquoi ?**

# %%
with chrono("même requête, source Parquet"):
    n_parquet = requete.count()
with chrono("même requête, source JSON"):
    n_json = (df.filter(F.col("niveau") == "ERROR")
                .filter(F.col("latence_ms") > 100)
                .select("cell_id", "latence_ms").count())
print("Mêmes résultats :", n_parquet == n_json, "-", f"{n_parquet:,}".replace(",", " "), "lignes")

# %% [markdown]
# **Réponse Q3.5 :**

# %% [markdown]
# ## 3.5 — À vous : votre première requête DataFrame
#
# Vous en aurez besoin en séquence 4 : c'est la même mécanique.
#
# **TODO 3** — Sur `dfp` (le Parquet), pour les événements de niveau `WARN`
# uniquement, calculez **par cellule** le nombre d'événements et la latence
# moyenne arrondie à 1 décimale. Affichez les 5 cellules à la latence moyenne
# la plus élevée. Puis affichez le plan avec `.explain()` et retrouvez-y le
# `PartitionFilters`.
#
# Les briques dont vous avez besoin, dans l'ordre :
#
# ```python
# dfp.filter(F.col("niveau") == "WARN")
#    .groupBy("cell_id")
#    .agg(F.count("*").alias("evenements"),
#         F.round(F.avg("latence_ms"), 1).alias("latence_moy"))
#    .orderBy(F.desc(...))
# ```
#
# *En Python, une expression sur plusieurs lignes doit être entourée de
# parenthèses.*

# %%
# TODO 3
warn = None   # <-- remplacez

# warn.show(5)
# warn.explain()

# %% [markdown]
# ## 3.6 — Spark SQL : la même chose, en SQL

# %%
dfp.createOrReplaceTempView("telemetrie")

spark.sql("""
    SELECT cell_id,
           COUNT(*)                AS incidents,
           ROUND(AVG(latence_ms))  AS latence_moyenne
    FROM telemetrie
    WHERE niveau = 'ERROR'
    GROUP BY cell_id
    ORDER BY incidents DESC
    LIMIT 10
""").show()

# %% [markdown]
# **Q3.6** RDD, DataFrame, SQL : lequel vous laisse le plus de liberté ?
# Lequel en laisse le plus à l'optimiseur ? En quoi est-ce le même arbitrage
# qu'entre une requête SQL et une boucle écrite à la main dans un programme ?

# %% [markdown]
# **Réponse Q3.6 :**

# %% [markdown]
# ## Fin de séquence
#
# Explorez la Spark UI (onglet *SQL / DataFrame* : cliquez sur une requête
# pour voir son plan dessiné), **puis** exécutez la cellule ci-dessous.

# %%
pause()        # en mode script uniquement ; dans un notebook, ne fait rien
spark.stop()
