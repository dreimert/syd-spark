# %% [markdown]
# # Séquence 2 — Évaluation paresseuse, DAG et coût du shuffle  (25 min)
#
# Le cœur du TD. Cette séquence se joue **autant dans la Spark UI que dans le
# notebook** : gardez http://localhost:4040 ouvert à côté.
#
# Onglets à connaître :
# * **Jobs** — un job par action déclenchée
# * **Stages** — un stage par « mur » de shuffle ; colonnes *Shuffle Read* /
#   *Shuffle Write* : les octets réellement transportés d'un stage à l'autre
# * **SQL / DataFrame** — les plans (séquence 3)

# %%
from lib import session, chrono, pause, metriques_stages, NDJSON
import json

spark = session("seq2-dag-shuffle")
sc = spark.sparkContext
logs = sc.textFile(NDJSON)

# %% [markdown]
# ## 2.1 — Paresse : une chaîne de transformations ne coûte rien

# %%
with chrono("5 transformations enchaînées (aucune action)"):
    chaine = (
        logs.filter(lambda l: '"niveau":"INFO"' not in l)
            .map(json.loads)
            .filter(lambda r: r["latence_ms"] > 50)
            .map(lambda r: (r["cell_id"], r["latence_ms"]))
            .filter(lambda kv: kv[0] != "CELL_0000")
    )

print("Regardez l'onglet Jobs de la Spark UI : il est vide. On n'a construit qu'un PLAN.")

# %%
with chrono("count() — la première action"):
    n = chaine.count()
print("Enregistrements retenus :", f"{n:,}".replace(",", " "))

# %% [markdown]
# **Q2.1** Combien de jobs voyez-vous maintenant dans l'onglet *Jobs* ?
# Combien de stages pour ce job ? Pourquoi ce nombre-là ?

# %% [markdown]
# **Réponse Q2.1 :**

# %% [markdown]
# ## 2.2 — Lire le lignage : dépendances étroites et larges
#
# `toDebugString` affiche le graphe de dépendances. Chaque décalage
# d'indentation marque une **frontière de stage**, c'est-à-dire un shuffle.

# %%
etroit = logs.map(lambda l: len(l)).filter(lambda n: n > 100)
large = logs.map(lambda l: (l[18:20], 1)).reduceByKey(lambda a, b: a + b)

print("=== Dépendances ÉTROITES (map + filter) : un seul stage ===")
print(etroit.toDebugString().decode())
print()
print("=== Dépendance LARGE (reduceByKey) : deux stages, une frontière ===")
print(large.toDebugString().decode())

# %% [markdown]
# **Q2.2** Dans le second graphe, repérez `ShuffledRDD`. Que se passe-t-il
# physiquement à cet endroit précis ? Où vont les octets ?

# %% [markdown]
# **Réponse Q2.2 :**

# %% [markdown]
# ## 2.3 — Ce que le chronomètre ne dit pas
#
# On veut la **latence moyenne par cellule**. Deux écritures correctes, qui
# rendent exactement le même résultat.
#
# La première regroupe **toutes** les latences de chaque cellule, puis fait
# la moyenne. La seconde transporte des couples `(somme, compte)` qu'on peut
# additionner au fur et à mesure.

# %%
from pyspark.storagelevel import StorageLevel

recs = logs.map(json.loads).persist(StorageLevel.MEMORY_AND_DISK)
with chrono("parsing JSON + mise en cache (payé une seule fois)"):
    recs.count()

# %% [markdown]
# **TODO 2** — Complétez la fonction `combine`, que la version A2 donne à
# `reduceByKey`. Elle reçoit deux couples `(somme, compte)` de la même cellule
# et doit renvoyer un seul couple `(somme, compte)`.
#
# Exemple : `(120, 2)` et `(30, 1)` doivent donner `(150, 3)`.
#
# *Rappels Python : `x[0]` est le premier élément du couple `x`, et
# `(a, b)` construit un couple.*

# %%
def combine(x, y):
    return ...        # TODO 2

# Vérification locale, sans Spark : corrigez tant que ceci échoue.
assert combine((120, 2), (30, 1)) == (150, 3), "combine est incorrecte : relisez l'exemple"
print("combine est correcte")

# %%
with chrono("A1  groupByKey  -> moyenne"):
    a1 = (recs.map(lambda r: (r["cell_id"], r["latence_ms"]))
              .groupByKey()
              .mapValues(lambda v: sum(v) / len(v))
              .collect())

with chrono("A2  reduceByKey (somme, compte) -> moyenne"):
    a2 = (recs.map(lambda r: (r["cell_id"], (r["latence_ms"], 1)))
              .reduceByKey(combine)
              .mapValues(lambda s: s[0] / s[1])
              .collect())

print("Résultats identiques :", sorted(a1) == sorted(a2))
metriques_stages(spark, n=4)

# %% [markdown]
# Dans le tableau ci-dessus (le plus récent en haut), chaque version a produit
# deux stages : l'un **écrit** le shuffle (colonne *shuffle write*), le suivant
# le **relit** (*shuffle read*). Les deux lignes du bas sont A1, les deux du
# haut A2. Retrouvez-les dans l'onglet *Stages* de la Spark UI.
#
# **Q2.3 — la plus importante du TD.**
# Les deux durées sont voisines. Les **octets transférés** ne le sont pas :
# relevez le *shuffle write* des deux versions et calculez le rapport.
#
# Que transporte `groupByKey` ? Que transporte `reduceByKey` ?
# Nommez le mécanisme responsable de l'écart.
# *(Indice : l'un agrège côté émetteur avant d'expédier ; l'autre expédie tout
# pour ne regrouper qu'à l'arrivée.)*
#
# **Q2.4** Pourquoi l'écart de durée est-il, lui, si faible ?
# Sur quel support transite le shuffle en mode `local[*]` ? Que deviendrait
# cet écart sur 40 machines partageant un lien réseau de 10 Gb/s ?

# %% [markdown]
# **Réponse Q2.3 :**
#
# **Réponse Q2.4 :**

# %% [markdown]
# ## 2.5 — Projeter avant de mélanger
#
# Ici, la durée parle aussi. On compte les événements par cellule, de deux
# façons.

# %%
with chrono("B1  shuffle de l'ENREGISTREMENT ENTIER"):
    b1 = recs.map(lambda r: (r["cell_id"], r)).groupByKey().mapValues(len).collect()

with chrono("B2  projection AVANT le shuffle"):
    b2 = recs.map(lambda r: (r["cell_id"], 1)).reduceByKey(lambda x, y: x + y).collect()

print("Résultats identiques :", sorted(b1) == sorted(b2))
metriques_stages(spark, n=4)

# %% [markdown]
# **Q2.5** Relevez volumes et durées. En B1, on a transporté sept champs par
# enregistrement pour n'en compter aucun. Formulez la règle en une phrase —
# c'est celle que l'optimiseur appliquera tout seul en séquence 3, et que vous
# devez appliquer à la main sur les RDD.

# %% [markdown]
# **Réponse Q2.5 :**

# %% [markdown]
# ## 2.6 — *Facultatif, si vous êtes en avance* : le nombre de partitions

# %%
paires = recs.map(lambda r: (r["cell_id"], 1))

for p in (1, 8, 200):
    with chrono(f"reduceByKey avec {p:>3d} partitions de sortie"):
        paires.reduceByKey(lambda x, y: x + y, numPartitions=p).count()

# %% [markdown]
# **Q2.6** Comparez les trois durées et expliquez-les.
# Pistes : combien de clés distinctes y a-t-il en sortie ? Combien de tâches
# Spark crée-t-il dans chaque cas, et que fait chacune ? Quel est le coût fixe
# d'une tâche (planification, lancement, écriture d'un fichier de shuffle) ?
#
# 200 est la **valeur par défaut** de `spark.sql.shuffle.partitions` — celle
# que vous subirez si vous n'y touchez pas.

# %% [markdown]
# **Réponse Q2.6 :**

# %% [markdown]
# ## 2.7 — Démonstration au tableau : tolérance aux pannes
#
# L'enseignant lance ce même calcul sur un petit cluster de trois machines et
# en arrête une pendant le job.
#
# **Q2.7** Le job échoue-t-il ? Spark recalcule-t-il tout, ou seulement les
# partitions perdues ? À quoi sert le lignage, concrètement ?

# %% [markdown]
# **Réponse Q2.7 :**

# %% [markdown]
# ## Fin de séquence
#
# Explorez la Spark UI, **puis** exécutez la cellule ci-dessous.

# %%
pause()        # en mode script uniquement ; dans un notebook, ne fait rien
spark.stop()
