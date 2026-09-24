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
# 87 lignes d'un côté, 5 millions de l'autre. Deux stratégies :
#
# * **broadcast join** : on envoie une copie de la petite table à chaque
#   exécuteur, la grande ne bouge pas ;
# * **sort-merge join** : on redistribue les **deux** tables par clé (shuffle),
#   on les trie, puis on les fusionne.

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
# par hachage. Commençons par une simple agrégation par cellule.

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
# jointures**. Forçons une jointure par shuffle.

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

# %%
spark.conf.set("spark.sql.adaptive.enabled", True)

with chrono("même jointure — AVEC AQE"):
    (telemetrie.join(cellules, "cell_id")
               .groupBy("secteur").agg(F.avg("latence_ms")).count())
profil_taches(spark)
metriques_stages(spark, n=4)

print("\nRéglages AQE pour les partitions déséquilibrées :")
for cle in ("spark.sql.adaptive.skewJoin.enabled",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"):
    print(f"  {cle} = {spark.conf.get(cle)}")

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
# si une seule clé produit plus de données que la mémoire d'un exécuteur ?

# %% [markdown]
# **Réponse Q4.6 :**
#
# **Réponse Q4.7 :**

# %% [markdown]
# ## 4.4 — Le cache
#
# La supervision va interroger `charge` plusieurs fois de suite. Sans cache,
# **chaque** action relit le Parquet, refait la jointure et l'agrégation.

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
