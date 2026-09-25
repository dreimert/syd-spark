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
# Chaque RDD retient **de quel(s) RDD il provient et par quelle opération** :
# c'est son **lignage** (*lineage*). Spark s'en sert pour construire le plan
# d'exécution. Ce qui compte pour le plan, c'est la nature de chaque lien.
#
# **Dépendance étroite** (*narrow*) — `map`, `filter`… Chaque partition du
# résultat ne dépend que de **la partition correspondante** du parent. Chaque
# ligne est traitée seule, sans rien savoir des autres. Un cœur peut donc
# enchaîner `map` puis `filter` sur sa partition, ligne par ligne, sans
# attendre personne ni échanger quoi que ce soit.
#
# **Dépendance large** (*wide*) — `reduceByKey`, `groupByKey`, `join`… Une
# partition du résultat a besoin de données venant de **toutes** les
# partitions du parent. Pour compter les lignes de 17 h, il faut réunir *toutes*
# les lignes de 17 h, alors qu'elles sont éparpillées dans toutes les
# partitions du fichier. Spark doit redistribuer les données par clé : c'est le
# **shuffle**.
#
# ```
#   étroite (map, filter)            large (reduceByKey)
#
#   P0 ──► P0                        P0 ─┐     ┌─► R0
#   P1 ──► P1                        P1 ─┼─────┼─► R1
#   P2 ──► P2                        P2 ─┘     └─► R2
#                                    chaque Pi envoie un morceau à chaque Rj
#   un seul stage                    stage 1 │ shuffle │ stage 2
# ```
#
# Un **stage** est une suite d'opérations étroites qui s'enchaînent sans
# échange. Chaque dépendance large coupe le plan : le stage suivant ne peut pas
# commencer tant que le précédent n'a pas fini de produire ses données.
#
# Les deux RDD ci-dessous illustrent chaque cas :
#
# * `etroit` calcule la longueur de chaque ligne et ne garde que celles de
#   plus de 100 caractères ;
# * `large` compte les lignes **par heure**. Dans
#   `{"ts":"2026-10-24T17:28:53.601Z",…`, les caractères 18 et 19 sont l'heure
#   (`"17"`) : `l[18:20]` l'extrait sans parser le JSON.

# %%
# Deux transformations étroites : chaque ligne est traitée indépendamment.
etroit = logs.map(lambda l: len(l)).filter(lambda n: n > 100)

# map étroit (ligne -> (heure, 1)), puis reduceByKey : dépendance large.
large = logs.map(lambda l: (l[18:20], 1)).reduceByKey(lambda a, b: a + b)

# toDebugString ne lance aucun calcul : il affiche le lignage.
print("=== Dépendances ÉTROITES (map + filter) : un seul stage ===")
print(etroit.toDebugString().decode())
print()
print("=== Dépendance LARGE (reduceByKey) : deux stages, une frontière ===")
print(large.toDebugString().decode())

# %% [markdown]
# **Lire la sortie de `toDebugString`.** Elle se lit **de bas en haut**, du
# fichier vers le résultat. Chaque ligne est un RDD intermédiaire.
#
# * `(N)` : le nombre de partitions du RDD, donc de tâches pour ce stage.
# * `HadoopRDD` / `MapPartitionsRDD … at textFile` : la lecture du fichier.
# * `PythonRDD` : vos lambdas Python. Remarquez que `map` et `filter`
#   n'apparaissent **pas** séparément dans le premier graphe : Spark les a
#   fusionnés en un seul `PythonRDD`. Chaque ligne passe par `map` puis
#   `filter` en une seule passe, sans RDD intermédiaire en mémoire.
# * `|` : même stage que la ligne du dessus.
# * `+-` avec un décalage d'indentation : **frontière de stage**. Tout ce qui
#   est en dessous et plus indenté appartient au stage précédent. Il n'y en a
#   aucun dans le premier graphe, un seul dans le second.

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

# %% [markdown]
# **Les deux versions, pas à pas.** Prenons une cellule `CELL_0001` qui aurait
# seulement quatre mesures de latence : 40, 80, 30 et 50 ms (moyenne : 50).
#
# **A1 — `groupByKey` puis moyenne**
#
# ```
# map          ("CELL_0001", 40)  ("CELL_0001", 80)  ("CELL_0001", 30)  ("CELL_0001", 50)
# groupByKey   ("CELL_0001", [40, 80, 30, 50])       une clé -> la liste de TOUTES ses valeurs
# mapValues    ("CELL_0001", 200 / 4 = 50.0)
# ```
#
# **A2 — `reduceByKey` sur des couples `(somme, compte)`**
#
# ```
# map          ("CELL_0001", (40, 1))  ("CELL_0001", (80, 1))  ("CELL_0001", (30, 1))  ("CELL_0001", (50, 1))
# reduceByKey  combine((40, 1), (80, 1))   = (120, 2)
#              combine((30, 1), (50, 1))   = (80, 2)
#              combine((120, 2), (80, 2))  = (200, 4)
# mapValues    ("CELL_0001", 200 / 4 = 50.0)
# ```
#
# **Pourquoi ne pas réduire directement les moyennes ?** Parce que la moyenne
# de deux moyennes est fausse dès que les groupes n'ont pas la même taille.
# Exemple : moyenne(10) = 10 et moyenne(20, 30, 40) = 30 ; la moyenne de ces
# deux moyennes vaut 20, alors que la vraie moyenne des quatre valeurs est
# 100 / 4 = 25. Le couple `(somme, compte)`, lui,
# se combine sans erreur dans n'importe quel ordre : on ne divise qu'à la fin.
#
# Deux opérations nouvelles :
#
# * `mapValues(f)` applique `f` à la **valeur** de chaque couple et garde la
#   clé telle quelle. C'est un `map` qui ne touche pas à la clé.
# * `collect()` est une **action** : elle déclenche le calcul et rapatrie
#   tout le résultat dans le driver, sous forme de liste Python. À réserver
#   aux petits résultats (ici, une ligne par cellule).

# %%
# A1 : on regroupe toutes les latences de chaque cellule, puis on moyenne.
with chrono("A1  groupByKey  -> moyenne"):
    a1 = (recs.map(lambda r: (r["cell_id"], r["latence_ms"]))   # (cellule, latence)
              .groupByKey()                                      # (cellule, [latences…])
              .mapValues(lambda v: sum(v) / len(v))              # (cellule, moyenne)
              .collect())

# A2 : on transporte des (somme, compte), combinables deux à deux.
with chrono("A2  reduceByKey (somme, compte) -> moyenne"):
    a2 = (recs.map(lambda r: (r["cell_id"], (r["latence_ms"], 1)))  # (cellule, (latence, 1))
              .reduceByKey(combine)                                  # (cellule, (somme, compte))
              .mapValues(lambda s: s[0] / s[1])                      # (cellule, moyenne)
              .collect())

# collect() ne garantit pas l'ordre des cellules : on trie avant de comparer.
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
# façons. Seul le contenu de la **valeur** change dans le `map` :
#
# * **B1** garde l'enregistrement entier comme valeur : `r` est le
#   dictionnaire Python issu du JSON, avec ses sept champs (`ts`, `cell_id`,
#   `niveau`, `event`, `connexions_actives`, `latence_ms`, `octets`).
#   `groupByKey` rassemble la liste de ces dictionnaires pour chaque cellule,
#   et `mapValues(len)` compte les éléments de la liste.
# * **B2** remplace l'enregistrement par un simple `1` (on dit qu'on le
#   **projette** : on ne garde que ce dont le calcul a besoin), puis additionne
#   les `1`. C'est exactement le comptage de la séquence 1.
#
# ```
# B1  map        ("CELL_0001", {"ts": …, "cell_id": …, "niveau": …, … 7 champs})
#     groupByKey ("CELL_0001", [{…}, {…}, {…}])
#     mapValues  ("CELL_0001", 3)
#
# B2  map        ("CELL_0001", 1)
#     reduceByKey 1 + 1 + 1
#                ("CELL_0001", 3)
# ```

# %%
# B1 : valeur = l'enregistrement complet ; on ne s'en sert que pour compter.
with chrono("B1  shuffle de l'ENREGISTREMENT ENTIER"):
    b1 = (recs.map(lambda r: (r["cell_id"], r))    # (cellule, {7 champs})
              .groupByKey()                         # (cellule, [{…}, {…}, …])
              .mapValues(len)                       # (cellule, nombre)
              .collect())

# B2 : valeur = 1 ; on additionne les 1.
with chrono("B2  projection AVANT le shuffle"):
    b2 = (recs.map(lambda r: (r["cell_id"], 1))    # (cellule, 1)
              .reduceByKey(lambda x, y: x + y)      # (cellule, nombre)
              .collect())

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
# ## 2.7 — Tolérance aux pannes : à quoi sert le lignage
#
# En mode `local[*]`, tout tourne dans un seul processus : on ne peut pas
# « perdre une machine ». Voici ce qui se passe sur un vrai cluster.
#
# **Le scénario.** Le calcul A2 (latence moyenne par cellule) tourne sur trois
# machines M1, M2, M3, chacune avec un executor. Le fichier est stocké sur un
# système de fichiers répliqué (HDFS, S3…) : chaque bloc existe en plusieurs
# copies, et reste lisible même si une machine tombe. Le job a deux stages :
#
# * **stage 1** : chaque tâche lit une partition du fichier, applique le `map`
#   et écrit sa part du shuffle **sur le disque local de sa machine** ;
# * **stage 2** : chaque tâche va chercher, sur toutes les machines, les
#   morceaux de shuffle qui la concernent, puis finit la réduction.
#
# Au milieu du stage 2, on débranche M3.
#
# **Ce que fait Spark.**
#
# 1. **Détection.** Le driver ne reçoit plus de nouvelles de l'executor de M3
#    et le déclare perdu.
# 2. **Les tâches du stage 2 qui tournaient sur M3** sont simplement relancées
#    sur M1 ou M2. Une tâche n'a pas d'état caché : la relancer donne le même
#    résultat.
# 3. **Les sorties du stage 1 écrites sur M3 ont disparu** avec son disque.
#    Les tâches du stage 2 qui en avaient besoin échouent en voulant les lire
#    (*fetch failed*). Spark relance alors le stage 1, mais **uniquement pour
#    les partitions dont la sortie était sur M3**. Les autres sont intactes
#    sur M1 et M2 : on ne les recalcule pas.
# 4. **Comment Spark sait-il recalculer ces partitions ?** Grâce au lignage.
#    Pour la partition 7 du stage 1, il connaît la recette : « lire le bloc 7
#    du fichier, appliquer `json.loads`, puis le `map` vers
#    `(cell_id, (latence, 1))` ». Le fichier étant répliqué, le bloc 7 est
#    relu depuis une autre copie, et la recette est rejouée sur M1 ou M2.
# 5. Le stage 2 reprend et **le job réussit**, simplement plus lentement.
#    Dans la Spark UI, on verrait des tâches en échec et un stage marqué
#    *retry*.
#
# **L'idée clé.** Pour survivre aux pannes, on pourrait **recopier** chaque
# résultat intermédiaire sur plusieurs machines. Ce serait sûr, mais coûteux à
# chaque étape, même quand rien ne tombe en panne. Spark fait l'inverse : il
# ne réplique que les données d'entrée (c'est le rôle du système de
# fichiers), et retient pour tout le reste **la recette** plutôt que le
# résultat. Il ne paie le recalcul que si une panne survient, et seulement
# pour les partitions perdues. C'est ce que veut dire *Resilient* dans RDD,
# et c'est pour cela qu'un RDD doit être immuable (séquence 1) : une recette
# rejouée sur les mêmes entrées doit redonner exactement le même résultat.
#
# **Les limites.**
#
# * Le **cache** est perdu lui aussi : les partitions de `recs` mises en
#   mémoire sur M3 sont recalculées depuis leur lignage, à la première
#   utilisation.
# * Si **le driver** tombe, tout est perdu : c'est lui qui détient le lignage
#   et le plan. Il faut relancer l'application.
# * Si les données d'entrée ne sont **pas relisibles** (un fichier présent
#   uniquement sur le disque de M3), la recette ne peut pas être rejouée.
# * Une tâche qui échoue trop souvent (4 fois par défaut) fait échouer le job.
#
# **Q2.7** Dans le scénario ci-dessus, pourquoi Spark n'a-t-il pas besoin de
# recalculer les partitions du stage 1 produites par M1 et M2 ? Et que
# se passerait-il si M3 tombait **pendant le stage 1**, avant tout shuffle ?

# %% [markdown]
# **Réponse Q2.7 :**

# %% [markdown]
# ## Fin de séquence
#
# Explorez la Spark UI, **puis** exécutez la cellule ci-dessous.

# %%
pause()        # en mode script uniquement ; dans un notebook, ne fait rien
spark.stop()
