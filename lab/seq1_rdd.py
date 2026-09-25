# %% [markdown]
# # Séquence 1 — Du JavaScript fonctionnel au RDD  (15 min)
#
# Vous savez déjà écrire ceci en JavaScript :
#
# ```js
# const incidents = lignes
#   .filter(l => l.includes('"niveau":"ERROR"'))
#   .map(l => [JSON.parse(l).event, 1])
#   .reduce(...)
# ```
#
# Vous allez écrire *le même code*. Ce qui change n'est pas la syntaxe :
# c'est **où** et **comment** il s'exécute.
#
# **Mode d'emploi.** Exécutez les cellules une par une avec `Maj+Entrée`.
# Gardez la Spark UI (http://localhost:4040) ouverte dans un autre onglet et
# regardez-la entre deux cellules. Répondez aux questions dans les cellules
# **Réponse** prévues à cet effet : ce fichier fait partie du livrable.

# %%
from lib import session, chrono, pause, NDJSON
import json

with chrono("démarrage de la session Spark"):
    spark = session("seq1-rdd")
sc = spark.sparkContext

# %% [markdown]
# Notez cette durée : vous en aurez besoin à la question Q1.3.
#
# ## 1.1 — Le RDD : une collection découpée en partitions
#
# Un **RDD** (*Resilient Distributed Dataset*) est la collection de base de
# Spark. Pensez à un tableau JavaScript, avec trois différences :
#
# * **Distributed** : il n'est pas stocké d'un seul bloc. Il est découpé en
#   **partitions**, et chaque partition peut être traitée par un cœur (ou une
#   machine) différent.
# * **Resilient** : si une partition est perdue (panne d'un executor), Spark
#   sait la recalculer. Vous verrez comment en séquence 2.
# * **Immuable** : on ne le modifie jamais. `filter` ou `map` ne changent pas
#   le RDD d'origine, ils en créent un nouveau (section 1.5).
#
# `textFile` ne lit rien. Il décrit une collection distribuée de lignes.

# %%
logs = sc.textFile(NDJSON)

print("Nombre de partitions :", logs.getNumPartitions())
print("Une partition = une unité de travail = une tâche exécutée sur un cœur.")
print()
with chrono("take(3)"):
    premieres = logs.take(3)
for l in premieres:
    print("  ", l)

# %% [markdown]
# **Q1.1** Combien de partitions ? Comparez à votre nombre de cœurs.
# D'où vient ce découpage — de vous, du fichier, ou de Spark ?
#
# **Q1.2** `take(3)` a-t-il lu tout le fichier ? Regardez sa durée, et le
# nombre de tâches du job correspondant dans l'onglet *Jobs* de la Spark UI.

# %% [markdown]
# **Réponse Q1.1 :**
#
# **Réponse Q1.2 :**
#

# %% [markdown]
# ## 1.2 — `filter` / `map` / `reduceByKey` : vos primitives, distribuées
#
# **Objectif :** compter les incidents critiques (niveau `ERROR`) par type
# d'événement. Chaque ligne du fichier est un objet JSON, par exemple :
#
# ```json
# {"ts":"…","cell_id":"CELL_0042","niveau":"ERROR","event":"BACKHAUL_LOSS", …}
# ```
#
# La chaîne se lit en trois étapes. Chacune prend un RDD et en produit un
# nouveau :
#
# | étape | RDD produit | contenu d'un élément |
# |---|---|---|
# | `filter` | `incidents` | une ligne brute (texte), uniquement les `ERROR` |
# | `map` | `paires` | un couple `(event, 1)`, ex. `("BACKHAUL_LOSS", 1)` |
# | `reduceByKey` | `comptes` | un couple `(event, total)`, ex. `("BACKHAUL_LOSS", n)` |
#
# **Pourquoi des couples `(clé, valeur)` ?** Spark ne sait pas ce que
# représentent vos données. Tout ce qu'il comprend, c'est cette convention : le
# 1er élément du couple est la **clé** qui sert à regrouper, le 2e est la
# **valeur** à combiner. Le `1` signifie « j'ai vu cet événement une fois ».
#
# **Que fait `reduceByKey` ?** Il rassemble toutes les valeurs qui ont la même
# clé, puis les réduit deux à deux avec la fonction que vous lui donnez :
#
# ```
# ("BACKHAUL_LOSS", 1), ("AUTH_FAILURE", 1), ("BACKHAUL_LOSS", 1), ("BACKHAUL_LOSS", 1)
#   BACKHAUL_LOSS : 1 + 1 = 2, puis 2 + 1 = 3   →  ("BACKHAUL_LOSS", 3)
#   AUTH_FAILURE  : une seule valeur            →  ("AUTH_FAILURE", 1)
# ```
#
# En JS, vous feriez un `.reduce()` qui remplit un objet `{event: total}` :
# une seule boucle, sur un seul cœur, avec un accumulateur partagé.
# `reduceByKey` fait la même chose, mais les lignes sont réparties sur
# plusieurs partitions traitées en parallèle. Personne ne tient un
# accumulateur global, donc Spark doit **réunir** les valeurs d'une même clé.
# Ce qu'il en coûte est le sujet de la séquence 2.

# %%
# Étape 1 — garder uniquement les lignes d'erreur.
# On teste le texte brut, sans parser le JSON : c'est bien plus rapide, et
# la grande majorité des lignes (INFO, WARN) sont éliminées ici.
# JS : lignes.filter(l => l.includes('"niveau":"ERROR"'))
incidents = logs.filter(lambda l: '"niveau":"ERROR"' in l)

# Étape 2 — transformer chaque ligne retenue en couple (clé, valeur).
# On ne parse le JSON que pour les lignes qui ont passé le filtre.
# Clé = type d'événement, valeur = 1 (une occurrence).
# JS : .map(l => [JSON.parse(l).event, 1])
paires = incidents.map(lambda l: (json.loads(l)["event"], 1))

# Étape 3 — additionner les 1 de chaque clé.
# La lambda reçoit deux valeurs de la MÊME clé (jamais la clé elle-même) et
# renvoie leur combinaison. Spark l'applique jusqu'à ce qu'il ne reste
# qu'une valeur par clé.
# JS : pas d'équivalent direct — c'est ici que Spark cesse d'être du JS.
comptes = paires.reduceByKey(lambda a, b: a + b)

print("Aucun calcul n'a encore eu lieu. Rien n'est apparu dans la Spark UI.")

# %%
with chrono("comptage des incidents par type"):
    resultat = sorted(comptes.collect(), key=lambda kv: -kv[1])

print("Incidents critiques par type :")
for evenement, n in resultat:
    print(f"  {evenement:<20s} {n:>12,}".replace(",", " "))

# %% [markdown]
# ## 1.3 — À vous : quelle cellule tombe le plus en panne ?
#
# **TODO 1** — Recopiez la chaîne ci-dessus en changeant **une seule chose** :
# on veut compter les incidents par cellule (`cell_id`) et non plus par type
# d'événement. Affichez les 5 cellules qui ont le plus d'incidents.
#
# *Indice : dans `.map(...)`, c'est la clé du couple qui décide du regroupement.*

# %%
# TODO 1
par_cellule = None   # <-- remplacez

# for cell_id, n in sorted(par_cellule.collect(), key=lambda kv: -kv[1])[:5]:
#     print(cell_id, n)

# %% [markdown]
# Gardez en tête la cellule en tête de ce classement : elle reviendra en
# séquence 4.

# %% [markdown]
# ## 1.4 — La comparaison qui compte
#
# Ouvrez un terminal dans JupyterLab (*File → New → Terminal*) et lancez la
# version Node.js du même calcul, sur **le même fichier**, avec **le même
# algorithme** (lisez `producer/bench-node.js` : même filtre, même parsing) :
#
# ```bash
# node producer/bench-node.js
# ```
#
# **Q1.3** Notez les deux durées (Node sur 1 thread, Spark sur tous vos
# cœurs). Le rapport est-il égal à votre nombre de cœurs ? Proposez au moins
# trois explications. Pistes : ce que chaque ligne doit traverser avant
# d'arriver dans votre lambda Python ; ce que les cœurs se partagent ; ce que
# Spark doit organiser avant de lancer la moindre tâche. Et n'oubliez pas la
# durée de démarrage de la session, mesurée plus haut : Node ne la paie pas.
#
# **Q1.4** Sur ce volume, sur votre portable, Spark est-il le bon outil ?
# À partir de quand le deviendrait-il ? La question n'est pas rhétorique :
# savoir *ne pas* sortir Spark fait partie du métier.

# %% [markdown]
# **Réponse Q1.3 :**
#
# **Réponse Q1.4 :**

# %% [markdown]
# ## 1.5 — Immuabilité
#
# Le réflexe JavaScript `array.push()` n'existe pas ici.

# %%
try:
    logs.append("nouvelle ligne")           # noqa
except AttributeError as e:
    print("Erreur attendue :", e)

print()
print("Un RDD ne se modifie pas : chaque transformation en produit un NOUVEAU.")
print("C'est ce qui rend le recalcul après panne possible (séquence 2).")

# %% [markdown]
# ## Fin de séquence
#
# Explorez la Spark UI autant que vous voulez, **puis** exécutez la cellule
# ci-dessous : elle arrête Spark et libère la mémoire pour la séquence suivante.

# %%
pause()        # en mode script uniquement ; dans un notebook, ne fait rien
spark.stop()
