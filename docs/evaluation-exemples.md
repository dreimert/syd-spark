# Évaluation — exemples de questions

**INSA Lyon · 4TC · TD Apache Spark**

Les questions ci-dessous donnent le **type** de questions posées à
l'évaluation. Elles ne sont pas celles du sujet, mais elles portent sur les
mêmes notions.

Vous n'aurez pas vos mesures du TD sous les yeux, et on ne vous les demandera
pas : les chiffres dépendent de votre portable. On vous demandera de
**raisonner sur le modèle d'exécution** : prédire ce que Spark va faire,
lire un plan, estimer un volume, choisir entre deux écritures.

La règle de l'énoncé s'applique : une réponse sans justification ne rapporte
rien, une justification juste avec un calcul approximatif rapporte tout.

---

## 1. Paresse, actions, jobs et stages

**E1.** On exécute le code suivant. Le fichier est lu en 16 partitions.

```python
logs    = sc.textFile("telemetrie.ndjson")
erreurs = logs.filter(lambda l: '"niveau":"ERROR"' in l)
paires  = erreurs.map(lambda l: (json.loads(l)["cell_id"], 1))
comptes = paires.reduceByKey(lambda a, b: a + b)

print(erreurs.count())
print(comptes.collect())
```

1. À quelle ligne Spark lit-il le fichier pour la première fois ?
2. Combien de jobs apparaissent dans la Spark UI ? Combien de stages
   dans chacun, et combien de tâches par stage ?
3. Combien de fois le fichier est-il lu en tout ? Ajoutez **une** ligne
   pour qu'il ne soit lu qu'une fois, et dites où la placer.

**E2.** Vrai ou faux ? Justifiez chaque réponse en une ou deux phrases.

1. `persist()` met immédiatement le RDD en mémoire.
2. Un `filter` placé **après** un `groupByKey` réduit le volume du shuffle.
3. Sur un RDD, Spark déplace de lui-même un `filter` avant un `map` s'il
   voit que c'est plus efficace.
4. Un broadcast join ne transfère aucune donnée entre machines.
5. `collect()` est une transformation.

**E3.** Voici la sortie de `toDebugString` d'un RDD (simplifiée).

```
(8) PythonRDD[6] at collect
 |  MapPartitionsRDD[5] at mapPartitions
 |  ShuffledRDD[4] at partitionBy
 +-(24) PairwiseRDD[3] at reduceByKey
    |  PythonRDD[2] at reduceByKey
    |  telemetrie.ndjson MapPartitionsRDD[1] at textFile
    |  telemetrie.ndjson HadoopRDD[0] at textFile
```

1. Combien de stages ? Combien de tâches dans chacun ?
2. Le programmeur a-t-il fixé le nombre de partitions de sortie ? Comment le
   savez-vous ?
3. Entre quelles lignes les données sont-elles écrites sur disque, puis
   relues par d'autres tâches ?

---

## 2. Le coût du shuffle

**E4.** 5 000 000 d'événements, 87 cellules, 24 partitions en entrée. On
calcule la latence moyenne par cellule de deux façons, comme en séquence 2 :
A1 avec `groupByKey`, A2 avec `reduceByKey` sur des couples
`(somme, compte)`.

1. Combien d'**enregistrements** chaque version écrit-elle, au plus, dans le
   shuffle ? Donnez un ordre de grandeur du rapport.
2. Le rapport mesuré **en octets** sera-t-il exactement celui-là ?
   Pourquoi ?
3. Nommez le mécanisme qui explique l'écart.

**E5.** Pour chaque statistique par cellule, dites si on peut la calculer avec
un `reduceByKey`. Si oui, donnez la valeur transportée et la fonction de
combinaison ; si non, expliquez pourquoi.

1. la latence maximale ;
2. le nombre d'événements `ERROR` ;
3. l'écart-type des latences ;
4. la latence médiane.

**E6.** Un collègue a écrit :

```python
(recs.map(lambda r: (r["cell_id"], r))
     .groupByKey()
     .mapValues(lambda rs: max(x["octets"] for x in rs))
     .collect())
```

Réécrivez-le pour réduire le shuffle. Expliquez les **deux** améliorations
que vous avez faites, et ce que chacune enlève du shuffle.

**E7.** Sur un cluster de 40 machines reliées par un lien partagé de
10 Gb/s, la version A d'un calcul écrit 30 Go de shuffle, la version B
15 Mo.

1. Estimez la durée minimale du transfert réseau pour chaque version.
2. Au TD, en `local[*]`, l'écart de durée entre les deux versions était
   bien plus faible que l'écart d'octets. Pourquoi cette mesure ne
   prouvait-elle pas qu'elles se valent ?

**E8.** Tolérance aux pannes (section 2.7). Un job a deux stages, séparés
par un shuffle. Il tourne sur trois machines M1, M2, M3.

1. M2 tombe au milieu du stage 2. Quelles tâches Spark relance-t-il ?
   Pourquoi certaines tâches du stage 1 doivent-elles être rejouées ?
2. Pourquoi Spark ne recopie-t-il pas chaque résultat intermédiaire sur
   plusieurs machines ? Qu'utilise-t-il à la place ?
3. Quelle panne ce mécanisme ne couvre-t-il pas ?

---

## 3. DataFrames, Catalyst et Parquet

**E9.** Voici un extrait de plan physique sur le Parquet de la séquence 3
(partitionné par `niveau`).

```
Project [cell_id, octets]
+- Filter (isnotnull(octets) AND (octets > 1000000))
   +- FileScan parquet [cell_id, octets, niveau]
        PartitionFilters: [isnotnull(niveau), (niveau = WARN)]
        PushedFilters:    [IsNotNull(octets), GreaterThan(octets,1000000)]
        ReadSchema:       struct<cell_id:string,octets:bigint>
```

1. Écrivez une requête DataFrame qui produit ce plan, puis la même en SQL.
2. Quels sous-dossiers du Parquet sont ouverts ? Combien de colonnes sont
   lues dans les fichiers, et d'où vient la valeur de `niveau` ?
3. Quelle différence entre `PartitionFilters` et `PushedFilters` ?
4. Ce plan contient-il un shuffle ? À quoi le verriez-vous ?

**E10.** On remplace le filtre sur `octets` par une fonction Python :

```python
gros = F.udf(lambda o: o > 1_000_000, BooleanType())
dfp.filter(F.col("niveau") == "WARN").filter(gros("octets"))
```

Qu'est-ce qui disparaît du plan de E9 ? Pourquoi Catalyst ne peut-il plus
faire ce qu'il faisait ? Quel lien avec les lambdas de la séquence 1 ?

**E11.** Parquet.

1. Donnez trois raisons pour lesquelles le même jeu de données prend
   beaucoup moins de place en Parquet qu'en NDJSON.
2. Un analyste lance `SELECT * FROM telemetrie`, sans filtre. Le gain de
   Parquet sur JSON sera-t-il aussi grand que pour la requête de E9 ?
   Pourquoi ?
3. Les requêtes de la supervision filtrent presque toujours sur l'heure.
   Faut-il écrire le Parquet avec `partitionBy("heure")` ? Et avec
   `partitionBy("ts")` ? Justifiez.

**E12.** On doit lire 2 To de logs JSON **une seule fois**, pour un seul
calcul. Combien de lectures complètes du fichier Spark fait-il avec
`spark.read.json(...)` sans schéma ? Et avec un schéma explicite ? Que
risque-t-on si on déclare un schéma faux ?

---

## 4. Jointures, déséquilibre, cache

**E13.** On doit joindre la télémétrie à un référentiel d'**abonnés** :
40 millions de lignes, 6 Go. Le cluster a 50 executors.

1. Catalyst choisira-t-il un broadcast join avec le seuil par défaut ?
2. Si on le forçait, combien de données faudrait-il déplacer au total, et
   combien de mémoire chaque executor devrait-il réserver ? Par où la table
   passe-t-elle avant d'être diffusée ?
3. Quelle stratégie choisir, et que coûte-t-elle en échange ?

**E14.** Un stage a 200 tâches. 199 durent 1 s, une dure 40 s (elle traite
toutes les lignes de la cellule de la gare).

1. Estimez la durée du stage sur 8 cœurs, puis sur 80 cœurs.
2. Quelle loi reconnaissez-vous ?
3. Pourquoi `groupBy("cell_id").agg(F.avg("latence_ms"))` ne souffre-t-il
   pas de ce déséquilibre, alors que `join(cellules, "cell_id")` en
   souffre ?
4. Proposez un moyen, vu au TD, de supprimer ce déséquilibre dans la
   jointure.

**E15.** AQE considère une partition comme déséquilibrée si elle dépasse
**à la fois** 5 fois la taille médiane et 256 Mo. La médiane vaut 2 Mo.

1. Une partition de 120 Mo sera-t-elle découpée ? Et une de 400 Mo ?
2. Pourquoi une seule condition ne suffirait-elle pas ?
3. AQE peut-il découper une partition qui ne contient qu'**une** clé ?
   Que fait-il pour que la jointure reste juste ?

**E16.** Le cache.

```python
charge = (telemetrie.join(F.broadcast(cellules), "cell_id")
                    .groupBy("cell_id", "heure").agg(...))
charge.persist(StorageLevel.MEMORY_AND_DISK)
for requete in trois_requetes:
    requete(charge)
```

1. Combien de fois le Parquet est-il lu ? Et sans la ligne `persist` ?
2. Le cache est-il rempli au moment du `persist` ? Si non, quand ?
3. Que se passe-t-il si `charge` ne tient pas en mémoire ? Et avec
   `MEMORY_ONLY` au lieu de `MEMORY_AND_DISK` ?
4. Dans quel cas mettre en cache ne sert à rien ?

---

## 5. Synthèse et architecture

**E17.** Pour chaque situation, Spark est-il le bon outil ? Justifiez.

1. Un rapport quotidien sur un CSV de 800 Mo, sur un portable.
2. 3 To de logs par jour, à agréger chaque nuit.
3. Une API qui renvoie en moins de 10 ms l'état d'une antenne, 50 000
   fois par seconde.

**E18.** Spark écrit sa sortie dans `data/out/saturation/`.

1. Pourquoi un dossier de fichiers `part-*` et non un seul fichier ?
   Combien y aura-t-il de fichiers `part-*` ?
2. À quoi sert le fichier `_SUCCESS` ? Que risque un consommateur qui
   l'ignore et lit le dossier pendant l'écriture ?
3. Pourquoi l'architecture du TD fait-elle passer Node.js et Spark par des
   fichiers plutôt que de les brancher directement l'un sur l'autre ?

**E19.** En cinq lignes maximum, expliquez à un collègue pourquoi « Spark
met 12 s sur mon portable, Node.js 9 s, donc Spark est inutile » est un
raisonnement faux, **et** dans quel cas sa conclusion serait pourtant juste.
