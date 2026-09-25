"""
INSA Lyon — 4TC — TD Apache Spark
Petite boîte à outils commune aux quatre séquences.
"""
import os
import sys
import time
from contextlib import contextmanager

# Le worker Python et le driver Python DOIVENT avoir la même version mineure.
# Par défaut Spark lance `python3`, qui n'est pas forcément celui du driver.
# On le neutralise ici, avant tout import Spark.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(RACINE, "data")
NDJSON = os.path.join(DATA, "telemetrie.ndjson")
CELLULES = os.path.join(DATA, "cellules.json")
PARQUET = os.path.join(DATA, "parquet", "telemetrie")
SORTIE = os.path.join(DATA, "out")


def _dans_un_notebook():
    return "ipykernel" in sys.modules


def session(nom, shuffle_partitions=8, aqe=False, broadcast=True):
    """Ouvre une SparkSession calibrée pour le TD.

    shuffle_partitions=8 (au lieu de 200) : sur nos volumes, 200 partitions
        produisent 200 tâches quasi vides et une Spark UI illisible.

    aqe=False par défaut : l'Adaptive Query Execution réécrit le plan pendant
        l'exécution. Excellent en production, mais il MASQUE les phénomènes
        qu'on veut observer. On l'active volontairement en séquence 4.

    broadcast=True : seuil de diffusion par défaut (10 Mo). On le désactive en
        séquence 4 pour comparer broadcast join et sort-merge join.
    """
    if not os.path.exists(NDJSON):
        raise FileNotFoundError(
            f"{NDJSON} introuvable. Générez d'abord les données, dans un terminal "
            "JupyterLab :  node producer/generate.js"
        )
    b = (
        SparkSession.builder.appName(f"4TC-{nom}")
        .master(os.environ.get("TD_MASTER", "local[*]"))
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(aqe).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760" if broadcast else "-1")
        # Les horodatages sont en UTC : sans ceci, `hour()` suit le fuseau de la
        # machine et les heures sont décalées selon le poste.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "true")
    )
    if os.environ.get("TD_DRIVER_HOST"):
        b = b.config("spark.driver.host", os.environ["TD_DRIVER_HOST"])

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    port = sc.uiWebUrl.rsplit(":", 1)[-1]
    print(f"Spark {spark.version} | master = {sc.master} | {os.cpu_count()} cœurs visibles")
    print(f">>> Spark UI : http://localhost:{port}")
    if port != "4040":
        print("    (port différent de 4040 : une autre session Spark est encore ouverte —"
              " arrêtez le noyau des séquences précédentes)")
    print(f"    AQE = {aqe} | shuffle.partitions = {shuffle_partitions}")
    return spark


@contextmanager
def chrono(libelle):
    """Mesure le temps mur d'un bloc. Attention : ne mesure quelque chose que
    si le bloc contient une ACTION (count, collect, write, show...).

    Les jobs lancés dans le bloc portent `libelle` comme description dans
    l'onglet Jobs de la Spark UI, au lieu d'un nom interne illisible."""
    from pyspark import SparkContext
    sc = SparkContext._active_spark_context
    if sc is not None:
        sc.setJobDescription(libelle)
    t0 = time.perf_counter()
    try:
        yield
    finally:
        if sc is not None:
            sc.setJobDescription(None)
    print(f"  [chrono] {libelle:<46s} {time.perf_counter() - t0:7.2f} s")


def pause(message="Regardez la Spark UI (http://localhost:4040) puis Entrée pour terminer...",
          secondes=180):
    """En mode script uniquement : maintient l'application vivante, car la
    Spark UI disparaît dès que le driver s'arrête.

    Dans un notebook, ne fait rien : la session vit aussi longtemps que le noyau.
    Sans terminal interactif, tient la session `secondes` secondes.
    `TD_PAUSE=0` désactive complètement la pause.
    """
    if _dans_un_notebook():
        return
    duree = os.environ.get("TD_PAUSE")
    if duree is not None and duree.strip() == "0":
        return
    if sys.stdin.isatty():
        try:
            input(f"\n>>> {message}\n")
        except EOFError:
            pass
        return
    secondes = int(duree) if duree else secondes
    print(f"\n>>> {message}")
    print(f">>> Pas de terminal interactif : Spark UI maintenue {secondes} s "
          f"(Ctrl-C pour abréger, TD_PAUSE=0 pour supprimer la pause).")
    try:
        time.sleep(secondes)
    except KeyboardInterrupt:
        print("\n>>> Interrompu.")


def taille_disque(chemin):
    """Taille d'un fichier ou d'un répertoire, en Mo."""
    if os.path.isfile(chemin):
        return os.path.getsize(chemin) / 1e6
    total = 0
    for racine, _, fichiers in os.walk(chemin):
        for f in fichiers:
            total += os.path.getsize(os.path.join(racine, f))
    return total / 1e6


def _octets(n):
    """Affiche un volume sans l'écraser à 0,1 Mo : un rapport calculé sur une
    valeur arrondie ne veut rien dire."""
    if n >= 1e6:
        return f"{n / 1e6:.1f} Mo"
    return f"{n / 1e3:.1f} Ko"


def _api(spark, chemin):
    import json as _json
    import urllib.request

    base = spark.sparkContext.uiWebUrl
    with urllib.request.urlopen(f"{base}/api/v1/applications", timeout=5) as r:
        app = _json.load(r)[0]["id"]
    with urllib.request.urlopen(f"{base}/api/v1/applications/{app}/{chemin}", timeout=5) as r:
        return _json.load(r)


def metriques_stages(spark, n=6):
    """Interroge l'API REST de la Spark UI et affiche, pour les derniers
    stages, ce qui a réellement transité entre les stages (le shuffle).

    La Spark UI n'est pas qu'une page web : tout ce qu'elle affiche est
    disponible sur /api/v1/. C'est ainsi qu'on instrumente un cluster en
    production (export Prometheus, alerting sur le shuffle...).
    """
    try:
        stages = _api(spark, "stages")
    except Exception as e:                                   # pragma: no cover
        print(f"  (métriques indisponibles : {e})")
        return

    stages = [s for s in stages if s.get("status") == "COMPLETE"]
    stages.sort(key=lambda s: s["stageId"], reverse=True)

    print(f"\n  {'stage':<6} {'tâches':>7} {'shuffle write':>14} {'shuffle read':>14}")
    print("  " + "-" * 45)
    for s in stages[:n]:
        print(f"  {s['stageId']:<6} {s.get('numCompleteTasks', 0):>7} "
              f"{_octets(s.get('shuffleWriteBytes', 0)):>14} "
              f"{_octets(s.get('shuffleReadBytes', 0)):>14}")


def profil_taches(spark, stage_id=None, recents=6):
    """Distribution des durées de tâches d'un stage : c'est là que se voit le
    déséquilibre (data skew). Un stage se termine quand sa tâche la plus lente
    se termine — la médiane ne dit rien du temps réellement subi."""
    try:
        stages = [s for s in _api(spark, "stages") if s.get("status") == "COMPLETE"]
        if not stages:
            return
        if stage_id is not None:
            cible = next(s for s in stages if s["stageId"] == stage_id)
        else:
            # Parmi les stages RÉCENTS seulement, celui qui a réellement lu un
            # shuffle : c'est là que le déséquilibre se paie.
            recents_l = sorted(stages, key=lambda s: s["stageId"], reverse=True)[:recents]
            cible = max(recents_l, key=lambda s: (s.get("shuffleReadBytes", 0), -s["stageId"]))
        taches = _api(spark, f"stages/{cible['stageId']}/0/taskList?length=2000")
    except Exception as e:                                   # pragma: no cover
        print(f"  (métriques indisponibles : {e})")
        return

    durees = sorted(t.get("duration", 0) or 0 for t in taches)
    if not durees:
        return
    med = durees[len(durees) // 2]
    print(f"\n  Stage {cible['stageId']} — {len(durees)} tâches")
    print(f"    médiane {med:>7} ms | p90 {durees[int(.9 * len(durees)) - 1]:>7} ms "
          f"| MAX {durees[-1]:>7} ms")
    if med:
        print(f"    rapport max/médiane : {durees[-1] / med:.1f}×")
    print("    Le stage n'est terminé qu'au retour de la tâche la plus lente.")
