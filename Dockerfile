# INSA Lyon — 4TC — TD Apache Spark
#
# Tout le TD tourne dans cette image : Spark (PySpark + Java), JupyterLab et
# Node.js. Sur leur portable, les étudiants n'installent que Docker.
#
# Java 17 + Python 3.12 : combinaison supportée par Spark 3.5. Java 22+ et
# Python 3.13+, fréquents sur les portables, ne le sont pas.
FROM python:3.12-slim-bookworm

# nodejs de Debian bookworm = Node 18 : suffisant, le TD n'a aucune dépendance npm.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless nodejs procps curl less \
    && rm -rf /var/lib/apt/lists/*

# psutil   : sans lui, Spark gère mal le débordement sur disque (spill).
# jupytext : ouvre les fichiers `# %%` du dossier lab/ comme des notebooks.
RUN pip install --no-cache-dir \
        "pyspark==3.5.9" psutil pandas pyarrow "jupyterlab>=4,<5" jupytext

# Un double-clic sur un .py dans JupyterLab l'ouvre directement en notebook.
RUN mkdir -p /usr/local/share/jupyter/lab/settings && printf '%s\n' \
    '{' \
    '  "@jupyterlab/docmanager-extension:plugin": {' \
    '    "defaultViewers": { "python": "Jupytext Notebook" }' \
    '  }' \
    '}' > /usr/local/share/jupyter/lab/settings/overrides.json

# pip installe la distribution Spark complète (jars + bin/) dans le package.
RUN ln -s "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:/opt/spark/bin:/opt/spark/sbin"
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

WORKDIR /work
