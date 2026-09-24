#!/usr/bin/env node
'use strict';
/**
 * INSA Lyon — 4TC — TD Apache Spark — Séquence 1
 * ---------------------------------------------------------------------------
 * LA VERSION QUE VOUS AURIEZ ÉCRITE EN NODE.JS.
 *
 * Compte les incidents critiques par type d'événement, en JavaScript, sur le
 * même fichier et avec le même algorithme que Spark. Un seul thread, une
 * seule machine, une seule passe.
 *
 *   node producer/bench-node.js
 *
 * Gardez le temps affiché : vous le comparerez au temps Spark.
 */
const fs = require('fs');
const readline = require('readline');

async function main() {
  const fichier = process.argv[2] || 'data/telemetrie.ndjson';
  const t0 = Date.now();

  const rl = readline.createInterface({
    input: fs.createReadStream(fichier),
    crlfDelay: Infinity,
  });

  let total = 0;
  const parEvent = new Map();

  for await (const ligne of rl) {
    total++;
    // Exactement le même travail que la version Spark de la séquence 1 :
    //   .filter(l => '"niveau":"ERROR"' in l)     recherche de texte, sans parser
    //   .map(l => [JSON.parse(l).event, 1])        on ne parse que les lignes retenues
    //   .reduceByKey(...)
    if (!ligne.includes('"niveau":"ERROR"')) continue;
    const rec = JSON.parse(ligne);
    parEvent.set(rec.event, (parEvent.get(rec.event) || 0) + 1);
  }

  const dt = (Date.now() - t0) / 1000;
  const tri = [...parEvent.entries()].sort((a, b) => b[1] - a[1]);
  console.log('Incidents critiques par type :');
  for (const [e, n] of tri) console.log('  ' + e.padEnd(20) + n.toLocaleString('fr-FR'));
  console.log('');
  console.log('  lignes lues : ' + total.toLocaleString('fr-FR'));
  console.log('  DURÉE NODE.JS (1 thread) : ' + dt.toFixed(2) + ' s');
}

main().catch((e) => { console.error(e); process.exit(1); });
