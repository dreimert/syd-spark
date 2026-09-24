#!/usr/bin/env node
'use strict';
/**
 * INSA Lyon — 4TC — TD Apache Spark
 * ---------------------------------------------------------------------------
 * CONSOMMATEUR : microservice Node.js exposant les résultats calculés par
 * Spark sous forme d'API REST, pour le dashboard de supervision réseau.
 *
 *   node consumer/api.js        ->  http://localhost:3000
 *
 * Deux détails qui ne sont pas des détails :
 *  1. Spark n'écrit pas UN fichier, il écrit un RÉPERTOIRE de `part-*`.
 *     Un consommateur doit savoir les recoller (et ignorer _SUCCESS).
 *  2. On ne branche pas Node.js sur Spark : on le branche sur la SORTIE de
 *     Spark. Le découplage par le stockage, c'est l'architecture réelle.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const RACINE = path.join(__dirname, '..');
const SORTIE = path.join(RACINE, 'data', 'out', 'saturation');

function lireResultatsSpark(repertoire) {
  if (!fs.existsSync(repertoire)) return null;
  const parts = fs
    .readdirSync(repertoire)
    .filter((f) => f.startsWith('part-') && f.endsWith('.json'));

  const lignes = [];
  for (const p of parts) {
    const contenu = fs.readFileSync(path.join(repertoire, p), 'utf8');
    for (const l of contenu.split('\n')) {
      if (l.trim()) lignes.push(JSON.parse(l));
    }
  }
  return lignes.sort((a, b) => b.taux_charge - a.taux_charge);
}

function agregerParSecteur(lignes) {
  const m = new Map();
  for (const l of lignes) {
    const e = m.get(l.secteur) || { secteur: l.secteur, alertes: 0, taux_max: 0, cellules: new Set() };
    e.alertes++;
    e.taux_max = Math.max(e.taux_max, l.taux_charge);
    e.cellules.add(l.cell_id);
    m.set(l.secteur, e);
  }
  return [...m.values()]
    .map((e) => ({ secteur: e.secteur, alertes: e.alertes, taux_max: e.taux_max, cellules: e.cellules.size }))
    .sort((a, b) => b.alertes - a.alertes);
}

function json(res, code, corps) {
  const c = JSON.stringify(corps);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(c) });
  res.end(c);
}

const serveur = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/' || url.pathname === '/index.html') {
    const page = fs.readFileSync(path.join(__dirname, 'dashboard.html'));
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(page);
  }

  const lignes = lireResultatsSpark(SORTIE);
  if (lignes === null) {
    return json(res, 503, {
      erreur: 'Aucun résultat Spark disponible.',
      attendu: SORTIE,
      indice: 'Exécutez la séquence 4 (TODO 3) pour produire les données.',
    });
  }

  if (url.pathname === '/api/saturation') {
    const heure = url.searchParams.get('heure');
    const filtrees = heure === null ? lignes : lignes.filter((l) => String(l.heure) === heure);
    return json(res, 200, { total: filtrees.length, resultats: filtrees.slice(0, 500) });
  }

  if (url.pathname === '/api/secteurs') {
    return json(res, 200, { secteurs: agregerParSecteur(lignes) });
  }

  if (url.pathname === '/api/sante') {
    return json(res, 200, {
      statut: 'ok',
      alertes: lignes.length,
      derniere_maj: fs.statSync(SORTIE).mtime,
    });
  }

  json(res, 404, { erreur: 'Route inconnue', routes: ['/', '/api/saturation', '/api/secteurs', '/api/sante'] });
});

serveur.listen(PORT, () => {
  console.log(`Supervision réseau — API sur http://localhost:${PORT}`);
  console.log(`Lecture des sorties Spark : ${SORTIE}`);
});
