#!/usr/bin/env node
'use strict';
/**
 * INSA Lyon — 4TC — TD Apache Spark
 * ---------------------------------------------------------------------------
 * PRODUCTEUR : microservice Node.js émettant de la télémétrie d'antennes
 * mobiles au format NDJSON (une ligne = un événement réseau).
 *
 * C'est le rôle réel de Node.js dans une plateforme data : produire le flux.
 * Le traitement distribué, lui, se fait côté Spark.
 *
 *   node producer/generate.js                    # 5 000 000 lignes (~750 Mo)
 *   node producer/generate.js --rows 500000      # version rapide
 *   node producer/generate.js --rows 20000000    # démo projecteur
 *
 * Le générateur est DÉTERMINISTE (PRNG à graine fixe) : tous les binômes
 * obtiennent exactement le même jeu de données, donc des mesures comparables.
 */

const fs = require('fs');
const path = require('path');

// --- PRNG déterministe (mulberry32) --------------------------------------
function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// --- Référentiel des secteurs de la Métropole de Lyon ---------------------
// poids = part du trafic total. Lyon_PartDieu est volontairement dominant :
// c'est ce déséquilibre (data skew) qui sera étudié en séquence 4.
const SECTEURS = [
  { nom: 'Lyon_PartDieu',           poids: 34, nbCells: 14, capacite: 1600, lat: 45.7606, lon: 4.8598, techno: '5G' },
  { nom: 'Lyon_Bellecour',          poids: 11, nbCells: 10, capacite: 1200, lat: 45.7578, lon: 4.8320, techno: '5G' },
  { nom: 'Villeurbanne_Charpennes', poids:  9, nbCells:  9, capacite: 1100, lat: 45.7700, lon: 4.8630, techno: '5G' },
  { nom: 'INSA_Doua',               poids:  8, nbCells:  8, capacite: 1000, lat: 45.7821, lon: 4.8780, techno: '5G' },
  { nom: 'Lyon_Confluence',         poids:  7, nbCells:  7, capacite: 1000, lat: 45.7420, lon: 4.8160, techno: '5G' },
  { nom: 'Lyon_Guillotiere',        poids:  6, nbCells:  7, capacite:  900, lat: 45.7550, lon: 4.8430, techno: '4G' },
  { nom: 'Lyon_Vaise',              poids:  5, nbCells:  6, capacite:  800, lat: 45.7800, lon: 4.8040, techno: '4G' },
  { nom: 'Bron_Parilly',            poids:  5, nbCells:  6, capacite:  800, lat: 45.7280, lon: 4.9070, techno: '4G' },
  { nom: 'Venissieux_Centre',       poids:  4, nbCells:  5, capacite:  700, lat: 45.6970, lon: 4.8860, techno: '4G' },
  { nom: 'Ecully_Centre',           poids:  4, nbCells:  5, capacite:  600, lat: 45.7740, lon: 4.7770, techno: '4G' },
  { nom: 'Caluire_Centre',          poids:  4, nbCells:  5, capacite:  600, lat: 45.7950, lon: 4.8460, techno: '4G' },
  { nom: 'StPriest_Zone',           poids:  3, nbCells:  5, capacite:  600, lat: 45.6960, lon: 4.9440, techno: '4G' },
];

// Profil horaire du trafic mobile (heures creuses la nuit, pointes 8h et 18-19h)
const PROFIL_HORAIRE = [
  0.30, 0.20, 0.15, 0.15, 0.20, 0.40, 0.90, 1.60,
  2.20, 2.00, 1.70, 1.60, 1.70, 1.60, 1.50, 1.50,
  1.70, 2.10, 2.40, 2.30, 1.90, 1.50, 1.00, 0.60,
];

const EVENTS = {
  INFO:  ['HANDOVER_OK', 'ATTACH', 'SESSION_START', 'SESSION_END', 'PAGING_OK'],
  WARN:  ['CONGESTION', 'HIGH_LATENCY', 'HANDOVER_RETRY', 'LOW_SIGNAL'],
  ERROR: ['CONNECTION_DROP', 'HARDWARE_FAILURE', 'AUTH_FAILURE', 'BACKHAUL_LOSS'],
};

const JOUR = '2026-10-24';

// --- Construction du référentiel de cellules ------------------------------
function construireCellules() {
  const rnd = mulberry32(4242);
  const cellules = [];
  let n = 1;
  for (const s of SECTEURS) {
    for (let i = 0; i < s.nbCells; i++) {
      // Pression structurelle : toutes les antennes ne sont pas dimensionnées
      // au même niveau de service. C'est ce déséquilibre que la supervision
      // doit découvrir — il n'apparaît donc PAS dans le référentiel.
      const densite = s.nom === 'Lyon_PartDieu' ? 1.28 : s.techno === '5G' ? 1.06 : 0.86;
      cellules.push({
        _pression: +(densite * (0.75 + 0.5 * rnd())).toFixed(3),
        cell_id: 'CELL_' + String(n).padStart(4, '0'),
        secteur: s.nom,
        techno: i % 4 === 3 ? (s.techno === '5G' ? '4G' : '5G') : s.techno,
        capacite_max: Math.round(s.capacite * (0.8 + 0.4 * rnd())),
        lat: +(s.lat + (rnd() - 0.5) * 0.012).toFixed(5),
        lon: +(s.lon + (rnd() - 0.5) * 0.016).toFixed(5),
        mise_en_service: 2016 + Math.floor(rnd() * 10) + '-0' + (1 + Math.floor(rnd() * 9)) + '-01',
      });
      n++;
    }
  }
  return cellules;
}

// --- Tables de tirage pondéré ---------------------------------------------
function cumul(poids) {
  const c = [];
  let t = 0;
  for (const p of poids) { t += p; c.push(t); }
  return { c, total: t };
}

function tirer(table, r) {
  const x = r * table.total;
  for (let i = 0; i < table.c.length; i++) if (x < table.c[i]) return i;
  return table.c.length - 1;
}

// --- Programme principal ---------------------------------------------------
function parseArgs(argv) {
  const a = { rows: 5_000_000, out: 'data/telemetrie.ndjson', seed: 20261024 };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === '--rows') a.rows = parseInt(argv[++i], 10);
    else if (argv[i] === '--out') a.out = argv[++i];
    else if (argv[i] === '--seed') a.seed = parseInt(argv[++i], 10);
    else if (argv[i] === '--help') { console.log('usage: node producer/generate.js [--rows N] [--out fichier] [--seed N]'); process.exit(0); }
  }
  return a;
}

async function main() {
  const args = parseArgs(process.argv);
  const cellules = construireCellules();

  // Référentiel : petite table de dimension, écrite en JSON (87 lignes).
  // Elle servira à la jointure distribuée de la séquence 4.
  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  const refPath = path.join(path.dirname(args.out), 'cellules.json');
  fs.writeFileSync(
    refPath,
    cellules
      .map(({ _pression, ...publie }) => JSON.stringify(publie))
      .join('\n') + '\n'
  );

  // Index : pour chaque secteur, l'intervalle de cellules correspondant
  const parSecteur = SECTEURS.map((s) => ({
    ...s,
    cells: cellules.filter((c) => c.secteur === s.nom),
  }));
  const tSecteur = cumul(SECTEURS.map((s) => s.poids));
  const tHeure = cumul(PROFIL_HORAIRE);

  const HH = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, '0'));
  const MM = Array.from({ length: 60 }, (_, i) => String(i).padStart(2, '0'));

  const rnd = mulberry32(args.seed);
  const stream = fs.createWriteStream(args.out);
  const LOT = 20000;

  const t0 = Date.now();
  let buf = [];
  for (let i = 0; i < args.rows; i++) {
    const sIdx = tirer(tSecteur, rnd());
    const sec = parSecteur[sIdx];

    // Déséquilibre intra-secteur : à la Part-Dieu, la cellule de la gare
    // absorbe 60 % du trafic du secteur (~20 % du trafic total).
    let cell;
    if (sec.nom === 'Lyon_PartDieu' && rnd() < 0.6) cell = sec.cells[0];
    else cell = sec.cells[Math.floor(rnd() * sec.cells.length)];

    const h = tirer(tHeure, rnd());
    const facteur = PROFIL_HORAIRE[h] / 2.4;
    const ts = JOUR + 'T' + HH[h] + ':' + MM[(rnd() * 60) | 0] + ':' + MM[(rnd() * 60) | 0]
             + '.' + String((rnd() * 1000) | 0).padStart(3, '0') + 'Z';

    // Les secteurs denses saturent et dégradent : plus de WARN/ERROR, latence plus haute
    const dense = sec.nom === 'Lyon_PartDieu';
    const pErr = dense ? 0.030 : 0.012;
    const pWarn = dense ? 0.140 : 0.055;
    const u = rnd();
    const niveau = u < pErr ? 'ERROR' : u < pErr + pWarn ? 'WARN' : 'INFO';
    const evts = EVENTS[niveau];
    const event = evts[(rnd() * evts.length) | 0];

    const baseLat = dense ? 70 : 25;
    const latence = Math.max(3, Math.round(baseLat * (0.4 + 1.2 * facteur) * (0.5 + rnd() * rnd() * 3)));
    const conn = Math.max(0, Math.round(cell.capacite_max * cell._pression * (0.20 + 0.85 * facteur) * (0.75 + 0.5 * rnd())));
    const octets = 400 + Math.floor(rnd() * rnd() * rnd() * 6_000_000);

    buf.push('{"ts":"' + ts + '","cell_id":"' + cell.cell_id + '","niveau":"' + niveau
      + '","event":"' + event + '","connexions_actives":' + conn
      + ',"latence_ms":' + latence + ',"octets":' + octets + '}\n');

    if (buf.length >= LOT) {
      const ok = stream.write(buf.join(''));
      buf = [];
      if (!ok) await new Promise((r) => stream.once('drain', r));
      if (i % 1_000_000 < LOT) process.stderr.write('  ... ' + (i / 1e6).toFixed(0) + ' M lignes\r');
    }
  }
  if (buf.length) stream.write(buf.join(''));
  await new Promise((r) => stream.end(r));

  const dt = (Date.now() - t0) / 1000;
  const mo = fs.statSync(args.out).size / 1e6;
  console.log('');
  console.log('Télémétrie générée   : ' + args.out);
  console.log('  lignes             : ' + args.rows.toLocaleString('fr-FR'));
  console.log('  taille             : ' + mo.toFixed(0) + ' Mo NDJSON');
  console.log('  durée              : ' + dt.toFixed(1) + ' s  (' + Math.round(args.rows / dt / 1000) + ' k lignes/s, MONO-THREAD)');
  console.log('Référentiel cellules : ' + refPath + '  (' + cellules.length + ' cellules)');
}

main().catch((e) => { console.error(e); process.exit(1); });
