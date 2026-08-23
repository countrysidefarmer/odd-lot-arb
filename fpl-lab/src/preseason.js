'use strict';
// The gameweek-1 problem: predict early-season minutes with no current-season
// games to look at. Trains on what actually happened in 2025-26 GW1-6, using
// only information available before a ball was kicked.
const fs = require('fs');
const path = require('path');
const { parseCSV, num, sum, mean, sd, spearman, ols, zscore } = require('./lib');

const DATA = path.join(__dirname, '..', 'data');
const POS = { GK: 1, GKP: 1, DEF: 2, MID: 3, FWD: 4 };
const load = f => parseCSV(fs.readFileSync(path.join(DATA, f), 'utf8'));

function seasonAgg(gwFile, rawFile) {
  const gws = load(gwFile), raw = load(rawFile);
  const idToCode = new Map(raw.map(r => [String(r.id), String(r.code)]));
  const byCode = new Map();
  for (const r of gws) {
    const c = idToCode.get(String(r.element));
    if (!c) continue;
    if (!byCode.has(c)) byCode.set(c, []);
    byCode.get(c).push(r);
  }
  return { byCode, raw, idToCode };
}

const prior = seasonAgg('gw_2024-25.csv', 'players_raw_2024-25.csv');
const target = seasonAgg('gw_2025-26.csv', 'players_raw_2025-26.csv');

// Preseason attributes for 2025-26: start price and set-piece duty.
const preseason = new Map(target.raw.map(r => [String(r.code), {
  startCost: (num(r.now_cost) - num(r.cost_change_start)) / 10,
  pos: num(r.element_type),
  team: r.team_code,
  pen: r.penalties_order && r.penalties_order !== 'None' ? num(r.penalties_order) : 0,
  ck: r.corners_and_indirect_freekicks_order && r.corners_and_indirect_freekicks_order !== 'None' ? num(r.corners_and_indirect_freekicks_order) : 0,
  fk: r.direct_freekicks_order && r.direct_freekicks_order !== 'None' ? num(r.direct_freekicks_order) : 0,
}]));
const priorTeam = new Map(prior.raw.map(r => [String(r.code), r.team_code]));

const rows = [];
for (const [code, pre] of preseason) {
  const early = (target.byCode.get(code) || []).filter(r => num(r.GW) <= 6);
  if (early.length < 4) continue;                       // must be in the game early on
  const last = prior.byCode.get(code) || [];
  const hadHistory = last.length >= 10;
  const mins = sum(last.map(r => num(r.minutes)));
  const starts = last.filter(r => num(r.starts) > 0).length;
  const tail = last.slice().sort((a, b) => num(a.GW) - num(b.GW)).slice(-6);
  rows.push({
    code, pos: pre.pos, hadHistory: hadHistory ? 1 : 0,
    y: mean(early.map(r => num(r.minutes))),
    f_lastMins: hadHistory ? mins : 0,
    f_lastStartRate: hadHistory && last.length ? starts / last.length : 0,
    f_tailStartRate: hadHistory && tail.length ? tail.filter(r => num(r.starts) > 0).length / tail.length : 0,
    f_tailMins: hadHistory && tail.length ? mean(tail.map(r => num(r.minutes))) : 0,
    f_price: pre.startCost,
    f_pen: pre.pen === 1 ? 1 : 0,
    f_penAny: pre.pen ? 1 : 0,
    f_setpiece: (pre.ck && pre.ck <= 2) || (pre.fk && pre.fk <= 2) ? 1 : 0,
    f_newClub: hadHistory && priorTeam.get(code) !== pre.team ? 1 : 0,
    f_noHistory: hadHistory ? 0 : 1,
  });
}

const FEATS = ['f_lastMins', 'f_lastStartRate', 'f_tailStartRate', 'f_tailMins', 'f_price',
  'f_pen', 'f_setpiece', 'f_newClub', 'f_noHistory'];
const L = {
  f_lastMins: 'Last season total minutes', f_lastStartRate: 'Last season start rate',
  f_tailStartRate: 'Start rate, last 6 of last season', f_tailMins: 'Minutes, last 6 of last season',
  f_price: 'Price this season', f_pen: 'Is first-choice penalty taker',
  f_setpiece: 'Takes corners or free kicks', f_newClub: 'Changed club', f_noHistory: 'No Premier League history',
};

const y = rows.map(r => r.y);
const fit = fs_ => {
  const X = rows.map(r => fs_.map(f => r[f]));
  const Z = fs_.map((_, j) => zscore(X.map(r => r[j])));
  return ols(X.map((_, i) => Z.map(c => c[i])), y);
};

console.log(`Predicting average minutes in GW1-6 of 2025-26 — n=${rows.length} players\n`);
console.log('Single predictors (Spearman):');
FEATS.filter(f => sd(rows.map(r => r[f])) > 0)
  .map(f => ({ f, rho: spearman(rows.map(r => r[f]), y) }))
  .sort((a, b) => Math.abs(b.rho) - Math.abs(a.rho))
  .forEach(u => console.log(`  ${u.rho >= 0 ? ' ' : ''}${u.rho.toFixed(3)}  ${L[u.f]}`));

console.log('\nNested models (R^2):');
const models = [
  [['f_lastStartRate'], 'last season start rate alone'],
  [['f_tailStartRate'], 'start rate over last 6 of last season alone'],
  [['f_price'], 'this season price alone'],
  [['f_pen'], 'penalty duty alone'],
  [['f_lastStartRate', 'f_tailStartRate', 'f_tailMins'], 'last season minutes signals'],
  [['f_lastStartRate', 'f_tailStartRate', 'f_tailMins', 'f_price'], '+ price'],
  [['f_lastStartRate', 'f_tailStartRate', 'f_tailMins', 'f_price', 'f_pen', 'f_setpiece'], '+ set-piece duty'],
  [FEATS, 'everything'],
];
models.forEach(([f, n]) => console.log(`  ${(fit(f).r2 * 100).toFixed(1).padStart(5)}%  ${n}`));

const full = fit(FEATS);
console.log('\nStandardised weights in the full model:');
FEATS.map((f, i) => ({ f, b: full.beta[i + 1] })).sort((a, b) => Math.abs(b.b) - Math.abs(a.b))
  .forEach(x => console.log(`  ${x.b >= 0 ? ' ' : ''}${x.b.toFixed(1).padStart(6)}  ${L[x.f]}`));

// How much do penalty takers actually play?
const pen = rows.filter(r => r.f_pen), noPen = rows.filter(r => !r.f_pen);
console.log(`\nPenalty takers averaged ${mean(pen.map(r => r.y)).toFixed(1)} mins in GW1-6 (n=${pen.length});`
  + ` everyone else ${mean(noPen.map(r => r.y)).toFixed(1)} (n=${noPen.length}).`);
const nu = rows.filter(r => r.f_noHistory), nc = rows.filter(r => r.f_newClub);
console.log(`No PL history: ${mean(nu.map(r => r.y)).toFixed(1)} mins (n=${nu.length}). Changed club: ${mean(nc.map(r => r.y)).toFixed(1)} mins (n=${nc.length}).`);

// The players the model is actually blind on: no Premier League record at all.
// Promoted squads, overseas signings, academy graduates.
const blind = rows.filter(r => r.f_noHistory);
const by = blind.map(r => r.y);
const bfit = fs_ => {
  const X = blind.map(r => fs_.map(f => r[f]));
  const Z = fs_.map((_, j) => zscore(X.map(r => r[j])));
  return ols(X.map((_, i) => Z.map(c => c[i])), by).r2;
};
console.log(`\n${'='.repeat(70)}\nPlayers with NO Premier League history — n=${blind.length}`);
['f_price', 'f_pen', 'f_setpiece'].forEach(f => {
  if (sd(blind.map(r => r[f])) > 0) console.log(`  ${spearman(blind.map(r => r[f]), by).toFixed(3).padStart(7)}  ${L[f]}`);
});
console.log(`  R^2: price alone ${(bfit(['f_price']) * 100).toFixed(1)}%, price + duty ${(bfit(['f_price', 'f_pen', 'f_setpiece']) * 100).toFixed(1)}%`);
const bands = [[0, 4.5], [4.5, 5.5], [5.5, 6.5], [6.5, 8], [8, 20]];
console.log('  Average GW1-6 minutes by price band:');
for (const [lo, hi] of bands) {
  const g = blind.filter(r => r.f_price >= lo && r.f_price < hi);
  if (g.length) console.log(`    £${lo}-${hi}m: ${mean(g.map(r => r.y)).toFixed(1)} mins  (n=${g.length})`);
}
const bp = blind.filter(r => r.f_setpiece), bnp = blind.filter(r => !r.f_setpiece);
console.log(`  Set-piece duty: ${mean(bp.map(r => r.y)).toFixed(1)} mins (n=${bp.length}) vs ${mean(bnp.map(r => r.y)).toFixed(1)} (n=${bnp.length})`);
