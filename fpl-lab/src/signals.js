'use strict';
// Collects the two follow-up studies into data/signals.json for the dashboard:
//   - how predictable minutes are, and off what
//   - what predicts points once a player is already starting
//   - the gameweek-1 case, where there is no current-season evidence at all
const fs = require('fs');
const path = require('path');
const { sum, mean, sd, spearman, ols, zscore } = require('./lib');
const { panel, load, LABELS, AVAIL, QUALITY } = require('./minutes');

const DATA = path.join(__dirname, '..', 'data');

function fitR2(rows, target, feats) {
  const use = feats.filter(f => sd(rows.map(r => r[f])) > 0);
  if (!use.length) return 0;
  const y = rows.map(r => r[target]);
  const X = rows.map(r => use.map(f => r[f]));
  const Z = use.map((_, j) => zscore(X.map(r => r[j])));
  return ols(X.map((_, i) => Z.map(c => c[i])), y).r2;
}
function uni(rows, target, feats, n = 8) {
  const y = rows.map(r => r[target]);
  return feats.filter(f => sd(rows.map(r => r[f])) > 0)
    .map(f => ({ key: f, label: LABELS[f], rho: +spearman(rows.map(r => r[f]), y).toFixed(3) }))
    .sort((a, b) => Math.abs(b.rho) - Math.abs(a.rho)).slice(0, n);
}

function forSeason(season) {
  const p = panel(load(season));
  const starters = p.filter(r => r.started === 1);
  const byPos = {};
  for (const [pos, nm] of [[2, 'Defenders'], [3, 'Midfielders'], [4, 'Forwards']]) {
    const sub = starters.filter(r => r.pos === pos);
    if (sub.length > 400) byPos[nm] = { n: sub.length, r2: fitR2(sub, 'y_pts', QUALITY), top: uni(sub, 'y_pts', QUALITY, 6) };
  }
  return {
    n: p.length,
    minutes: {
      top: uni(p, 'y_mins', AVAIL),
      models: [
        { name: 'Started last game alone', r2: fitR2(p, 'y_mins', ['f_startedLast']) },
        { name: 'Average minutes, last 3', r2: fitR2(p, 'y_mins', ['f_mins3']) },
        { name: 'Season-long start rate', r2: fitR2(p, 'y_mins', ['f_startRateSeason']) },
        { name: 'Recent minutes and start rate', r2: fitR2(p, 'y_mins', ['f_startedLast', 'f_mins3', 'f_startRate6']) },
        { name: 'Everything, incl. price and transfer flow', r2: fitR2(p, 'y_mins', AVAIL) },
      ],
    },
    quality: { n: starters.length, top: uni(starters, 'y_pts', QUALITY), r2: fitR2(starters, 'y_pts', QUALITY), byPos },
  };
}

function run() {
  const out = { generated: new Date().toISOString(), seasons: {} };
  for (const season of ['2025-26', '2024-25']) out.seasons[season] = forSeason(season);

  // The gameweek-1 case, reported by src/preseason.js.
  const pre = require('child_process').execSync(`node ${path.join(__dirname, 'preseason.js')}`, { encoding: 'utf8' });
  const grab = re => { const m = pre.match(re); return m ? parseFloat(m[1]) : null; };
  out.preseason = {
    n: grab(/n=(\d+) players/),
    lastSeasonStartRate: grab(/([\d.]+)%\s+last season start rate alone/),
    priceAlone: grab(/([\d.]+)%\s+this season price alone/),
    minutesSignals: grab(/([\d.]+)%\s+last season minutes signals/),
    plusPrice: grab(/([\d.]+)%\s+\+ price/),
    plusSetPiece: grab(/([\d.]+)%\s+\+ set-piece duty/),
    everything: grab(/([\d.]+)%\s+everything/),
    noHistoryN: grab(/NO Premier League history — n=(\d+)/),
    noHistoryPrice: grab(/price alone ([\d.]+)%/),
    noHistoryPriceDuty: grab(/price \+ duty ([\d.]+)%/),
    bands: [...pre.matchAll(/£([\d.]+)-([\d.]+)m: ([\d.]+) mins\s+\(n=(\d+)\)/g)]
      .map(m => ({ lo: +m[1], hi: +m[2], mins: +m[3], n: +m[4] })),
    setPieceMins: grab(/Set-piece duty: ([\d.]+) mins/),
    noSetPieceMins: grab(/mins \(n=\d+\) vs ([\d.]+)/),
  };

  fs.writeFileSync(path.join(DATA, 'signals.json'), JSON.stringify(out, null, 1));
  console.log('wrote data/signals.json');
  for (const [s, v] of Object.entries(out.seasons)) {
    console.log(`  ${s}: minutes R2 ${(v.minutes.models[4].r2 * 100).toFixed(1)}%,`
      + ` points-given-a-start R2 ${(v.quality.r2 * 100).toFixed(1)}% (n=${v.quality.n})`);
  }
  console.log(`  preseason: ${out.preseason.everything}% overall, ${out.preseason.noHistoryPriceDuty}% for players with no PL record`);
}
if (require.main === module) run();
module.exports = { run };
