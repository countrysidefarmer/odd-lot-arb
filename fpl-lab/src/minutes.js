'use strict';
// Two questions the headline backtest conflated:
//   1. What predicts MINUTES?
//   2. Given a player starts, what predicts POINTS?
// Availability and quality are different problems and want different signals.
const fs = require('fs');
const path = require('path');
const { parseCSV, num, sum, mean, sd, spearman, ols, zscore } = require('./lib');

const DATA = path.join(__dirname, '..', 'data');
const POS = { GK: 1, GKP: 1, DEF: 2, MID: 3, FWD: 4 };

function load(season) {
  const rows = parseCSV(fs.readFileSync(path.join(DATA, `gw_${season}.csv`), 'utf8'));
  const raw = parseCSV(fs.readFileSync(path.join(DATA, `players_raw_${season}.csv`), 'utf8'));
  // Set-piece duty is only published as an end-of-season snapshot, so it is a
  // season-level attribute here, not a point-in-time one.
  const duty = new Map(raw.map(r => [String(r.id), {
    pen: r.penalties_order && r.penalties_order !== 'None' ? num(r.penalties_order) : 0,
    sp: r.corners_and_indirect_freekicks_order && r.corners_and_indirect_freekicks_order !== 'None' ? num(r.corners_and_indirect_freekicks_order) : 0,
    fk: r.direct_freekicks_order && r.direct_freekicks_order !== 'None' ? num(r.direct_freekicks_order) : 0,
  }]));
  return rows.map(r => {
    const d = duty.get(String(r.element)) || { pen: 0, sp: 0, fk: 0 };
    return {
      id: r.element, name: r.name, pos: POS[r.position] || 0, gw: num(r.GW),
      opp: num(r.opponent_team), home: r.was_home === 'True',
      pts: num(r.total_points), mins: num(r.minutes), started: num(r.starts) > 0 ? 1 : 0,
      xg: num(r.expected_goals), xa: num(r.expected_assists),
      threat: num(r.threat), creativity: num(r.creativity), influence: num(r.influence),
      ict: num(r.ict_index), bps: num(r.bps), bonus: num(r.bonus),
      defcon: num(r.defensive_contribution), saves: num(r.saves),
      value: num(r.value) / 10, selected: num(r.selected), tbal: num(r.transfers_balance),
      penTaker: d.pen === 1 ? 1 : 0, penOrder: d.pen, spTaker: d.sp && d.sp <= 2 ? 1 : 0,
      fkTaker: d.fk && d.fk <= 2 ? 1 : 0,
    };
  }).filter(r => r.gw >= 1 && r.gw <= 38);
}

function panel(rows) {
  const byPlayer = new Map();
  for (const r of rows) {
    if (!byPlayer.has(r.id)) byPlayer.set(r.id, []);
    byPlayer.get(r.id).push(r);
  }
  // Squad depth: how many team-mates in the same position started that gameweek.
  const out = [];
  for (const hist of byPlayer.values()) {
    hist.sort((a, b) => a.gw - b.gw);
    for (let i = 6; i < hist.length; i++) {
      const cur = hist[i], prior = hist.slice(0, i);
      const played = prior.filter(p => p.mins > 0);
      if (played.length < 3) continue;
      const pMins = sum(prior.map(p => p.mins));
      if (pMins < 180) continue;
      const per90 = v => (v * 90) / pMins;
      const last = prior[prior.length - 1];
      const l3 = prior.slice(-3), l6 = prior.slice(-6);
      out.push({
        pos: cur.pos, gw: cur.gw, name: cur.name,
        y_mins: cur.mins, y_start: cur.started, y_pts: cur.pts, started: cur.started,
        f_startedLast: last.started,
        f_minsLast: last.mins,
        f_mins3: mean(l3.map(p => p.mins)),
        f_mins6: mean(l6.map(p => p.mins)),
        f_startRate6: mean(l6.map(p => p.started)),
        f_startRateSeason: mean(prior.map(p => p.started)),
        f_startStreak: (() => { let n = 0; for (let k = prior.length - 1; k >= 0 && prior[k].started; k--) n++; return Math.min(n, 10); })(),
        f_price: cur.value,
        f_selected: Math.log10(1 + cur.selected),
        f_tbal: Math.sign(cur.tbal) * Math.log10(1 + Math.abs(cur.tbal)),
        // Quality, per 90, from prior gameweeks only.
        f_xg90: per90(sum(prior.map(p => p.xg))),
        f_xa90: per90(sum(prior.map(p => p.xa))),
        f_xgi90: per90(sum(prior.map(p => p.xg + p.xa))),
        f_threat90: per90(sum(prior.map(p => p.threat))),
        f_creativity90: per90(sum(prior.map(p => p.creativity))),
        f_influence90: per90(sum(prior.map(p => p.influence))),
        f_ict90: per90(sum(prior.map(p => p.ict))),
        f_bps90: per90(sum(prior.map(p => p.bps))),
        f_bonus90: per90(sum(prior.map(p => p.bonus))),
        f_defcon90: per90(sum(prior.map(p => p.defcon))),
        f_pts90: per90(sum(prior.map(p => p.pts))),
        f_form4: mean(prior.slice(-4).map(p => p.pts)),
        f_pen: cur.penTaker, f_penOrder: cur.penOrder ? 4 - Math.min(3, cur.penOrder) : 0,
        f_sp: cur.spTaker, f_fk: cur.fkTaker,
      });
    }
  }
  return out;
}

const LABELS = {
  f_startedLast: 'Started last game', f_minsLast: 'Minutes last game',
  f_mins3: 'Minutes (avg last 3)', f_mins6: 'Minutes (avg last 6)',
  f_startRate6: 'Start rate, last 6', f_startRateSeason: 'Start rate, season',
  f_startStreak: 'Consecutive starts', f_price: 'Price',
  f_selected: 'Ownership (log)', f_tbal: 'Net transfers this week',
  f_xg90: 'xG per 90', f_xa90: 'xA per 90', f_xgi90: 'xG+xA per 90',
  f_threat90: 'Threat per 90', f_creativity90: 'Creativity per 90',
  f_influence90: 'Influence per 90', f_ict90: 'ICT per 90',
  f_bps90: 'BPS per 90', f_bonus90: 'Bonus per 90', f_defcon90: 'Defensive actions per 90',
  f_pts90: 'Points per 90', f_form4: 'Form (last 4)',
  f_pen: 'Takes penalties', f_penOrder: 'Penalty order', f_sp: 'Takes corners', f_fk: 'Takes free kicks',
};

function report(rows, target, feats, title) {
  const y = rows.map(r => r[target]);
  const uni = feats.filter(f => sd(rows.map(r => r[f])) > 0)
    .map(f => ({ f, rho: spearman(rows.map(r => r[f]), y) }))
    .sort((a, b) => Math.abs(b.rho) - Math.abs(a.rho));
  const fit = fs_ => {
    const X = rows.map(r => fs_.map(f => r[f]));
    const Z = fs_.map((_, j) => zscore(X.map(r => r[j])));
    return ols(X.map((_, i) => Z.map(c => c[i])), y);
  };
  console.log(`\n### ${title}  (n=${rows.length})`);
  console.log('  Top single predictors (Spearman):');
  uni.slice(0, 9).forEach(u => console.log(`    ${u.rho >= 0 ? ' ' : ''}${u.rho.toFixed(3)}  ${LABELS[u.f]}`));
  const all = fit(feats.filter(f => sd(rows.map(r => r[f])) > 0));
  console.log(`  All features together: R^2 = ${(all.r2 * 100).toFixed(1)}%`);
  return { uni, r2: all.r2, fit };
}

const AVAIL = ['f_startedLast', 'f_minsLast', 'f_mins3', 'f_mins6', 'f_startRate6',
  'f_startRateSeason', 'f_startStreak', 'f_price', 'f_selected', 'f_tbal'];
const QUALITY = ['f_xg90', 'f_xa90', 'f_xgi90', 'f_threat90', 'f_creativity90', 'f_influence90',
  'f_ict90', 'f_bps90', 'f_bonus90', 'f_defcon90', 'f_pts90', 'f_form4',
  'f_pen', 'f_penOrder', 'f_sp', 'f_fk', 'f_price'];

function run() {
  for (const season of ['2025-26', '2024-25']) {
    const p = panel(load(season));
    console.log(`\n${'='.repeat(74)}\n${season}  —  ${p.length} player-gameweeks`);

    report(p, 'y_mins', AVAIL, 'Predicting MINUTES next gameweek');

    // Incremental: does anything beat simply knowing they started last week?
    const base = ['f_startedLast'];
    const fitR2 = fs_ => {
      const y = p.map(r => r.y_mins);
      const X = p.map(r => fs_.map(f => r[f]));
      const Z = fs_.map((_, j) => zscore(X.map(r => r[j])));
      return ols(X.map((_, i) => Z.map(c => c[i])), y).r2;
    };
    console.log('  Minutes, nested models:');
    console.log(`    ${(fitR2(base) * 100).toFixed(1)}%  started last game alone`);
    console.log(`    ${(fitR2(['f_mins3']) * 100).toFixed(1)}%  average minutes last 3 alone`);
    console.log(`    ${(fitR2(['f_startedLast', 'f_mins3', 'f_startRate6']) * 100).toFixed(1)}%  + recent start rate`);
    console.log(`    ${(fitR2(AVAIL) * 100).toFixed(1)}%  everything incl. price, ownership, transfer flow`);

    // Quality, conditional on actually starting.
    const starters = p.filter(r => r.started === 1);
    report(starters, 'y_pts', QUALITY, 'Points GIVEN the player started');
    for (const [pos, nm] of [[2, 'Defenders'], [3, 'Midfielders'], [4, 'Forwards']]) {
      const sub = starters.filter(r => r.pos === pos);
      if (sub.length > 400) report(sub, 'y_pts', QUALITY, `Points given a start — ${nm}`);
    }
  }
}
if (require.main === module) run();
module.exports = { run, panel, load, report, AVAIL, QUALITY, LABELS };
