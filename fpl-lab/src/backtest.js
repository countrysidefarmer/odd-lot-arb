'use strict';
// Does the model actually work? Replays 2025-26 gameweek by gameweek, building
// every input only from matches already played, and scores the resulting
// projections against what really happened — alongside FPL's own published xP.
const fs = require('fs');
const path = require('path');
const { parseCSV, num, sum, mean, poissonP } = require('./lib');

const DATA = path.join(__dirname, '..', 'data');
const POS = { GK: 1, GKP: 1, DEF: 2, MID: 3, FWD: 4 };
const GOAL_PTS = { 1: 10, 2: 6, 3: 5, 4: 4 };
const CS_PTS = { 1: 4, 2: 4, 3: 1, 4: 0 };
const DEFCON = { 1: 99, 2: 10, 3: 12, 4: 12 };
const LEAGUE_GPG = 1.45, HOME = 1.12, AWAY = 0.89, PRIOR_GAMES = 6;
const START_GW = 9;   // needs a run of history before the first projection

function load() {
  return parseCSV(fs.readFileSync(path.join(DATA, 'gw_2025-26.csv'), 'utf8')).map(r => ({
    id: r.element, name: r.name, pos: POS[r.position] || 3, team: r.team,
    gw: num(r.GW), opp: num(r.opponent_team), home: r.was_home === 'True',
    fixture: r.fixture, pts: num(r.total_points), fplXP: num(r.xP),
    mins: num(r.minutes), starts: num(r.starts),
    xg: num(r.expected_goals), xa: num(r.expected_assists),
    goals: num(r.goals_scored), assists: num(r.assists),
    bonus: num(r.bonus), saves: num(r.saves), yellow: num(r.yellow_cards),
    defcon: num(r.defensive_contribution),
    hs: num(r.team_h_score), as: num(r.team_a_score),
  })).filter(r => r.gw >= 1 && r.gw <= 38);
}

// Team id map, recovered from the two sides of each fixture.
function teamIds(rows) {
  const byFix = new Map();
  for (const r of rows) {
    if (!byFix.has(r.fixture)) byFix.set(r.fixture, new Map());
    byFix.get(r.fixture).set(r.team, r.opp);
  }
  const map = new Map();
  for (const sides of byFix.values()) {
    const e = [...sides.entries()];
    if (e.length === 2) { map.set(e[1][0], e[0][1]); map.set(e[0][0], e[1][1]); }
  }
  return map;
}

function run() {
  const rows = load();
  const ids = teamIds(rows);
  const idToName = new Map([...ids.entries()].map(([n, i]) => [i, n]));

  // Team results per gameweek, for strength built from prior matches only.
  const teamGames = new Map();
  const seen = new Set();
  for (const r of rows) {
    const k = r.fixture + '|' + r.team;
    if (seen.has(k)) continue;
    seen.add(k);
    const id = ids.get(r.team);
    if (id == null) continue;
    if (!teamGames.has(id)) teamGames.set(id, []);
    teamGames.get(id).push({ gw: r.gw, gf: r.home ? r.hs : r.as, ga: r.home ? r.as : r.hs });
  }
  const strengthAt = (id, gw) => {
    const g = (teamGames.get(id) || []).filter(x => x.gw < gw);
    if (g.length < 3) return { attack: 1, defence: 1 };
    return { attack: mean(g.map(x => x.gf)) / LEAGUE_GPG, defence: mean(g.map(x => x.ga)) / LEAGUE_GPG };
  };

  const byPlayer = new Map();
  for (const r of rows) {
    if (!byPlayer.has(r.id)) byPlayer.set(r.id, []);
    byPlayer.get(r.id).push(r);
  }

  const preds = [];
  for (const hist of byPlayer.values()) {
    hist.sort((a, b) => a.gw - b.gw);
    for (const cur of hist) {
      if (cur.gw < START_GW) continue;
      const prior = hist.filter(h => h.gw < cur.gw);
      if (prior.length < 6) continue;
      const pMins = sum(prior.map(p => p.mins));
      if (pMins < 180) continue;

      const per90 = v => (v * 90) / pMins;
      const tail = prior.slice(-15);
      const pPlay = tail.filter(p => p.mins > 0).length / tail.length;
      const p60 = tail.filter(p => p.mins >= 60).length / tail.length;
      const startRate = tail.filter(p => p.starts > 0).length / tail.length;
      const startMins = prior.filter(p => p.starts > 0);
      const minsPerStart = startMins.length ? mean(startMins.map(p => p.mins)) : 80;
      const expMins = startRate * Math.max(60, minsPerStart) + (1 - startRate) * 12;

      const g90 = 0.7 * per90(sum(prior.map(p => p.xg))) + 0.3 * per90(sum(prior.map(p => p.goals)));
      const a90 = 0.7 * per90(sum(prior.map(p => p.xa))) + 0.3 * per90(sum(prior.map(p => p.assists)));
      const bonus90 = per90(sum(prior.map(p => p.bonus)));
      const saves90 = per90(sum(prior.map(p => p.saves)));
      const yellow90 = per90(sum(prior.map(p => p.yellow)));
      const starts = prior.filter(p => p.starts > 0);
      const defconRate = starts.length
        ? starts.filter(p => p.defcon >= DEFCON[cur.pos]).length / starts.length : 0;

      const mine = strengthAt(ids.get(cur.team), cur.gw);
      const opp = strengthAt(cur.opp, cur.gw);
      const xGF = LEAGUE_GPG * mine.attack * opp.defence * (cur.home ? HOME : AWAY);
      const xGA = LEAGUE_GPG * opp.attack * mine.defence * (cur.home ? AWAY : HOME);
      const attMult = Math.max(0.35, Math.min(2.6, xGF / Math.max(0.35, LEAGUE_GPG * mine.attack)));
      const m = expMins / 90;

      let xp = pPlay + p60;
      xp += g90 * m * attMult * GOAL_PTS[cur.pos];
      xp += a90 * m * attMult * 3;
      xp += poissonP(0, xGA) * p60 * CS_PTS[cur.pos];
      xp += defconRate * startRate * 2;
      xp += bonus90 * m * (0.7 + 0.3 * attMult);
      xp -= yellow90 * m;
      if (cur.pos <= 2) xp -= 0.5 * xGA * m;
      if (cur.pos === 1) xp += (saves90 * m * (xGA / Math.max(0.4, LEAGUE_GPG * mine.defence))) / 3;

      preds.push({
        gw: cur.gw, pos: cur.pos, name: cur.name, actual: cur.pts,
        mine: Math.max(0, xp),
        // Baselines: what you would get without a model at all.
        ppg: mean(prior.map(pp => pp.pts)),                 // season average to date
        form: mean(prior.slice(-4).map(pp => pp.pts)),      // last four games
        fpl: cur.fplXP, hasFpl: cur.fplXP > 0,
        expMins, likely: p60 >= 0.5,
      });
    }
  }

  const mae = (a, f) => mean(a.map((x, i) => Math.abs(x - f[i])));
  const rmse = (a, f) => Math.sqrt(mean(a.map((x, i) => (x - f[i]) ** 2)));
  const report = (label, set) => {
    if (set.length < 50) return;
    const A = set.map(p => p.actual);
    const M = set.map(p => p.mine), P = set.map(p => p.ppg), Fm = set.map(p => p.form);
    console.log(`${label.padEnd(22)} n=${String(set.length).padStart(5)}   MAE:`
      + ` model ${mae(A, M).toFixed(3)}`
      + `  season-avg ${mae(A, P).toFixed(3)}`
      + `  form ${mae(A, Fm).toFixed(3)}`
      + `   RMSE model ${rmse(A, M).toFixed(3)}`
      + `   mean actual ${mean(A).toFixed(2)}`);
  };

  console.log(`Backtest on 2025-26, gameweeks ${START_GW}-38 — ${preds.length} player-gameweeks`);
  console.log('Lower MAE is better. Baselines are what you would get with no model:');
  console.log('season-avg = the player\'s points per game so far; form = his last four games.\n');
  report('All players', preds);
  report('Likely starters', preds.filter(p => p.likely));
  for (const [q, n] of [[1, 'Goalkeepers'], [2, 'Defenders'], [3, 'Midfielders'], [4, 'Forwards']]) {
    report('  ' + n, preds.filter(p => p.likely && p.pos === q));
  }

  // Captaincy: pick the highest projection each gameweek, see what it returned.
  const gws = [...new Set(preds.map(p => p.gw))].sort((a, b) => a - b);
  const capMine = [], capPpg = [], capForm = [];
  for (const gw of gws) {
    const set = preds.filter(p => p.gw === gw && p.likely);
    if (set.length < 20) continue;
    capMine.push(set.slice().sort((a, b) => b.mine - a.mine)[0].actual);
    capPpg.push(set.slice().sort((a, b) => b.ppg - a.ppg)[0].actual);
    capForm.push(set.slice().sort((a, b) => b.form - a.form)[0].actual);
  }
  console.log(`\nTop captain pick, averaged over ${capMine.length} gameweeks:`
    + `  model ${mean(capMine).toFixed(2)}   season-avg ${mean(capPpg).toFixed(2)}   form ${mean(capForm).toFixed(2)}`);

  // Does it rank correctly? Average actual points of the top 20 each gameweek.
  const topMine = [], topPpg = [], topForm = [];
  for (const gw of gws) {
    const set = preds.filter(p => p.gw === gw && p.likely);
    if (set.length < 40) continue;
    const top = (key) => mean(set.slice().sort((a, b) => b[key] - a[key]).slice(0, 20).map(p => p.actual));
    topMine.push(top('mine')); topPpg.push(top('ppg')); topForm.push(top('form'));
  }
  console.log(`Top 20 by projection, average actual points:  model ${mean(topMine).toFixed(2)}`
    + `   season-avg ${mean(topPpg).toFixed(2)}   form ${mean(topForm).toFixed(2)}`);

  // FPL publishes its own xP, but this dataset only carries it for one gameweek.
  const withFpl = preds.filter(p => p.hasFpl);
  if (withFpl.length > 100) {
    const A = withFpl.map(p => p.actual);
    console.log(`\nWhere FPL's own xP exists (n=${withFpl.length}, GW${withFpl[0].gw} only):`
      + `  model ${mae(A, withFpl.map(p => p.mine)).toFixed(3)}   FPL ${mae(A, withFpl.map(p => p.fpl)).toFixed(3)}`);
  }

  // Calibration: are projections systematically high or low?
  console.log(`\nCalibration — mean projection ${mean(preds.map(p => p.mine)).toFixed(2)}`
    + ` vs mean actual ${mean(preds.map(p => p.actual)).toFixed(2)}`);

  // Where the error concentrates: are the misses on hauls or on blanks?
  const big = preds.filter(p => p.actual >= 10);
  const blank = preds.filter(p => p.actual <= 2 && p.likely);
  console.log(`Hauls (10+ pts, n=${big.length}): projected ${mean(big.map(p => p.mine)).toFixed(2)}, actual ${mean(big.map(p => p.actual)).toFixed(2)}`);
  console.log(`Blanks (<=2 pts among likely starters, n=${blank.length}): projected ${mean(blank.map(p => p.mine)).toFixed(2)}, actual ${mean(blank.map(p => p.actual)).toFixed(2)}`);
}

if (require.main === module) run();
module.exports = { run };
