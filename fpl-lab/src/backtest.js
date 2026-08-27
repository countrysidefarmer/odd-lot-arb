'use strict';
// Does the model actually work? Replays 2025-26 gameweek by gameweek, building
// every input only from matches already played, and scores the resulting
// projections against what really happened — alongside FPL's own published xP.
const fs = require('fs');
const path = require('path');
const { parseCSV, num, sum, mean, poissonP } = require('./lib');
const { fitRulebook, bpsMean, simulateBonus } = require('./bonus');

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
    bps: num(r.bps), cs: num(r.clean_sheets), gc: num(r.goals_conceded),
    yc: num(r.yellow_cards),
    rc: num(r.red_cards), og: num(r.own_goals),
    ps: num(r.penalties_saved), pm: num(r.penalties_missed),
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

  // The BPS rulebook is fitted only on gameweeks before the backtest window, so
  // nothing here has seen the matches it is scored against.
  const rulebook = fitRulebook(rows.filter(r => r.gw < START_GW && r.mins > 0));

  // Typical BPS per 90 by position, for players with no usable history.
  const POS_BPS = {};
  for (const q of [1, 2, 3, 4]) {
    const v = rows.filter(r => r.gw < START_GW && r.mins >= 60 && r.pos === q);
    POS_BPS[q] = v.length ? mean(v.map(r => r.bps * 90 / r.mins)) : 18;
  }
  const field = new Map();

  const preds = [];
  for (const hist of byPlayer.values()) {
    hist.sort((a, b) => a.gw - b.gw);
    for (const cur of hist) {
      if (cur.gw < START_GW) continue;
      // Bonus is decided among everyone on the pitch, so the ranking contest
      // needs the whole field — including players too new to project properly.
      // They get a crude BPS estimate rather than being left out, because an
      // absent competitor would silently inflate everyone else's bonus odds.
      {
        const pr = hist.filter(h => h.gw < cur.gw);
        const pm = sum(pr.map(x => x.mins));
        const mu = pm >= 90
          ? (sum(pr.map(x => x.bps)) * 90 / pm) * (mean(pr.slice(-6).map(x => x.mins)) / 90)
          : POS_BPS[cur.pos] * 0.35;
        const pp = pr.length ? pr.slice(-8).filter(x => x.mins > 0).length / Math.min(8, pr.length) : 0.3;
        if (!field.has(cur.fixture)) field.set(cur.fixture, []);
        field.get(cur.fixture).push({ id: cur.id, mu, pPlay: pp });
      }

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
      const bps90 = per90(sum(prior.map(p => p.bps)));
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
      const bonusRate = bonus90 * m * (0.7 + 0.3 * attMult);   // the old model
      xp -= yellow90 * m;
      if (cur.pos <= 2) xp -= 0.5 * xGA * m;
      if (cur.pos === 1) xp += (saves90 * m * (xGA / Math.max(0.4, LEAGUE_GPG * mine.defence))) / 3;

      // Expected BPS for the ranking contest: the player's rate, corrected only
      // for how this fixture differs from a neutral one.
      const neutralGA = LEAGUE_GPG * mine.defence;
      const bpsMu = bpsMean(rulebook[cur.pos], {
        bps90, expMins, p60,
        dGoals: g90 * m * (attMult - 1),
        dAssists: a90 * m * (attMult - 1),
        dCs: cur.pos <= 2 ? poissonP(0, xGA) - poissonP(0, neutralGA) : 0,
        dGc: cur.pos <= 2 ? (xGA - neutralGA) * m : 0,
        dSaves: cur.pos === 1 ? saves90 * m * (xGA / Math.max(0.4, neutralGA) - 1) : 0,
      });

      preds.push({
        gw: cur.gw, pos: cur.pos, name: cur.name, actual: cur.pts,
        fixture: cur.fixture, id: cur.id,
        xpNoBonus: xp, bonusRate, bpsMu, pPlayDraw: pPlay,
        actualBonus: cur.bonus,
        mine: Math.max(0, xp + bonusRate),
        // Baselines: what you would get without a model at all.
        ppg: mean(prior.map(pp => pp.pts)),                 // season average to date
        form: mean(prior.slice(-4).map(pp => pp.pts)),      // last four games
        fpl: cur.fplXP, hasFpl: cur.fplXP > 0,
        expMins, likely: p60 >= 0.5,
      });
    }
  }

  // ---- bonus, as a ranking contest inside each fixture --------------------
  const better = new Map(preds.map(p => [p.fixture + "|" + p.id, p]));
  const pool = new Map();
  for (const [fid, entries] of field) {
    pool.set(fid, entries.map(e => {
      const b = better.get(fid + "|" + e.id);
      return b ? { id: e.id, mu: b.bpsMu, pPlay: b.pPlayDraw } : e;
    }));
  }
  const byKey = new Map(preds.map(p => [p.fixture + '|' + p.id, p]));
  for (const [fid, entries] of pool) {
    const awarded = simulateBonus(entries, { seed: Number(fid) || 1 });
    for (const [pid, xb] of awarded) {
      const p = byKey.get(fid + '|' + pid);
      if (p) p.bonusRank = xb;
    }
  }
  for (const p of preds) {
    p.bonusRank = p.bonusRank || 0;
    p.mineRank = Math.max(0, p.xpNoBonus + p.bonusRank);
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

  // ---- A/B: bonus as a per-90 rate vs as a ranking contest ----------------
  const AB = preds.map(p => p.actualBonus);
  const ACT = preds.map(p => p.actual);
  const corr = (x, y) => {
    const mx = mean(x), my = mean(y);
    let a = 0, b = 0, c = 0;
    for (let i = 0; i < x.length; i++) { const u = x[i] - mx, v = y[i] - my; a += u * v; b += u * u; c += v * v; }
    return b && c ? a / Math.sqrt(b * c) : 0;
  };
  console.log('\nBonus points — per-90 rate vs BPS ranking contest:');
  for (const [label, key] of [['rate (old)', 'bonusRate'], ['ranking (new)', 'bonusRank']]) {
    const P = preds.map(p => p[key]);
    console.log(`  ${label.padEnd(14)} corr ${corr(P, AB).toFixed(3)}`
      + `   RMSE ${rmse(AB, P).toFixed(4)}`
      + `   predicted total ${sum(P).toFixed(0)} vs actual ${sum(AB).toFixed(0)}`);
  }
  console.log('\nWhole-projection effect of the bonus change:');
  for (const [label, key] of [['rate (old)', 'mine'], ['ranking (new)', 'mineRank']]) {
    const P = preds.map(p => p[key]);
    console.log(`  ${label.padEnd(14)} MAE ${mae(ACT, P).toFixed(4)}`
      + `   RMSE ${rmse(ACT, P).toFixed(4)}`
      + `   mean proj ${mean(P).toFixed(3)} vs actual ${mean(ACT).toFixed(3)}`);
  }
  {
    const gws2 = [...new Set(preds.map(p => p.gw))].sort((a, b) => a - b);
    const line = key => {
      const v = [];
      for (const gw of gws2) {
        const set = preds.filter(p => p.gw === gw && p.likely);
        if (set.length < 40) continue;
        v.push(mean(set.slice().sort((a, b) => b[key] - a[key]).slice(0, 20).map(p => p.actual)));
      }
      return mean(v);
    };
    console.log(`  Top 20 by projection, average actual points:  rate ${line('mine').toFixed(3)}   ranking ${line('mineRank').toFixed(3)}`);
  }

  // Where the error concentrates: are the misses on hauls or on blanks?
  const big = preds.filter(p => p.actual >= 10);
  const blank = preds.filter(p => p.actual <= 2 && p.likely);
  console.log(`Hauls (10+ pts, n=${big.length}): projected ${mean(big.map(p => p.mine)).toFixed(2)}, actual ${mean(big.map(p => p.actual)).toFixed(2)}`);
  console.log(`Blanks (<=2 pts among likely starters, n=${blank.length}): projected ${mean(blank.map(p => p.mine)).toFixed(2)}, actual ${mean(blank.map(p => p.actual)).toFixed(2)}`);
}

if (require.main === module) run();
module.exports = { run };
