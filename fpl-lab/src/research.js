'use strict';
// Backtest: which pre-gameweek signals actually predict FPL points?
// Every feature is built only from gameweeks strictly before the one being predicted.
const fs = require('fs');
const path = require('path');
const { parseCSV, num, sum, mean, sd, spearman, ols, zscore } = require('./lib');

const DATA = path.join(__dirname, '..', 'data');
const POS = { GK: 1, GKP: 1, DEF: 2, MID: 3, FWD: 4 };

function loadSeason(season) {
  const rows = parseCSV(fs.readFileSync(path.join(DATA, `gw_${season}.csv`), 'utf8'));
  return rows.map(r => ({
    id: r.element,
    name: r.name,
    pos: POS[r.position] || 0,
    team: r.team,
    gw: num(r.GW),
    fixture: r.fixture,
    opp: num(r.opponent_team),
    home: r.was_home === 'True' || r.was_home === 'true',
    pts: num(r.total_points),
    mins: num(r.minutes),
    starts: num(r.starts),
    xg: num(r.expected_goals),
    xa: num(r.expected_assists),
    xgc: num(r.expected_goals_conceded),
    bps: num(r.bps),
    ict: num(r.ict_index),
    threat: num(r.threat),
    creativity: num(r.creativity),
    defcon: num(r.defensive_contribution),
    gs: num(r.goals_scored),
    ga: num(r.assists),
    cs: num(r.clean_sheets),
    conceded: num(r.goals_conceded),
    saves: num(r.saves),
    value: num(r.value) / 10,
    hs: num(r.team_h_score),
    as: num(r.team_a_score),
  })).filter(r => r.gw >= 1 && r.gw <= 38);
}

// Recover team-name -> FPL team id using the two sides of each fixture.
function teamIdMap(rows) {
  const byFix = new Map();
  for (const r of rows) {
    if (!byFix.has(r.fixture)) byFix.set(r.fixture, new Map());
    byFix.get(r.fixture).set(r.team, r.opp);
  }
  const map = new Map();
  for (const sides of byFix.values()) {
    const entries = [...sides.entries()];
    if (entries.length !== 2) continue;
    // If X's opponent id is p, then the other team Y carries id p.
    map.set(entries[1][0], entries[0][1]);
    map.set(entries[0][0], entries[1][1]);
  }
  return map;
}

// Cumulative team form entering each gameweek (no leakage: strictly prior GWs).
function teamStrength(rows, ids) {
  const matches = new Map(); // fixture -> per-team result
  for (const r of rows) {
    const key = r.fixture + '|' + r.team;
    if (matches.has(key)) continue;
    const gf = r.home ? r.hs : r.as;
    const ga = r.home ? r.as : r.hs;
    matches.set(key, { gw: r.gw, id: ids.get(r.team), gf, ga, home: r.home });
  }
  const perTeam = new Map();
  for (const m of matches.values()) {
    if (m.id == null) continue;
    if (!perTeam.has(m.id)) perTeam.set(m.id, []);
    perTeam.get(m.id).push(m);
  }
  // strength[id][gw] = record from all matches before gw
  const out = new Map();
  for (const [id, ms] of perTeam) {
    ms.sort((a, b) => a.gw - b.gw);
    const table = new Map();
    for (let gw = 1; gw <= 39; gw++) {
      const prior = ms.filter(m => m.gw < gw);
      table.set(gw, {
        n: prior.length,
        gfpg: prior.length ? mean(prior.map(m => m.gf)) : 1.4,
        gapg: prior.length ? mean(prior.map(m => m.ga)) : 1.4,
      });
    }
    out.set(id, table);
  }
  return out;
}

function buildPanel(rows) {
  const ids = teamIdMap(rows);
  const strength = teamStrength(rows, ids);
  const byPlayer = new Map();
  for (const r of rows) {
    if (!byPlayer.has(r.id)) byPlayer.set(r.id, []);
    byPlayer.get(r.id).push(r);
  }
  const panel = [];
  for (const hist of byPlayer.values()) {
    hist.sort((a, b) => a.gw - b.gw);
    for (let i = 0; i < hist.length; i++) {
      const cur = hist[i];
      const prior = hist.slice(0, i);
      if (prior.length < 5) continue;              // need a real track record
      const played = prior.filter(p => p.mins > 0);
      if (played.length < 3) continue;
      const last4 = prior.slice(-4), last3 = prior.slice(-3), last6 = prior.slice(-6);
      const pMins = sum(prior.map(p => p.mins));
      const per90 = v => (pMins > 0 ? (v * 90) / pMins : 0);
      const opp = strength.get(cur.opp) || { get: () => null };
      const oppRec = opp.get ? opp.get(cur.gw) : null;
      if (!oppRec || oppRec.n < 3) continue;       // opponent needs a sample too

      // Next-5 target: points across this GW and the four that follow it.
      const next5 = hist.filter(h => h.gw >= cur.gw && h.gw < cur.gw + 5);
      // Fixture ease averaged over that same 5-game window, judged only on what
      // each opponent had shown *before* the current gameweek.
      const ease5 = [], att5 = [];
      for (const h of next5) {
        const t = strength.get(h.opp);
        const rec = t ? t.get(cur.gw) : null;
        if (rec && rec.n >= 3) { ease5.push(rec.gapg); att5.push(-rec.gfpg); }
      }
      panel.push({
        pos: cur.pos, gw: cur.gw, name: cur.name,
        y: cur.pts,
        y5: sum(next5.map(h => h.pts)),
        y5n: next5.length,
        f_form4: mean(last4.map(p => p.pts)),
        f_form6: mean(last6.map(p => p.pts)),
        f_ppg: mean(prior.map(p => p.pts)),
        f_ppg90: per90(sum(prior.map(p => p.pts))),
        f_xgi90: per90(sum(prior.map(p => p.xg + p.xa))),
        f_threat90: per90(sum(prior.map(p => p.threat))),
        f_bps90: per90(sum(prior.map(p => p.bps))),
        f_ict90: per90(sum(prior.map(p => p.ict))),
        f_defcon90: per90(sum(prior.map(p => p.defcon))),
        f_mins3: mean(last3.map(p => p.mins)),
        f_startrate: mean(prior.map(p => (p.starts > 0 ? 1 : 0))),
        f_price: cur.value,
        f_home: cur.home ? 1 : 0,
        // Fixture ease from the opponent's own record entering this GW.
        f_oppweak_def: oppRec.gapg,   // opponent concedes a lot -> good for attackers
        f_oppweak_att: -oppRec.gfpg,  // opponent scores little -> good for clean sheets
        f_oppweak_def5: ease5.length ? mean(ease5) : oppRec.gapg,
        f_oppweak_att5: att5.length ? mean(att5) : -oppRec.gfpg,
        f_home5: next5.length ? mean(next5.map(h => (h.home ? 1 : 0))) : (cur.home ? 1 : 0),
        f_startmins: mean(last3.map(p => p.mins)) / 90,
      });
    }
  }
  return panel;
}

const FEATURES = ['f_form4', 'f_form6', 'f_ppg', 'f_ppg90', 'f_xgi90', 'f_threat90',
  'f_bps90', 'f_ict90', 'f_defcon90', 'f_mins3', 'f_startrate', 'f_price',
  'f_home', 'f_oppweak_def', 'f_oppweak_att'];

const LABELS = {
  f_form4: 'Form (pts/gm, last 4)', f_form6: 'Form (pts/gm, last 6)',
  f_ppg: 'Season points per game', f_ppg90: 'Season points per 90',
  f_xgi90: 'xG+xA per 90', f_threat90: 'Threat per 90', f_bps90: 'BPS per 90',
  f_ict90: 'ICT per 90', f_defcon90: 'Defensive contribution per 90',
  f_mins3: 'Minutes (avg last 3)', f_startrate: 'Start rate',
  f_price: 'Price', f_home: 'Home fixture', f_oppweak_def: 'Fixture: opponent leaks goals',
  f_oppweak_att: 'Fixture: opponent is toothless',
  f_oppweak_def5: 'Fixture run: opponents leak goals (5gw avg)',
  f_oppweak_att5: 'Fixture run: opponents are toothless (5gw avg)',
  f_home5: 'Home games in next 5',
};

function analyse(panel, target, label) {
  const rows = target === 'y5' ? panel.filter(p => p.y5n === 5) : panel;
  const y = rows.map(p => p[target]);
  // Over a 5-game horizon the fixture signal is the average of that run, not one game.
  const h5 = target === 'y5';
  const FIX = h5 ? ['f_oppweak_def5', 'f_oppweak_att5', 'f_home5']
                 : ['f_oppweak_def', 'f_oppweak_att', 'f_home'];
  const featureList = FEATURES.filter(f => !/oppweak|home/.test(f)).concat(FIX);
  const res = { label, n: rows.length, univariate: [], models: [] };
  for (const f of featureList) {
    const x = rows.map(p => p[f]);
    if (sd(x) === 0) continue;
    res.univariate.push({ key: f, label: LABELS[f], rho: spearman(x, y) });
  }
  res.univariate.sort((a, b) => Math.abs(b.rho) - Math.abs(a.rho));

  const fit = (feats, name) => {
    const X = rows.map(p => feats.map(f => p[f]));
    const Z = feats.map((_, j) => zscore(X.map(r => r[j])));
    const Xz = X.map((_, i) => Z.map(col => col[i]));
    const m = ols(Xz, y);
    return {
      name, r2: m.r2,
      betas: feats.map((f, j) => ({ key: f, label: LABELS[f], beta: m.beta[j + 1] }))
        .sort((a, b) => Math.abs(b.beta) - Math.abs(a.beta)),
    };
  };
  const FORM = ['f_form4', 'f_ppg'];
  const MINS = ['f_mins3', 'f_startrate'];
  const UND = ['f_xgi90', 'f_bps90', 'f_threat90'];
  res.models = [
    fit(MINS, 'Minutes only'),
    fit(FORM, 'Form only'),
    fit(FIX, 'Fixture only'),
    fit(UND, 'Underlying rates only'),
    fit([...MINS, ...FORM], 'Minutes + form'),
    fit([...MINS, ...FIX], 'Minutes + fixture'),
    fit([...MINS, ...FORM, ...FIX], 'Minutes + form + fixture'),
    fit([...MINS, ...FORM, ...FIX, ...UND], 'Everything'),
  ];
  res.full = fit(featureList.filter(f => rows.some(p => p[f] !== 0)), 'All features');
  // Incremental value of the fixture, once minutes and form are already known.
  const base = fit([...MINS, ...FORM], 'base');
  const withFix = fit([...MINS, ...FORM, ...FIX], 'base+fix');
  const withUnd = fit([...MINS, ...FORM, ...UND], 'base+und');
  res.increment = {
    base: base.r2,
    fixtureGain: withFix.r2 - base.r2,
    underlyingGain: withUnd.r2 - base.r2,
  };
  return res;
}

function run() {
  const out = { generated: new Date().toISOString(), seasons: {} };
  for (const season of ['2025-26', '2024-25']) {
    const rows = loadSeason(season);
    const panel = buildPanel(rows);
    // "Nailed" = the players you would realistically own: near-ever-present starters.
    const nailed = panel.filter(p => p.f_startrate >= 0.7 && p.f_mins3 >= 60);
    const byPos = {};
    for (const [pos, nm] of [[1, 'Goalkeepers'], [2, 'Defenders'], [3, 'Midfielders'], [4, 'Forwards']]) {
      const sub = nailed.filter(p => p.pos === pos);
      if (sub.length > 300) byPos[nm] = analyse(sub, 'y5', nm);
    }
    out.seasons[season] = {
      rows: rows.length,
      panel: panel.length,
      next1: analyse(panel, 'y', 'Next gameweek — all players'),
      next5: analyse(panel, 'y5', 'Next 5 gameweeks — all players'),
      nailed1: analyse(nailed, 'y', 'Next gameweek — nailed starters only'),
      nailed5: analyse(nailed, 'y5', 'Next 5 gameweeks — nailed starters only'),
      byPos,
    };
    console.log(`${season}: ${rows.length} rows -> ${panel.length} usable observations`);
  }
  fs.writeFileSync(path.join(DATA, 'research.json'), JSON.stringify(out, null, 1));
  return out;
}

if (require.main === module) {
  const out = run();
  for (const [season, s] of Object.entries(out.seasons)) {
    for (const key of ['next1', 'next5', 'nailed1', 'nailed5']) {
      const a = s[key];
      console.log(`\n=== ${season} :: ${a.label} (n=${a.n})`);
      console.log('Top univariate (Spearman):');
      a.univariate.slice(0, 8).forEach(u => console.log(`   ${u.rho >= 0 ? ' ' : ''}${u.rho.toFixed(3)}  ${u.label}`));
      console.log('Models (R^2):');
      a.models.forEach(m => console.log(`   ${(m.r2 * 100).toFixed(2).padStart(6)}%  ${m.name}`));
      console.log(`Incremental R^2 over minutes+form: fixture +${(a.increment.fixtureGain*100).toFixed(2)}pp, underlying +${(a.increment.underlyingGain*100).toFixed(2)}pp`);
    }
  }
}
module.exports = { run, loadSeason, teamIdMap };
