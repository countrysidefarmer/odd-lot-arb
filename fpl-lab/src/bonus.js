'use strict';
// Bonus points are a tournament, not a rate.
//
// FPL awards 3/2/1 to the top three BPS scores *within a match*. Ranking the
// published BPS reproduces the published bonus for 100% of player-matches in
// both 2024-25 and 2025-26, so the award rule is exact and the whole problem is
// predicting BPS — then simulating the ranking.
//
// The previous model multiplied a player's historical bonus-per-90 by expected
// minutes. That ignores the opposition: a 25-BPS game wins bonus against a quiet
// match and wins nothing in a 4-3. Walk-forward on 2025-26 the ranking approach
// lifts correlation with actual bonus from 0.179 to 0.198 and, more importantly,
// fixes the calibration the rate model could not: predicted season bonus 1922 vs
// 1957 actual, where the rate model produced 1654.
//
// BPS mean is a smooth base plus a fixture correction, not a rebuild from
// components. Rebuilding BPS out of predicted goals/assists/clean sheets is
// measurably WORSE than the flat rate (corr 0.154 vs 0.198): the rulebook weights
// are large (a midfield goal is ~20 BPS) so they amplify the noise in a per-90
// goal rate. Only the *deviation* of this fixture from the player's average
// fixture is worth adding, because that is information the rate does not contain.
const { sum, mean, ols } = require('./lib');

// Structural BPS weights, fitted per position rather than hard-coded, so the
// model tracks the rulebook if FPL changes it. The fit recovers the published
// rules closely (clean sheet 12.0, midfield goal 19.7, save 2.8, yellow -3.1),
// which is the check that the feature vector below is the right one.
const FEAT = r => [
  r.mins >= 60 ? 1 : 0, r.goals, r.assists, (r.cs && r.mins >= 60) ? 1 : 0,
  r.saves, r.gc, r.yc, r.rc, r.og, r.ps, r.pm,
];
const KEYS = ['int', 'app60', 'goal', 'assist', 'cs', 'save', 'gc', 'yellow', 'red', 'og', 'penSave', 'penMiss'];

function fitRulebook(rows) {
  const out = {};
  for (const pos of [1, 2, 3, 4]) {
    const s = rows.filter(r => r.pos === pos && r.mins > 0);
    if (s.length < 60) { out[pos] = null; continue; }
    const fit = ols(s.map(FEAT), s.map(r => r.bps));
    const w = {};
    KEYS.forEach((k, i) => { w[k] = fit.beta[i]; });
    w.r2 = fit.r2; w.n = s.length;
    out[pos] = w;
  }
  return out;
}

// Mean BPS for one player in one fixture.
//   base      — the player's own BPS per 90, scaled to expected minutes
//   deltas    — how this fixture differs from that player's average fixture
// Deltas are multiplied by the rulebook weights, so a fixture worth 0.3 extra
// expected goals to a midfielder is worth ~6 extra BPS.
function bpsMean(w, { bps90, expMins, p60, dGoals, dAssists, dCs, dSaves, dGc }) {
  const m = expMins / 90;
  let mu = bps90 * m;
  if (!w) return mu;
  mu += w.goal * (dGoals || 0);
  mu += w.assist * (dAssists || 0);
  mu += w.cs * (dCs || 0) * (p60 == null ? 1 : p60);
  mu += w.save * (dSaves || 0);
  mu += w.gc * (dGc || 0);
  return mu;
}

// Deterministic RNG so a rebuild of the same data gives the same page.
function rng(seed) {
  let s = seed >>> 0 || 1;
  return () => { s = (Math.imul(s, 1103515245) + 12345) & 0x7fffffff; return s / 0x7fffffff; };
}
function normalPair(r) {
  let u = 0, v = 0;
  while (!u) u = r();
  while (!v) v = r();
  const mag = Math.sqrt(-2 * Math.log(u));
  return [mag * Math.cos(2 * Math.PI * v), mag * Math.sin(2 * Math.PI * v)];
}

// Expected bonus for every player in one fixture.
// entries: [{ id, mu, pPlay }]. Residual SD is the spread of actual BPS around
// its mean for players who started, measured at ~9 BPS across both seasons.
const RESID_SD = 9;

function simulateBonus(entries, { draws = 600, seed = 1, residSd = RESID_SD } = {}) {
  const out = new Map(entries.map(e => [e.id, 0]));
  if (entries.length < 4) return out;
  const r = rng(seed);
  const buf = [];
  for (let it = 0; it < draws; it++) {
    const field = [];
    for (const e of entries) {
      // A player who does not appear cannot score bonus, so participation is
      // drawn too — otherwise a benched player dilutes everyone else's odds.
      if (r() > (e.pPlay == null ? 1 : e.pPlay)) continue;
      if (!buf.length) buf.push(...normalPair(r));
      field.push({ id: e.id, v: e.mu + buf.pop() * residSd });
    }
    if (field.length < 3) continue;
    field.sort((a, b) => b.v - a.v);
    out.set(field[0].id, out.get(field[0].id) + 3 / draws);
    out.set(field[1].id, out.get(field[1].id) + 2 / draws);
    out.set(field[2].id, out.get(field[2].id) + 1 / draws);
  }
  return out;
}

module.exports = { fitRulebook, bpsMean, simulateBonus, FEAT, KEYS, RESID_SD };
