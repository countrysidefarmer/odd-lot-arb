'use strict';
// Multi-week transfer planner.
//
// A single-gameweek transfer menu cannot see that the best move now blocks a
// better one in three weeks, or that banking a transfer buys a double move later.
// This searches sequences of transfers across a horizon and keeps the best ones.
//
// Exhaustive search is intractable (15 x 600 moves per week, compounding), so this
// is a beam search: at each gameweek expand a shortlist of promising moves from each
// surviving plan, score the whole horizon, and keep the best BEAM plans.
const fs = require('fs');
const path = require('path');

const DATA = path.join(__dirname, '..', 'data');
const read = f => JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'));

const HORIZON = +(process.env.PLAN_HORIZON || 6);
const BEAM = +(process.env.PLAN_BEAM || 60);
const CANDIDATES = +(process.env.PLAN_CANDIDATES || 3);   // replacements considered per player
const HIT = 4;
const MAX_FREE = 5;
const SQUAD = { 1: 2, 2: 5, 3: 5, 4: 3 };

// Highest-scoring legal XI from 15, plus the captain's doubled score.
function bestXI(squad, gw, xpOf) {
  const byPos = { 1: [], 2: [], 3: [], 4: [] };
  for (const p of squad) byPos[p.po].push(p);
  for (const k of [1, 2, 3, 4]) byPos[k].sort((a, b) => xpOf(b, gw) - xpOf(a, gw));
  let best = -1, bestSet = null;
  for (let d = 3; d <= 5; d++) {
    for (let m = 2; m <= 5; m++) {
      const f = 10 - d - m;
      if (f < 1 || f > 3) continue;
      if (!byPos[1].length || byPos[2].length < d || byPos[3].length < m || byPos[4].length < f) continue;
      const xi = [byPos[1][0], ...byPos[2].slice(0, d), ...byPos[3].slice(0, m), ...byPos[4].slice(0, f)];
      const total = xi.reduce((s, p) => s + xpOf(p, gw), 0);
      if (total > best) { best = total; bestSet = xi; }
    }
  }
  if (!bestSet) return { pts: 0, xi: [], captain: null };
  const cap = bestSet.reduce((a, b) => (xpOf(b, gw) > xpOf(a, gw) ? b : a));
  return { pts: best + xpOf(cap, gw), xi: bestSet, captain: cap };
}

function main() {
  const proj = read('projections.json');
  const entry = read('entry.json');
  if (!entry.picks) { console.error('no published squad yet'); process.exit(1); }

  const byId = new Map(proj.players.map(p => [p.id, {
    id: p.id, n: p.name, po: p.pos, t: p.team, ts: p.teamShort, c: p.cost,
    em: p.expMins, own: p.owned, st: p.status,
    gw: Object.fromEntries(p.fixtures.map(f => [f.gw, f.xp])),
  }]));
  const xpOf = (p, gw) => p.gw[gw] || 0;

  const gws = [];
  for (let g = proj.deadlineGw; gws.length < HORIZON && g <= 38; g++) gws.push(g);

  const squad0 = entry.picks.picks.map(pk => byId.get(pk.element)).filter(Boolean);
  const bank0 = entry.picks.entry_history.bank / 10;
  const free0 = +(process.env.PLAN_FREE || 1);

  // Only players worth considering: projected to play, and not already worse than
  // everything else in their position band.
  const pool = proj.players
    .filter(p => p.expMins >= 55 && p.status === 'a')
    .map(p => byId.get(p.id))
    .filter(Boolean);
  const poolByPos = { 1: [], 2: [], 3: [], 4: [] };
  for (const p of pool) poolByPos[p.po].push(p);

  const horizonValue = (p, from) => gws.filter(g => g >= from).reduce((s, g) => s + xpOf(p, g), 0);

  const clubCounts = squad => {
    const c = {};
    for (const p of squad) c[p.t] = (c[p.t] || 0) + 1;
    return c;
  };

  // Candidate one-for-one swaps from a squad, ranked by horizon gain.
  function moves(squad, bank, from) {
    const owned = new Set(squad.map(p => p.id));
    const clubs = clubCounts(squad);
    const out = [];
    for (const sell of squad) {
      const gains = [];
      for (const buy of poolByPos[sell.po]) {
        if (owned.has(buy.id)) continue;
        if (buy.c > sell.c + bank) continue;
        if ((clubs[buy.t] || 0) - (buy.t === sell.t ? 1 : 0) >= 3) continue;
        const gain = horizonValue(buy, from) - horizonValue(sell, from);
        if (gain <= 0) continue;
        gains.push({ sell, buy, gain });
      }
      gains.sort((a, b) => b.gain - a.gain);
      out.push(...gains.slice(0, CANDIDATES));
    }
    out.sort((a, b) => b.gain - a.gain);
    return out.slice(0, 24);
  }

  const applyMove = (squad, m) => squad.map(p => (p.id === m.sell.id ? m.buy : p));

  // ---- beam search -------------------------------------------------------
  let beam = [{ squad: squad0, bank: bank0, free: free0, pts: 0, hits: 0, log: [] }];

  for (const gw of gws) {
    const next = [];
    for (const state of beam) {
      // Every plan may make 0, 1 or 2 transfers this week.
      const options = [{ moves: [], cost: 0 }];
      const cand = moves(state.squad, state.bank, gw);
      for (const m of cand.slice(0, 12)) options.push({ moves: [m], cost: 0 });
      // Two transfers: pair the best move with the best compatible second.
      for (const m of cand.slice(0, 5)) {
        const s2 = applyMove(state.squad, m);
        const b2 = state.bank + m.sell.c - m.buy.c;
        for (const m2 of moves(s2, b2, gw).slice(0, 3)) options.push({ moves: [m, m2], cost: 0 });
      }

      for (const opt of options) {
        let squad = state.squad, bank = state.bank;
        for (const m of opt.moves) { squad = applyMove(squad, m); bank += m.sell.c - m.buy.c; }
        if (bank < -1e-9) continue;
        const used = opt.moves.length;
        const paid = Math.max(0, used - state.free);
        const hitCost = paid * HIT;
        const week = bestXI(squad, gw, xpOf);
        if (!week.xi.length) continue;
        next.push({
          squad, bank,
          free: Math.min(MAX_FREE, state.free - used + paid + 1),
          pts: state.pts + week.pts - hitCost,
          hits: state.hits + paid,
          log: state.log.concat([{
            gw, transfers: opt.moves.map(m => ({ out: m.sell, in: m.buy })),
            hit: hitCost, pts: +week.pts.toFixed(2),
            xi: week.xi.map(p => p.id), captain: week.captain.id,
          }]),
        });
      }
    }
    next.sort((a, b) => b.pts - a.pts);
    // Keep the beam diverse: cap how many plans share the same first-week action.
    const seen = new Map();
    beam = [];
    for (const s of next) {
      const key = (s.log[0].transfers.map(t => t.in.id).sort().join(',') || 'roll');
      const n = seen.get(key) || 0;
      if (n >= Math.max(3, Math.floor(BEAM / 6))) continue;
      seen.set(key, n + 1);
      beam.push(s);
      if (beam.length >= BEAM) break;
    }
    if (!beam.length) { beam = next.slice(0, BEAM); break; }
  }

  const best = beam[0];
  // What does doing nothing at all score? The benchmark every plan must beat.
  let rollSquad = squad0, rollPts = 0;
  for (const gw of gws) rollPts += bestXI(rollSquad, gw, xpOf).pts;

  const plan = {
    generated: new Date().toISOString(),
    horizon: gws, freeTransfers: free0, bank: bank0,
    noTransferPoints: +rollPts.toFixed(2),
    best: {
      points: +best.pts.toFixed(2),
      gain: +(best.pts - rollPts).toFixed(2),
      hits: best.hits,
      weeks: best.log.map(w => ({
        gw: w.gw, pts: w.pts, hit: w.hit,
        captain: { id: w.captain, name: byId.get(w.captain).n, team: byId.get(w.captain).ts },
        transfers: w.transfers.map(t => ({
          out: { id: t.out.id, name: t.out.n, team: t.out.ts, cost: t.out.c },
          in: { id: t.in.id, name: t.in.n, team: t.in.ts, cost: t.in.c },
        })),
        xi: w.xi,
      })),
    },
    alternatives: (() => {
      const seenOpening = new Set();
      const first = best.log[0].transfers.map(t => t.in.id).sort().join(',');
      seenOpening.add(first);
      const out = [];
      for (const s of beam) {
        const key = s.log[0].transfers.map(t => t.in.id).sort().join(',');
        if (seenOpening.has(key)) continue;
        seenOpening.add(key);
        out.push({
          points: +s.pts.toFixed(2), gain: +(s.pts - rollPts).toFixed(2), hits: s.hits,
          cost: +(best.pts - s.pts).toFixed(2),
          opening: s.log[0].transfers.map(t => ({
            out: { name: t.out.n, team: t.out.ts, cost: t.out.c },
            in: { name: t.in.n, team: t.in.ts, cost: t.in.c },
          })),
        });
        if (out.length >= 4) break;
      }
      return out;
    })(),
  };
  fs.writeFileSync(path.join(DATA, 'plan.json'), JSON.stringify(plan, null, 1));

  console.log(`Planning gameweeks ${gws[0]}-${gws[gws.length - 1]} (${gws.length}), `
    + `beam ${BEAM}, starting with ${free0} free transfer${free0 > 1 ? 's' : ''}, £${bank0.toFixed(1)}m banked\n`);
  console.log(`  Hold everything: ${rollPts.toFixed(1)} pts`);
  console.log(`  Best plan:       ${best.pts.toFixed(1)} pts  (+${(best.pts - rollPts).toFixed(1)}, ${best.hits} hit${best.hits === 1 ? '' : 's'})\n`);
  for (const w of plan.best.weeks) {
    const t = w.transfers.length
      ? w.transfers.map(x => `${x.out.name} -> ${x.in.name}`).join(', ') + (w.hit ? `  (-${w.hit})` : '')
      : 'roll the transfer';
    console.log(`  GW${String(w.gw).padEnd(2)}  ${w.pts.toFixed(1).padStart(5)} pts  C: ${w.captain.name.padEnd(14)} ${t}`);
  }
  if (plan.alternatives.length) {
    console.log('\n  Other openings, and what choosing them costs over the horizon:');
    plan.alternatives.forEach(a => {
      const label = a.opening.map(o => `${o.out.name} -> ${o.in.name}`).join(', ') || 'roll the transfer';
      console.log(`    ${label.padEnd(46)} ${a.points.toFixed(1)} pts  (-${a.cost.toFixed(1)} vs best)`);
    });
  }
}

if (require.main === module) main();
module.exports = { main };
