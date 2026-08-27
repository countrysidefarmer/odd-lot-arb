'use strict';
// Squad competition for minutes.
//
// Two versions of "who else plays here" were tested (see the notes in README).
// As a *historical* feature it is worthless: adding competitors-ahead, squad
// depth and share-of-positional-minutes to the recency features moves minutes
// R^2 by +0.02pp on 2025-26 and +0.02pp on 2024-25. A player's own recent
// minutes already encode who he is competing with, so the depth chart tells you
// nothing new after the fact.
//
// As a *live availability constraint* it is a different quantity entirely, and
// the backtest cannot see it: last season's files record minutes, not injuries,
// so "his rival is out this week" simply is not in the historical data. It is in
// today's data, and right now it is large — Spurs have five defenders and five
// midfielders flagged, Brighton six midfielders.
//
// The constraint that makes this usable: the number of players a team starts in
// each position is nearly deterministic. Over 2025-26, per team per match:
//   GK 1.00 (sd 0.00)   DEF 4.19 (sd 0.60)   MID 4.71 (sd 0.83)   FWD 1.10 (sd 0.54)
// So start probabilities inside a team-position must add up to the slot count.
// The raw per-player start rates do not, and cannot react when a starter is
// ruled out. Normalising them to the slot count does exactly that: it lifts the
// deputies by however much the absentees vacated, and no further.
const { num, sum, mean } = require('./lib');

// Measured from last season's starting elevens, with a sane fallback.
const DEFAULT_SLOTS = { 1: 1.0, 2: 4.19, 3: 4.71, 4: 1.10 };

function slotsFromHistory(rows) {
  const byGroup = new Map();
  for (const r of rows) {
    const pos = { GK: 1, GKP: 1, DEF: 2, MID: 3, FWD: 4 }[r.position];
    if (!pos) continue;
    const k = `${r.team}|${r.fixture}|${pos}`;
    byGroup.set(k, (byGroup.get(k) || 0) + (num(r.starts) > 0 ? 1 : 0));
  }
  const acc = { 1: [], 2: [], 3: [], 4: [] };
  for (const [k, v] of byGroup) acc[k.split('|')[2]].push(v);
  const out = {};
  for (const pos of [1, 2, 3, 4]) {
    out[pos] = acc[pos].length >= 100 ? mean(acc[pos]) : DEFAULT_SLOTS[pos];
  }
  return out;
}

// Rescale start probabilities within each team-position so they sum to the
// number of players the team actually starts there.
//
// Scaling is multiplicative on the odds rather than the probability, so a
// nailed-on starter at 0.95 cannot be pushed past 1 and a fringe player is not
// dragged to a certainty by one absence. `cap` keeps any single player from
// being asserted as a guaranteed starter on the strength of an arithmetic
// identity — squad news is never that clean.
function allocate(group, slots, { cap = 0.97, maxLift = 40 } = {}) {
  const live = group.filter(p => p.avail > 0.02 && p.raw > 0);
  if (!live.length) return;
  const total = sum(live.map(p => p.raw));
  if (total <= 1e-6) return;

  // Only the starts VACATED by unavailable players are redistributed — the
  // group is not simply rescaled to its slot count. This matters: forcing a
  // fully fit squad onto the slot count makes minutes prediction worse
  // (MAE 19.18 -> 19.52 on 2025-26, 19.49 -> 19.87 on 2024-25), because with
  // nobody injured the constraint adds no information and only moves noise
  // around. Written this way the adjustment is an exact no-op when everyone is
  // available, and does something only when there is news to react to.
  const vacated = sum(group.map(p => Math.max(0, (p.raw0 ?? p.raw) - p.raw)));
  if (vacated <= 1e-6) return;
  const target = Math.min(Math.min(slots, live.length), total + vacated);

  // Solve for the odds multiplier k that makes the probabilities sum to target.
  const f = k => sum(live.map(p => {
    const o = (p.raw / (1 - Math.min(0.999, p.raw))) * k;
    return Math.min(cap, o / (1 + o));
  }));
  const at = k => { for (const p of live) { const o = (p.raw / (1 - Math.min(0.999, p.raw))) * k; p.adj = Math.min(cap, o / (1 + o)); } };
  let lo = 1 / maxLift, hi = maxLift;
  // A position stripped of everyone cannot reach its slot count once each
  // survivor is capped, so it settles below it rather than inventing a starter.
  if (f(hi) < target) { at(hi); return; }
  if (f(lo) > target) { at(lo); return; }
  for (let i = 0; i < 40; i++) {
    const mid = Math.sqrt(lo * hi);
    if (f(mid) > target) hi = mid; else lo = mid;
  }
  const k = Math.sqrt(lo * hi);
  for (const p of live) {
    const o = (p.raw / (1 - Math.min(0.999, p.raw))) * k;
    p.adj = Math.min(cap, o / (1 + o));
  }
}

// players: [{ id, team, pos, raw, avail, name }] where `raw` is the
// availability-adjusted start rate the per-player model produced.
// Returns a Map id -> { pStart, lift, competing, absent } and a per-team-position
// depth chart for the dashboard.
function applyDepth(players, slots = DEFAULT_SLOTS) {
  const groups = new Map();
  for (const p of players) {
    const k = `${p.team}|${p.pos}`;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push({ ...p, adj: p.raw });
  }
  const out = new Map();
  const charts = [];
  for (const [k, group] of groups) {
    const [team, pos] = k.split('|').map(Number);
    allocate(group, slots[pos] || DEFAULT_SLOTS[pos]);
    const absent = group.filter(p => p.avail <= 0.5 && p.raw0 >= 0.35);
    for (const p of group) {
      out.set(p.id, {
        pStart: +(p.adj ?? p.raw).toFixed(4),
        lift: +(((p.adj ?? p.raw) - p.raw)).toFixed(4),
        competing: group.length,
        absent: absent.length,
      });
    }
    charts.push({
      team, pos,
      slots: +(slots[pos] || DEFAULT_SLOTS[pos]).toFixed(2),
      absent: absent.map(p => p.name),
      order: group.slice().sort((a, b) => (b.adj ?? b.raw) - (a.adj ?? a.raw))
        .slice(0, 6)
        .map(p => ({ id: p.id, name: p.name, p: +(p.adj ?? p.raw).toFixed(2), lift: +((p.adj ?? p.raw) - p.raw).toFixed(2) })),
    });
  }
  return { adjusted: out, charts };
}

module.exports = { applyDepth, slotsFromHistory, allocate, DEFAULT_SLOTS };
