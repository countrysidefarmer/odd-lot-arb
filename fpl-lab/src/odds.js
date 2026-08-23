'use strict';
// Prediction-market odds from Polymarket, converted into expected goals.
//
// Polymarket publishes 1X2 markets per Premier League fixture on a public,
// unauthenticated API, and tags each with a gameStartTime that matches the FPL
// kickoff exactly — so fixtures join reliably. Market prices beat any model
// built on a handful of games, but coverage runs only a gameweek or two ahead,
// so these are used where they exist and the model's own estimate elsewhere.
const fs = require('fs');
const path = require('path');
const { poissonP } = require('./lib');

const DATA = path.join(__dirname, '..', 'data');
const GAMMA = 'https://gamma-api.polymarket.com/events';

async function fetchEvents() {
  const out = [];
  for (let offset = 0; offset < 600; offset += 100) {
    const res = await fetch(`${GAMMA}?closed=false&limit=100&offset=${offset}&tag_slug=epl`,
      { headers: { 'User-Agent': 'fpl-lab/1.0' } });
    if (!res.ok) throw new Error(`Polymarket ${res.status}`);
    const page = await res.json();
    out.push(...page);
    if (page.length < 100) break;
  }
  return out;
}

// FPL team names are abbreviated ("Spurs", "Nott'm Forest") where Polymarket
// spells them out. Match on a distinctive keyword rather than fuzzy substrings:
// three-letter codes produce false positives ("ma-nche-ster" contains "che").
const SYNONYM = {
  tottenham: 'spurs', hotspur: 'spurs', nottingham: "nott'm", wolverhampton: 'wolves',
  'manchester city': 'man city', 'manchester united': 'man utd', 'manchester utd': 'man utd',
  bournemouth: 'bournemouth', brighton: 'brighton', newcastle: 'newcastle',
  'west ham': 'west ham', 'sheffield united': 'sheffield utd',
};
const clean = s => String(s).toLowerCase().replace(/[^a-z ]/g, '').replace(/\s+/g, ' ').trim();
const GENERIC = new Set(['fc', 'afc', 'city', 'united', 'utd', 'town', 'albion', 'hove', 'and', 'the']);

function matchTeam(pmName, teams) {
  const n = clean(pmName);
  // Longest synonym first, so "manchester city" beats "manchester".
  for (const key of Object.keys(SYNONYM).sort((a, b) => b.length - a.length)) {
    if (n.includes(key)) {
      const target = clean(SYNONYM[key]);
      const hit = teams.find(t => clean(t.name).includes(target) || target.includes(clean(t.name)));
      if (hit) return hit;
    }
  }
  // Otherwise match on the most distinctive word the two names share.
  let best = null;
  for (const t of teams) {
    for (const word of clean(t.name).split(' ')) {
      if (word.length < 4 || GENERIC.has(word)) continue;
      if (n.includes(word) && (!best || word.length > best.len)) best = { t, len: word.length };
    }
  }
  return best ? best.t : null;
}

const price = m => {
  try {
    const p = JSON.parse(m.outcomePrices);
    const outs = JSON.parse(m.outcomes);
    const i = outs.findIndex(o => /^yes$/i.test(o));
    return i >= 0 ? parseFloat(p[i]) : null;
  } catch (e) { return null; }
};

// Find the pair of Poisson means whose implied 1X2 best matches the market.
function solveGoals(pHome, pDraw, pAway) {
  const outcome = (lh, la) => {
    const MAX = 9, ph = [], pa = [];
    for (let k = 0; k <= MAX; k++) { ph.push(poissonP(k, lh)); pa.push(poissonP(k, la)); }
    let h = 0, d = 0, a = 0;
    for (let i = 0; i <= MAX; i++) for (let j = 0; j <= MAX; j++) {
      const q = ph[i] * pa[j];
      if (i > j) h += q; else if (i === j) d += q; else a += q;
    }
    return [h, d, a];
  };
  const err = (lh, la) => {
    const [h, d, a] = outcome(lh, la);
    return (h - pHome) ** 2 + (d - pDraw) ** 2 + (a - pAway) ** 2;
  };
  let best = { lh: 1.4, la: 1.2, e: Infinity };
  for (let lh = 0.2; lh <= 4.0; lh += 0.1) {
    for (let la = 0.2; la <= 4.0; la += 0.1) {
      const e = err(lh, la);
      if (e < best.e) best = { lh, la, e };
    }
  }
  // Refine around the grid winner.
  for (let lh = best.lh - 0.1; lh <= best.lh + 0.1; lh += 0.01) {
    for (let la = best.la - 0.1; la <= best.la + 0.1; la += 0.01) {
      if (lh <= 0 || la <= 0) continue;
      const e = err(lh, la);
      if (e < best.e) best = { lh, la, e };
    }
  }
  return { xGF: +best.lh.toFixed(3), xGA: +best.la.toFixed(3), fit: Math.sqrt(best.e) };
}

async function main() {
  const boot = JSON.parse(fs.readFileSync(path.join(DATA, 'bootstrap.json'), 'utf8'));
  const fixtures = JSON.parse(fs.readFileSync(path.join(DATA, 'fixtures.json'), 'utf8'));
  const events = await fetchEvents();

  const games = events.filter(e => / vs\. /.test(e.title || '') && !/ - /.test(e.title || ''));
  console.log(`Polymarket: ${events.length} EPL events, ${games.length} match markets`);

  const out = {};
  let joined = 0, skipped = 0;
  for (const g of games) {
    const [hName, aName] = g.title.split(/ vs\. /);
    const home = matchTeam(hName, boot.teams);
    const away = matchTeam(aName, boot.teams);
    if (!home || !away) { skipped++; continue; }

    const ms = g.markets || [];
    const draw = ms.find(m => /end in a draw/i.test(m.question));
    const hWin = ms.find(m => new RegExp(`^Will ${escapeRe(hName)} win`, 'i').test(m.question));
    const aWin = ms.find(m => new RegExp(`^Will ${escapeRe(aName)} win`, 'i').test(m.question));
    if (!draw || !hWin || !aWin) { skipped++; continue; }
    const ph = price(hWin), pd = price(draw), pa = price(aWin);
    if (ph == null || pd == null || pa == null) { skipped++; continue; }

    // Prediction-market prices carry a small overround; normalise to probabilities.
    const total = ph + pd + pa;
    if (!(total > 0.8 && total < 1.3)) { skipped++; continue; }

    // Join to the FPL fixture on kickoff time plus the team pair.
    const kick = (hWin.gameStartTime || '').slice(0, 16).replace(' ', 'T');
    const fx = fixtures.find(f => f.team_h === home.id && f.team_a === away.id
      && (!kick || f.kickoff_time.slice(0, 16) === kick));
    if (!fx) { skipped++; continue; }

    const solved = solveGoals(ph / total, pd / total, pa / total);
    out[fx.id] = {
      gw: fx.event, home: home.short_name, away: away.short_name,
      pHome: +(ph / total).toFixed(3), pDraw: +(pd / total).toFixed(3), pAway: +(pa / total).toFixed(3),
      xGF: solved.xGF, xGA: solved.xGA, fit: +solved.fit.toFixed(4),
      liquidity: Math.round(g.liquidity || 0), volume: Math.round(g.volume || 0),
      kickoff: fx.kickoff_time,
    };
    joined++;
  }

  fs.writeFileSync(path.join(DATA, 'odds.json'),
    JSON.stringify({ fetched: new Date().toISOString(), source: 'polymarket', fixtures: out }, null, 1));

  const byGw = {};
  Object.values(out).forEach(o => { byGw[o.gw] = (byGw[o.gw] || 0) + 1; });
  console.log(`joined ${joined} fixtures, skipped ${skipped}`);
  console.log('coverage by gameweek:', Object.entries(byGw).map(([g, n]) => `GW${g}: ${n}/10`).join(', '));
  const sample = Object.values(out).sort((a, b) => b.liquidity - a.liquidity).slice(0, 6);
  console.log('\nsample (highest liquidity):');
  sample.forEach(o => console.log(`  GW${o.gw} ${o.home} v ${o.away}  ${(o.pHome * 100).toFixed(0)}/${(o.pDraw * 100).toFixed(0)}/${(o.pAway * 100).toFixed(0)}`
    + `  ->  xG ${o.xGF} - ${o.xGA}  (fit ${o.fit}, liq £${o.liquidity.toLocaleString()})`));
}

const escapeRe = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

main().catch(e => { console.error('odds fetch failed:', e.message); process.exit(1); });
