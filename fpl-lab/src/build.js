'use strict';
// Renders the dashboard: trims the model output to a compact payload and
// injects it into the page template.
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const DATA = path.join(ROOT, 'data');
const read = f => JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'));

function main() {
  const proj = read('projections.json');
  const research = read('research.json');
  const signals = read('signals.json');
  let plan = null;
  try { plan = read('plan.json'); } catch (e) { /* optional */ }
  const entryFile = read('entry.json');
  const boot = read('bootstrap.json');
  const meta = read('meta.json');

  const teamNames = {};
  boot.teams.forEach(t => { teamNames[t.id] = { short: t.short_name, name: t.name }; });

  // Compact keys keep the embedded payload small enough to stay snappy.
  const players = proj.players.map(p => ({
    i: p.id, n: p.name, t: p.team, ts: p.teamShort, po: p.pos, c: p.cost,
    o: p.owned, st: p.status, nw: p.news, ch: p.chance,
    ps: p.pStart, em: p.expMins, xgi: p.xgi90, b90: p.bonus90, dc: p.defconRate,
    lp: p.lastSeasonPoints, lm: p.lastSeasonMins, tp: p.totalPoints,
    pen: p.penOrder, sp: p.setPiece, rn: p.roleNotes, ovr: p.overridden,
    x1: p.xp1, x3: p.xp3, x5: p.xp5, x10: p.xp10,
    sw3: p.swing3, sw5: p.swing5, sw10: p.swing10,
    // Full detail only on the fixtures the player card actually shows; the rest
    // just need enough for the fixture strip.
    f: p.fixtures.slice(0, 10).map((f, i) => {
      const light = { g: f.gw, o: f.opp, h: f.home ? 1 : 0, a: f.xGA, s: f.xGF, x: f.xp, od: f.odds };
      if (i >= 6) return light;
      return { ...light, cs: f.cs, pr: f.probs, eg: f.expGoals, ea: f.expAssists, m: f.mins,
        pt: Object.fromEntries(Object.entries(f.parts).filter(([, v]) => Math.abs(v) > 0.004)) };
    }),
  }));

  const squad = entryFile.picks ? entryFile.picks.picks.map(pk => ({
    id: pk.element, pos: pk.position, cap: pk.is_captain, vice: pk.is_vice_captain,
  })) : [];

  const payload = {
    generated: new Date().toISOString(),
    fetched: meta.fetched,
    nextGw: proj.nextGw,
    deadlineGw: proj.deadlineGw,
    deadline: proj.deadline,
    lastSeason: meta.lastSeason,
    entry: {
      id: entryFile.entry.id,
      name: entryFile.entry.name,
      manager: `${entryFile.entry.player_first_name} ${entryFile.entry.player_last_name}`,
      bank: entryFile.picks ? entryFile.picks.entry_history.bank / 10 : 0,
      value: entryFile.picks ? entryFile.picks.entry_history.value / 10 : 100,
      squadGw: entryFile.picks ? entryFile.picks.gw : null,
      overall: entryFile.entry.summary_overall_points,
    },
    squad,
    players,
    ticker: proj.ticker,
    oddsCoverage: proj.players[0] ? proj.players[0].fixtures.filter(f => f.odds).length : 0,
    teams: teamNames,
    research,
    signals,
    plan,
  };

  const tpl = fs.readFileSync(path.join(__dirname, 'template.html'), 'utf8');
  const html = tpl.replace('"__DATA__"', () => JSON.stringify(payload));
  const outDir = path.join(ROOT, 'site');
  fs.mkdirSync(outDir, { recursive: true });
  const out = path.join(outDir, 'index.html');
  fs.writeFileSync(out, html);
  console.log(`built ${out} — ${(html.length / 1024 / 1024).toFixed(2)} MB, ${players.length} players, squad of ${squad.length}`);
}
main();
