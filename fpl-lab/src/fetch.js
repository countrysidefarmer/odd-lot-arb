'use strict';
// Pulls everything the model needs: live FPL state + last season's per-gameweek history.
const fs = require('fs');
const path = require('path');

const DATA = path.join(__dirname, '..', 'data');
const ENTRY = process.env.FPL_ENTRY || '5529307';
const API = 'https://fantasy.premierleague.com/api';
const VAASTAV = 'https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data';
const LAST_SEASON = '2025-26';

async function get(url, asText = false) {
  const res = await fetch(url, { headers: { 'User-Agent': 'fpl-lab/1.0' } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} for ${url}`);
  return asText ? res.text() : res.json();
}

async function main() {
  fs.mkdirSync(DATA, { recursive: true });
  const boot = await get(`${API}/bootstrap-static/`);
  fs.writeFileSync(path.join(DATA, 'bootstrap.json'), JSON.stringify(boot));
  console.log(`bootstrap: ${boot.elements.length} players, ${boot.teams.length} teams`);

  const fixtures = await get(`${API}/fixtures/`);
  fs.writeFileSync(path.join(DATA, 'fixtures.json'), JSON.stringify(fixtures));
  console.log(`fixtures: ${fixtures.length}`);

  // Current gameweek: the last one with a passed deadline, else 1.
  const now = Date.now();
  const past = boot.events.filter(e => new Date(e.deadline_time).getTime() <= now);
  const currentGw = past.length ? past[past.length - 1].id : 1;

  // Picks are public only once a gameweek's deadline has passed.
  let picks = null;
  for (let gw = currentGw; gw >= 1 && !picks; gw--) {
    try {
      picks = { gw, ...(await get(`${API}/entry/${ENTRY}/event/${gw}/picks/`)) };
    } catch (e) { /* not published yet, walk back */ }
  }
  const entry = await get(`${API}/entry/${ENTRY}/`);
  fs.writeFileSync(path.join(DATA, 'entry.json'), JSON.stringify({ entry, picks, currentGw }));
  console.log(picks ? `entry: squad from GW${picks.gw}, ${picks.picks.length} picks`
                    : 'entry: no published squad yet');

  // Per-gameweek results for the recent form strip. One request covers every
  // player for a gameweek, so the last five cost five calls rather than one per
  // player. Early in a season there may only be one gameweek to show.
  const playedGws = boot.events
    .filter(e => e.finished || e.is_current || new Date(e.deadline_time).getTime() <= now)
    .map(e => e.id)
    .sort((a, b) => a - b)
    .slice(-5);
  const recent = {};
  for (const gw of playedGws) {
    try {
      const live = await get(`${API}/event/${gw}/live/`);
      recent[gw] = Object.fromEntries(live.elements
        .filter(e => e.stats.minutes > 0 || e.stats.total_points !== 0)
        .map(e => [e.id, {
          p: e.stats.total_points, m: e.stats.minutes, b: e.stats.bonus,
          bps: e.stats.bps, s: e.stats.starts, g: e.stats.goals_scored,
          a: e.stats.assists, cs: e.stats.clean_sheets,
          // What he actually generated, so a past gameweek can be read on the
          // same axes as a projected one.
          xg: +e.stats.expected_goals, xa: +e.stats.expected_assists,
        }]));
    } catch (err) { console.log(`  live GW${gw}: ${err.message}`); }
  }
  // Which opponent each team faced, so the strip can label the fixtures.
  const opponents = {};
  for (const gw of playedGws) {
    const done = f => Boolean(f.finished || f.finished_provisional);
    opponents[gw] = fixtures.filter(f => f.event === gw).flatMap(f => ([
      { team: f.team_h, opp: f.team_a, home: 1, gs: f.team_h_score, ga: f.team_a_score, fin: done(f) },
      { team: f.team_a, opp: f.team_h, home: 0, gs: f.team_a_score, ga: f.team_h_score, fin: done(f) },
    ]));
  }
  // Whether the gameweek as a whole is over, for teams that had no fixture in it.
  const gwFinished = Object.fromEntries(playedGws.map(gw => {
    const e = boot.events.find(x => x.id === gw);
    return [gw, Boolean(e && e.finished)];
  }));
  fs.writeFileSync(path.join(DATA, 'recent.json'), JSON.stringify({ gws: playedGws, recent, opponents, gwFinished }));
  console.log(`recent form: gameweeks ${playedGws.join(', ') || '(none yet)'}`);

  // Stable player codes let us join last season's rows onto this season's ids.
  const raw = await get(`${VAASTAV}/${LAST_SEASON}/players_raw.csv`, true);
  fs.writeFileSync(path.join(DATA, `players_raw_${LAST_SEASON}.csv`), raw);
  console.log(`players_raw ${LAST_SEASON}: ${raw.split('\n').length - 1} rows`);

  for (const season of [LAST_SEASON, '2024-25']) {
    const f = path.join(DATA, `gw_${season}.csv`);
    if (fs.existsSync(f)) { console.log(`gw_${season}.csv: cached`); continue; }
    fs.writeFileSync(f, await get(`${VAASTAV}/${season}/gws/merged_gw.csv`, true));
    console.log(`gw_${season}.csv: downloaded`);
  }
  fs.writeFileSync(path.join(DATA, 'meta.json'),
    JSON.stringify({ fetched: new Date().toISOString(), currentGw, entry: ENTRY, lastSeason: LAST_SEASON }));
}

main().catch(e => { console.error('fetch failed:', e.message); process.exit(1); });
