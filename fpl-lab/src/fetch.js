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
