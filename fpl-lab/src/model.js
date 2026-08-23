'use strict';
// Expected-points model. Every player, every remaining fixture.
//
// Shape of the calculation, per player per fixture:
//   expected minutes  x  per-90 rates (shrunk toward a price-based positional prior)
//   x  a fixture multiplier derived from the opponent's attack/defence strength
//   -> goals, assists, clean sheet, defensive contribution, saves, bonus, cards
//   -> points, via the FPL scoring table for that position.
const fs = require('fs');
const path = require('path');
const { parseCSV, num, sum, mean, poissonP } = require('./lib');

const DATA = path.join(__dirname, '..', 'data');
const read = f => JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'));

const LEAGUE_GPG = 1.45;        // goals per team per game
const HOME_BOOST = 1.12, AWAY_BOOST = 0.89;
const PRIOR_GAMES = 6;          // strength of the positional prior, in games
const TEAM_PRIOR_GAMES = 5;
const DEFCON_THRESHOLD = { 1: 99, 2: 10, 3: 12, 4: 12 };

// FPL only sets finished=true once bonus points are confirmed, which can be a day
// after the whistle. finished_provisional flips as soon as the match ends, so a
// played match must be treated as played or it gets projected all over again.
// Match outcome probabilities from two independent Poissons on the expected goals.
function matchProbs(xGF, xGA) {
  const MAX = 9;
  const pf = [], pa = [];
  for (let k = 0; k <= MAX; k++) { pf.push(poissonP(k, xGF)); pa.push(poissonP(k, xGA)); }
  let win = 0, draw = 0, loss = 0, score2 = 0;
  for (let i = 0; i <= MAX; i++) {
    for (let j = 0; j <= MAX; j++) {
      const q = pf[i] * pa[j];
      if (i > j) win += q; else if (i === j) draw += q; else loss += q;
    }
    if (i >= 2) score2 += pf[i];
  }
  return {
    win: +win.toFixed(3), draw: +draw.toFixed(3), loss: +loss.toFixed(3),
    cs: +pa[0].toFixed(3), score2: +score2.toFixed(3),
  };
}

const isPlayed = f => Boolean(f.finished || f.finished_provisional
  || (f.started && f.minutes >= 90 && f.team_h_score != null));

const GOAL_PTS = { 1: 10, 2: 6, 3: 5, 4: 4 };
const CS_PTS = { 1: 4, 2: 4, 3: 1, 4: 0 };

// ---------------------------------------------------------------- last season
function lastSeasonBaselines(lastSeason) {
  const rows = parseCSV(fs.readFileSync(path.join(DATA, `gw_${lastSeason}.csv`), 'utf8'));
  const raw = parseCSV(fs.readFileSync(path.join(DATA, `players_raw_${lastSeason}.csv`), 'utf8'));
  const idToCode = new Map(raw.map(r => [String(r.id), String(r.code)]));
  // Last season's set-piece duty, so this season's can be compared against it.
  const ord = v => (v && v !== 'None' && v !== '' ? num(v) : 0);
  const lastDuty = new Map(raw.map(r => [String(r.code), {
    pen: ord(r.penalties_order),
    ck: ord(r.corners_and_indirect_freekicks_order),
    fk: ord(r.direct_freekicks_order),
    team: r.team_code,
  }]));

  const byCode = new Map();
  for (const r of rows) {
    const code = idToCode.get(String(r.element));
    if (!code) continue;
    if (!byCode.has(code)) byCode.set(code, []);
    byCode.get(code).push(r);
  }
  const out = new Map();
  for (const [code, gws] of byCode) {
    const mins = sum(gws.map(g => num(g.minutes)));
    const games = gws.length;
    const starts = gws.filter(g => num(g.starts) > 0).length;
    const startGames = gws.filter(g => num(g.starts) > 0);
    // Start rate over the run-in as well as the whole season: an injury in
    // August should not permanently mark a player down as a rotation risk.
    const sorted = gws.slice().sort((a, b) => num(a.GW) - num(b.GW));
    const tail = sorted.slice(-12);
    const tail15 = sorted.slice(-15);
    const startRateRecent = tail.length ? tail.filter(g => num(g.starts) > 0).length / tail.length : 0;
    const per90 = v => (mins >= 90 ? (v * 90) / mins : 0);
    const pos = { GK: 1, GKP: 1, DEF: 2, MID: 3, FWD: 4 }[gws[0].position] || 3;
    const thr = DEFCON_THRESHOLD[pos];
    out.set(code, {
      pos, mins, games, starts,
      startRate: games ? Math.max(starts / games, 0.5 * (starts / games) + 0.5 * startRateRecent) : 0,
      startRateRecent,
      minsPerStart: startGames.length ? mean(startGames.map(g => num(g.minutes))) : 0,
      // The distribution matters more than the average: appearance points turn on
      // P(plays) and P(60+), which are different questions from "how many minutes".
      pPlay15: tail15.length ? tail15.filter(g => num(g.minutes) > 0).length / tail15.length : 0,
      p60_15: tail15.length ? tail15.filter(g => num(g.minutes) >= 60).length / tail15.length : 0,
      mins15: tail15.length ? mean(tail15.map(g => num(g.minutes))) : 0,
      n15: tail15.length,
      xg90: per90(sum(gws.map(g => num(g.expected_goals)))),
      xa90: per90(sum(gws.map(g => num(g.expected_assists)))),
      goals90: per90(sum(gws.map(g => num(g.goals_scored)))),
      assists90: per90(sum(gws.map(g => num(g.assists)))),
      bonus90: per90(sum(gws.map(g => num(g.bonus)))),
      saves90: per90(sum(gws.map(g => num(g.saves)))),
      yellow90: per90(sum(gws.map(g => num(g.yellow_cards)))),
      pts90: per90(sum(gws.map(g => num(g.total_points)))),
      // Empirical rate of clearing the defensive-contribution threshold when starting.
      defconRate: startGames.length
        ? startGames.filter(g => num(g.defensive_contribution) >= thr).length / startGames.length
        : 0,
      defconN: startGames.length,
      totalPoints: sum(gws.map(g => num(g.total_points))),
    });
  }
  return { baselines: out, rows, lastDuty };
}

// Team attack/defence, relative to a league-average team (1.0 = average).
function teamStrengths(rows, boot, fixtures) {
  const ids = new Map();
  const byFix = new Map();
  for (const r of rows) {
    if (!byFix.has(r.fixture)) byFix.set(r.fixture, new Map());
    byFix.get(r.fixture).set(r.team, num(r.opponent_team));
  }
  for (const sides of byFix.values()) {
    const e = [...sides.entries()];
    if (e.length === 2) { ids.set(e[1][0], e[0][1]); ids.set(e[0][0], e[1][1]); }
  }
  // Last season's goals for / against, per team name.
  const seen = new Set();
  const rec = new Map();
  for (const r of rows) {
    const key = r.fixture + '|' + r.team;
    if (seen.has(key)) continue;
    seen.add(key);
    const home = r.was_home === 'True';
    const gf = home ? num(r.team_h_score) : num(r.team_a_score);
    const ga = home ? num(r.team_a_score) : num(r.team_h_score);
    if (!rec.has(r.team)) rec.set(r.team, { gf: 0, ga: 0, n: 0 });
    const t = rec.get(r.team);
    t.gf += gf; t.ga += ga; t.n++;
  }

  // Current season, from results so far.
  const cur = new Map(boot.teams.map(t => [t.id, { gf: 0, ga: 0, n: 0 }]));
  for (const f of fixtures) {
    if (!isPlayed(f) || f.team_h_score == null) continue;
    const h = cur.get(f.team_h), a = cur.get(f.team_a);
    h.gf += f.team_h_score; h.ga += f.team_a_score; h.n++;
    a.gf += f.team_a_score; a.ga += f.team_h_score; a.n++;
  }

  const out = new Map();
  for (const t of boot.teams) {
    // Match last season's record by short name, then full name.
    let prior = null;
    for (const [name, r] of rec) {
      if (name === t.name || name === t.short_name) { prior = r; break; }
    }
    const c = cur.get(t.id);
    // Promoted sides have no top-flight record: treat them as clearly below average.
    const pAtt = prior && prior.n ? prior.gf / prior.n : LEAGUE_GPG * 0.78;
    const pDef = prior && prior.n ? prior.ga / prior.n : LEAGUE_GPG * 1.22;
    const w = c.n / (c.n + TEAM_PRIOR_GAMES);
    const att = w * (c.n ? c.gf / c.n : pAtt) + (1 - w) * pAtt;
    const def = w * (c.n ? c.ga / c.n : pDef) + (1 - w) * pDef;
    out.set(t.id, {
      name: t.name, short: t.short_name,
      attack: att / LEAGUE_GPG,
      defence: def / LEAGUE_GPG,
      promoted: !(prior && prior.n),
      played: c.n,
    });
  }
  return out;
}

// Price-based positional prior, fitted on last season, so new signings and
// promoted players are not projected at zero.
function positionalPriors(baselines, boot) {
  const byCode = new Map(boot.elements.map(e => [String(e.code), e]));
  const buckets = new Map();
  for (const [code, b] of baselines) {
    const el = byCode.get(code);
    if (!el || b.mins < 450) continue;
    const key = `${b.pos}|${Math.min(4, Math.floor((el.now_cost / 10 - 3.8) / 1.6))}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(b);
  }
  const fields = ['xg90', 'xa90', 'goals90', 'assists90', 'bonus90', 'saves90', 'yellow90', 'pts90', 'defconRate'];
  const priors = new Map();
  for (const [key, list] of buckets) {
    const p = {};
    for (const f of fields) p[f] = mean(list.map(b => b[f]));
    p.startRate = mean(list.map(b => b.startRate));
    p.minsPerStart = mean(list.map(b => b.minsPerStart));
    priors.set(key, p);
  }
  // Fallback per position, if a price bucket is empty.
  const posFallback = new Map();
  for (const pos of [1, 2, 3, 4]) {
    const list = [...baselines.values()].filter(b => b.pos === pos && b.mins >= 450);
    const p = {};
    for (const f of fields) p[f] = mean(list.map(b => b[f]));
    p.startRate = 0.5; p.minsPerStart = 80;
    posFallback.set(pos, p);
  }
  return (pos, cost) => {
    const key = `${pos}|${Math.min(4, Math.floor((cost / 10 - 3.8) / 1.6))}`;
    return priors.get(key) || posFallback.get(pos);
  };
}

// Expected minutes for a player with no Premier League record — promoted squads,
// overseas signings, academy graduates. Price is the only real signal, and
// src/preseason.js measures how it lands: these are the observed GW1-6 averages.
function blindMinutes(cost, hasSetPiece) {
  const c = cost / 10;
  const base = c >= 8 ? 71 : c >= 6.5 ? 34 : c >= 5.5 ? 34 : c >= 4.5 ? 20 : 12;
  // Set-piece duty on an unknown player is a strong tell that he is in the plans.
  return hasSetPiece ? Math.max(base, 53) : base;
}

// A designated penalty taker is worth roughly 0.10 expected goals per 90 from the
// spot: about 0.13 penalties per game for the taker at ~0.79 xG each. Historical
// xG already contains this, so it is only applied as a delta when duty CHANGES.
const PEN_XG90 = 0.10;

// ------------------------------------------------------------------ the model
function project() {
  const boot = read('bootstrap.json');
  const fixtures = read('fixtures.json');
  const meta = read('meta.json');
  const { baselines, rows, lastDuty } = lastSeasonBaselines(meta.lastSeason);
  const strengths = teamStrengths(rows, boot, fixtures);
  const priorFor = positionalPriors(baselines, boot);
  const teamName = new Map(boot.teams.map(t => [t.id, t.short_name]));

  // Hand overrides, for things no dataset can know: a fit player whose backup is
  // injured, a confirmed role change, a manager's press conference.
  // Prediction-market expected goals, where the market covers the fixture.
  let odds = { fixtures: {} };
  try { odds = read('odds.json'); } catch (e) { /* optional */ }

  let overrides = {};
  try { overrides = read('overrides.json'); } catch (e) { /* optional file */ }

  const upcoming = fixtures
    .filter(f => !isPlayed(f) && f.event != null)
    .sort((a, b) => a.event - b.event);
  const nextGw = upcoming.length ? upcoming[0].event : 38;
  const now = Date.now();
  const nextEvent = boot.events.find(e => new Date(e.deadline_time).getTime() > now);
  const deadlineGw = nextEvent ? nextEvent.id : nextGw;
  const deadline = nextEvent ? nextEvent.deadline_time : null;

  const players = [];
  for (const el of boot.elements) {
    const pos = el.element_type;
    const cost = el.now_cost;
    const prior = priorFor(pos, cost);
    const base = baselines.get(String(el.code));
    const ov = overrides[el.web_name] || overrides[String(el.id)] || null;

    // Current-season aggregates carry more weight as the season goes on.
    const curMins = el.minutes;
    const curGames = Math.max(0, strengths.get(el.team).played);
    const per90cur = v => (curMins >= 90 ? (v * 90) / curMins : 0);

    // Blend: last season -> shrunk toward the price prior, then blended with this season.
    const histW = base ? base.mins / (base.mins + PRIOR_GAMES * 90) : 0;
    const blendHist = f => (base ? histW * base[f] + (1 - histW) * prior[f] : prior[f]);
    const curW = curMins / (curMins + PRIOR_GAMES * 90);
    const rate = (f, curVal) => (1 - curW) * blendHist(f) + curW * per90cur(curVal);

    const xg90 = rate('xg90', num(el.expected_goals));
    const xa90 = rate('xa90', num(el.expected_assists));
    // Finishing: lean on xG but give real goals some weight.
    let g90 = 0.7 * xg90 + 0.3 * rate('goals90', el.goals_scored);
    let a90 = 0.7 * xa90 + 0.3 * rate('assists90', el.assists);
    const bonus90 = rate('bonus90', el.bonus);
    const saves90 = rate('saves90', el.saves);
    const yellow90 = rate('yellow90', el.yellow_cards);

    // Defensive contribution: empirical hit rate, shrunk by sample size.
    const dcThr = DEFCON_THRESHOLD[pos];
    const dcN = base ? base.defconN : 0;
    const dcW = dcN / (dcN + 8);
    let defconRate = dcW * (base ? base.defconRate : 0) + (1 - dcW) * prior.defconRate;
    if (curGames >= 3 && curMins >= 180) {
      const curRate = num(el.defensive_contribution) / Math.max(1, curMins / 90) / dcThr;
      defconRate = 0.6 * defconRate + 0.4 * Math.min(1, curRate);
    }

    // Availability. status: a=available, d=doubtful, i=injured, s=suspended,
    // u=unavailable, n=on loan / not in squad.
    const chance = el.chance_of_playing_next_round;
    let avail = 1;
    if (chance != null) avail = chance / 100;
    else if (el.status !== 'a') avail = 0;
    if (el.status === 'u' || el.status === 'n') avail = 0;

    // ---- minutes -----------------------------------------------------------
    // Backtesting (src/minutes.js) says recency beats season averages by a
    // distance: minutes in the last game correlate 0.67 with the next, a
    // season-long start rate only 0.48. So the run-in is weighted over the season,
    // and this season's evidence takes over quickly once it exists.
    const duty = {
      pen: el.penalties_order || 0,
      ck: el.corners_and_indirect_freekicks_order || 0,
      fk: el.direct_freekicks_order || 0,
    };
    const hasSetPiece = (duty.ck && duty.ck <= 2) || (duty.fk && duty.fk <= 2);
    const prev = lastDuty.get(String(el.code)) || { pen: 0, ck: 0, fk: 0 };

    let startRate;
    if (base && base.games >= 5) {
      const hist = 0.35 * base.startRate + 0.65 * base.startRateRecent;
      startRate = (1 - curW) * hist + curW * (el.starts / Math.max(1, curGames));
    } else if (curGames >= 3) {
      startRate = el.starts / Math.max(1, curGames);
    } else {
      // No Premier League record: price and set-piece duty are all there is.
      startRate = blindMinutes(cost, hasSetPiece) / 90;
    }

    // ---- role changes ------------------------------------------------------
    // Historical xG already contains the penalties a player was taking then, so
    // duty is only worth a correction where it has actually changed hands.
    const roleNotes = [];
    const gainedPens = duty.pen === 1 && prev.pen !== 1;
    const lostPens = prev.pen === 1 && duty.pen !== 1;
    if (base && base.mins > 0) {
      if (gainedPens) { g90 += PEN_XG90; roleNotes.push('New penalty taker'); }
      if (lostPens) { g90 = Math.max(0, g90 - PEN_XG90); roleNotes.push('Lost penalties'); }
      const gainedSp = hasSetPiece && !((prev.ck && prev.ck <= 2) || (prev.fk && prev.fk <= 2));
      if (gainedSp) { a90 += 0.04; roleNotes.push('New set-piece taker'); }
    } else if (duty.pen === 1) {
      // No history to contain it, so the prior needs the penalty share added.
      g90 += PEN_XG90;
      roleNotes.push('Penalty taker');
    }
    if (el.team !== 0 && prev.team && String(prev.team) !== String(el.team_code)) roleNotes.push('Changed club');
    if (!base || base.mins < 90) roleNotes.push('No Premier League record');
    if (ov && ov.note) roleNotes.push(ov.note);

    const minsPerStart = base && base.starts >= 3 ? base.minsPerStart : (prior.minsPerStart || 80);
    // Empirical play/60+ rates from the last 15 games, blended with this season's,
    // falling back to the start rate where there is no usable history.
    const haveDist = base && base.n15 >= 8 && !(ov && ov.pStart != null);
    const curGamesPlayed = Math.max(1, curGames);
    const distPlay = haveDist
      ? (1 - curW) * base.pPlay15 + curW * Math.min(1, (el.starts + (curMins > 0 ? 1 : 0)) / curGamesPlayed)
      : null;
    const dist60 = haveDist
      ? (1 - curW) * base.p60_15 + curW * Math.min(1, el.starts / curGamesPlayed)
      : null;
    let pStart = Math.max(0, Math.min(1, startRate)) * avail;
    if (ov && ov.pStart != null) pStart = Math.max(0, Math.min(1, ov.pStart));
    const expMins = pStart * Math.max(60, minsPerStart) + (1 - pStart) * avail * 12;
    const p60 = haveDist
      ? Math.min(1, dist60 * avail)
      : pStart * (minsPerStart >= 70 ? 0.88 : 0.55);
    const pPlay = haveDist
      ? Math.min(1, Math.max(p60, distPlay * avail))
      : Math.min(1, pStart + (1 - pStart) * avail * 0.35);

    const mine = strengths.get(el.team);
    const teamRow = { id: el.team, ...mine };

    const perFixture = [];
    for (const f of upcoming) {
      if (f.team_h !== el.team && f.team_a !== el.team) continue;
      const home = f.team_h === el.team;
      const oppId = home ? f.team_a : f.team_h;
      const opp = strengths.get(oppId);
      if (!opp) continue;

      // Expected goals for this match. The market's view wins where it exists:
      // it prices in team news, transfers and form far faster than a goals-based
      // model built on a handful of games.
      const NEUTRAL = process.env.NEUTRAL_FIXTURES === '1';
      const mk = NEUTRAL ? null : odds.fixtures[f.id];
      let xGF, xGA, oddsUsed = 0;
      if (mk) {
        xGF = home ? mk.xGF : mk.xGA;
        xGA = home ? mk.xGA : mk.xGF;
        oddsUsed = 1;
      } else if (NEUTRAL) {
        xGF = LEAGUE_GPG * mine.attack;
        xGA = LEAGUE_GPG * mine.defence;
      } else {
        xGF = LEAGUE_GPG * mine.attack * opp.defence * (home ? HOME_BOOST : AWAY_BOOST);
        xGA = LEAGUE_GPG * opp.attack * mine.defence * (home ? AWAY_BOOST : HOME_BOOST);
      }
      // How much easier/harder than this team's average game? Derived from the
      // expected goals actually used, so the two stay consistent.
      const teamNorm = Math.max(0.35, LEAGUE_GPG * mine.attack);
      const attMult = Math.max(0.35, Math.min(2.6, xGF / teamNorm));

      const m = expMins / 90;
      // Scored twice: once for the real fixture, once against a league-average
      // opponent. The difference is how much of the projection is the fixture
      // rather than the player, which is what a rotation decision turns on.
      const score = (gf, ga, mult) => {
        const parts = {
          appearance: pPlay * 1 + p60 * 1,
          goals: g90 * m * mult * GOAL_PTS[pos],
          assists: a90 * m * mult * 3,
          cleanSheet: poissonP(0, ga) * p60 * CS_PTS[pos],
          defcon: defconRate * pStart * 2,
          bonus: bonus90 * m * (0.7 + 0.3 * mult),
          cards: -yellow90 * m,
          conceding: (pos === 1 || pos === 2) ? -0.5 * ga * m : 0,
          saves: pos === 1 ? (saves90 * m * (ga / Math.max(0.4, LEAGUE_GPG * mine.defence))) / 3 : 0,
        };
        return { parts, pts: Object.values(parts).reduce((a, b) => a + b, 0) };
      };
      const real = score(xGF, xGA, attMult);
      const parts = real.parts;
      const pts = real.pts;
      // Neutral: this team's own quality, but an average opponent and no venue effect.
      const neutralPts = score(LEAGUE_GPG * mine.attack, LEAGUE_GPG * mine.defence, 1).pts;
      perFixture.push({
        gw: f.event,
        opp: teamName.get(oppId),
        oppId,
        home,
        fdr: home ? f.team_h_difficulty : f.team_a_difficulty,
        xGF: +xGF.toFixed(2),
        xGA: +xGA.toFixed(2),
        cs: +(poissonP(0, xGA) * p60).toFixed(3),
        xp: +Math.max(0, pts).toFixed(2),
        xpn: +Math.max(0, neutralPts).toFixed(2),
        parts: Object.fromEntries(Object.entries(parts).map(([k, v]) => [k, +v.toFixed(3)])),
        probs: matchProbs(xGF, xGA),
        odds: oddsUsed,
        expGoals: +(g90 * m * attMult).toFixed(3), expAssists: +(a90 * m * attMult).toFixed(3), mins: +expMins.toFixed(0),
      });
    }

    const horizonOf = (key, n) => {
      const gws = [...new Set(perFixture.map(f => f.gw))].sort((a, b) => a - b).slice(0, n);
      const set = new Set(gws);
      return +sum(perFixture.filter(f => set.has(f.gw)).map(f => f[key])).toFixed(2);
    };
    const horizon = n => {
      const gws = [...new Set(perFixture.map(f => f.gw))].sort((a, b) => a - b).slice(0, n);
      const set = new Set(gws);
      return +sum(perFixture.filter(f => set.has(f.gw)).map(f => f.xp)).toFixed(2);
    };

    players.push({
      id: el.id,
      code: el.code,
      name: el.web_name,
      full: `${el.first_name} ${el.second_name}`,
      pos,
      team: el.team,
      teamShort: teamName.get(el.team),
      cost: cost / 10,
      owned: +el.selected_by_percent,
      status: el.status,
      news: el.news || '',
      chance,
      form: +el.form,
      ppg: +el.points_per_game,
      totalPoints: el.total_points,
      lastSeasonPoints: base ? base.totalPoints : null,
      lastSeasonMins: base ? base.mins : 0,
      pStart: +pStart.toFixed(2),
      expMins: +expMins.toFixed(1),
      xg90: +g90.toFixed(3),
      xa90: +a90.toFixed(3),
      xgi90: +(g90 + a90).toFixed(3),
      bonus90: +bonus90.toFixed(3),
      defconRate: +defconRate.toFixed(3),
      penOrder: duty.pen || 0,
      setPiece: hasSetPiece ? 1 : 0,
      roleNotes,
      overridden: ov ? 1 : 0,
      fixtures: perFixture.slice(0, 12),
      xp1: horizon(1),
      xp3: horizon(3),
      xp5: horizon(5),
      xp10: horizon(10),
      // Points attributable to the fixture run rather than the player.
      swing3: +(horizon(3) - horizonOf('xpn', 3)).toFixed(2),
      swing5: +(horizon(5) - horizonOf('xpn', 5)).toFixed(2),
      swing10: +(horizon(10) - horizonOf('xpn', 10)).toFixed(2),
    });
  }

  for (const p of players) {
    p.value5 = +(p.xp5 / p.cost).toFixed(3);
    p.value10 = +(p.xp10 / p.cost).toFixed(3);
  }

  // Team fixture ticker: average difficulty of each team's next 10.
  const ticker = [...strengths.entries()].map(([id, s]) => {
    const fs_ = upcoming.filter(f => f.team_h === id || f.team_a === id).slice(0, 10)
      .map(f => {
        const home = f.team_h === id;
        const oppId = home ? f.team_a : f.team_h;
        const opp = strengths.get(oppId);
        return {
          gw: f.event, opp: teamName.get(oppId), home,
          fdr: home ? f.team_h_difficulty : f.team_a_difficulty,
          xGF: +(LEAGUE_GPG * s.attack * opp.defence * (home ? HOME_BOOST : AWAY_BOOST)).toFixed(2),
          xGA: +(LEAGUE_GPG * opp.attack * s.defence * (home ? AWAY_BOOST : HOME_BOOST)).toFixed(2),
        };
      });
    return {
      id, name: s.name, short: s.short, promoted: s.promoted,
      attack: +s.attack.toFixed(2), defence: +s.defence.toFixed(2),
      fixtures: fs_,
      att5: +mean(fs_.slice(0, 5).map(f => f.xGF)).toFixed(2),
      def5: +mean(fs_.slice(0, 5).map(f => f.xGA)).toFixed(2),
      att10: +mean(fs_.map(f => f.xGF)).toFixed(2),
      def10: +mean(fs_.map(f => f.xGA)).toFixed(2),
    };
  }).sort((a, b) => a.def5 - b.def5);

  return { nextGw, deadlineGw, deadline, players, ticker, meta };
}

if (require.main === module) {
  const out = project();
  fs.writeFileSync(path.join(DATA, 'projections.json'), JSON.stringify(out));
  const top = [...out.players].sort((a, b) => b.xp5 - a.xp5).slice(0, 15);
  console.log(`next GW: ${out.nextGw}\nTop 15 by expected points over next 5:`);
  for (const p of top) {
    console.log(`  ${p.name.padEnd(16)} ${p.teamShort} £${p.cost.toFixed(1)}  xP5=${p.xp5.toFixed(1)}  xP10=${p.xp10.toFixed(1)}  mins~${p.expMins}`);
  }
}
module.exports = { project };
