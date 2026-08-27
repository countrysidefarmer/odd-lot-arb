# FPL Lab

An expected-points model and analysis dashboard for Fantasy Premier League entry
`5529307`, built from the official FPL API and two seasons of per-gameweek history.

## Use it

```bash
npm run refresh      # re-fetch the API, rebuild projections, regenerate the site
open site/index.html
```

`npm run refresh:full` also re-runs the backtest. That only needs doing when a
season finishes and new historical data lands.

Point it at a different team with `FPL_ENTRY=1234567 npm run refresh`.

## Automatic refresh

A launchd agent rebuilds the dashboard every morning at 07:30, so the local page is
always current. Bookmark `site/index.html` and it behaves as a live dashboard.

```
~/Library/LaunchAgents/com.thomasfielden.fpl-lab.refresh.plist   the schedule
bin/refresh.sh                                                   what it runs
logs/refresh.log                                                 last ~500 lines of output
```

| | |
|---|---|
| Run it now | `launchctl kickstart -p gui/$(id -u)/com.thomasfielden.fpl-lab.refresh` |
| Check it | `launchctl print gui/$(id -u)/com.thomasfielden.fpl-lab.refresh \| grep -E 'runs \|last exit'` |
| Change the time | edit `StartCalendarInterval` in the plist, then bootout + bootstrap it |
| Turn it off | `launchctl bootout gui/$(id -u)/com.thomasfielden.fpl-lab.refresh` |
| Turn it back on | `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.thomasfielden.fpl-lab.refresh.plist` |

It reloads itself at login. If the Mac is asleep at 07:30 the job runs on wake rather
than skipping the day. A failed run (no network) logs the failure and leaves the
previous `site/index.html` untouched.

Note this refreshes the **local** file only. The published artifact is a separate
snapshot and only moves when it is republished.

## What the site shows

| Tab | What it answers |
|---|---|
| My squad | The best legal XI from your fifteen, bench order, captain, and who is a problem |
| Plan | Multi-week transfer plan — sequences searched across a horizon, not one week in isolation |
| Transfers | Every legal one-for-one move ranked by the points it adds, budget and 3-per-club respected |
| Best buys | The whole game ranked by projected points or by points per £m |
| Fixture ticker | Ten gameweeks per club, shaded by the model's expected goals, split by defensive and attacking view |
| Where value is | Market line per position; who is underpriced for what they will deliver |
| What predicts points | The backtests — fixtures vs form, minutes vs quality, and the gameweek-1 case |
| Method | How the projections are built, and where they should not be trusted |

Every projection respects the horizon toggle: 1, 3, 5 or 10 gameweeks.

The player card's **fixture-by-fixture table runs played and upcoming gameweeks
together on one set of axes** — minutes, xG, xA, BPS, bonus, points, and the
team's own scoreline. Played rows are tinted, carry a solid left edge and shade
their points cell on the return scale; projected rows are left plain, because a
projection should never be coloured as though it happened. On a played row BPS
and bonus are what he scored and was awarded; on a projected row they are his
expected tally and how often he finishes a match's top three across the simulated
rankings. Putting them in one column is the point: it shows whether the
underlying numbers backed up a return, since a big score off 0.1 xG is not a
repeatable one.

A gameweek counts as played only once that player's own fixture has finished, not
when the gameweek is merely current — otherwise a match still to kick off shows up
as the player failing to appear in it.

The squad and best-buys tables carry a **recent form strip**: one cell per
gameweek actually played, up to the last five, newest on the right. The number is
always the points scored, so colour is a second cue rather than the only one, and
a gameweek the player sat out is drawn hollow — a blank and an absence mean
different things when you are reading a squad. The bar under each number is
minutes played, and hovering gives the opponent, score and the full return.
Gameweeks that have not happened are simply absent, so in August this is honestly
one cell wide rather than a row of blanks.

## The model

For each player and each upcoming fixture:

```
expected minutes
  x per-90 rates (last season, shrunk toward a price-and-position prior,
                  blended into this season's data as it accumulates)
  x fixture multiplier (opponent attack/defence strength, home advantage)
  -> goals, assists, clean sheets, defensive contribution, saves, bonus, cards
  -> points, via the scoring table for that position
```

Clean sheets use a Poisson probability on the opponent's expected goals. Defensive
contribution uses each player's empirical rate of clearing the threshold (10 actions
for defenders, 12 for everyone else) when they start. Players with no Premier League
history fall back to the price prior rather than projecting at zero.

Calibration check: a strong XI projects around 55 points a gameweek, a mid-table one
around 30 — in line with real FPL scoring.

## Overriding it

`data/overrides.json` sets a player's start probability by hand, for what no dataset
can see — a returning injury, an understudy ruled out, a confirmed role change:

```json
{ "Isak": { "pStart": 0.9, "note": "Fit, and the understudy ahead of him is injured." } }
```

Overridden players are chipped as such in the value table. Delete the entry once the
season's own data has caught up.

## The research

Three scripts, all no-hindsight: every predictor is built only from gameweeks
strictly before the one being predicted.

`src/research.js` builds a panel of ~28,000 player-gameweeks across 2025-26 and
2024-25. Every predictor uses only gameweeks strictly before the one being predicted,
so there is no hindsight in it. Findings, both seasons agreeing:

- **Minutes dominate.** Across all players, expected minutes explain more than form,
  fixtures and underlying stats combined.
- **Among nailed starters, everything is small.** The best model explains under 10%
  of a single gameweek. Football is noise plus a thin edge.
- **Fixtures matter for defenders, not attackers.** The fixture run adds ~3-4pp of
  explanatory power for defenders over five gameweeks, and ~0.1-1.6pp for midfielders
  and forwards. Keepers swing between seasons — treat as unsettled.
- **Season-long quality beats four-game form** over a five-gameweek horizon.

`src/minutes.js` splits availability from quality, which the headline backtest conflated:

- **Minutes are an order of magnitude more predictable than points.** ~49% of the
  variation in next-week minutes is explainable, against 2.8-7.3% of the points a
  player scores once he is on the pitch. The winnable part of FPL is being right
  about who plays.
- **Recency beats averages.** Minutes in the single most recent match predict better
  (rho 0.67) than a season-long start rate (0.48).
- **Underlying numbers do carry signal**, just thinly per game. Given a start, xG+xA
  per 90, ICT, bonus rate and penalty duty all rank above form — and unlike form they
  persist, so they compound over a horizon.

`src/preseason.js` tests the gameweek-1 case — predicting the opening six gameweeks
with no current-season evidence:

- Overall R² is 35%, against ~49% mid-season. Preseason is genuinely harder.
- For the 198 players with **no Premier League record**, price is almost the only
  signal (10.8% alone, 14.5% with set-piece duty). The model uses the measured price
  bands directly.
- An unknown player on set-piece duty averaged **53 minutes** against **19** for the
  rest. Being trusted with corners is a strong tell.
- **Penalty duty is a role signal, not a points bonus.** Penalty takers' historical xG
  already contains their penalties, so a bonus on top would double-count; and it does
  not predict minutes independently once last season's are known. The model applies it
  only when duty changes hands, or when there is no history to contain it.

## Layout

```
src/fetch.js       pulls the FPL API + vaastav/Fantasy-Premier-League history
src/research.js    fixtures vs form -> data/research.json
src/minutes.js     minutes vs quality, reported to stdout
src/depth.js       squad competition -> start probabilities that react to team news
src/bonus.js       the BPS rulebook and the per-match bonus tournament
src/preseason.js   the gameweek-1 case, reported to stdout
src/signals.js     runs both of the above -> data/signals.json
src/model.js       the projections -> data/projections.json
src/odds.js        Polymarket 1X2 -> expected goals -> data/odds.json
src/planner.js     multi-week transfer beam search -> data/plan.json
src/backtest.js    replays a past season to score the model
src/build.js       injects the payload into the template
src/template.html  the dashboard (styles, markup, client-side logic)
src/lib.js         CSV parsing, stats, Poisson
```

## Bonus points

Bonus is a tournament, not a rate. FPL gives 3/2/1 to the top three BPS scores
**in a match**, and ranking the published BPS reproduces the published bonus for
100% of player-matches in both 2024-25 and 2025-26 — the award rule is exact.
The model used to multiply a historical bonus-per-90 by expected minutes, which
cannot see the opposition: 25 BPS wins bonus in a quiet game and nothing in a 4-3.

Now every player in a fixture gets an expected BPS, the match is simulated, and
expected bonus is how often he finishes top three. Per-match calibration is exact
by construction — for the two unplayed GW1 fixtures the model distributes 12.0
expected bonus points against the 12 really on offer.

Expected BPS is a **smooth base plus a fixture correction**, deliberately not a
rebuild from components. `src/bonus.js` fits the BPS rulebook per position from
last season, and the fit recovers the real rules closely — clean sheet 12.0,
midfield goal 19.7, save 2.8, yellow -3.1, at 78-95% R². But feeding *predicted*
goals and assists through those weights is measurably **worse** than the flat
rate (correlation with actual bonus 0.154 vs 0.198): a midfield goal is worth
~20 BPS, so the weight amplifies the noise in a per-90 goal rate. Only the
deviation of this fixture from a neutral one is added.

Worth knowing where the ceiling is. Given the *actual* match statistics, the
ranking simulation predicts bonus at correlation **0.759**; from a player's
history it manages **0.198**. Nothing is wrong with the award model — bonus is
hard because BPS and minutes are hard. In the full backtest, where minutes are
uncertain too, the change is a wash on accuracy (MAE 1.9094 -> 1.9043, top-20
returns 4.19 -> 4.21) and is kept for being structurally right and exactly
calibrated per match, not for the decimal.

## Squad competition and minutes

The obvious idea — feed the depth chart into the minutes model — does not work,
and it is worth recording why. Adding competitors-ahead, squad depth and share of
positional minutes to the existing recency features moves minutes R^2 by
**+0.02pp** on 2025-26 and +0.02pp on 2024-25. Individually those features look
strong (share of positional minutes correlates 0.79 with next-week minutes), but
a player's own recent minutes already encode who he is competing with.

What does work is the same idea as a **live availability constraint**, which no
backtest can see: last season's files record minutes, not injuries, so "his rival
is out this week" simply is not in the history. It is in today's data, and right
now it is large — Spurs have five defenders and five midfielders flagged,
Brighton six midfielders.

The constraint that makes it usable is that the number of players a team starts
in each position is nearly deterministic. Per team per match over 2025-26:

| Position | Started | sd |
|---|---|---|
| GK | 1.00 | 0.00 |
| DEF | 4.19 | 0.60 |
| MID | 4.71 | 0.83 |
| FWD | 1.10 | 0.54 |

So `src/depth.js` redistributes the starts **vacated by unavailable players**
across their available team-mates, scaling on the odds so nobody is pushed past a
0.97 ceiling. It redistributes only what was vacated rather than rescaling the
group to its slot count, because forcing a fully fit squad onto the slot count
makes minutes prediction *worse* (MAE 19.18 -> 19.52 on 2025-26, 19.49 -> 19.87
on 2024-25) — with nobody injured the constraint carries no information and only
moves noise around. Written this way it is an exact no-op when everyone is
available, and reacts only when there is news. Players it promotes are labelled
"Deputising" on the dashboard.

## How far ahead the odds reach

Polymarket lists about three gameweeks, and liquidity collapses across them
(GW1 ~£3M, GW2 ~£135k, GW3 ~£3k with zero volume). Beyond that the model uses its
own team strength.

This is fine, and measurably so. Neutralising every fixture input — opponent, home
advantage, odds — changes a 10-gameweek projection by **2.7%** and leaves 48 of the
top 50 unchanged; at one gameweek it changes it by **11.7%** and reorders the top 10.
Fixture runs mean-revert over ten games. But the average hides the decision: the median
player's run is worth nothing while the spread is large. Over five gameweeks, best minus
worst run is **4.2 pts for a defender**, 2.6 for a midfielder, 2.1 for a forward — and among
comparable players (same position, within £0.5m, similar rates) the run reverses which is
better in **10% of defender pairs**. Fixtures do not change who the best players are; they
routinely decide which of two similar defenders to rotate in. The `swing3/5/10` fields on
each player isolate that.

Reproduce with `NEUTRAL_FIXTURES=1 npm run model`.

## The planner

`npm run plan` searches sequences of transfers across a horizon rather than picking the
best move this week. Exhaustive search is intractable (15 x ~600 moves per week,
compounding), so it is a beam search: expand a shortlist of promising moves from each
surviving plan each gameweek, score the full horizon, keep the best.

It respects the bank, 3-per-club, squad shape, one free transfer a week (capped at 5)
and charges 4 points per extra transfer. Runs in about 0.2s.

```bash
npm run plan
PLAN_HORIZON=8 PLAN_FREE=2 PLAN_BEAM=120 npm run plan
```

| Variable | Default | Meaning |
|---|---|---|
| `PLAN_HORIZON` | 6 | gameweeks to plan over |
| `PLAN_FREE` | 1 | free transfers available now |
| `PLAN_BEAM` | 60 | plans kept per gameweek — higher is slower and slightly better |
| `PLAN_CANDIDATES` | 3 | replacements considered per squad player |

The reported gain is measured against the model's own projections, so it ranks plans
rather than forecasting your score. Only the first gameweek is a decision; the rest is
the reasoning behind it and gets re-planned next week. Chips, blanks and doubles are
not modelled, and selling prices are assumed equal to current prices.

## Does it work?

`npm run backtest` replays gameweeks 9-38 of 2025-26, rebuilding every input from
matches already played (11,812 player-gameweeks).

| MAE | model | season average | last 4 games |
|---|---|---|---|
| All players | **1.91** | 1.98 | 1.95 |
| Likely starters | **2.40** | 2.45 | 2.49 |

It beats doing nothing by 2-4%. Its real strength is calibration: 2.20 projected vs
2.18 actual, bias +0.015, which is what makes projections safe to sum across a squad
and a horizon. Top 20 by projection returned 4.19 a head vs 4.00 for season average.

Measured weakness: it cannot see hauls — 10+ point returns were projected at 3.2.
That is inherent to a mean estimator on a skewed distribution.

Captaincy is just the highest projection, which is correct: doubling is linear, so
maximising expected points means captaining the largest expected score. A backtest
showed the model's captain returning 4.83/gw vs 5.57 for a season-average rule, but
the two pick the same player in 17 of 30 gameweeks, the difference is not significant
(t = -1.33), and over the top three picks the model leads 4.90 to 4.61.

Note FPL's own published `xP` is **not** a usable benchmark here — the historical
dataset carries it for one gameweek only and zeroes it thereafter (92.8% zeros).

Do not tune the constants against MAE. FPL points are heavily right-skewed, so MAE is
minimised by shrinking every projection downward: a 3.65% "improvement" moves the bias
from +0.015 to -0.21 while barely touching RMSE. Calibration is the objective that matters.

## Limits

The depth chart reacts to FPL's own availability flags, which is not the same as
reading a predicted line-up — it knows Saliba is out, not who the manager picks
instead beyond what minutes history implies. Its effect is also untestable
historically, since the per-gameweek archives carry no injury status; the
backtest can only confirm it does no harm when everyone is fit.

It does not read team news or predicted line-ups, does not model blanks and doubles,
and tracks set-piece duty only as a change signal. Early in a season it leans heavily on
last year, so anyone in a genuinely new role will be mispriced. A manager's Friday
press conference beats it every time.

## Data

- Official FPL API — prices, ownership, availability, fixtures, your picks.
- [vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League)
  — per-gameweek history for 2025-26 and 2024-25.
