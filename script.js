/* Odd-Lot Arbitrage — table renderer + historical backtest chart */

// ── Shared formatting ────────────────────────────────────────────────────────

function fmt_price(v) {
  return v != null ? '$' + v.toFixed(2) : '—';
}

function fmt_range(lower, upper) {
  if (lower == null || upper == null) return '—';
  if (lower === upper) return '$' + lower.toFixed(2);
  return '$' + lower.toFixed(2) + '–$' + upper.toFixed(2);
}

function fmt_date(iso) {
  if (!iso) return '—';
  const d = new Date(iso + 'T12:00:00Z');
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function fmt_last_updated(iso) {
  if (!iso) return 'Never';
  const d = new Date(iso);
  return d.toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZoneName: 'short'
  });
}

function days_until(iso) {
  if (!iso) return Infinity;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const exp = new Date(iso + 'T12:00:00Z');
  return Math.round((exp - today) / 86400000);
}

function escape_html(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Live opportunities table ─────────────────────────────────────────────────

function render_live(data) {
  document.getElementById('last-updated').textContent = fmt_last_updated(data.last_updated);

  const opps = data.opportunities || [];
  document.getElementById('loading').style.display = 'none';

  if (opps.length === 0) {
    document.getElementById('empty').style.display = '';
    return;
  }

  const tbody = document.getElementById('table-body');
  opps.forEach(op => {
    const until = days_until(op.expiry);
    const expiring_soon = until <= 7;
    const profit_neg = op.max_profit < 0;

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="cell-ticker">${escape_html(op.ticker)}</td>
      <td class="cell-company">${escape_html(op.company_name)}</td>
      <td><span class="cell-exchange">${escape_html(op.exchange)}</span></td>
      <td class="cell-range">${fmt_range(op.price_lower, op.price_upper)}</td>
      <td class="cell-price">${fmt_price(op.current_price)}</td>
      <td class="cell-profit${profit_neg ? ' negative' : ''}">${fmt_price(op.max_profit)}</td>
      <td class="cell-expiry${expiring_soon ? ' expiring-soon' : ''}">${fmt_date(op.expiry)}${expiring_soon ? ' ⚡' : ''}</td>
      <td class="cell-filing"><a href="${escape_html(op.filing_link)}" target="_blank" rel="noopener">SEC &rarr;</a></td>
    `;
    tbody.appendChild(tr);
  });

  document.getElementById('table-wrap').style.display = '';
}

fetch('./data/opportunities.json?v=' + Date.now())
  .then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
  .then(render_live)
  .catch(err => {
    document.getElementById('loading').textContent = 'Could not load data. Try refreshing.';
    console.error(err);
  });

// ── Historical backtest ──────────────────────────────────────────────────────

const YEAR_COLORS = [
  '#10d48a', '#4a9eff', '#f59e0b', '#f87171', '#a78bfa',
  '#34d399', '#60a5fa', '#fb923c', '#e879f9', '#86efac', '#fde68a',
];

function day_of_year(iso) {
  const d = new Date(iso + 'T12:00:00Z');
  const start = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.floor((d - start) / 86400000) + 1;
}

function render_stats(trades) {
  const total_pnl = trades.reduce((s, t) => s + t.realized_pnl, 0);

  // Annual P&L for Sharpe
  const by_year = {};
  trades.forEach(t => {
    const yr = t.expiry.slice(0, 4);
    by_year[yr] = (by_year[yr] || 0) + t.realized_pnl;
  });
  const annual = Object.values(by_year);
  const mean_a = annual.reduce((s, v) => s + v, 0) / annual.length;
  const std_a = Math.sqrt(annual.reduce((s, v) => s + (v - mean_a) ** 2, 0) / annual.length);
  const sharpe = std_a > 0 ? (mean_a / std_a).toFixed(2) : '—';
  const best_yr = Object.entries(by_year).sort((a, b) => b[1] - a[1])[0];

  document.getElementById('hist-stats').innerHTML = `
    <div class="stat-card"><div class="stat-val">${trades.length}</div><div class="stat-lbl">Completed trades</div></div>
    <div class="stat-card"><div class="stat-val ${total_pnl >= 0 ? 'green' : 'red'}">$${total_pnl.toFixed(0)}</div><div class="stat-lbl">Total P&amp;L (99 sh per trade)</div></div>
    <div class="stat-card"><div class="stat-val green">$${best_yr ? best_yr[1].toFixed(0) : '—'}</div><div class="stat-lbl">Best year (${best_yr ? best_yr[0] : '—'})</div></div>
    <div class="stat-card"><div class="stat-val">${sharpe}</div><div class="stat-lbl">Annual Sharpe (rf=0)</div></div>
  `;
}

function render_chart(trades) {
  const years = [...new Set(trades.map(t => t.expiry.slice(0, 4)))].sort();

  const datasets = years.map((yr, i) => {
    const yr_trades = trades
      .filter(t => t.expiry.startsWith(yr))
      .sort((a, b) => a.expiry.localeCompare(b.expiry));

    // Build cumsum points: start at (0, 0), add a point per trade
    const points = [{ x: 0, y: 0 }];
    let cum = 0;
    yr_trades.forEach(t => {
      cum += t.realized_pnl;
      points.push({ x: day_of_year(t.expiry), y: Math.round(cum * 100) / 100 });
    });
    // Extend line to end of year for completed years
    const current_year = new Date().getUTCFullYear().toString();
    if (yr !== current_year) {
      points.push({ x: 365, y: Math.round(cum * 100) / 100 });
    }

    const color = YEAR_COLORS[i % YEAR_COLORS.length];
    return {
      label: yr,
      data: points,
      borderColor: color,
      backgroundColor: color + '20',
      borderWidth: 2,
      pointRadius: 3,
      pointHoverRadius: 5,
      tension: 0,
      fill: false,
    };
  });

  const ctx = document.getElementById('pnlChart').getContext('2d');

  // Month labels on x-axis (day of year → month name)
  const month_ticks = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];
  const month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  new Chart(ctx, {
    type: 'scatter',
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: false },
      plugins: {
        legend: {
          labels: { color: '#8899b4', font: { size: 12 } },
        },
        tooltip: {
          callbacks: {
            label: ctx => {
              const yr = ctx.dataset.label;
              const doy = ctx.parsed.x;
              const pnl = ctx.parsed.y;
              // Reconstruct approximate date from day-of-year
              const d = new Date(Date.UTC(parseInt(yr), 0, doy));
              const ds = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
              return `${yr} ${ds}: $${pnl.toFixed(2)} cumulative`;
            },
          },
        },
      },
      scales: {
        x: {
          type: 'linear',
          min: 0,
          max: 365,
          ticks: {
            color: '#8899b4',
            callback: val => {
              const idx = month_ticks.indexOf(val);
              return idx >= 0 ? month_labels[idx] : '';
            },
            values: month_ticks,
          },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
        y: {
          ticks: {
            color: '#8899b4',
            callback: val => '$' + val.toFixed(0),
          },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
      },
    },
  });
}

function source_badge(src) {
  if (!src) return '';
  if (src === 'fixed') return '<span class="badge-inline badge-blue">fixed</span>';
  if (src.startsWith('amendment')) return '<span class="badge-inline badge-green">amendment</span>';
  return '<span class="badge-inline badge-amber">est.</span>';
}

function render_hist_table(trades) {
  const tbody = document.getElementById('hist-table-body');
  // Most recent first
  const sorted = [...trades].sort((a, b) => b.expiry.localeCompare(a.expiry));
  sorted.forEach(t => {
    const pnl_neg = t.realized_pnl < 0;
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="cell-ticker">${escape_html(t.ticker)}</td>
      <td class="cell-company">${escape_html(t.company_name)}</td>
      <td><span class="cell-exchange">${escape_html(t.exchange)}</span></td>
      <td class="cell-range">${fmt_range(t.price_lower, t.price_upper)}</td>
      <td class="cell-price">${fmt_price(t.clearing_price)} ${source_badge(t.clearing_price_source)}</td>
      <td class="cell-price">${fmt_price(t.t1_price)}</td>
      <td class="cell-profit${pnl_neg ? ' negative' : ''}">${fmt_price(t.realized_pnl)}</td>
      <td class="cell-expiry">${fmt_date(t.expiry)}</td>
      <td class="cell-filing"><a href="${escape_html(t.filing_link)}" target="_blank" rel="noopener">SEC &rarr;</a></td>
    `;
    tbody.appendChild(tr);
  });
}

function render_historical(data) {
  document.getElementById('hist-loading').style.display = 'none';
  const trades = data.trades || [];
  if (trades.length === 0) {
    document.getElementById('hist-error').textContent = 'No historical data yet. Run backtest.py to generate it.';
    document.getElementById('hist-error').style.display = '';
    return;
  }
  document.getElementById('hist-content').style.display = '';
  render_stats(trades);
  render_chart(trades);
  render_hist_table(trades);
}

fetch('./data/historical.json?v=' + Date.now())
  .then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
  .then(render_historical)
  .catch(err => {
    document.getElementById('hist-loading').style.display = 'none';
    document.getElementById('hist-error').style.display = '';
    console.error('Historical data:', err);
  });
