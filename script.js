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

function render_bar_chart(trades) {
  // Group trades by year
  const by_year = {};
  trades.forEach(t => {
    const yr = t.expiry.slice(0, 4);
    if (!by_year[yr]) by_year[yr] = { total: 0, trades: [] };
    by_year[yr].total = Math.round((by_year[yr].total + t.realized_pnl) * 100) / 100;
    by_year[yr].trades.push(t);
  });
  const years = Object.keys(by_year).sort();
  const totals = years.map(yr => by_year[yr].total);
  const colors = totals.map(v => v >= 0 ? '#10d48a' : '#f87171');
  const bg_colors = totals.map(v => v >= 0 ? 'rgba(16,212,138,0.2)' : 'rgba(248,113,113,0.2)');

  const ctx = document.getElementById('pnlChart').getContext('2d');

  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: years,
      datasets: [{
        data: totals,
        backgroundColor: bg_colors,
        borderColor: colors,
        borderWidth: 2,
        borderRadius: 4,
        borderSkipped: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: items => items[0].label,
            afterTitle: items => {
              const yr = items[0].label;
              const total = by_year[yr].total;
              return 'Total: $' + total.toFixed(0);
            },
            label: () => '',
            footer: items => {
              const yr = items[0].label;
              return by_year[yr].trades
                .slice()
                .sort((a, b) => b.realized_pnl - a.realized_pnl)
                .map(t =>
                  `${t.ticker.padEnd(6)}  $${t.t1_price.toFixed(2)} → $${t.clearing_price.toFixed(2)}  P&L $${t.realized_pnl.toFixed(0)}`
                );
            },
          },
          titleFont: { size: 13, weight: 'bold' },
          footerFont: { family: 'monospace', size: 11 },
          footerColor: '#8899b4',
          padding: 12,
        },
      },
      scales: {
        x: {
          ticks: { color: '#8899b4', font: { size: 12 } },
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
  const sorted = [...trades].sort((a, b) => b.expiry.localeCompare(a.expiry));
  sorted.forEach(t => {
    const pnl_neg = t.realized_pnl < 0;
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="cell-ticker">${escape_html(t.ticker)}</td>
      <td class="cell-company">${escape_html(t.company_name)}</td>
      <td><span class="cell-exchange">${escape_html(t.exchange)}</span></td>
      <td class="cell-range">${fmt_price(t.clearing_price)}</td>
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
  render_bar_chart(trades);
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
