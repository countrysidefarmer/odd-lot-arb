/* Odd-Lot Arbitrage — table renderer */

const DATA_URL = './data/opportunities.json';

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
  const d = new Date(iso + 'T12:00:00Z');  // noon UTC avoids timezone day-shift
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

function render(data) {
  // Update last-updated badge
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

function escape_html(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

fetch(DATA_URL)
  .then(r => {
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return r.json();
  })
  .then(render)
  .catch(err => {
    document.getElementById('loading').textContent = 'Could not load data. Try refreshing.';
    console.error(err);
  });
