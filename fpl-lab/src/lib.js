'use strict';
// Shared helpers: CSV parsing, stats, Poisson.

function parseCSV(text) {
  const rows = [];
  let row = [], field = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; }
      else field += c;
    } else if (c === '"') inQ = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); field = ''; rows.push(row); row = []; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  const head = rows.shift();
  return rows.filter(r => r.length === head.length).map(r => {
    const o = {};
    head.forEach((h, i) => { o[h] = r[i]; });
    return o;
  });
}

const num = v => { const n = parseFloat(v); return Number.isFinite(n) ? n : 0; };
const sum = a => a.reduce((x, y) => x + y, 0);
const mean = a => (a.length ? sum(a) / a.length : 0);

function sd(a) {
  if (a.length < 2) return 0;
  const m = mean(a);
  return Math.sqrt(sum(a.map(x => (x - m) ** 2)) / (a.length - 1));
}

function pearson(x, y) {
  const n = Math.min(x.length, y.length);
  if (n < 3) return 0;
  const mx = mean(x), my = mean(y);
  let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < n; i++) {
    const a = x[i] - mx, b = y[i] - my;
    sxy += a * b; sxx += a * a; syy += b * b;
  }
  return sxx && syy ? sxy / Math.sqrt(sxx * syy) : 0;
}

// Average ranks, ties shared — needed for Spearman on the many tied zero-point rows.
function rank(a) {
  const idx = a.map((v, i) => [v, i]).sort((p, q) => p[0] - q[0]);
  const r = new Array(a.length);
  let i = 0;
  while (i < idx.length) {
    let j = i;
    while (j + 1 < idx.length && idx[j + 1][0] === idx[i][0]) j++;
    const avg = (i + j) / 2 + 1;
    for (let k = i; k <= j; k++) r[idx[k][1]] = avg;
    i = j + 1;
  }
  return r;
}

const spearman = (x, y) => pearson(rank(x), rank(y));

// OLS with intercept via Gaussian elimination on the normal equations.
function ols(X, y) {
  const n = X.length, p = X[0].length + 1;
  const A = Array.from({ length: p }, () => new Array(p + 1).fill(0));
  for (let r = 0; r < n; r++) {
    const xr = [1, ...X[r]];
    for (let i = 0; i < p; i++) {
      for (let j = 0; j < p; j++) A[i][j] += xr[i] * xr[j];
      A[i][p] += xr[i] * y[r];
    }
  }
  for (let i = 0; i < p; i++) A[i][i] += 1e-8; // ridge nudge for stability
  for (let c = 0; c < p; c++) {
    let piv = c;
    for (let r = c + 1; r < p; r++) if (Math.abs(A[r][c]) > Math.abs(A[piv][c])) piv = r;
    [A[c], A[piv]] = [A[piv], A[c]];
    if (Math.abs(A[c][c]) < 1e-12) continue;
    for (let r = 0; r < p; r++) {
      if (r === c) continue;
      const f = A[r][c] / A[c][c];
      for (let k = c; k <= p; k++) A[r][k] -= f * A[c][k];
    }
  }
  const beta = A.map((row, i) => (Math.abs(row[i]) < 1e-12 ? 0 : row[p] / row[i]));
  const pred = X.map(xr => beta[0] + sum(xr.map((v, i) => v * beta[i + 1])));
  const my = mean(y);
  const ssTot = sum(y.map(v => (v - my) ** 2));
  const ssRes = sum(y.map((v, i) => (v - pred[i]) ** 2));
  return { beta, r2: ssTot ? 1 - ssRes / ssTot : 0, pred };
}

function zscore(a) {
  const m = mean(a), s = sd(a) || 1;
  return a.map(v => (v - m) / s);
}

const poissonP = (k, lam) => {
  let f = 1;
  for (let i = 2; i <= k; i++) f *= i;
  return Math.exp(-lam) * lam ** k / f;
};

module.exports = { parseCSV, num, sum, mean, sd, pearson, spearman, ols, zscore, poissonP, rank };
