// SQL tokenizer → highlighted HTML for the Axon editor.
// Returns a string with <span class="t-xx"> wrapping each token.

const SQL_KEYWORDS = new Set([
  'select',
  'from',
  'where',
  'group',
  'by',
  'order',
  'having',
  'limit',
  'offset',
  'with',
  'as',
  'and',
  'or',
  'not',
  'in',
  'join',
  'left',
  'right',
  'inner',
  'outer',
  'full',
  'on',
  'using',
  'union',
  'all',
  'distinct',
  'case',
  'when',
  'then',
  'else',
  'end',
  'is',
  'null',
  'true',
  'false',
  'insert',
  'into',
  'values',
  'update',
  'set',
  'delete',
  'create',
  'drop',
  'alter',
  'table',
  'view',
  'index',
  'materialized',
  'between',
  'like',
  'ilike',
  'asc',
  'desc',
  'interval',
  'filter',
  'over',
  'partition',
  'window',
  'return',
  'cross',
  'lateral',
  'exists',
  'any',
  'some',
  'cast',
  'array',
  'row',
  'rows',
  'fetch',
  'first',
  'next',
  'only',
  'returning',
  'conflict',
  'do',
  'nothing',
  'do update',
  'schema',
  'database',
]);
const SQL_FUNCTIONS = new Set([
  'count',
  'sum',
  'avg',
  'min',
  'max',
  'coalesce',
  'nullif',
  'date_trunc',
  'now',
  'current_date',
  'current_timestamp',
  'extract',
  'abs',
  'round',
  'floor',
  'ceil',
  'length',
  'lower',
  'upper',
  'substring',
  'trim',
  'concat',
  'json_build_object',
  'jsonb_build_object',
  'to_char',
  'to_date',
  'greatest',
  'least',
  'array_agg',
  'string_agg',
  'row_number',
  'rank',
  'dense_rank',
  'lag',
  'lead',
  'first_value',
  'last_value',
  'generate_series',
  'percentile_cont',
]);

function highlightSql(src) {
  const out = [];
  const len = src.length;
  let i = 0;
  const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  while (i < len) {
    const c = src[i];

    // line comment
    if (c === '-' && src[i + 1] === '-') {
      let j = src.indexOf('\n', i);
      if (j === -1) j = len;
      out.push(`<span class="t-cmt">${esc(src.slice(i, j))}</span>`);
      i = j;
      continue;
    }
    // block comment
    if (c === '/' && src[i + 1] === '*') {
      let j = src.indexOf('*/', i + 2);
      if (j === -1) j = len;
      else j += 2;
      out.push(`<span class="t-cmt">${esc(src.slice(i, j))}</span>`);
      i = j;
      continue;
    }
    // string
    if (c === "'" || c === '"') {
      const q = c;
      let j = i + 1;
      while (j < len) {
        if (src[j] === '\\') {
          j += 2;
          continue;
        }
        if (src[j] === q) {
          j++;
          break;
        }
        j++;
      }
      out.push(`<span class="t-str">${esc(src.slice(i, j))}</span>`);
      i = j;
      continue;
    }
    // number
    if (/[0-9]/.test(c)) {
      let j = i;
      while (j < len && /[0-9.]/.test(src[j])) j++;
      out.push(`<span class="t-num">${esc(src.slice(i, j))}</span>`);
      i = j;
      continue;
    }
    // identifier / keyword / function
    if (/[A-Za-z_]/.test(c)) {
      let j = i;
      while (j < len && /[A-Za-z0-9_]/.test(src[j])) j++;
      const word = src.slice(i, j);
      const wl = word.toLowerCase();
      // peek next non-space to see if it's a function call
      let k = j;
      while (k < len && /\s/.test(src[k])) k++;
      const isCall = src[k] === '(';
      let cls = 't-id';
      if (SQL_KEYWORDS.has(wl) || /^(select|from|where|join|on|group by|order by)$/.test(wl))
        cls = 't-kw';
      else if (SQL_FUNCTIONS.has(wl) || isCall) cls = 't-fn';
      out.push(`<span class="${cls}">${esc(word)}</span>`);
      i = j;
      continue;
    }
    // operators / punctuation
    if (/[=<>!+\-*/%(),;.]/.test(c)) {
      out.push(`<span class="t-op">${esc(c)}</span>`);
      i++;
      continue;
    }
    // whitespace / other
    let j = i;
    while (j < len && /\s/.test(src[j])) j++;
    if (j > i) {
      out.push(esc(src.slice(i, j)));
      i = j;
      continue;
    }
    out.push(esc(c));
    i++;
  }
  return out.join('');
}

window.highlightSql = highlightSql;
window.SQL_KEYWORDS = SQL_KEYWORDS;
window.SQL_FUNCTIONS = SQL_FUNCTIONS;
