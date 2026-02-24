/**
 * AllFluence Video Counter — Google Apps Script
 *
 * Roda 2x/dia via trigger. Puxa dados do ClickUp (lista Produção de Criativos),
 * lê campos "Primeira Edição" e "Pontos", calcula ranking e TURBO por editor.
 * Expõe resultado via doGet() para widget consumir.
 *
 * Setup:
 *   1. Crie um novo Google Apps Script em script.google.com
 *   2. Cole este código
 *   3. Em Propriedades do Script, adicione: CLICKUP_API_KEY = pk_xxx
 *   4. Deploy > Web App > Execute as: Me, Access: Anyone
 *   5. Adicione trigger: videoCounterMain(), Time-driven, Every 12 hours
 *
 * @version 1.0.0
 */

// ─── Config ──────────────────────────────────────────────────────────────────

const CONFIG = {
  LIST_IDS: {
    producao: '901303868623',     // Produção de Criativos (PRINCIPAL)
    filaFixo: '901324270156',     // Fila de Edição (time fixo)
    filaFreelas: '901324715701',  // Fila de Edição FREELAS
  },
  BONUS: {
    productivity: [
      { rank: 1, value: 500 },
      { rank: 2, value: 250 },
    ],
    metaDiaria: 6, // meta mínima diária: 6 pontos
    turbo: { value: 100, threshold: 8, tag: 'turbo' }, // TURBO: tag "turbo" (+R$100/vid)
    turbinho: { value: 10 }, // R$10 por criativo sem ajuste
    fds: { perTask: { 1: 35, 2: 50 }, tags: ['fds edição', 'feriado edição'] },
    ajusteStatuses: ['para ajustar', 'para ajustar cliente'],
    freelaPerTask: { 1: 35, 2: 50 },
  },
  // Fallback weight map when "Pontos" field is empty
  WEIGHT_MAP: {
    bbb: 1, symphony: 1, ttcx: 2, gov: 2, motion: 4, longform: 5, clp: 1,
  },
  // Time fixo — apenas esses editores recebem TURBO e Turbinho
  TIME_FIXO: [
    'pedro ximenes', 'lílian elen', 'lilian elen',
    'rafael nóbrega', 'rafael nobrega',
    'bruna', 'vinícius', 'vinicius', 'daniel', 'ricardo',
  ],
  TIME_IA: ['rafael gomes'],
  FREELAS: [
    'bianca', 'ághata', 'agatha', 'maria eduarda',
    'gabriel bonilha', 'raphael', 'saturno', 'gustavo', 'hugo',
  ],
  // Display name aliases (clickup username → display name)
  NAME_ALIASES: { 'saturno': 'Raphael (Saturno)' },
  CLICKUP_API_BASE: 'https://api.clickup.com/api/v2',
  CACHE_KEY: 'VIDEO_COUNTER_RESULT',
};

// ─── ClickUp API ─────────────────────────────────────────────────────────────

function getApiKey_() {
  const key = PropertiesService.getScriptProperties().getProperty('CLICKUP_API_KEY');
  if (!key) throw new Error('CLICKUP_API_KEY not set in Script Properties');
  return key;
}

function clickupGet_(endpoint) {
  const url = CONFIG.CLICKUP_API_BASE + endpoint;
  const options = {
    method: 'get',
    headers: { 'Authorization': getApiKey_() },
    muteHttpExceptions: true,
  };
  const response = UrlFetchApp.fetch(url, options);
  if (response.getResponseCode() !== 200) {
    throw new Error(`ClickUp API error ${response.getResponseCode()}: ${response.getContentText()}`);
  }
  return JSON.parse(response.getContentText());
}

function getTasks_(listId, page, dateRange) {
  let url = `/list/${listId}/task?page=${page}&archived=false&include_closed=true`;
  if (dateRange) {
    // Only fetch tasks updated within the month window (± 15 days buffer)
    const buffer = 15 * 24 * 60 * 60 * 1000;
    url += '&date_updated_gt=' + (dateRange.start - buffer);
    url += '&date_updated_lt=' + (dateRange.end + buffer);
  }
  const data = clickupGet_(url);
  return data.tasks || [];
}

// ─── Field Helpers ───────────────────────────────────────────────────────────

function findField_(task, fieldName) {
  if (!task.custom_fields) return null;
  const needle = fieldName.toLowerCase();
  // Try exact match first (ignoring emojis/symbols)
  const stripEmoji = s => s.replace(/[^\p{L}\p{N}\s]/gu, '').trim().toLowerCase();
  const exact = task.custom_fields.find(
    cf => stripEmoji(cf.name) === needle
  );
  if (exact) return exact;
  // Fallback: includes match
  return task.custom_fields.find(
    cf => cf.name.toLowerCase().includes(needle)
  );
}

function parseFieldValue_(field) {
  if (!field || field.value === null || field.value === undefined) return null;
  switch (field.type) {
    case 'drop_down':
      if (field.type_config && field.type_config.options) {
        const opt = field.type_config.options.find(
          o => o.id === field.value || o.orderindex === field.value
        );
        return opt ? opt.name : field.value;
      }
      return field.value;
    case 'date':
      return field.value ? new Date(parseInt(field.value)) : null;
    case 'number':
    case 'currency':
      return typeof field.value === 'object' ? field.value.current : field.value;
    case 'users':
      return Array.isArray(field.value) ? field.value : [field.value];
    default:
      return field.value;
  }
}

function getPrimeiraEdicao_(task) {
  const field = findField_(task, 'Primeira Edição');
  if (!field) return null;
  const val = parseFieldValue_(field);
  if (!val) return null;
  const d = val instanceof Date ? val : new Date(parseInt(val));
  return isNaN(d.getTime()) ? null : d;
}

function getPontos_(task) {
  // 1. Try "Pontos" custom field
  const field = findField_(task, 'Pontos');
  if (field) {
    const val = parseFieldValue_(field);
    if (val !== null && val !== undefined) {
      const num = parseInt(String(val).replace(/[^0-9]/g, ''));
      if (!isNaN(num) && num > 0) return num;
    }
  }

  // 2. Fallback: identify type
  const type = identifyType_(task);
  if (type !== 'unknown') return CONFIG.WEIGHT_MAP[type] || 1;

  return null;
}

function identifyType_(task) {
  const TASK_NAME_RE = /\[\d+\]\s*\[[A-Z]\d+\]\[([A-Z]+)\]/i;
  const CLIENT_MAP = {
    'MC': 'bbb', 'MELI': 'bbb', 'BBB': 'bbb', 'TTCX': 'ttcx',
    'GOV': 'gov', 'MG': 'motion', 'LF': 'longform', 'SYM': 'symphony', 'CLP': 'clp',
  };
  const NAME_PATTERNS = [
    [/bbb|react|moda|cpg|mercado\s*livre/i, 'bbb'],
    [/ttcx|anúncio|anuncio|tiktok/i, 'ttcx'],
    [/symphony|sinfonia/i, 'symphony'],
    [/motion/i, 'motion'],
    [/long\s*form|youtube|podcast/i, 'longform'],
    [/gov(erno)?|institucional/i, 'gov'],
    [/clp|landing/i, 'clp'],
  ];

  // Try Produto field
  const prodField = findField_(task, 'Produto');
  if (prodField) {
    const val = parseFieldValue_(prodField);
    if (val) {
      const norm = String(val).toLowerCase();
      for (const key of Object.keys(CONFIG.WEIGHT_MAP)) {
        if (norm.includes(key)) return key;
      }
    }
  }

  // Try client code
  const match = TASK_NAME_RE.exec(task.name);
  if (match && CLIENT_MAP[match[1].toUpperCase()]) {
    return CLIENT_MAP[match[1].toUpperCase()];
  }

  // Regex fallback
  for (const [re, type] of NAME_PATTERNS) {
    if (re.test(task.name)) return type;
  }

  return 'unknown';
}

function extractEditors_(task) {
  // Only use "Editor" custom field — never fall back to assignees
  // (assignees can be accounts, clients, etc.)
  const field = findField_(task, 'Editor');
  if (field) {
    const val = parseFieldValue_(field);
    if (val && Array.isArray(val) && val.length > 0) {
      return val.map(u => ({ id: u.id, name: u.username || u.email || 'User ' + u.id }));
    }
  }
  return [];
}

// ─── FDS & Turbinho Helpers ──────────────────────────────────────────────────

function isFdsTask_(task) {
  if (!task.tags || !Array.isArray(task.tags)) return false;
  return task.tags.some(function(t) {
    return CONFIG.BONUS.fds.tags.indexOf((t.name || '').toLowerCase()) !== -1;
  });
}

function isTurboTask_(task) {
  if (!task.tags || !Array.isArray(task.tags)) return false;
  var tag = (CONFIG.BONUS.turbo.tag || 'turbo').toLowerCase();
  return task.tags.some(function(t) {
    return (t.name || '').toLowerCase() === tag;
  });
}

function getTimeInStatus_(taskId) {
  try {
    return clickupGet_('/task/' + taskId + '/time_in_status');
  } catch (e) {
    Logger.log('Error getting status history for ' + taskId + ': ' + e.message);
    return null;
  }
}

function hadAjuste_(taskId) {
  var data = getTimeInStatus_(taskId);
  if (!data || !data.status_history) return false;
  return data.status_history.some(function(s) {
    return CONFIG.BONUS.ajusteStatuses.indexOf((s.status || '').toLowerCase()) !== -1;
  });
}

// ─── Core Logic ──────────────────────────────────────────────────────────────

function getMonthRange_(monthStr) {
  const parts = monthStr.split('-');
  const year = parseInt(parts[0]);
  const month = parseInt(parts[1]);
  const start = new Date(year, month - 1, 1);
  const end = new Date(year, month, 0, 23, 59, 59, 999);
  return { start: start.getTime(), end: end.getTime() };
}

function formatDate_(d) {
  return Utilities.formatDate(d, Session.getScriptTimeZone(),  'yyyy-MM-dd');
}

function fetchAllTasks_(listId, dateRange) {
  const all = [];
  let page = 0;
  let hasMore = true;

  while (hasMore) {
    const tasks = getTasks_(listId, page, dateRange);
    Logger.log('  Page ' + page + ': ' + tasks.length + ' tasks');
    if (tasks.length === 0) { hasMore = false; break; }

    const filtered = tasks.filter(t => {
      const pe = getPrimeiraEdicao_(t);
      if (!pe) return false;
      const ts = pe.getTime();
      return ts >= dateRange.start && ts <= dateRange.end;
    });

    all.push(...filtered);
    page++;
    if (tasks.length < 100) hasMore = false;

    // GAS safety: avoid timeout on huge lists
    if (page > 20) { hasMore = false; break; }
  }

  return all;
}

function calculatePontos_(tasks) {
  const editorMap = {};
  const editorTaskIds = {}; // editorId -> [taskId, ...]
  const editorFds = {}; // editorId -> { tasks, bonus }
  const editorTaskWeights = {}; // editorId -> [peso, peso, ...]
  const editorTaskNames = {}; // editorId -> [{ name, pontos }, ...]
  const editorTurboTasks = {}; // editorId -> [{ name, task_id, date, pontos }]
  const unmatched = [];

  tasks.forEach(task => {
    const pontos = getPontos_(task);
    const editors = extractEditors_(task);
    const pe = getPrimeiraEdicao_(task);
    const dateStr = pe ? formatDate_(pe) : null;
    const fds = isFdsTask_(task);

    if (pontos === null) {
      unmatched.push({ task_id: task.id, task_name: task.name, reason: 'Sem pontos' });
      return;
    }
    if (editors.length === 0) {
      unmatched.push({ task_id: task.id, task_name: task.name, reason: 'Sem editor' });
      return;
    }

    const split = editors.length;
    editors.forEach(editor => {
      if (!editorMap[editor.id]) {
        var displayName = CONFIG.NAME_ALIASES[(editor.name || '').toLowerCase()] || editor.name;
        editorMap[editor.id] = { id: editor.id, name: displayName, team: classifyTeam_(editor.name), tasks_count: 0, pontos: 0, daily: {} };
      }
      const ed = editorMap[editor.id];
      const pts = pontos / split;
      ed.tasks_count += 1 / split;
      ed.pontos += pts;
      if (dateStr) {
        ed.daily[dateStr] = (ed.daily[dateStr] || 0) + pts;
      }

      // Track task IDs for turbinho + task weights for freela bonus
      if (!editorTaskIds[editor.id]) editorTaskIds[editor.id] = [];
      editorTaskIds[editor.id].push(task.id);
      if (!editorTaskWeights[editor.id]) editorTaskWeights[editor.id] = [];
      editorTaskWeights[editor.id].push(pontos);
      if (!editorTaskNames[editor.id]) editorTaskNames[editor.id] = [];
      editorTaskNames[editor.id].push({
        name: task.name,
        pontos: pontos,
        task_id: task.id,
        primeira_edicao: dateStr,
        status: task.status ? task.status.status : '',
        status_color: task.status ? task.status.color : '',
        is_turbo: isTurboTask_(task),
      });

      // Track turbo-tagged tasks
      if (isTurboTask_(task)) {
        if (!editorTurboTasks[editor.id]) editorTurboTasks[editor.id] = [];
        editorTurboTasks[editor.id].push({ name: task.name, task_id: task.id, date: dateStr, pontos: pontos });
      }

      // Track FDS tasks by weight
      if (fds) {
        if (!editorFds[editor.id]) editorFds[editor.id] = { tasks: [], bonus: 0 };
        var fdsValue = CONFIG.BONUS.fds.perTask[pontos] || 0;
        editorFds[editor.id].tasks.push({ peso: pontos, valor: fdsValue });
        editorFds[editor.id].bonus += fdsValue / split;
      }
    });
  });

  // Round
  const editors = Object.values(editorMap).map(e => {
    e.tasks_count = Math.round(e.tasks_count);
    e.pontos = Math.round(e.pontos * 10) / 10;
    Object.keys(e.daily).forEach(d => { e.daily[d] = Math.round(e.daily[d] * 10) / 10; });
    return e;
  });

  return { editors, unmatched, editorTaskIds, editorFds, editorTaskWeights, editorTaskNames, editorTurboTasks };
}

function matchList_(name, list) {
  const n = (name || '').toLowerCase();
  return list.some(f => n.includes(f));
}

function isTimeFixo_(name) { return matchList_(name, CONFIG.TIME_FIXO); }

function classifyTeam_(name) {
  if (matchList_(name, CONFIG.TIME_FIXO)) return 'fixed';
  if (matchList_(name, CONFIG.TIME_IA)) return 'ia';
  if (matchList_(name, CONFIG.FREELAS)) return 'freela';
  return 'freela'; // default: unknown goes to freela
}

function calculateTurbo_(editors, editorTurboTasks) {
  const turboDays = {};
  editors.forEach(editor => {
    if (!isTimeFixo_(editor.name)) return;
    const tasks = editorTurboTasks[editor.id] || [];
    if (tasks.length === 0) return;

    // Group by date for display
    const byDate = {};
    tasks.forEach(t => {
      var d = t.date || 'sem-data';
      if (!byDate[d]) byDate[d] = { date: d, pontos: editor.daily[d] || 0, turbo_count: 0 };
      byDate[d].turbo_count++;
    });

    turboDays[editor.id] = {
      name: editor.name,
      count: tasks.length,
      total_bonus: tasks.length * CONFIG.BONUS.turbo.value,
      tasks: tasks,
      days: Object.values(byDate).sort((a, b) => a.date.localeCompare(b.date)),
    };
  });
  return turboDays;
}

function getBulkTimeInStatus_(taskIds) {
  // ClickUp bulk endpoint: GET /task/bulk_time_in_status/task_ids?task_ids=a&task_ids=b
  // Returns { taskId: { current_status, status_history }, ... }
  var qs = taskIds.map(function(id) { return 'task_ids=' + id; }).join('&');
  try {
    return clickupGet_('/task/bulk_time_in_status/task_ids?' + qs);
  } catch (e) {
    Logger.log('Bulk time_in_status error: ' + e.message);
    return {};
  }
}

function calculateTurbinho_(editors, editorTaskIds) {
  const turbinhoData = {};
  const allTaskIds = {};

  // Collect unique task IDs
  Object.values(editorTaskIds).forEach(ids => {
    ids.forEach(id => { allTaskIds[id] = true; });
  });

  const uniqueIds = Object.keys(allTaskIds);
  Logger.log('Turbinho: Checking status history for ' + uniqueIds.length + ' tasks...');

  // Safety: skip Turbinho if too many tasks (GAS 6-min timeout risk)
  if (uniqueIds.length > 500) {
    Logger.log('Turbinho: SKIPPED — too many tasks (' + uniqueIds.length + '). Limit is 500.');
    return turbinhoData;
  }

  // Use bulk endpoint in batches of 100 (ClickUp limit)
  const taskAjusteMap = {};
  for (var i = 0; i < uniqueIds.length; i += 100) {
    var batch = uniqueIds.slice(i, i + 100);
    Logger.log('Turbinho bulk check: ' + (i + batch.length) + '/' + uniqueIds.length);
    var bulkResult = getBulkTimeInStatus_(batch);

    batch.forEach(function(taskId) {
      var data = bulkResult[taskId];
      if (!data || !data.status_history) {
        taskAjusteMap[taskId] = false;
        return;
      }
      taskAjusteMap[taskId] = data.status_history.some(function(s) {
        return CONFIG.BONUS.ajusteStatuses.indexOf((s.status || '').toLowerCase()) !== -1;
      });
    });
  }

  // Calculate per editor (only time fixo)
  editors.forEach(editor => {
    if (!isTimeFixo_(editor.name)) return;
    const taskIds = editorTaskIds[editor.id] || [];
    const semAjuste = taskIds.filter(id => !taskAjusteMap[id]).length;
    const comAjuste = taskIds.filter(id => taskAjusteMap[id]).length;

    if (semAjuste > 0) {
      turbinhoData[editor.id] = {
        name: editor.name,
        total_tasks: taskIds.length,
        sem_ajuste: semAjuste,
        com_ajuste: comAjuste,
        bonus: semAjuste * CONFIG.BONUS.turbinho.value,
      };
    }
  });

  return turbinhoData;
}

function generateReport_(counts, turboDays, turbinhoData, month, totalTasks) {
  const { editors, unmatched, editorFds, editorTaskWeights, editorTaskNames } = counts;

  // Rank: only time fixo editors compete for ranking/bonus
  const fixedEditors = editors.filter(e => isTimeFixo_(e.name));
  const otherEditors = editors.filter(e => !isTimeFixo_(e.name));
  fixedEditors.sort((a, b) => b.pontos - a.pontos);
  fixedEditors.forEach((e, i) => { e.rank = i + 1; });
  otherEditors.sort((a, b) => b.pontos - a.pontos);

  // Assign bonus + tasks — fixed team only
  fixedEditors.forEach(e => {
    const bonusEntry = CONFIG.BONUS.productivity.find(b => b.rank === e.rank);
    const prodBonus = bonusEntry ? bonusEntry.value : 0;
    const turboData = turboDays[e.id];
    const turboBonus = turboData ? turboData.total_bonus : 0;
    const turbinho = turbinhoData[e.id];
    const turbinhoBonus = turbinho ? turbinho.bonus : 0;
    const fdsData = editorFds[e.id];
    const fdsBonus = fdsData ? Math.round(fdsData.bonus * 100) / 100 : 0;
    const fdsCount = fdsData ? fdsData.tasks.length : 0;
    e.bonus = {
      productivity: prodBonus,
      turbo: turboBonus,
      turbo_days: turboData ? turboData.count : 0,
      turbo_tasks: turboData ? turboData.tasks : [],
      turbinho: turbinhoBonus,
      turbinho_count: turbinho ? turbinho.sem_ajuste : 0,
      fds: fdsBonus,
      fds_count: fdsCount,
      total: prodBonus + turboBonus + turbinhoBonus + fdsBonus,
    };
    e.tasks = (editorTaskNames[e.id] || []).map(t => ({
      name: t.name, pts: t.pontos, task_id: t.task_id,
      primeira_edicao: t.primeira_edicao, status: t.status, status_color: t.status_color,
      is_turbo: t.is_turbo || false,
    }));
  });

  // Assign bonus — other editors (freelas get per-task by weight, rest get nothing)
  otherEditors.forEach(e => {
    if (e.team === 'freela') {
      const weights = editorTaskWeights[e.id] || [];
      const freelaTotal = weights.reduce((sum, peso) => sum + (CONFIG.BONUS.freelaPerTask[peso] || 0), 0);
      const tasks = (editorTaskNames[e.id] || []).map(t => ({
        name: t.name, pts: t.pontos, task_id: t.task_id,
        primeira_edicao: t.primeira_edicao, status: t.status, status_color: t.status_color,
        is_turbo: t.is_turbo || false,
      }));
      e.bonus = { freelaTotal: Math.round(freelaTotal * 100) / 100, tasks: tasks };
    } else {
      e.bonus = { productivity: 0, turbo: 0, turbo_days: 0, turbinho: 0, turbinho_count: 0, fds: 0, total: 0 };
    }
  });

  const allEditors = fixedEditors.concat(otherEditors);

  return {
    metadata: {
      month, generated_at: new Date().toISOString(),
      total_tasks: totalTasks,
      meta_diaria: CONFIG.BONUS.metaDiaria,
      turbo_threshold: CONFIG.BONUS.turbo.threshold,
      unit: 'pontos',
    },
    editors: allEditors.map(e => ({
      name: e.name, team: e.team,
      totals: { raw_count: e.tasks_count, pontos: e.pontos },
      daily: e.daily, rank: e.rank, bonus: e.bonus,
      tasks: e.tasks || (e.bonus && e.bonus.tasks) || [],
    })),
    turbo_days: turboDays,
    turbinho_summary: turbinhoData,
    summary: {
      total_pontos: Math.round(allEditors.reduce((a, e) => a + e.pontos, 0) * 10) / 10,
      total_editors: allEditors.length,
      ranking: fixedEditors.map(e => ({ name: e.name, rank: e.rank, pontos: e.pontos })),
    },
    unmatched,
  };
}

// ─── Entry Points ────────────────────────────────────────────────────────────

/**
 * Main function — runs on trigger (2x/day).
 * Fetches data, calculates, caches result.
 */
function videoCounterMain(customMonth) {
  const now = new Date();
  const month = customMonth || Utilities.formatDate(now, Session.getScriptTimeZone(), 'yyyy-MM');

  Logger.log('VideoCounter: Starting for month ' + month);

  const dateRange = getMonthRange_(month);
  const lists = [
    { id: CONFIG.LIST_IDS.producao, name: 'Produção de Criativos', team: 'fixed' },
    { id: CONFIG.LIST_IDS.filaFixo, name: 'Fila de Edição (fixo)', team: 'fixed' },
    { id: CONFIG.LIST_IDS.filaFreelas, name: 'Fila de Edição FREELAS', team: 'freela' },
  ];

  let allTasks = [];
  lists.forEach(list => {
    try {
      Logger.log('Querying: ' + list.name);
      const tasks = fetchAllTasks_(list.id, dateRange);
      if (list.team === 'freela') {
        tasks.forEach(t => { t._team = 'freela'; });
      }
      allTasks = allTasks.concat(tasks);
    } catch (e) {
      Logger.log('Error fetching ' + list.name + ': ' + e.message);
    }
  });

  Logger.log('Total tasks: ' + allTasks.length);

  const counts = calculatePontos_(allTasks);

  // Tag freelas
  counts.editors.forEach(editor => {
    const freelaTasks = allTasks.filter(t =>
      t._team === 'freela' && extractEditors_(t).some(e => e.id === editor.id)
    );
    const totalTasks = allTasks.filter(t =>
      extractEditors_(t).some(e => e.id === editor.id)
    );
    if (freelaTasks.length > 0 && freelaTasks.length >= totalTasks.length / 2) {
      editor.team = 'freela';
    }
  });

  const turboDays = calculateTurbo_(counts.editors, counts.editorTurboTasks);
  const turbinhoData = calculateTurbinho_(counts.editors, counts.editorTaskIds);
  const report = generateReport_(counts, turboDays, turbinhoData, month, allTasks.length);

  // Cache result in Script Properties (persists between runs)
  const cache = PropertiesService.getScriptProperties();
  cache.setProperty(CONFIG.CACHE_KEY, JSON.stringify(report));
  cache.setProperty(CONFIG.CACHE_KEY + '_TIMESTAMP', new Date().toISOString());

  Logger.log('VideoCounter: Done. ' + report.summary.total_pontos + ' pontos, ' +
    report.summary.total_editors + ' editors');

  return report;
}

/**
 * Web App endpoint — returns cached JSON.
 * Deploy as Web App to get URL for widget.
 */
function doGet(e) {
  const output = ContentService.createTextOutput();
  output.setMimeType(ContentService.MimeType.JSON);

  const requestedMonth = e && e.parameter && e.parameter.month ? e.parameter.month : null;
  const cache = PropertiesService.getScriptProperties();

  if (requestedMonth) {
    // Specific month requested — check cache or generate
    const cacheKey = CONFIG.CACHE_KEY + '_' + requestedMonth;
    const cached = cache.getProperty(cacheKey);
    if (cached) {
      const data = JSON.parse(cached);
      data.metadata.cached_at = cache.getProperty(cacheKey + '_TS') || 'unknown';
      output.setContent(JSON.stringify(data));
    } else {
      const report = videoCounterMain(requestedMonth);
      cache.setProperty(cacheKey, JSON.stringify(report));
      cache.setProperty(cacheKey + '_TS', new Date().toISOString());
      output.setContent(JSON.stringify(report));
    }
  } else {
    // Default: current month (cached)
    const cached = cache.getProperty(CONFIG.CACHE_KEY);
    if (cached) {
      const data = JSON.parse(cached);
      data.metadata.cached_at = cache.getProperty(CONFIG.CACHE_KEY + '_TIMESTAMP') || 'unknown';
      output.setContent(JSON.stringify(data));
    } else {
      const report = videoCounterMain();
      output.setContent(JSON.stringify(report));
    }
  }

  return output;
}

/**
 * Force refresh — can be called manually from Apps Script editor.
 */
function forceRefresh() {
  const report = videoCounterMain();
  Logger.log(JSON.stringify(report.summary, null, 2));
}
