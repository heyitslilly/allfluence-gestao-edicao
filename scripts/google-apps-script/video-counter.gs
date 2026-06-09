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
 *
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  🎯  COMO FUNCIONAM OS BÔNUS                                   ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║                                                                ║
 * ║  🏆 PRODUTIVIDADE (Time Fixo)                                  ║
 * ║     🥇 1º lugar no ranking mensal ──────────────── R$ 500      ║
 * ║     🥈 2º lugar no ranking mensal ──────────────── R$ 250      ║
 * ║                                                                ║
 * ║  ⚡ TURBO  +R$100 por vídeo (Time Fixo)                        ║
 * ║     Em qualquer dia que o editor fizer mais de 8 pontos.       ║
 * ║     Identificado pela tag "turbo" 🏷️ no ClickUp.              ║
 * ║                                                                ║
 * ║  📊 CUMULATIVO (Time Fixo)                                     ║
 * ║     50 pts = R$ 250 · 60 pts = R$ 500                         ║
 * ║     70 pts = R$ 750 · 80 pts = R$ 1.000                       ║
 * ║                                                                ║
 * ║  📅 FDS / FERIADO (Time Fixo)                                  ║
 * ║     Peso 1 ─────────────────────────────────────── R$ 35       ║
 * ║     Peso 2 ─────────────────────────────────────── R$ 50       ║
 * ║     Tags: "fds edição" 🏷️ ou "feriado edição" 🏷️             ║
 * ║                                                                ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║                                                                ║
 * ║  💰 FREELAS (por criativo editado)                             ║
 * ║     Peso 1 ─────────────────────────────────────── R$ 35       ║
 * ║     Peso 2 ─────────────────────────────────────── R$ 50       ║
 * ║                                                                ║
 * ╚══════════════════════════════════════════════════════════════════╝
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
    cumulativo: [
      { min: 80, value: 1000 },
      { min: 70, value: 750 },
      { min: 60, value: 500 },
      { min: 50, value: 250 },
    ],
    fds: { perTask: { 1: 35, 2: 50 }, tags: ['fds edição', 'feriado edição'] },
    freelaPerTask: { 1: 35, 2: 50 },
  },
  // Fallback weight map when "Pontos" field is empty
  WEIGHT_MAP: {
    bbb: 1, symphony: 1, ttcx: 2, gov: 2, motion: 4, longform: 5, clp: 1,
  },
  // Time fixo — apenas esses editores recebem TURBO e Cumulativo
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
  // Filtro de data na API APENAS no limite inferior (date_updated_gt), com margem
  // generosa: exclui o backlog de tarefas antigas (muito mais rápido) SEM risco de
  // perder tarefas do mês — uma tarefa com "Primeira Edição" no mês foi, por
  // definição, atualizada a partir do início do mês (setar o campo atualiza a task).
  // NUNCA usar date_updated_lt (limite superior): tarefas editadas em meses
  // seguintes (mudança de status) seriam perdidas — foi o bug que motivou remover
  // o filtro antigo. Sem dateRange (ex: diagnoseMonth) faz a varredura completa.
  // subtasks=true: inclui SUBTAREFAS na resposta (por padrão a API só traz tarefas
  // de topo). Cada subtarefa vem como uma task com seus próprios campos/tags, então
  // é avaliada normalmente por "Primeira Edição", "Editor", "Pontos" e tags (fds).
  let url = `/list/${listId}/task?page=${page}&archived=false&include_closed=true&subtasks=true`;
  if (dateRange && dateRange.start) {
    const buffer = 31 * 24 * 60 * 60 * 1000; // 31 dias de margem de segurança
    url += '&date_updated_gt=' + (dateRange.start - buffer);
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
  // O valor pode ser um número puro ("6") ou uma opção rotulada onde o
  // primeiro número é a pontuação, ex: "1 = BBB 30min", "12 = Longa 12 a 16h".
  // Pegamos apenas o PRIMEIRO número do texto (a pontuação), nunca os demais.
  const field = findField_(task, 'Pontos');
  if (field) {
    const val = parseFieldValue_(field);
    if (val !== null && val !== undefined) {
      const match = String(val).match(/\d+/);
      const num = match ? parseInt(match[0], 10) : NaN;
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

// ─── FDS & TURBO Helpers ─────────────────────────────────────────────────────

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

    // GAS safety: avoid timeout on huge lists (50 pages = 5000 tasks max)
    if (page > 50) {
      Logger.log('WARNING: Pagination limit reached at page ' + page + '. Some tasks may be missing.');
      hasMore = false; break;
    }
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

      // Track task IDs + task weights for freela bonus
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

// ─── Cumulativo Calculation ──────────────────────────────────────────────────

function calculateCumulativo_(editors) {
  const cumulData = {};
  editors.forEach(editor => {
    if (!isTimeFixo_(editor.name)) return;
    const tier = CONFIG.BONUS.cumulativo.find(t => editor.pontos >= t.min);
    if (tier) {
      cumulData[editor.id] = {
        name: editor.name,
        pontos: editor.pontos,
        threshold: tier.min,
        bonus: tier.value,
      };
    }
  });
  return cumulData;
}

function previousMonthStr_(month) {
  const parts = month.split('-').map(Number);
  let y = parts[0], m = parts[1] - 1;
  if (m < 1) { m = 12; y -= 1; }
  return y + '-' + String(m).padStart(2, '0');
}

/**
 * Soma quantos PONTOS cada editor (por nome de exibição) fez no MÊS ANTERIOR.
 * Usado APENAS como critério de desempate do bônus de ranking (500/250):
 * em empate de pontos no mês atual, vence quem fez mais PONTOS no mês anterior.
 * Prefere o relatório do mês anterior já em cache (sem custo de API); se não
 * existir, busca as tarefas na API. Retorna { prevMonth, counts: { "Nome": pts } }.
 */
function getPrevMonthCounts_(month) {
  const prevMonth = previousMonthStr_(month);
  const cache = PropertiesService.getScriptProperties();
  const counts = {};

  // 1. Preferir relatório do mês anterior já em cache (sem chamada de API)
  const cached = cache.getProperty(CONFIG.CACHE_KEY + '_' + prevMonth);
  if (cached) {
    try {
      const data = JSON.parse(cached);
      (data.editors || []).forEach(function(ed) {
        if (ed && ed.name && ed.totals) {
          counts[ed.name] = (counts[ed.name] || 0) + (ed.totals.pontos || 0);
        }
      });
      return { prevMonth: prevMonth, counts: counts };
    } catch (err) {
      Logger.log('getPrevMonthCounts_: cache invalido, buscando na API. ' + err.message);
    }
  }

  // 2. Sem cache → buscar tarefas do mês anterior e somar pontos por editor
  const dateRange = getMonthRange_(prevMonth);
  const listIds = [CONFIG.LIST_IDS.producao, CONFIG.LIST_IDS.filaFixo, CONFIG.LIST_IDS.filaFreelas];
  listIds.forEach(function(listId) {
    try {
      fetchAllTasks_(listId, dateRange).forEach(function(task) {
        const pontos = getPontos_(task);
        if (pontos === null) return;
        const editors = extractEditors_(task);
        if (editors.length === 0) return;
        const split = editors.length;
        editors.forEach(function(ed) {
          var displayName = CONFIG.NAME_ALIASES[(ed.name || '').toLowerCase()] || ed.name;
          counts[displayName] = (counts[displayName] || 0) + pontos / split;
        });
      });
    } catch (err) {
      Logger.log('getPrevMonthCounts_: erro na lista ' + listId + ': ' + err.message);
    }
  });
  Object.keys(counts).forEach(function(k) { counts[k] = Math.round(counts[k] * 10) / 10; });
  return { prevMonth: prevMonth, counts: counts };
}

function generateReport_(counts, turboDays, cumulData, prevMonthData, month, totalTasks) {
  const { editors, unmatched, editorFds, editorTaskWeights, editorTaskNames } = counts;

  // Rank: only time fixo editors compete for ranking/bonus
  const fixedEditors = editors.filter(e => isTimeFixo_(e.name));
  const otherEditors = editors.filter(e => !isTimeFixo_(e.name));

  // Desempate do bônus de ranking (500/250): em EMPATE DE PONTOS, fica melhor
  // colocado quem fez mais PONTOS no MÊS ANTERIOR.
  const prevCounts = (prevMonthData && prevMonthData.counts) || {};
  const prevMonthLabel = (prevMonthData && prevMonthData.prevMonth) || '';
  function prevPtsOf(e) { return prevCounts[e.name] || 0; }

  fixedEditors.sort(function(a, b) {
    if (b.pontos !== a.pontos) return b.pontos - a.pontos;
    return prevPtsOf(b) - prevPtsOf(a); // empate → mais pontos no mês anterior na frente
  });
  fixedEditors.forEach((e, i) => { e.rank = i + 1; });
  otherEditors.sort((a, b) => b.pontos - a.pontos);

  // Assign bonus + tasks — fixed team only
  fixedEditors.forEach(e => {
    const bonusEntry = CONFIG.BONUS.productivity.find(b => b.rank === e.rank);
    const prodBonus = bonusEntry ? bonusEntry.value : 0;
    const turboData = turboDays[e.id];
    const turboBonus = turboData ? turboData.total_bonus : 0;
    const cumul = cumulData[e.id];
    const cumulBonus = cumul ? cumul.bonus : 0;
    const fdsData = editorFds[e.id];
    const fdsBonus = fdsData ? Math.round(fdsData.bonus * 100) / 100 : 0;
    const fdsCount = fdsData ? fdsData.tasks.length : 0;
    e.bonus = {
      productivity: prodBonus,
      turbo: turboBonus,
      turbo_days: turboData ? turboData.count : 0,
      turbo_tasks: turboData ? turboData.tasks : [],
      cumulativo: cumulBonus,
      fds: fdsBonus,
      fds_count: fdsCount,
      total: prodBonus + turboBonus + cumulBonus + fdsBonus,
    };
    e.tasks = (editorTaskNames[e.id] || []).map(t => ({
      name: t.name, pts: t.pontos, task_id: t.task_id,
      primeira_edicao: t.primeira_edicao, status: t.status, status_color: t.status_color,
      is_turbo: t.is_turbo || false,
    }));
  });

  // ── Explicação do desempate do bônus de ranking (500/250) ──
  // Só anexa quando o empate de PONTOS envolve uma posição de bônus (1º ou 2º).
  // O resultado fica em e.bonus.rank_tiebreak.note para o widget mostrar no hover.
  if (fixedEditors.length >= 2) {
    const byPts = {};
    fixedEditors.forEach(function(e) {
      const k = String(e.pontos);
      (byPts[k] = byPts[k] || []).push(e);
    });
    Object.keys(byPts).forEach(function(k) {
      const grp = byPts[k];
      if (grp.length < 2) return; // sem empate
      const minRank = Math.min.apply(null, grp.map(function(e) { return e.rank; }));
      if (minRank > 2) return; // empate fora do 1º/2º lugar → não muda bônus de ranking
      // ordena pelo desempate: mais pontos no mês anterior = melhor colocação
      const ordered = grp.slice().sort(function(a, b) { return prevPtsOf(b) - prevPtsOf(a); });
      const involved = ordered.map(function(e) {
        return {
          name: e.name,
          prev_pontos: prevPtsOf(e),
          rank: e.rank,
          bonus: (e.bonus && e.bonus.productivity) || 0,
        };
      });
      const top = involved[0], second = involved[1];
      var note;
      if (top.prev_pontos === second.prev_pontos) {
        note = 'Empate de pontos (' + grp[0].pontos + ' pts) entre '
          + involved.map(function(x) { return x.name.split(' ')[0]; }).join(', ')
          + '. No mes anterior (' + (prevMonthLabel || '—') + ') o empate tambem persiste ('
          + top.prev_pontos + ' pts cada) — mantida a ordem atual.';
      } else {
        note = 'Empate de pontos (' + grp[0].pontos + ' pts). Desempate pelos pontos do mes anterior ('
          + (prevMonthLabel || '—') + '): '
          + involved.map(function(x) { return x.name.split(' ')[0] + ' = ' + x.prev_pontos + ' pts'; }).join(' · ')
          + '. ' + top.name.split(' ')[0] + ' fica em ' + top.rank + 'º'
          + (top.bonus > 0 ? ' (R$' + top.bonus + ')' : ' (sem bonus)') + '.';
      }
      grp.forEach(function(e) {
        if (!e.bonus) e.bonus = {};
        e.bonus.rank_tiebreak = {
          prev_month: prevMonthLabel,
          pontos: grp[0].pontos,
          involved: involved,
          note: note,
        };
      });
    });
  }

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
      e.bonus = { productivity: 0, turbo: 0, turbo_days: 0, cumulativo: 0, fds: 0, total: 0 };
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
    cumulativo_summary: cumulData,
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
  // Quando chamado por um ACIONADOR, o GAS passa um objeto de evento como 1º
  // argumento. Por isso só usamos customMonth se for realmente um texto "yyyy-MM";
  // caso contrário (acionador, ou sem argumento) usa o mês atual.
  const month = (typeof customMonth === 'string' && customMonth)
    ? customMonth
    : Utilities.formatDate(now, Session.getScriptTimeZone(), 'yyyy-MM');

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
  const cumulData = calculateCumulativo_(counts.editors);

  // Desempate do ranking: só vale a pena buscar o mês anterior (custo de API) se
  // houver EMPATE DE PONTOS afetando o 1º ou 2º lugar. Sem empate no topo, pula.
  const fixedPtsDesc = counts.editors
    .filter(e => isTimeFixo_(e.name))
    .map(e => e.pontos)
    .sort((a, b) => b - a);
  const needsTiebreak =
    (fixedPtsDesc.length >= 2 && fixedPtsDesc[0] === fixedPtsDesc[1]) ||
    (fixedPtsDesc.length >= 3 && fixedPtsDesc[1] === fixedPtsDesc[2]);
  const prevMonthData = needsTiebreak ? getPrevMonthCounts_(month) : null;
  if (needsTiebreak) Logger.log('Empate no topo detectado — buscando mes anterior para desempate.');

  const report = generateReport_(counts, turboDays, cumulData, prevMonthData, month, allTasks.length);

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
  const forceRefresh = e && e.parameter && e.parameter.refresh === 'true';
  const cache = PropertiesService.getScriptProperties();

  // Cache TTL alinhado à atualização diária: mês atual = 26h (não recalcula
  // sozinho dentro do dia — quem atualiza é o acionador diário warmRecentMonths;
  // a folga de 2h cobre variação de horário do acionador). Meses passados = 7 dias.
  // O botão "Atualizar" (widget de gestão) força recalcular quando precisar.
  const CURRENT_MONTH_TTL = 26 * 60 * 60 * 1000;
  const PAST_MONTH_TTL = 7 * 24 * 60 * 60 * 1000;
  const now = new Date();
  const currentMonth = Utilities.formatDate(now, Session.getScriptTimeZone(), 'yyyy-MM');

  if (requestedMonth) {
    const cacheKey = CONFIG.CACHE_KEY + '_' + requestedMonth;
    const tsKey = cacheKey + '_TS';
    const cached = cache.getProperty(cacheKey);
    const cachedTs = cache.getProperty(tsKey);
    const ttl = requestedMonth === currentMonth ? CURRENT_MONTH_TTL : PAST_MONTH_TTL;
    const isExpired = !cachedTs || (now.getTime() - new Date(cachedTs).getTime()) > ttl;

    if (cached && !forceRefresh && !isExpired) {
      const data = JSON.parse(cached);
      data.metadata.cached_at = cachedTs || 'unknown';
      data.metadata.cache_ttl_hours = ttl / (60 * 60 * 1000);
      output.setContent(JSON.stringify(data));
    } else {
      Logger.log('doGet: Regenerating data for ' + requestedMonth +
        (forceRefresh ? ' (forced)' : isExpired ? ' (expired)' : ' (no cache)'));
      const report = videoCounterMain(requestedMonth);
      const tsNow = new Date().toISOString();
      cache.setProperty(cacheKey, JSON.stringify(report));
      cache.setProperty(tsKey, tsNow);
      report.metadata.cached_at = tsNow;
      report.metadata.cache_ttl_hours = ttl / (60 * 60 * 1000);
      output.setContent(JSON.stringify(report));
    }
  } else {
    // Default: current month
    const cached = cache.getProperty(CONFIG.CACHE_KEY);
    const cachedTs = cache.getProperty(CONFIG.CACHE_KEY + '_TIMESTAMP');
    const isExpired = !cachedTs || (now.getTime() - new Date(cachedTs).getTime()) > CURRENT_MONTH_TTL;

    if (cached && !forceRefresh && !isExpired) {
      const data = JSON.parse(cached);
      data.metadata.cached_at = cachedTs || 'unknown';
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

/**
 * Diagnóstico — roda manualmente para entender dados faltantes.
 * Mostra quantas tasks existem, quantas têm "Primeira Edição", quantas têm "Editor", etc.
 * Execute: selecione diagnoseMonth no editor e clique Run.
 */
function diagnoseMonth() {
  const month = '2026-02'; // ← mude aqui para o mês desejado
  const dateRange = getMonthRange_(month);

  const lists = [
    { id: CONFIG.LIST_IDS.producao, name: 'Produção de Criativos' },
    { id: CONFIG.LIST_IDS.filaFixo, name: 'Fila de Edição (fixo)' },
    { id: CONFIG.LIST_IDS.filaFreelas, name: 'Fila de Edição FREELAS' },
  ];

  let grandTotal = 0;
  let grandWithPE = 0;
  let grandInRange = 0;
  let grandWithEditor = 0;
  let grandWithPontos = 0;
  const fieldNames = {};

  lists.forEach(list => {
    Logger.log('\n━━━ ' + list.name + ' (ID: ' + list.id + ') ━━━');
    let page = 0;
    let total = 0;
    let withPE = 0;
    let peInRange = 0;
    let withEditor = 0;
    let withPontos = 0;
    let hasMore = true;

    while (hasMore) {
      const tasks = getTasks_(list.id, page);
      if (tasks.length === 0) break;
      total += tasks.length;

      tasks.forEach(t => {
        // Catalog all custom field names (first page only)
        if (page === 0 && t.custom_fields) {
          t.custom_fields.forEach(cf => { fieldNames[cf.name] = (fieldNames[cf.name] || 0) + 1; });
        }

        const pe = getPrimeiraEdicao_(t);
        if (pe) {
          withPE++;
          const ts = pe.getTime();
          if (ts >= dateRange.start && ts <= dateRange.end) {
            peInRange++;
            const editors = extractEditors_(t);
            if (editors.length > 0) withEditor++;
            const pontos = getPontos_(t);
            if (pontos !== null) withPontos++;
          }
        }
      });

      page++;
      if (tasks.length < 100) hasMore = false;
      if (page > 80) { Logger.log('  ⚠️ Stopped at page 80'); break; }
    }

    Logger.log('  Total tasks na lista: ' + total + ' (' + page + ' pages)');
    Logger.log('  Com "Primeira Edição": ' + withPE);
    Logger.log('  "Primeira Edição" em ' + month + ': ' + peInRange);
    Logger.log('  Com Editor (no range): ' + withEditor);
    Logger.log('  Com Pontos (no range): ' + withPontos);
    Logger.log('  SEM "Primeira Edição": ' + (total - withPE));

    grandTotal += total;
    grandWithPE += withPE;
    grandInRange += peInRange;
    grandWithEditor += withEditor;
    grandWithPontos += withPontos;
  });

  Logger.log('\n━━━ RESUMO GERAL ━━━');
  Logger.log('Total tasks (todas as listas): ' + grandTotal);
  Logger.log('Com "Primeira Edição": ' + grandWithPE);
  Logger.log('"Primeira Edição" em ' + month + ': ' + grandInRange);
  Logger.log('Com Editor (no range): ' + grandWithEditor);
  Logger.log('Com Pontos (no range): ' + grandWithPontos);
  Logger.log('Perdas:');
  Logger.log('  Sem "Primeira Edição": ' + (grandTotal - grandWithPE) + ' tasks ignoradas');
  Logger.log('  Fora do range ' + month + ': ' + (grandWithPE - grandInRange) + ' tasks (outros meses)');
  Logger.log('  Sem Editor: ' + (grandInRange - grandWithEditor) + ' tasks sem crédito');
  Logger.log('  Sem Pontos: ' + (grandInRange - grandWithPontos) + ' tasks sem peso');

  Logger.log('\n━━━ CAMPOS CUSTOM ENCONTRADOS (amostra) ━━━');
  Object.keys(fieldNames).sort().forEach(name => {
    Logger.log('  "' + name + '" — aparece em ' + fieldNames[name] + ' tasks');
  });
}

/**
 * Limpa TODO o cache de resultados (mês atual + todos os meses já abertos),
 * SEM apagar a CLICKUP_API_KEY. Rode esta função uma vez no editor depois de
 * colar uma versão nova do código — assim todo mês recalcula do zero na
 * próxima vez que for aberto no widget.
 * (No editor: selecione "clearCache" na barra de funções e clique Executar.)
 */
function clearCache() {
  const cache = PropertiesService.getScriptProperties();
  const all = cache.getProperties();
  let removed = 0;
  Object.keys(all).forEach(function(k) {
    if (k.indexOf(CONFIG.CACHE_KEY) === 0) {
      cache.deleteProperty(k);
      removed++;
    }
  });
  Logger.log('Cache limpo: ' + removed + ' chave(s) removida(s). CLICKUP_API_KEY preservada.');
}

/**
 * Pré-aquece o cache dos últimos meses (mais antigo → mais novo), para o widget
 * abrir sempre do cache (rápido) e nunca dar "Failed to fetch" por timeout.
 *
 * Rode no EDITOR (não tem o limite de tempo do navegador) ou — melhor ainda —
 * aponte o acionador agendado para esta função em vez de videoCounterMain.
 * Aquecer do mês mais antigo p/ o mais novo faz o desempate usar o cache do mês
 * anterior em vez de buscar tudo de novo na API.
 */
function warmRecentMonths() {
  const MONTHS_BACK = 2; // mês atual + 2 anteriores
  const now = new Date();
  const tz = Session.getScriptTimeZone();
  const cache = PropertiesService.getScriptProperties();

  const months = [];
  for (let i = MONTHS_BACK; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push(Utilities.formatDate(d, tz, 'yyyy-MM'));
  }

  months.forEach(function(m) {
    Logger.log('Aquecendo ' + m + '...');
    const report = videoCounterMain(m);
    const key = CONFIG.CACHE_KEY + '_' + m;
    cache.setProperty(key, JSON.stringify(report));
    cache.setProperty(key + '_TS', new Date().toISOString());
    Logger.log('  ' + m + ' OK — ' + report.summary.total_pontos + ' pts, ' +
      report.summary.total_editors + ' editores');
  });
  Logger.log('warmRecentMonths: concluido (' + months.join(', ') + ')');
}
