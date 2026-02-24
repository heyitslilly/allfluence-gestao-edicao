#!/usr/bin/env node

/**
 * ClickUp Video Counter — Gestão de Edição
 *
 * Pulls data from ClickUp "Produção de Criativos" list,
 * reads "Primeira Edição" date and "Pontos" custom field,
 * calculates pontos per editor, TURBO days, ranking and bonus.
 *
 * Data source: field "Primeira Edição" (date) — not status-based.
 * Weight source: field "Pontos" (dropdown: 1, 2, 4, 5).
 *
 * Usage:
 *   node scripts/clickup-video-counter.js                    # current month
 *   node scripts/clickup-video-counter.js --month 2026-02    # specific month
 *   node scripts/clickup-video-counter.js --list fixed       # only fixed team list
 *   node scripts/clickup-video-counter.js --dry-run          # show without saving
 *
 * @module clickup-video-counter
 */

const path = require('path');
const fs = require('fs');

// Load env from project root
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

// Reuse existing ClickUp service modules
const clickup = require('../.aios-core/infrastructure/services/clickup');
const clickupClient = require('../.aios-core/infrastructure/services/clickup/client');
const { findFieldByName, parseFieldValue } = require('../.aios-core/infrastructure/services/clickup/custom-fields');

// ─── Constants ───────────────────────────────────────────────────────────────

const LIST_IDS = {
  producao: '901303868623',     // Produção de Criativos (PRINCIPAL)
  filaFixo: '901324270156',     // Fila de Edição (time fixo) - secundária
  filaFreelas: '901324715701',  // Fila de Edição FREELAS - secundária
};

// Fallback weight map (used when "Pontos" field is empty)
const WEIGHT_MAP = {
  bbb: 1,
  symphony: 1,
  ttcx: 2,
  gov: 2,
  motion: 4,
  longform: 5,
  clp: 1,
};

const BONUS_CONFIG = {
  productivity: [
    { rank: 1, value: 500 },
    { rank: 2, value: 250 },
  ],
  metaDiaria: 6, // meta mínima diária: 6 pontos
  turbo: { value: 100, threshold: 8 }, // TURBO: 8+ pontos/dia
  turbinho: { value: 10 }, // R$10 por criativo sem ajuste
  fds: { perTask: { 1: 35, 2: 50 }, tags: ['fds edição', 'feriado edição'] },
  ajusteStatuses: ['para ajustar', 'para ajustar cliente'],
  freelaPerPonto: 35,
};

// Time fixo — apenas esses editores recebem TURBO e Turbinho
const TIME_FIXO = [
  'pedro ximenes', 'lílian elen', 'lilian elen',
  'rafael nóbrega', 'rafael nobrega',
  'bruna', 'vinícius', 'vinicius', 'daniel', 'ricardo',
];
const TIME_IA = ['rafael gomes'];
const FREELAS = [
  'bianca', 'ághata', 'agatha', 'maria eduarda',
  'gabriel bonilha', 'raphael', 'saturno', 'gustavo', 'hugo',
];
const NAME_ALIASES = { 'saturno': 'Raphael (Saturno)' };

function isTimeFixo(name) {
  const n = (name || '').toLowerCase();
  return TIME_FIXO.some(f => n.includes(f));
}

function classifyTeam(name) {
  const n = (name || '').toLowerCase();
  if (TIME_FIXO.some(f => n.includes(f))) return 'fixed';
  if (TIME_IA.some(f => n.includes(f))) return 'ia';
  return 'freela';
}

// ─── Task Name Pattern ───────────────────────────────────────────────────────
// [398] [P13][MC][21/02] MODA - Thais
const TASK_NAME_PATTERN = /\[(\d+)\]\s*\[([A-Z]\d+)\]\[([A-Z]+)\]\[(\d{2}\/\d{2})\]\s*(\w+)\s*-\s*(.+?)$/i;

const CLIENT_CODE_MAP = {
  'MC': 'bbb', 'MELI': 'bbb', 'BBB': 'bbb',
  'TTCX': 'ttcx', 'GOV': 'gov', 'MG': 'motion',
  'LF': 'longform', 'SYM': 'symphony', 'CLP': 'clp',
};

const NAME_PATTERNS = [
  { regex: /bbb|react|moda|cpg|mercado\s*livre/i, type: 'bbb' },
  { regex: /ttcx|anúncio|anuncio|tiktok/i, type: 'ttcx' },
  { regex: /symphony|sinfonia|ia\b/i, type: 'symphony' },
  { regex: /motion\s*graphics?|animação|animacao/i, type: 'motion' },
  { regex: /long\s*form|youtube|podcast/i, type: 'longform' },
  { regex: /gov(erno)?|institucional/i, type: 'gov' },
  { regex: /clp|landing\s*page/i, type: 'clp' },
];

// ─── Field Extraction ────────────────────────────────────────────────────────

/**
 * Get "Primeira Edição" date from task.
 * Returns Date object or null.
 */
function getPrimeiraEdicao(task) {
  const field = findFieldByName(task, 'Primeira Edição');
  if (!field) return null;
  const val = parseFieldValue(field);
  if (!val) return null;
  const d = new Date(typeof val === 'string' ? val : parseInt(val));
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Get "Pontos" from task custom field.
 * Falls back to project type identification + WEIGHT_MAP.
 */
function getPontos(task) {
  // 1. Try "Pontos" custom field
  const pontosField = findFieldByName(task, 'Pontos');
  if (pontosField) {
    const val = parseFieldValue(pontosField);
    if (val !== null && val !== undefined) {
      const num = parseInt(String(val).replace(/[^0-9]/g, ''));
      if (!isNaN(num) && num > 0) return num;
    }
  }

  // 2. Fallback: identify project type and use WEIGHT_MAP
  const type = identifyProjectType(task);
  if (type !== 'unknown') return WEIGHT_MAP[type] || 1;

  return null; // truly unknown
}

/**
 * Identify project type from task data (fallback for Pontos)
 */
function identifyProjectType(task) {
  // Try custom field "Produto"
  const produtoField = findFieldByName(task, 'Produto');
  if (produtoField) {
    const val = parseFieldValue(produtoField);
    if (val) {
      const normalized = String(val).toLowerCase().trim();
      for (const [key] of Object.entries(WEIGHT_MAP)) {
        if (normalized.includes(key)) return key;
      }
      if (/react|moda|cpg/i.test(normalized)) return 'bbb';
      if (/anúncio|anuncio/i.test(normalized)) return 'ttcx';
      if (/sinfonia/i.test(normalized)) return 'symphony';
    }
  }

  // Try client code from task name
  const match = TASK_NAME_PATTERN.exec(task.name);
  if (match) {
    const clientCode = match[3].toUpperCase();
    if (CLIENT_CODE_MAP[clientCode]) return CLIENT_CODE_MAP[clientCode];
  }

  // Regex fallback
  for (const { regex, type } of NAME_PATTERNS) {
    if (regex.test(task.name)) return type;
  }

  return 'unknown';
}

/**
 * Extract editor from task (custom field "Editor" or assignees)
 */
function extractEditor(task) {
  const editorField = findFieldByName(task, 'Editor');
  if (editorField) {
    const val = parseFieldValue(editorField);
    if (val && Array.isArray(val) && val.length > 0) {
      return val.map(u => ({
        id: u.id,
        name: u.username || u.email || `User ${u.id}`,
      }));
    }
  }

  // No fallback to assignees — only "Editor" field counts
  return [];
}

/**
 * Check if task has FDS/feriado tag
 */
function isFdsTask(task) {
  if (!task.tags || !Array.isArray(task.tags)) return false;
  return task.tags.some(t =>
    BONUS_CONFIG.fds.tags.includes((t.name || '').toLowerCase())
  );
}

/**
 * Check if task ever went through "PARA AJUSTAR" or "PARA AJUSTAR CLIENTE"
 * Uses ClickUp time_in_status API endpoint.
 * Returns true if task had adjustment (NOT eligible for turbinho).
 */
async function hadAjuste(taskId) {
  try {
    const data = await clickupClient.get(`/task/${taskId}/time_in_status`);
    if (!data || !data.status_history) return false;
    return data.status_history.some(s =>
      BONUS_CONFIG.ajusteStatuses.includes((s.status || '').toLowerCase())
    );
  } catch {
    // If API call fails, assume no ajuste (conservative for bonus)
    return false;
  }
}

/**
 * Get month date range as Unix timestamps (ms)
 */
function getMonthRange(monthStr) {
  const [year, month] = monthStr.split('-').map(Number);
  const start = new Date(year, month - 1, 1);
  const end = new Date(year, month, 0, 23, 59, 59, 999);
  return { start: start.getTime(), end: end.getTime() };
}

/**
 * Format date as YYYY-MM-DD
 */
function formatDate(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

// ─── Data Fetching ───────────────────────────────────────────────────────────

/**
 * Fetch all tasks with "Primeira Edição" in date range (paginated)
 */
async function fetchAllTasks(listId, dateRange) {
  const allTasks = [];
  let page = 0;
  let hasMore = true;

  while (hasMore) {
    console.log(`[VideoCounter] Fetching page ${page} from list ${listId}...`);
    const tasks = await clickup.getTasks(listId, { page, include_closed: true });

    if (tasks.length === 0) {
      hasMore = false;
    } else {
      // Filter by "Primeira Edição" date in range
      const filtered = tasks.filter(t => {
        const primeiraEdicao = getPrimeiraEdicao(t);
        if (!primeiraEdicao) return false;
        const ts = primeiraEdicao.getTime();
        return ts >= dateRange.start && ts <= dateRange.end;
      });

      allTasks.push(...filtered);
      page++;

      if (tasks.length < 100) hasMore = false;
    }
  }

  console.log(`[VideoCounter] Found ${allTasks.length} tasks with "Primeira Edição" in range from list ${listId}`);
  return allTasks;
}

// ─── Calculation ─────────────────────────────────────────────────────────────

/**
 * Calculate pontos per editor, including per-day breakdown for TURBO,
 * FDS pontos tracking, and task IDs for turbinho check.
 */
function calculatePontos(tasks) {
  const editorMap = new Map();
  const dailyMap = new Map(); // "editorId:YYYY-MM-DD" -> pontos
  const unmatched = [];
  // Track per-editor task IDs and FDS data for turbinho/fds calculations
  const editorTaskIds = new Map(); // editorId -> [taskId, ...]
  const editorFds = new Map(); // editorId -> { pontos, count }

  for (const task of tasks) {
    const pontos = getPontos(task);
    const editors = extractEditor(task);
    const primeiraEdicao = getPrimeiraEdicao(task);
    const dateStr = primeiraEdicao ? formatDate(primeiraEdicao) : null;
    const fds = isFdsTask(task);

    if (pontos === null) {
      unmatched.push({
        task_id: task.id,
        task_name: task.name,
        reason: 'Campo "Pontos" vazio e tipo não identificado',
      });
      continue;
    }

    if (editors.length === 0) {
      unmatched.push({
        task_id: task.id,
        task_name: task.name,
        reason: 'Nenhum editor atribuído',
      });
      continue;
    }

    // Attribute pontos to each editor
    const splitFactor = editors.length;
    for (const editor of editors) {
      if (!editorMap.has(editor.id)) {
        editorMap.set(editor.id, {
          id: editor.id,
          name: editor.name,
          team: 'fixed',
          tasks_count: 0,
          pontos: 0,
          daily: {}, // date -> pontos
        });
      }

      const ed = editorMap.get(editor.id);
      const pts = pontos / splitFactor;
      ed.tasks_count += 1 / splitFactor;
      ed.pontos += pts;

      // Track daily pontos for TURBO
      if (dateStr) {
        ed.daily[dateStr] = (ed.daily[dateStr] || 0) + pts;
        const key = `${editor.id}:${dateStr}`;
        dailyMap.set(key, (dailyMap.get(key) || 0) + pts);
      }

      // Track task IDs for turbinho status history check
      if (!editorTaskIds.has(editor.id)) editorTaskIds.set(editor.id, []);
      editorTaskIds.get(editor.id).push(task.id);

      // Track FDS pontos
      if (fds) {
        if (!editorFds.has(editor.id)) editorFds.set(editor.id, { pontos: 0, count: 0 });
        const fd = editorFds.get(editor.id);
        fd.pontos += pts;
        fd.count += 1 / splitFactor;
      }
    }
  }

  // Round values
  const editors = Array.from(editorMap.values()).map(e => {
    e.tasks_count = Math.round(e.tasks_count);
    e.pontos = Math.round(e.pontos * 10) / 10;
    for (const d of Object.keys(e.daily)) {
      e.daily[d] = Math.round(e.daily[d] * 10) / 10;
    }
    return e;
  });

  return { editors, unmatched, dailyMap, editorTaskIds, editorFds };
}

/**
 * Calculate TURBO days per editor
 */
function calculateTurbo(editors, threshold) {
  const turboDays = {};

  for (const editor of editors) {
    if (!isTimeFixo(editor.name)) continue;
    const days = [];
    for (const [date, pontos] of Object.entries(editor.daily)) {
      if (pontos > threshold) {
        days.push({ date, pontos });
      }
    }
    if (days.length > 0) {
      turboDays[editor.id] = {
        name: editor.name,
        count: days.length,
        total_bonus: days.length * BONUS_CONFIG.turbo.value,
        days: days.sort((a, b) => a.date.localeCompare(b.date)),
      };
    }
  }

  return turboDays;
}

// ─── Turbinho Calculation ────────────────────────────────────────────────────

/**
 * Bulk check time_in_status for multiple tasks at once.
 * Uses GET /task/bulk_time_in_status/task_ids?task_ids=a&task_ids=b
 */
async function getBulkTimeInStatus(taskIds) {
  const qs = taskIds.map(id => `task_ids=${id}`).join('&');
  try {
    return await clickupClient.get(`/task/bulk_time_in_status/task_ids?${qs}`);
  } catch {
    return {};
  }
}

/**
 * Check status history for all tasks and calculate Turbinho bonus.
 * Turbinho = R$10 per task that was approved without ever going through adjustment.
 * Uses bulk endpoint (100 tasks per call) for efficiency.
 */
async function calculateTurbinho(editors, editorTaskIds) {
  const turbinhoData = {};
  const allTaskIds = new Set();

  // Collect all unique task IDs
  for (const [, taskIds] of editorTaskIds) {
    for (const id of taskIds) allTaskIds.add(id);
  }

  const uniqueIds = Array.from(allTaskIds);
  console.log(`[VideoCounter] Checking status history for ${uniqueIds.length} tasks (Turbinho)...`);

  // Use bulk endpoint in batches of 100
  const taskAjusteMap = new Map();
  for (let i = 0; i < uniqueIds.length; i += 100) {
    const batch = uniqueIds.slice(i, i + 100);
    console.log(`[VideoCounter] Turbinho bulk check: ${Math.min(i + 100, uniqueIds.length)}/${uniqueIds.length}`);
    const bulkResult = await getBulkTimeInStatus(batch);

    for (const taskId of batch) {
      const data = bulkResult[taskId];
      if (!data || !data.status_history) {
        taskAjusteMap.set(taskId, false);
        continue;
      }
      const hadIt = data.status_history.some(s =>
        BONUS_CONFIG.ajusteStatuses.includes((s.status || '').toLowerCase())
      );
      taskAjusteMap.set(taskId, hadIt);
    }
  }

  // Calculate per-editor turbinho
  for (const editor of editors) {
    if (!isTimeFixo(editor.name)) continue;
    const taskIds = editorTaskIds.get(editor.id) || [];
    const semAjuste = taskIds.filter(id => !taskAjusteMap.get(id)).length;
    const comAjuste = taskIds.filter(id => taskAjusteMap.get(id)).length;

    if (semAjuste > 0) {
      turbinhoData[editor.id] = {
        name: editor.name,
        total_tasks: taskIds.length,
        sem_ajuste: semAjuste,
        com_ajuste: comAjuste,
        bonus: semAjuste * BONUS_CONFIG.turbinho.value,
      };
    }
  }

  return turbinhoData;
}

// ─── Report Generation ───────────────────────────────────────────────────────

function generateReport(counts, turboDays, turbinhoData, month, totalTasksFetched) {
  const { editors, unmatched, editorFds } = counts;

  // Rank: only time fixo editors compete for ranking/bonus
  const fixedEditors = editors.filter(e => isTimeFixo(e.name));
  const otherEditors = editors.filter(e => !isTimeFixo(e.name));
  fixedEditors.sort((a, b) => b.pontos - a.pontos);
  fixedEditors.forEach((e, i) => { e.rank = i + 1; });
  otherEditors.sort((a, b) => b.pontos - a.pontos);

  // Assign bonus — time fixo only
  for (const e of fixedEditors) {
    const bonusEntry = BONUS_CONFIG.productivity.find(b => b.rank === e.rank);
    const prodBonus = bonusEntry ? bonusEntry.value : 0;
    const turboData = turboDays[e.id];
    const turboBonus = turboData ? turboData.total_bonus : 0;
    const turbinho = turbinhoData[e.id];
    const turbinhoBonus = turbinho ? turbinho.bonus : 0;
    const fdsData = editorFds.get(e.id);
    const fdsPontos = fdsData ? Math.round(fdsData.pontos * 10) / 10 : 0;
    const fdsBonus = Math.round(fdsPontos * BONUS_CONFIG.fds.perPonto * 100) / 100;
    e.bonus = {
      productivity: prodBonus,
      turbo: turboBonus,
      turbo_days: turboData ? turboData.count : 0,
      turbinho: turbinhoBonus,
      turbinho_count: turbinho ? turbinho.sem_ajuste : 0,
      fds: fdsBonus,
      fds_pontos: fdsPontos,
      total: prodBonus + turboBonus + turbinhoBonus + fdsBonus,
    };
  }
  // Other editors: freelas get per-point, rest get nothing
  for (const e of otherEditors) {
    if (e.team === 'freela') {
      e.bonus = { freelaTotal: Math.round(e.pontos * BONUS_CONFIG.freelaPerPonto * 100) / 100 };
    } else {
      e.bonus = { productivity: 0, turbo: 0, turbo_days: 0, turbinho: 0, turbinho_count: 0, fds: 0, fds_pontos: 0, total: 0 };
    }
  }

  // Merge back: fixed first (ranked), then others
  const allEditors = [...fixedEditors, ...otherEditors];

  const totalPontos = allEditors.reduce((a, e) => a + e.pontos, 0);

  const report = {
    metadata: {
      month,
      generated_at: new Date().toISOString(),
      total_tasks: totalTasksFetched,
      meta_diaria: BONUS_CONFIG.metaDiaria,
      turbo_threshold: BONUS_CONFIG.turbo.threshold,
      unit: 'pontos',
    },
    editors: allEditors.map(e => ({
      name: e.name,
      team: e.team,
      totals: { raw_count: e.tasks_count, pontos: e.pontos },
      daily: e.daily,
      rank: e.rank,
      bonus: e.bonus,
    })),
    turbo_days: turboDays,
    turbinho_summary: turbinhoData,
    summary: {
      total_pontos: Math.round(totalPontos * 10) / 10,
      total_editors: allEditors.length,
      ranking: fixedEditors.map(e => ({
        name: e.name,
        rank: e.rank,
        pontos: e.pontos,
      })),
    },
    unmatched,
  };

  return report;
}

// ─── CLI ─────────────────────────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { month: null, list: 'all', dryRun: false, help: false };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--month': case '-m': opts.month = args[++i]; break;
      case '--list': case '-l': opts.list = args[++i]; break;
      case '--dry-run': case '-d': opts.dryRun = true; break;
      case '--help': case '-h': opts.help = true; break;
    }
  }

  if (!opts.month) {
    const now = new Date();
    opts.month = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  }

  return opts;
}

const HELP_TEXT = `
ClickUp Video Counter — Gestão de Edição AllFluence

Conta pontos por editor com base no campo "Primeira Edição" e "Pontos".

Usage:
  node scripts/clickup-video-counter.js [options]

Options:
  --month, -m <YYYY-MM>   Mês para contar (default: mês atual)
  --list, -l <name>       Filtro: all | producao | fixed | freelas (default: all)
  --dry-run, -d           Mostra resultado sem salvar arquivo
  --help, -h              Mostra esta ajuda

Output:
  docs/reports/video-count-YYYY-MM.json

Pontos:
  BBB React, Symphony, CLP  = 1 ponto
  TTCX Anúncios, Governo    = 2 pontos
  Motion Graphics            = 4 pontos
  Long Form (YouTube)        = 5 pontos

  Meta mínima diária: ${BONUS_CONFIG.metaDiaria} pontos
  TURBO: ${BONUS_CONFIG.turbo.threshold}+ pontos no dia = R$ ${BONUS_CONFIG.turbo.value}/dia
`;

async function main() {
  const opts = parseArgs();

  if (opts.help) {
    console.log(HELP_TEXT);
    process.exit(0);
  }

  console.log(`\n[VideoCounter] AllFluence Video Counter — Sistema de Pontos`);
  console.log(`[VideoCounter] Mês: ${opts.month}`);
  console.log(`[VideoCounter] Lista: ${opts.list}`);
  console.log(`[VideoCounter] Dry run: ${opts.dryRun}\n`);

  const listsToQuery = [];
  switch (opts.list) {
    case 'producao':
      listsToQuery.push({ id: LIST_IDS.producao, name: 'Produção de Criativos' });
      break;
    case 'fixed':
      listsToQuery.push({ id: LIST_IDS.filaFixo, name: 'Fila de Edição (fixo)' });
      break;
    case 'freelas':
      listsToQuery.push({ id: LIST_IDS.filaFreelas, name: 'Fila de Edição FREELAS' });
      break;
    default:
      listsToQuery.push({ id: LIST_IDS.producao, name: 'Produção de Criativos' });
      listsToQuery.push({ id: LIST_IDS.filaFixo, name: 'Fila de Edição (fixo)' });
      listsToQuery.push({ id: LIST_IDS.filaFreelas, name: 'Fila de Edição FREELAS' });
      break;
  }

  const dateRange = getMonthRange(opts.month);
  console.log(`[VideoCounter] Range: ${new Date(dateRange.start).toISOString()} → ${new Date(dateRange.end).toISOString()}\n`);

  let allTasks = [];
  for (const list of listsToQuery) {
    console.log(`[VideoCounter] Querying: ${list.name} (${list.id})`);
    try {
      const tasks = await fetchAllTasks(list.id, dateRange);
      if (list.id === LIST_IDS.filaFreelas) {
        tasks.forEach(t => { t._team = 'freela'; });
      }
      allTasks.push(...tasks);
    } catch (error) {
      console.error(`[VideoCounter] Erro em ${list.name}: ${error.message}`);
    }
  }

  console.log(`\n[VideoCounter] Total de tarefas com "Primeira Edição" no período: ${allTasks.length}\n`);

  if (allTasks.length === 0) {
    console.log('[VideoCounter] Nenhuma tarefa encontrada para este período.');
    if (opts.dryRun) process.exit(0);
  }

  // Calculate
  const counts = calculatePontos(allTasks);

  // Tag freela team
  for (const editor of counts.editors) {
    const freelaTasks = allTasks.filter(t =>
      t._team === 'freela' && extractEditor(t).some(e => e.id === editor.id)
    );
    const totalTasks = allTasks.filter(t =>
      extractEditor(t).some(e => e.id === editor.id)
    );
    if (freelaTasks.length > 0 && freelaTasks.length >= totalTasks.length / 2) {
      editor.team = 'freela';
    }
  }

  // TURBO
  const turboDays = calculateTurbo(counts.editors, BONUS_CONFIG.turbo.threshold);

  // Turbinho (status history check)
  const turbinhoData = await calculateTurbinho(counts.editors, counts.editorTaskIds);

  // Report
  const report = generateReport(counts, turboDays, turbinhoData, opts.month, allTasks.length);

  // Display
  console.log('═══════════════════════════════════════════════════');
  console.log(`  RANKING DE PONTOS — ${opts.month}`);
  console.log('═══════════════════════════════════════════════════\n');

  for (const editor of report.editors) {
    const medal = editor.rank === 1 ? '🥇' : editor.rank === 2 ? '🥈' : editor.rank === 3 ? '🥉' : `${editor.rank}º`;
    const bonus = editor.bonus.total || editor.bonus.freelaTotal || 0;
    const bonusStr = bonus > 0 ? ` (+R$ ${bonus})` : '';
    const teamTag = editor.team === 'freela' ? ' [FREELA]' : '';
    const turboTag = editor.bonus.turbo_days > 0 ? ` ⚡${editor.bonus.turbo_days}` : '';
    const turbinhoTag = editor.bonus.turbinho_count > 0 ? ` ✨${editor.bonus.turbinho_count}` : '';
    const fdsTag = editor.bonus.fds > 0 ? ` 📅R$${editor.bonus.fds}` : '';
    console.log(`  ${medal} ${editor.name}${teamTag}: ${editor.totals.pontos} pts${turboTag}${turbinhoTag}${fdsTag}${bonusStr}`);
  }

  console.log('\n───────────────────────────────────────────────────');
  console.log(`  Total: ${report.summary.total_pontos} pontos`);
  console.log(`  Editores: ${report.summary.total_editors}`);
  console.log(`  Meta diária: ${BONUS_CONFIG.metaDiaria} pts | TURBO: >${BONUS_CONFIG.turbo.threshold} pts/dia`);
  console.log('───────────────────────────────────────────────────');

  // TURBO details
  if (Object.keys(turboDays).length > 0) {
    console.log('\n⚡ TURBO Days:');
    for (const [, data] of Object.entries(turboDays)) {
      console.log(`  ${data.name}: ${data.count} dia(s) → R$ ${data.total_bonus}`);
      for (const day of data.days) {
        console.log(`    ${day.date}: ${day.pontos} pts`);
      }
    }
  }

  // Turbinho details
  if (Object.keys(turbinhoData).length > 0) {
    console.log('\n✨ Turbinho (sem ajuste):');
    for (const [, data] of Object.entries(turbinhoData)) {
      console.log(`  ${data.name}: ${data.sem_ajuste}/${data.total_tasks} criativos direto → R$ ${data.bonus}`);
    }
  }

  if (report.unmatched.length > 0) {
    console.log(`\n⚠️  ${report.unmatched.length} tarefas sem match:`);
    for (const u of report.unmatched.slice(0, 10)) {
      console.log(`    - [${u.task_id}] ${u.task_name} (${u.reason})`);
    }
    if (report.unmatched.length > 10) {
      console.log(`    ... e mais ${report.unmatched.length - 10}`);
    }
  }

  // Save
  if (!opts.dryRun) {
    const outDir = path.resolve(__dirname, '..', 'docs', 'reports');
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }
    const outFile = path.join(outDir, `video-count-${opts.month}.json`);
    fs.writeFileSync(outFile, JSON.stringify(report, null, 2));
    console.log(`\n✅ Relatório salvo em: ${outFile}`);
  } else {
    console.log('\n[dry-run] Relatório não salvo. Remova --dry-run para salvar.');
    console.log('\nJSON:');
    console.log(JSON.stringify(report, null, 2));
  }
}

main().catch(error => {
  console.error(`\n❌ Erro fatal: ${error.message}`);
  process.exit(1);
});
