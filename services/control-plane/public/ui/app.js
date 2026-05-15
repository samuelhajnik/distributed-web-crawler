import {
  cancelCrawlRun,
  getGraph,
  getSummary,
  getSummaryIfExists,
  getUrls,
  listCrawlRuns,
  startCrawl
} from "./api.js";
import { buildLineageGraph, formatNodeInfo } from "./graph-model.js";
import { createLineageGraphView } from "./graph-view.js";
import { createRunPoller } from "./poller.js";

const el = {
  form: document.getElementById("start-form"),
  seedUrl: document.getElementById("seed-url"),
  maxPages: document.getElementById("max-pages"),
  maxDepth: document.getElementById("max-depth"),
  scopeMode: document.getElementById("scope-mode"),
  includeDocuments: document.getElementById("include-documents"),
  followRedirects: document.getElementById("follow-redirects"),
  demoDelayMs: document.getElementById("demo-delay-ms"),
  requestTimeoutMs: document.getElementById("request-timeout-ms"),
  maxRetries: document.getElementById("max-retries"),
  startBtn: document.getElementById("start-btn"),
  startStatus: document.getElementById("start-status"),
  runId: document.getElementById("run-id"),
  runSeedUrl: document.getElementById("run-seed-url"),
  runStatus: document.getElementById("run-status"),
  queued: document.getElementById("c-queued"),
  inProgress: document.getElementById("c-in-progress"),
  visited: document.getElementById("c-visited"),
  redirect301: document.getElementById("c-redirect-301"),
  forbidden: document.getElementById("c-forbidden"),
  notFound: document.getElementById("c-not-found"),
  httpTerminal: document.getElementById("c-http-terminal"),
  failed: document.getElementById("c-failed"),
  cancelled: document.getElementById("c-cancelled"),
  discovered: document.getElementById("c-discovered"),
  graphActivityBadge: document.getElementById("graph-activity-badge"),
  pollStatus: document.getElementById("poll-status"),
  graphMeta: document.getElementById("graph-meta"),
  graphWarn: document.getElementById("graph-warn"),
  graphContainer: document.getElementById("graph-container"),
  graphNodeInfo: document.getElementById("graph-node-info"),
  runConfig: document.getElementById("run-config"),
  urlsBody: document.getElementById("urls-body"),
  urlsLoading: document.getElementById("urls-loading"),
  graphRefreshSlider: document.getElementById("graph-refresh-slider"),
  graphRefreshValue: document.getElementById("graph-refresh-value"),
  graphAutoCenterToggle: document.getElementById("graph-auto-center-toggle"),
  graphFitBtn: document.getElementById("graph-fit-btn"),
  graphPanelDetails: document.getElementById("graph-panel-details"),
  urlsPrev: document.getElementById("urls-prev"),
  urlsNext: document.getElementById("urls-next"),
  urlsPageStatus: document.getElementById("urls-page-status"),
  historyBody: document.getElementById("history-body"),
  historyLoading: document.getElementById("history-loading"),
  historyStatus: document.getElementById("history-status"),
  themeToggle: document.getElementById("theme-toggle")
};

const ACTIVE_RUN_STORAGE_KEY = "crawler-ui-active-run-id";
const THEME_STORAGE_KEY = "crawler-ui-theme";
const HISTORY_LIST_LIMIT = 20;
const HISTORY_REFRESH_MS = 3000;

let activeRunId = null;
/** Seed URL for the loaded run (summary or history row); shown in Run State when scrolled away from history. */
let activeRunSeedUrl = "";
/** Skips graphView.render when lineage model is logically unchanged (avoids vis churn). */
let lastGraphSignature = null;
/** Current table list offset (`GET /urls`); graph polling does not use this. */
let urlsTableOffset = 0;

/** Rows for the URLs panel only (demo UI keeps the table small). */
const URL_TABLE_LIMIT = 200;
/** Rows/edges fetched for lineage graph construction (aligned with GET /graph limit). */
const GRAPH_URL_LIMIT = 50000;

let lastRenderedGraphNodeCount = 0;
let lastRenderedGraphEdgeCount = 0;
/** COMPLETED/FAILED: graph-view freezes physics/hover for a static final picture. */
let graphRunTerminal = false;
let graphTerminalFinalized = false;
let graphTerminalFinalizationInProgress = false;
let graphTerminalFinalizationSeq = 0;
let graphTerminalVisibleFinalizeListener = null;
/** True only while graph subsystem is intentionally paused due to hidden-tab lifecycle. */
let graphPausedForHiddenTab = false;
let activityRunId = null;
let hasSeenInProgressOnce = false;
let historyRefreshTimer = null;
let historyRefreshInFlight = false;

function getStoredTheme() {
  try {
    return localStorage.getItem(THEME_STORAGE_KEY) === "dark" ? "dark" : "light";
  } catch {
    return document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
  }
}

function updateThemeToggleLabel(theme) {
  const isDark = theme === "dark";
  el.themeToggle.textContent = isDark ? "Light" : "Dark";
  el.themeToggle.setAttribute(
    "aria-label",
    isDark ? "Switch to light theme" : "Switch to dark theme"
  );
}

function refreshGraphColorsForTheme() {
  if (!activeRunId || !el.graphPanelDetails.open) {
    return;
  }
  void fetchGraphSnapshot(activeRunId).then((snap) => {
    if (!snap.urls?.urls?.length) {
      return;
    }
    renderGraph(snap, { forceRefresh: true, suppressAutoFit: true });
  });
}

function applyTheme(theme, options = {}) {
  const { refreshGraph = true } = options;
  const next = theme === "dark" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", next);
  try {
    localStorage.setItem(THEME_STORAGE_KEY, next);
  } catch {
    /* ignore quota / private mode */
  }
  updateThemeToggleLabel(next);
  if (refreshGraph) {
    refreshGraphColorsForTheme();
  }
}

const graphView = createLineageGraphView(el.graphContainer, el.graphNodeInfo, {
  // Real user navigation disables auto-zoom so the app stops fighting user-controlled viewport.
  onAutoZoomChange: (enabled) => {
    el.graphAutoCenterToggle.checked = enabled;
  }
});

/** Deterministic snapshot of UI-relevant graph content for comparing polls. */
function buildGraphSignature(model) {
  const nodeLines = model.nodes
    .map((n) =>
      JSON.stringify({
        id: n.id,
        label: n.label,
        title: n.title,
        bg: n.color?.background,
        bd: n.color?.border
      })
    )
    .sort();

  const edgeLines = model.edges
    .map((e) =>
      JSON.stringify({
        id: e.id != null ? String(e.id) : `${e.from}->${e.to}`,
        from: e.from,
        to: e.to,
        label: e.label ?? ""
      })
    )
    .sort();

  return `${nodeLines.join("\n")}\n--\n${edgeLines.join("\n")}`;
}

/** Terminal graph settle/freeze budget scales with rendered graph size (capped at 30s max). */
function getTerminalGraphSettleBudget(nodeCount, edgeCount) {
  const size = Math.max(nodeCount, edgeCount);

  if (size <= 300) {
    return { minVisibleSettleMs: 2500, maxSettleMs: 8000 };
  }
  if (size <= 1000) {
    return { minVisibleSettleMs: 4000, maxSettleMs: 12000 };
  }
  if (size <= 3000) {
    return { minVisibleSettleMs: 7000, maxSettleMs: 18000 };
  }
  if (size <= 8000) {
    return { minVisibleSettleMs: 10000, maxSettleMs: 25000 };
  }
  return { minVisibleSettleMs: 12000, maxSettleMs: 30000 };
}

/** Staged auto-fit delays during physics; buckets align with getTerminalGraphSettleBudget(). */
function getGraphFitSchedule(nodeCount, edgeCount) {
  const size = Math.max(nodeCount, edgeCount);

  if (size <= 300) {
    return [100, 700, 1600];
  }
  if (size <= 1000) {
    return [100, 1000, 2500, 5000];
  }
  if (size <= 3000) {
    return [100, 1000, 3000, 7000, 12000, 17000];
  }
  return [100, 1000, 3000, 7000, 12000, 20000, 28000];
}

/** Run state + URL table cadence (unchanged from phase 2.5). */
const MAIN_POLL_MS = 1500;
/** Default graph-only refresh rate; adjustable via slider (browser-only, not sent to API). */
const DEFAULT_GRAPH_REFRESH_SEC = 3;

function syncGraphRefreshLabel() {
  const sec = Number(el.graphRefreshSlider.value);
  el.graphRefreshValue.textContent = `${sec}s`;
  el.graphRefreshSlider.setAttribute("aria-valuenow", String(sec));
}

function isRunTerminal(status) {
  const u = String(status ?? "").toUpperCase();
  return u === "COMPLETED" || u === "FAILED" || u === "CANCELLED";
}

function formatRunStatusLabel(status) {
  const u = String(status ?? "").toUpperCase();
  const labels = {
    RUNNING: "Running",
    COMPLETED: "Completed",
    FAILED: "Failed",
    CANCELLED: "Cancelled",
    QUEUED: "Queued",
    PENDING: "Pending"
  };
  if (!u) {
    return "—";
  }
  return labels[u] ?? String(status);
}

function runStatusVariant(status) {
  const u = String(status ?? "").toUpperCase();
  if (u === "RUNNING") {
    return "running";
  }
  if (u === "COMPLETED") {
    return "completed";
  }
  if (u === "FAILED") {
    return "failed";
  }
  if (u === "CANCELLED") {
    return "cancelled";
  }
  return "neutral";
}

function formatUrlStatus(status, httpStatus = null, includeCode = false) {
  const u = String(status ?? "").toUpperCase();
  if (u === "REDIRECT_301") {
    return includeCode ? "Redirect (301)" : "Redirect";
  }
  if (u === "REDIRECT_FOLLOWED") {
    return includeCode ? `Redirect followed (${httpStatus ?? "?"})` : "Redirect followed";
  }
  if (u === "REDIRECT_OUT_OF_SCOPE") {
    return includeCode ? `Redirect out of scope (${httpStatus ?? "?"})` : "Redirect out of scope";
  }
  if (u === "FORBIDDEN") {
    return includeCode ? "Forbidden (403)" : "Forbidden";
  }
  if (u === "NOT_FOUND") {
    return includeCode ? "Not found (404)" : "Not found";
  }
  if (u === "HTTP_TERMINAL") {
    return includeCode && httpStatus != null ? `Other HTTP (${httpStatus})` : "Other HTTP";
  }
  const labels = {
    QUEUED: "Queued",
    IN_PROGRESS: "In progress",
    VISITED: "Visited",
    FAILED: "Failed",
    CANCELLED: "Cancelled"
  };
  if (labels[u]) {
    return labels[u];
  }
  return escapeHtml(status ?? "");
}

function setPollStatus(msg, isError = false) {
  if (!msg) {
    el.pollStatus.textContent = "";
    el.pollStatus.className = "poll-status muted";
    return;
  }
  el.pollStatus.textContent = msg;
  el.pollStatus.className = isError ? "poll-status err" : "poll-status muted";
}

/** When RUNNING with queued URLs but none in flight, after we've seen at least one claim (avoids startup false positive). */
function deriveWaitingHostBackoffBadge(summary) {
  const status = String(summary?.status ?? "").toUpperCase();
  const totals = summary?.totals ?? {};
  const inProgress = Number(totals.in_progress ?? 0);
  const queue = Number(totals.queued ?? 0);

  if (activityRunId !== summary?.crawl_run_id) {
    activityRunId = summary?.crawl_run_id ?? null;
    hasSeenInProgressOnce = false;
  }
  if (inProgress > 0) {
    hasSeenInProgressOnce = true;
  }

  if (status === "RUNNING" && queue > 0 && inProgress === 0 && hasSeenInProgressOnce) {
    return "waiting-host-backoff";
  }
  return null;
}

function renderHostBackoffWaitingBadge(summary) {
  const variant = deriveWaitingHostBackoffBadge(summary);
  if (!variant) {
    el.graphActivityBadge.textContent = "";
    el.graphActivityBadge.className = "graph-activity-badge";
    return;
  }
  el.graphActivityBadge.textContent = "Waiting · pacing";
  el.graphActivityBadge.className = `stat-status stat-status--${variant} graph-activity-badge graph-activity-badge--visible`;
}

function seedUrlFromRun(run) {
  return String(run?.seed_url ?? run?.root_url ?? run?.normalized_seed_url ?? "").trim();
}

function renderRunSeedUrl(seed) {
  if (seed) {
    activeRunSeedUrl = seed;
  }
  const display = activeRunSeedUrl || "—";
  el.runSeedUrl.textContent = display;
  el.runSeedUrl.title = activeRunSeedUrl || "";
}

function renderSummary(summary) {
  el.runId.textContent = String(summary?.crawl_run_id ?? activeRunId ?? "—");
  const summarySeed = seedUrlFromRun(summary);
  if (summarySeed) {
    activeRunSeedUrl = summarySeed;
  }
  renderRunSeedUrl(summarySeed);
  const st = summary?.status;
  el.runStatus.textContent = formatRunStatusLabel(st);
  el.runStatus.className = `stat-value stat-status stat-status--${runStatusVariant(st)}`;
  const totals = summary?.totals ?? {};
  el.queued.textContent = String(totals.queued ?? 0);
  el.inProgress.textContent = String(totals.in_progress ?? 0);
  el.visited.textContent = String(totals.visited ?? 0);
  el.redirect301.textContent = String(totals.redirect_301 ?? 0);
  el.forbidden.textContent = String(totals.forbidden ?? 0);
  el.notFound.textContent = String(totals.not_found ?? 0);
  el.httpTerminal.textContent = String(totals.http_terminal ?? 0);
  el.failed.textContent = String(totals.failed ?? 0);
  el.cancelled.textContent = String(totals.cancelled ?? 0);
  el.discovered.textContent = String(totals.discovered ?? 0);
  el.runConfig.textContent = JSON.stringify(summary?.run_config ?? {}, null, 2);

  renderHostBackoffWaitingBadge(summary);
}

function updateUrlPaginationUi(rows, pagination) {
  if (!activeRunId || !pagination) {
    el.urlsPageStatus.textContent = "";
    el.urlsPrev.disabled = true;
    el.urlsNext.disabled = true;
    return;
  }
  const { offset, total, has_more: hasMore } = pagination;
  const n = rows?.length ?? 0;
  if (total === 0) {
    el.urlsPageStatus.textContent = "No URLs yet.";
  } else if (n === 0) {
    el.urlsPageStatus.textContent = `No URLs on this page (${total} total).`;
  } else {
    el.urlsPageStatus.textContent = `Showing ${offset + 1}–${offset + n} of ${total}`;
  }
  el.urlsPrev.disabled = offset <= 0;
  el.urlsNext.disabled = !hasMore;
}

function renderUrls(rows, pagination) {
  if (!rows?.length) {
    el.urlsBody.innerHTML = "";
    updateUrlPaginationUi(rows, pagination);
    if (!pagination || pagination.total === 0) {
      el.urlsLoading.textContent = "No URLs yet.";
    } else {
      el.urlsLoading.textContent = "";
    }
    return;
  }
  el.urlsLoading.textContent = "";
  updateUrlPaginationUi(rows, pagination);
  el.urlsBody.innerHTML = rows
    .map(
      (r) => `<tr>
      <td class="col-id">${r.id ?? ""}</td>
      <td class="url col-url">${escapeHtml(r.normalized_url ?? "")}</td>
      <td class="col-status"><span class="cell-status">${formatUrlStatus(r.status, r.http_status, true)}</span></td>
      <td class="col-depth">${r.depth ?? 0}</td>
      <td class="col-parent">${r.discovered_from_url_id ?? ""}</td>
      <td class="col-error cell-error-text">${escapeHtml(r.last_error ?? "")}</td>
    </tr>`
    )
    .join("");
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function persistActiveRunId(runId) {
  if (runId == null) {
    return;
  }
  try {
    localStorage.setItem(ACTIVE_RUN_STORAGE_KEY, String(runId));
  } catch {
    /* ignore quota / private mode */
  }
}

function clearPersistedActiveRunId() {
  try {
    localStorage.removeItem(ACTIVE_RUN_STORAGE_KEY);
  } catch {
    /* ignore */
  }
}

function readPersistedActiveRunId() {
  try {
    const raw = localStorage.getItem(ACTIVE_RUN_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const id = Number(raw);
    return Number.isFinite(id) && id > 0 ? id : null;
  } catch {
    return null;
  }
}

function formatHistoryTime(value) {
  if (!value) {
    return "—";
  }
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    return "—";
  }
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function renderHistoryTable(runs) {
  if (!runs?.length) {
    el.historyBody.innerHTML = "";
    el.historyLoading.textContent = "No crawl runs yet.";
    return;
  }
  el.historyLoading.textContent = "";
  el.historyBody.innerHTML = runs
    .map((run) => {
      const id = Number(run.crawl_run_id ?? run.id);
      const totals = run.totals ?? {};
      const isActive = activeRunId != null && id === activeRunId;
      const seed = seedUrlFromRun(run);
      const status = String(run.status ?? "");
      const isRunning = String(status).toUpperCase() === "RUNNING";
      const loadBtn = isActive
        ? `<button type="button" class="btn-secondary history-load-btn" data-run-id="${id}" data-seed-url="${escapeHtml(seed)}" disabled aria-current="true">Loaded</button>`
        : `<button type="button" class="btn-secondary history-load-btn" data-run-id="${id}" data-seed-url="${escapeHtml(seed)}">Load</button>`;
      const cancelBtn = isRunning
        ? `<button type="button" class="btn-secondary history-cancel-btn" data-run-id="${id}">Cancel</button>`
        : "";
      return `<tr class="${isActive ? "history-row--active" : ""}" data-run-id="${id}">
      <td class="hist-col-id">${id}</td>
      <td class="hist-col-status"><span class="stat-status stat-status--${runStatusVariant(status)}">${escapeHtml(formatRunStatusLabel(status))}</span></td>
      <td class="hist-col-url" title="${escapeHtml(seed)}">${escapeHtml(seed)}</td>
      <td class="hist-col-num">${totals.discovered ?? 0}</td>
      <td class="hist-col-num">${totals.visited ?? 0}</td>
      <td class="hist-col-num">${totals.queued ?? 0}</td>
      <td class="hist-col-num">${totals.in_progress ?? 0}</td>
      <td class="hist-col-num">${totals.failed ?? 0}</td>
      <td class="hist-col-time">${formatHistoryTime(run.started_at)}</td>
      <td class="hist-col-time">${formatHistoryTime(run.finished_at ?? run.completed_at)}</td>
      <td class="hist-col-action"><div class="history-row-actions">${loadBtn}${cancelBtn}</div></td>
    </tr>`;
    })
    .join("");
}

async function refreshHistory() {
  if (historyRefreshInFlight) {
    return;
  }
  historyRefreshInFlight = true;
  try {
    const data = await listCrawlRuns(HISTORY_LIST_LIMIT);
    renderHistoryTable(data?.runs ?? []);
  } catch (err) {
    el.historyLoading.textContent = "";
    el.historyStatus.textContent = `History refresh failed: ${err?.message ?? String(err)}`;
    el.historyStatus.className = "history-status err";
  } finally {
    historyRefreshInFlight = false;
  }
}

function startHistoryRefreshLoop() {
  if (historyRefreshTimer !== null) {
    return;
  }
  historyRefreshTimer = setInterval(() => {
    void refreshHistory();
  }, HISTORY_REFRESH_MS);
}

function resetRunUiForActivation() {
  graphRunTerminal = false;
  graphTerminalFinalized = false;
  graphTerminalFinalizationInProgress = false;
  graphTerminalFinalizationSeq = 0;
  clearScheduledTerminalVisibleFinalization();
  graphPausedForHiddenTab = false;
  lastGraphSignature = null;
  lastRenderedGraphNodeCount = 0;
  lastRenderedGraphEdgeCount = 0;
  urlsTableOffset = 0;
  activityRunId = null;
  hasSeenInProgressOnce = false;
  el.urlsPageStatus.textContent = "";
  el.urlsPrev.disabled = true;
  el.urlsNext.disabled = true;
  el.urlsLoading.textContent = "Loading URLs...";
  el.graphMeta.textContent = "Loading…";
  el.graphWarn.textContent = "";
  el.graphNodeInfo.textContent = "Click a node for details.";
  graphView.clear("Click a node for details.");
  el.graphPanelDetails.open = true;
  setPollStatus("");
}

function beginActiveRun(crawlRunId, summary) {
  poller.stop();
  graphPoller.stop();
  resetRunUiForActivation();
  activeRunId = crawlRunId;
  persistActiveRunId(crawlRunId);
  if (summary) {
    renderSummary(summary);
    el.runConfig.textContent = JSON.stringify(summary.run_config ?? {}, null, 2);
    const status = String(summary.status ?? "").toUpperCase();
    if (isRunTerminal(status)) {
      graphRunTerminal = true;
    }
  }
}

function startRunPolling(crawlRunId) {
  poller.start(crawlRunId);
  if (!graphRunTerminal) {
    graphPoller.start(crawlRunId);
  }
}

async function refreshActiveRunViews(runId = activeRunId, options = {}) {
  const { fitGraph = false } = options;
  if (!runId) {
    return;
  }
  const mainSnap = await fetchMainSnapshot(runId);
  await applyMainSnapshot(mainSnap);
  if (!graphRunTerminal) {
    const graphSnap = await fetchGraphSnapshot(runId);
    renderGraph(graphSnap, { forceFit: fitGraph });
  }
}

/**
 * Single entry point for loading/restoring a crawl (history Load, localStorage auto-load, new start).
 */
async function loadCrawlRun(crawlRunId, options = {}) {
  const { fromAuto = false, fromUser = false, summary: providedSummary = null } = options;
  const id = Number(crawlRunId);
  if (!Number.isFinite(id) || id <= 0) {
    return false;
  }

  try {
    const summary = providedSummary ?? (await getSummaryIfExists(id));
    if (!summary) {
      const persisted = readPersistedActiveRunId();
      if (fromAuto || persisted === id) {
        clearPersistedActiveRunId();
      }
      el.historyStatus.textContent = fromAuto
        ? `Saved crawl run ${id} is no longer available. Pick a run from history below.`
        : `Crawl run ${id} was not found.`;
      el.historyStatus.className = "history-status muted";
      if (activeRunId === id) {
        activeRunId = null;
        renderRunSeedUrl("");
        poller.stop();
        graphPoller.stop();
        setPollStatus("");
      }
      void refreshHistory();
      return false;
    }

    beginActiveRun(id, summary);
    try {
      await refreshActiveRunViews(id, { fitGraph: true });
    } catch (viewErr) {
      setPollStatus(`Could not refresh views: ${viewErr?.message ?? String(viewErr)}`, true);
      el.graphWarn.textContent = `Load refresh failed: ${viewErr?.message ?? String(viewErr)}`;
    }
    startRunPolling(id);
    void refreshHistory();

    el.historyStatus.textContent = "";
    el.historyStatus.className = "history-status muted";
    return true;
  } catch (err) {
    el.historyStatus.textContent = `Could not load crawl run ${id}: ${err?.message ?? String(err)}`;
    el.historyStatus.className = "history-status err";
    return false;
  }
}

function renderGraph(snapshot, options = {}) {
  const { forceRefresh = false, suppressAutoFit = false, forceFit = false } = options;
  if (!activeRunId) {
    lastGraphSignature = null;
    lastRenderedGraphNodeCount = 0;
    lastRenderedGraphEdgeCount = 0;
    el.graphMeta.textContent = "No active crawl.";
    el.graphWarn.textContent = "";
    graphView.clear("Click a node for details.");
    return;
  }
  if (!el.graphPanelDetails.open) {
    return;
  }
  const urlsRows = snapshot.urls?.urls ?? [];
  if (!urlsRows.length) {
    lastGraphSignature = null;
    lastRenderedGraphNodeCount = 0;
    lastRenderedGraphEdgeCount = 0;
    el.graphMeta.textContent = "No URLs yet.";
    el.graphWarn.textContent = snapshot.graphError ? String(snapshot.graphError) : "";
    graphView.clear("Click a node for details.");
    return;
  }

  const model = buildLineageGraph(snapshot.urls, snapshot.graph);
  el.graphMeta.textContent = `${model.nodeCount.toLocaleString()} nodes · ${model.edgeCount.toLocaleString()} edges`;
  el.graphWarn.textContent = snapshot.graphError
    ? `Incomplete graph data: ${snapshot.graphError}`
    : "";

  const signature = buildGraphSignature(model);
  const shouldFreezeTerminalGraph = graphRunTerminal && graphTerminalFinalized;
  graphView.setCompletedMode(shouldFreezeTerminalGraph);
  const signatureChanged = signature !== lastGraphSignature;
  if (forceRefresh) {
    // Hidden-tab throttling can leave vis-network half-awake; force a full data/physics re-arm.
    graphView.refreshFromModel(model, { fit: false });
  } else if (signatureChanged) {
    graphView.render(model);
    if (!graphRunTerminal) {
      graphView.resumeLayout();
    }
  }

  if (signatureChanged || forceRefresh) {
    lastGraphSignature = signature;
    const grew =
      model.nodeCount > lastRenderedGraphNodeCount || model.edgeCount > lastRenderedGraphEdgeCount;
    if (!suppressAutoFit && graphView.isAutoZoomEnabled() && grew) {
      graphView.fitSoon({ delayMs: 350 });
    }
    lastRenderedGraphNodeCount = model.nodeCount;
    lastRenderedGraphEdgeCount = model.edgeCount;
  }

  if (forceFit && !suppressAutoFit && graphView.isAutoZoomEnabled() && model.nodeCount > 0) {
    graphView.scheduleStagedFit({
      delaysMs: getGraphFitSchedule(model.nodeCount, model.edgeCount)
    });
  }

  if (!el.graphNodeInfo.textContent || el.graphNodeInfo.textContent.includes("Click a node")) {
    const root = urlsRows.find((r) => r.discovered_from_url_id == null) ?? urlsRows[0];
    if (root) {
      el.graphNodeInfo.textContent = formatNodeInfo(root);
    }
  }
}

async function fetchMainSnapshot(crawlRunId) {
  const offsetAtFetch = urlsTableOffset;
  const [summary, urls] = await Promise.all([
    getSummary(crawlRunId),
    getUrls(crawlRunId, URL_TABLE_LIMIT, offsetAtFetch)
  ]);
  return { summary, urls, urlsOffsetAtFetch: offsetAtFetch };
}

/**
 * Fetch once and paint the graph after the run is COMPLETED/FAILED.
 * Caller must set `graphRunTerminal = true` (and stop graph poller) before calling so `renderGraph` applies static mode.
 */
async function fetchAndRenderTerminalGraph() {
  if (!activeRunId) {
    return;
  }
  const snap = await fetchGraphSnapshot(activeRunId);
  renderGraph(snap);
  const finalizationSeq = ++graphTerminalFinalizationSeq;
  const runIdAtRender = activeRunId;
  if (document.hidden) {
    scheduleTerminalFinalizationOnVisible(runIdAtRender, finalizationSeq);
    return;
  }
  clearScheduledTerminalVisibleFinalization();
  await finalizeTerminalGraphLayout(runIdAtRender, finalizationSeq);
}

function forceGraphRefresh(snapshot, options = {}) {
  renderGraph(snapshot, { forceRefresh: true, suppressAutoFit: true, ...options });
}

function clearScheduledTerminalVisibleFinalization() {
  if (graphTerminalVisibleFinalizeListener) {
    document.removeEventListener("visibilitychange", graphTerminalVisibleFinalizeListener);
    graphTerminalVisibleFinalizeListener = null;
  }
}

function scheduleTerminalFinalizationOnVisible(runId, finalizationSeq) {
  clearScheduledTerminalVisibleFinalization();
  graphTerminalVisibleFinalizeListener = () => {
    if (document.hidden) {
      return;
    }
    clearScheduledTerminalVisibleFinalization();
    void finalizeTerminalGraphLayout(runId, finalizationSeq);
  };
  document.addEventListener("visibilitychange", graphTerminalVisibleFinalizeListener);
}

async function finalizeTerminalGraphLayout(runIdAtRender, finalizationSeq) {
  if (graphTerminalFinalized || graphTerminalFinalizationInProgress) {
    return;
  }
  if (
    !graphRunTerminal ||
    !activeRunId ||
    activeRunId !== runIdAtRender ||
    finalizationSeq !== graphTerminalFinalizationSeq
  ) {
    return;
  }
  graphTerminalFinalizationInProgress = true;
  try {
    const liveCounts = graphView.getCounts();
    const nodeCount = liveCounts.nodeCount || lastRenderedGraphNodeCount;
    const edgeCount = liveCounts.edgeCount || lastRenderedGraphEdgeCount;
    const settleBudget = getTerminalGraphSettleBudget(nodeCount, edgeCount);
    if (graphView.isAutoZoomEnabled()) {
      const delaysMs = getGraphFitSchedule(nodeCount, edgeCount).filter(
        (delayMs) => delayMs <= settleBudget.maxSettleMs
      );
      if (delaysMs.length > 0) {
        graphView.scheduleStagedFit({ delaysMs });
      }
    }
    // One-time final fit/center first, then bounded foreground settle, then freeze.
    await graphView.finalizeCompletedLayout({
      fit: true,
      fitBeforeSettle: true,
      fitAfterFreeze: true,
      minVisibleSettleMs: settleBudget.minVisibleSettleMs,
      maxSettleMs: settleBudget.maxSettleMs
    });
    if (
      graphRunTerminal &&
      activeRunId === runIdAtRender &&
      finalizationSeq === graphTerminalFinalizationSeq
    ) {
      graphTerminalFinalized = true;
      graphView.setCompletedMode(true);
    }
  } finally {
    graphTerminalFinalizationInProgress = false;
  }
}

/**
 * Hidden tabs throttle animation/timers heavily; pause graph polling+physics instead of trying to keep
 * a stale simulation alive in the background.
 */
function pauseGraphForHiddenTab() {
  if (!activeRunId || !el.graphPanelDetails.open || graphRunTerminal || graphPausedForHiddenTab) {
    return;
  }
  graphPoller.stop();
  graphView.pauseForHiddenTab();
  graphPausedForHiddenTab = true;
}

/**
 * After tab restore, do one forced graph refresh, then restart normal graph polling.
 * This is more reliable than repeated startSimulation nudges against a stale hidden-tab instance.
 */
async function resumeGraphAfterHiddenTab() {
  if (!graphPausedForHiddenTab) {
    return;
  }
  if (!activeRunId) {
    graphPausedForHiddenTab = false;
    return;
  }
  if (!el.graphPanelDetails.open) {
    graphPausedForHiddenTab = false;
    return;
  }
  if (graphRunTerminal) {
    await fetchAndRenderTerminalGraph();
    graphPausedForHiddenTab = false;
    return;
  }
  try {
    const snap = await fetchGraphSnapshot(activeRunId);
    forceGraphRefresh(snap, { suppressAutoFit: true });
    graphView.resumeAfterHiddenTab();
  } finally {
    graphPoller.start(activeRunId);
    graphPausedForHiddenTab = false;
  }
}

async function applyMainSnapshot(snapshot) {
  if (snapshot.urlsOffsetAtFetch !== urlsTableOffset) {
    return;
  }
  renderSummary(snapshot.summary);
  renderUrls(snapshot.urls?.urls ?? [], snapshot.urls?.pagination);
  setPollStatus("");
  const status = String(snapshot.summary?.status ?? "").toUpperCase();
  if (isRunTerminal(status)) {
    if (!graphRunTerminal) {
      graphTerminalFinalized = false;
      graphTerminalFinalizationInProgress = false;
      clearScheduledTerminalVisibleFinalization();
    }
    graphRunTerminal = true;
    graphPausedForHiddenTab = false;
    graphPoller.stop();
    await fetchAndRenderTerminalGraph();
    setPollStatus("");
  }
}

async function fetchGraphSnapshot(crawlRunId) {
  const [urls, graphRes] = await Promise.all([
    getUrls(crawlRunId, GRAPH_URL_LIMIT),
    getGraph(crawlRunId, GRAPH_URL_LIMIT)
      .then((graph) => ({ graph, graphError: null }))
      .catch((err) => ({ graph: null, graphError: err?.message ?? String(err) }))
  ]);
  return { urls, graph: graphRes.graph, graphError: graphRes.graphError };
}

const graphPoller = createRunPoller(
  fetchGraphSnapshot,
  (snap) => {
    renderGraph(snap);
  },
  (err) => {
    el.graphWarn.textContent = `Graph poll failed: ${err?.message ?? String(err)}`;
  },
  DEFAULT_GRAPH_REFRESH_SEC * 1000
);

async function resumeGraphPollingAfterExpand() {
  if (!activeRunId) {
    return;
  }
  graphPausedForHiddenTab = false;
  if (graphRunTerminal) {
    await fetchAndRenderTerminalGraph();
    return;
  }
  graphPoller.start(activeRunId);
}

const poller = createRunPoller(
  fetchMainSnapshot,
  (snapshot) => {
    applyMainSnapshot(snapshot);
  },
  (err) => {
    setPollStatus(`Polling error: ${err?.message ?? String(err)}`, true);
    el.graphWarn.textContent = `Run poll failed: ${err?.message ?? String(err)}`;
  },
  MAIN_POLL_MS
);

el.graphRefreshSlider.addEventListener("input", () => {
  syncGraphRefreshLabel();
  graphPoller.setPollInterval(Number(el.graphRefreshSlider.value) * 1000);
});
syncGraphRefreshLabel();
el.graphAutoCenterToggle.checked = graphView.isAutoZoomEnabled();
el.graphAutoCenterToggle.addEventListener("change", () => {
  // Toggle controls future automatic fits only; does not trigger immediate fit.
  graphView.setAutoZoomEnabled(el.graphAutoCenterToggle.checked);
});

el.graphPanelDetails.addEventListener("toggle", () => {
  if (!el.graphPanelDetails.open) {
    graphPoller.stop();
    return;
  }
  void resumeGraphPollingAfterExpand();
});

el.graphFitBtn.addEventListener("click", () => {
  // Manual fit is independent from auto-zoom setting.
  graphView.fit();
});

/** Debounce coalesces visibility + focus + pageshow when returning to the tab/window. */
let tabReturnDebounceTimer = null;
const TAB_RETURN_DEBOUNCE_MS = 180;

/**
 * Background tabs throttle setInterval/requestAnimationFrame — polling lags until the next tick.
 * Force immediate main poll, then resume graph subsystem only if we actually paused while hidden.
 */
async function runTabReturnRefresh() {
  if (!activeRunId || document.visibilityState !== "visible") {
    return;
  }
  await poller.triggerNow();
  await resumeGraphAfterHiddenTab();
}

function scheduleTabReturnRefresh() {
  if (document.visibilityState !== "visible") {
    return;
  }
  if (tabReturnDebounceTimer !== null) {
    clearTimeout(tabReturnDebounceTimer);
  }
  tabReturnDebounceTimer = setTimeout(() => {
    tabReturnDebounceTimer = null;
    void runTabReturnRefresh();
  }, TAB_RETURN_DEBOUNCE_MS);
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "hidden") {
    pauseGraphForHiddenTab();
    return;
  }
  scheduleTabReturnRefresh();
});

window.addEventListener("focus", () => {
  scheduleTabReturnRefresh();
});

window.addEventListener("pageshow", () => {
  scheduleTabReturnRefresh();
});

el.urlsPrev.addEventListener("click", () => {
  if (!activeRunId || urlsTableOffset <= 0) {
    return;
  }
  urlsTableOffset = Math.max(0, urlsTableOffset - URL_TABLE_LIMIT);
  void fetchMainSnapshot(activeRunId).then((snap) => applyMainSnapshot(snap));
});

el.urlsNext.addEventListener("click", () => {
  if (!activeRunId) {
    return;
  }
  urlsTableOffset += URL_TABLE_LIMIT;
  void fetchMainSnapshot(activeRunId).then((snap) => applyMainSnapshot(snap));
});

el.form.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const seedUrl = el.seedUrl.value.trim();
  if (!seedUrl) {
    return;
  }
  const settings = {
    maxPages: Number(el.maxPages.value),
    maxDepth: Number(el.maxDepth.value),
    scopeMode: el.scopeMode.value,
    includeDocuments: el.includeDocuments.checked,
    followRedirects: el.followRedirects.checked,
    demoDelayMs: Number(el.demoDelayMs.value),
    requestTimeoutMs: Number(el.requestTimeoutMs.value),
    maxRetries: Number(el.maxRetries.value)
  };

  el.startBtn.disabled = true;
  el.startStatus.textContent = "Starting crawl...";
  el.startStatus.className = "muted";
  try {
    const run = await startCrawl(seedUrl, settings);
    graphView.setAutoZoomEnabled(true);
    const runId = Number(run.id);
    await loadCrawlRun(runId, {
      summary: {
        crawl_run_id: runId,
        status: run.status ?? "RUNNING",
        seed_url: seedUrl,
        root_url: seedUrl,
        run_config: run.run_config ?? settings,
        totals: { discovered: 1, queued: 1, in_progress: 0, visited: 0 }
      }
    });
    el.startStatus.textContent = "";
    el.startStatus.className = "muted";
  } catch (err) {
    el.startStatus.textContent = `Start failed: ${err?.message ?? String(err)}`;
    el.startStatus.className = "err";
  } finally {
    el.startBtn.disabled = false;
  }
});

const CANCEL_CONFIRM_MESSAGE =
  "Cancel this crawl? Queued and in-progress URLs will be marked CANCELLED. This cannot be resumed.";

async function handleCancelCrawlRun(runId) {
  if (!Number.isFinite(runId) || runId <= 0) {
    return;
  }
  if (!window.confirm(CANCEL_CONFIRM_MESSAGE)) {
    return;
  }
  try {
    await cancelCrawlRun(runId);
    await refreshHistory();
    if (activeRunId === runId) {
      const summary = await getSummary(runId);
      beginActiveRun(runId, summary);
      await refreshActiveRunViews(runId);
      poller.stop();
      graphPoller.stop();
      graphRunTerminal = true;
      await fetchAndRenderTerminalGraph();
      setPollStatus("");
    }
    el.historyStatus.textContent = `Crawl run ${runId} cancelled.`;
    el.historyStatus.className = "history-status ok";
  } catch (err) {
    setPollStatus(`Cancel failed: ${err?.message ?? String(err)}`, true);
    el.historyStatus.textContent = `Cancel failed: ${err?.message ?? String(err)}`;
    el.historyStatus.className = "history-status err";
  }
}

el.historyBody.addEventListener("click", (ev) => {
  const loadBtn = ev.target.closest(".history-load-btn");
  if (loadBtn && !loadBtn.disabled) {
    const runId = Number(loadBtn.dataset.runId);
    if (Number.isFinite(runId)) {
      const seed = loadBtn.dataset.seedUrl ?? "";
      if (seed) {
        renderRunSeedUrl(seed);
      }
      void loadCrawlRun(runId, { fromUser: true });
    }
    return;
  }
  const cancelBtn = ev.target.closest(".history-cancel-btn");
  if (cancelBtn) {
    const runId = Number(cancelBtn.dataset.runId);
    if (Number.isFinite(runId)) {
      void handleCancelCrawlRun(runId);
    }
  }
});

updateThemeToggleLabel(getStoredTheme());

el.themeToggle.addEventListener("click", () => {
  const next = getStoredTheme() === "dark" ? "light" : "dark";
  applyTheme(next);
});

void (async function initHistoryAndRestore() {
  await refreshHistory();
  startHistoryRefreshLoop();
  const persisted = readPersistedActiveRunId();
  if (persisted) {
    await loadCrawlRun(persisted, { fromAuto: true });
  }
})();
