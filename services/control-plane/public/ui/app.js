import { getGraph, getSummary, getUrls, startCrawl } from "./api.js";
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
  runStatus: document.getElementById("run-status"),
  queued: document.getElementById("c-queued"),
  inProgress: document.getElementById("c-in-progress"),
  visited: document.getElementById("c-visited"),
  redirect301: document.getElementById("c-redirect-301"),
  forbidden: document.getElementById("c-forbidden"),
  notFound: document.getElementById("c-not-found"),
  httpTerminal: document.getElementById("c-http-terminal"),
  failed: document.getElementById("c-failed"),
  discovered: document.getElementById("c-discovered"),
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
  graphFitBtn: document.getElementById("graph-fit-btn"),
  graphPanelDetails: document.getElementById("graph-panel-details"),
  urlsPrev: document.getElementById("urls-prev"),
  urlsNext: document.getElementById("urls-next"),
  urlsPageStatus: document.getElementById("urls-page-status")
};

let activeRunId = null;
/** Skips graphView.render when lineage model is logically unchanged (avoids vis churn). */
let lastGraphSignature = null;
/** Current table list offset (`GET /urls`); graph polling does not use this. */
let urlsTableOffset = 0;

/** Rows for the URLs panel only (demo UI keeps the table small). */
const URL_TABLE_LIMIT = 200;
/** Rows/edges fetched for lineage graph construction (aligned with GET /graph limit). */
const GRAPH_URL_LIMIT = 50000;

/** Auto-fit viewport on growth until the user pans/zooms; “Fit graph” re-enables. */
let graphAutoFitEnabled = true;
let lastRenderedGraphNodeCount = 0;
let lastRenderedGraphEdgeCount = 0;
/** COMPLETED/FAILED: graph-view freezes physics/hover for a static final picture. */
let graphRunTerminal = false;

const graphView = createLineageGraphView(el.graphContainer, el.graphNodeInfo, {
  onUserViewportInteraction: () => {
    graphAutoFitEnabled = false;
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

/** Run state + URL table cadence (unchanged from phase 2.5). */
const MAIN_POLL_MS = 1500;
/** Default graph-only refresh rate; adjustable via slider (browser-only, not sent to API). */
const DEFAULT_GRAPH_REFRESH_SEC = 3;

function syncGraphRefreshLabel() {
  const sec = Number(el.graphRefreshSlider.value);
  el.graphRefreshValue.textContent = `${sec}s`;
  el.graphRefreshSlider.setAttribute("aria-valuenow", String(sec));
}

function formatRunStatusLabel(status) {
  const u = String(status ?? "").toUpperCase();
  const labels = {
    RUNNING: "Running",
    COMPLETED: "Completed",
    FAILED: "Failed",
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
  return "neutral";
}

function formatUrlStatus(status, httpStatus = null, includeCode = false) {
  const u = String(status ?? "").toUpperCase();
  if (u === "REDIRECT_301") {
    return includeCode ? "Redirect (301)" : "Redirect";
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
    FAILED: "Failed"
  };
  if (labels[u]) {
    return labels[u];
  }
  return escapeHtml(status ?? "");
}

function setPollStatus(msg, isError = false) {
  el.pollStatus.textContent = msg;
  el.pollStatus.className = isError ? "err" : "muted";
}

function renderSummary(summary) {
  el.runId.textContent = String(summary?.crawl_run_id ?? activeRunId ?? "—");
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
  el.discovered.textContent = String(totals.discovered ?? 0);
  el.runConfig.textContent = JSON.stringify(summary?.run_config ?? {}, null, 2);
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

function renderGraph(snapshot) {
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
  el.graphWarn.textContent = snapshot.graphError ? `Incomplete graph data: ${snapshot.graphError}` : "";

  const signature = buildGraphSignature(model);
  if (signature !== lastGraphSignature) {
    graphView.render(model);
    if (!graphRunTerminal) {
      graphView.resumeLayout();
    }
    lastGraphSignature = signature;

    const grew =
      model.nodeCount > lastRenderedGraphNodeCount || model.edgeCount > lastRenderedGraphEdgeCount;
    if (graphAutoFitEnabled && grew) {
      graphView.fitSoon({ delayMs: 350 });
    }

    lastRenderedGraphNodeCount = model.nodeCount;
    lastRenderedGraphEdgeCount = model.edgeCount;
  }

  graphView.setCompletedMode(graphRunTerminal);

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

function applyMainSnapshot(snapshot) {
  if (snapshot.urlsOffsetAtFetch !== urlsTableOffset) {
    return;
  }
  renderSummary(snapshot.summary);
  renderUrls(snapshot.urls?.urls ?? [], snapshot.urls?.pagination);
  setPollStatus(`Polling run ${activeRunId}...`);
  const status = String(snapshot.summary?.status ?? "").toUpperCase();
  if (status === "COMPLETED" || status === "FAILED") {
    graphRunTerminal = true;
    graphPoller.stop();
    void fetchGraphSnapshot(activeRunId).then((s) => renderGraph(s));
    setPollStatus(`Run ${activeRunId} is ${status}. Polling stopped.`);
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

function resumeGraphPollingAfterExpand() {
  if (!activeRunId) {
    return;
  }
  if (graphRunTerminal) {
    void fetchGraphSnapshot(activeRunId).then((snap) => renderGraph(snap));
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

el.graphPanelDetails.addEventListener("toggle", () => {
  if (!el.graphPanelDetails.open) {
    graphPoller.stop();
    return;
  }
  resumeGraphPollingAfterExpand();
});

el.graphFitBtn.addEventListener("click", () => {
  graphAutoFitEnabled = true;
  graphView.fit();
});

/** Debounce coalesces visibility + focus + pageshow when returning to the tab/window. */
let tabReturnDebounceTimer = null;
const TAB_RETURN_DEBOUNCE_MS = 180;

/**
 * Nudge vis-network physics after fresh data while the crawl is active (skipped for completed/static graph).
 */
function wakeGraphIfAppropriate() {
  if (!activeRunId || !el.graphPanelDetails.open) {
    return;
  }
  const { nodeCount } = graphView.getCounts();
  if (nodeCount === 0) {
    return;
  }
  if (typeof graphView.isCompletedMode === "function" && graphView.isCompletedMode()) {
    return;
  }
  graphView.wakeFromBackgroundStrong();
}

/**
 * Background tabs throttle setInterval/requestAnimationFrame — polling lags until the next tick.
 * Force immediate main + graph fetches, then wake layout (physics only matters in normal mode).
 */
async function runTabReturnRefresh() {
  if (!activeRunId || document.visibilityState !== "visible") {
    return;
  }
  await poller.triggerNow();
  if (el.graphPanelDetails.open) {
    if (graphRunTerminal) {
      void fetchGraphSnapshot(activeRunId).then((snap) => renderGraph(snap));
    } else {
      await graphPoller.triggerNow();
    }
  }
  requestAnimationFrame(() => {
    wakeGraphIfAppropriate();
  });
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
    graphRunTerminal = false;
    lastGraphSignature = null;
    graphAutoFitEnabled = true;
    lastRenderedGraphNodeCount = 0;
    lastRenderedGraphEdgeCount = 0;
    urlsTableOffset = 0;
    activeRunId = Number(run.id);
    el.urlsPageStatus.textContent = "";
    el.urlsPrev.disabled = true;
    el.urlsNext.disabled = true;
    graphView.clear("Click a node for details.");
    el.startStatus.textContent = `Crawl started (run ${activeRunId}).`;
    el.startStatus.className = "ok";
    el.urlsLoading.textContent = "Loading URLs...";
    el.graphMeta.textContent = "Loading…";
    el.graphWarn.textContent = "";
    el.graphNodeInfo.textContent = "Click a node for details.";
    el.runConfig.textContent = JSON.stringify(run.run_config ?? settings, null, 2);
    el.graphPanelDetails.open = true;
    poller.start(activeRunId);
    graphPoller.start(activeRunId);
  } catch (err) {
    el.startStatus.textContent = `Start failed: ${err?.message ?? String(err)}`;
    el.startStatus.className = "err";
  } finally {
    el.startBtn.disabled = false;
  }
});
