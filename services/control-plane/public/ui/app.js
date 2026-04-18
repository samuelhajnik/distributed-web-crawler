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
  failed: document.getElementById("c-failed"),
  discovered: document.getElementById("c-discovered"),
  pollStatus: document.getElementById("poll-status"),
  graphStatus: document.getElementById("graph-status"),
  graphContainer: document.getElementById("graph-container"),
  graphNodeInfo: document.getElementById("graph-node-info"),
  runConfig: document.getElementById("run-config"),
  urlsBody: document.getElementById("urls-body"),
  urlsLoading: document.getElementById("urls-loading"),
  graphRefreshSlider: document.getElementById("graph-refresh-slider"),
  graphRefreshValue: document.getElementById("graph-refresh-value"),
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
const graphView = createLineageGraphView(el.graphContainer, el.graphNodeInfo);

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

function setPollStatus(msg, isError = false) {
  el.pollStatus.textContent = msg;
  el.pollStatus.className = isError ? "err" : "muted";
}

function renderSummary(summary) {
  el.runId.textContent = String(summary?.crawl_run_id ?? activeRunId ?? "-");
  el.runStatus.textContent = summary?.status ?? "-";
  const totals = summary?.totals ?? {};
  el.queued.textContent = String(totals.queued ?? 0);
  el.inProgress.textContent = String(totals.in_progress ?? 0);
  el.visited.textContent = String(totals.visited ?? 0);
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
      <td>${r.id ?? ""}</td>
      <td class="url">${escapeHtml(r.normalized_url ?? "")}</td>
      <td>${escapeHtml(r.status ?? "")}</td>
      <td>${r.depth ?? 0}</td>
      <td>${r.discovered_from_url_id ?? ""}</td>
      <td>${escapeHtml(r.last_error ?? "")}</td>
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
  const errSuffix = snapshot.graphError ? ` • graph endpoint degraded: ${snapshot.graphError}` : "";

  if (!activeRunId) {
    lastGraphSignature = null;
    el.graphStatus.textContent = "No active run yet.";
    graphView.clear("No node selected");
    return;
  }
  const urlsRows = snapshot.urls?.urls ?? [];
  if (!urlsRows.length) {
    lastGraphSignature = null;
    el.graphStatus.textContent = `Run exists but no URLs discovered yet.${errSuffix}`;
    graphView.clear("No node selected");
    return;
  }

  const model = buildLineageGraph(snapshot.urls, snapshot.graph);
  el.graphStatus.textContent = `Nodes: ${model.nodeCount} • Edges: ${model.edgeCount}${errSuffix}`;

  const signature = buildGraphSignature(model);
  if (signature !== lastGraphSignature) {
    graphView.render(model);
    lastGraphSignature = signature;
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

function applyMainSnapshot(snapshot) {
  if (snapshot.urlsOffsetAtFetch !== urlsTableOffset) {
    return;
  }
  renderSummary(snapshot.summary);
  renderUrls(snapshot.urls?.urls ?? [], snapshot.urls?.pagination);
  setPollStatus(`Polling run ${activeRunId}...`);
  const status = String(snapshot.summary?.status ?? "").toUpperCase();
  if (status === "COMPLETED" || status === "FAILED") {
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
    el.graphStatus.textContent = `Graph update failed: ${err?.message ?? String(err)}`;
  },
  DEFAULT_GRAPH_REFRESH_SEC * 1000
);

const poller = createRunPoller(
  fetchMainSnapshot,
  (snapshot) => {
    applyMainSnapshot(snapshot);
  },
  (err) => {
    setPollStatus(`Polling error: ${err?.message ?? String(err)}`, true);
    el.graphStatus.textContent = `Update failed: ${err?.message ?? String(err)}`;
  },
  MAIN_POLL_MS
);

el.graphRefreshSlider.addEventListener("input", () => {
  syncGraphRefreshLabel();
  graphPoller.setPollInterval(Number(el.graphRefreshSlider.value) * 1000);
});
syncGraphRefreshLabel();

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
    lastGraphSignature = null;
    urlsTableOffset = 0;
    activeRunId = Number(run.id);
    el.urlsPageStatus.textContent = "";
    el.urlsPrev.disabled = true;
    el.urlsNext.disabled = true;
    graphView.clear("Click a node to inspect details.");
    el.startStatus.textContent = `Crawl started (run ${activeRunId}).`;
    el.startStatus.className = "ok";
    el.urlsLoading.textContent = "Loading URLs...";
    el.graphStatus.textContent = "Loading lineage graph...";
    el.graphNodeInfo.textContent = "Click a node to inspect details.";
    el.runConfig.textContent = JSON.stringify(run.run_config ?? settings, null, 2);
    poller.start(activeRunId);
    graphPoller.start(activeRunId);
  } catch (err) {
    el.startStatus.textContent = `Start failed: ${err?.message ?? String(err)}`;
    el.startStatus.className = "err";
  } finally {
    el.startBtn.disabled = false;
  }
});
