import { getGraph, getSummary, getUrls, startCrawl } from "./api.js";
import { buildLineageGraph, formatNodeInfo } from "./graph-model.js";
import { createLineageGraphView } from "./graph-view.js";
import { createRunPoller } from "./poller.js";

const el = {
  form: document.getElementById("start-form"),
  seedUrl: document.getElementById("seed-url"),
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
  urlsBody: document.getElementById("urls-body"),
  urlsLoading: document.getElementById("urls-loading")
};

let activeRunId = null;
const graphView = createLineageGraphView(el.graphContainer, el.graphNodeInfo);

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
}

function renderUrls(rows) {
  if (!rows?.length) {
    el.urlsLoading.textContent = "No URLs yet.";
    el.urlsBody.innerHTML = "";
    return;
  }
  el.urlsLoading.textContent = "";
  el.urlsBody.innerHTML = rows
    .map(
      (r) => `<tr>
      <td>${r.id ?? ""}</td>
      <td class="url">${escapeHtml(r.normalized_url ?? "")}</td>
      <td>${escapeHtml(r.status ?? "")}</td>
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
  if (!activeRunId) {
    el.graphStatus.textContent = "No active run yet.";
    graphView.clear("No node selected");
    return;
  }
  const urlsRows = snapshot.urls?.urls ?? [];
  if (!urlsRows.length) {
    el.graphStatus.textContent = "Run exists but no URLs discovered yet.";
    graphView.clear("No node selected");
    return;
  }
  const model = buildLineageGraph(snapshot.urls, snapshot.graph);
  graphView.render(model);
  const errSuffix = snapshot.graphError ? ` • graph endpoint degraded: ${snapshot.graphError}` : "";
  el.graphStatus.textContent = `Nodes: ${model.nodeCount} • Edges: ${model.edgeCount}${errSuffix}`;
  if (!el.graphNodeInfo.textContent || el.graphNodeInfo.textContent.includes("Click a node")) {
    const root = urlsRows.find((r) => r.discovered_from_url_id == null) ?? urlsRows[0];
    if (root) {
      el.graphNodeInfo.textContent = formatNodeInfo(root);
    }
  }
}

async function fetchRunSnapshot(crawlRunId) {
  const [summary, urls, graphRes] = await Promise.all([
    getSummary(crawlRunId),
    getUrls(crawlRunId, 200),
    getGraph(crawlRunId, 50000)
      .then((graph) => ({ graph, graphError: null }))
      .catch((err) => ({ graph: null, graphError: err?.message ?? String(err) }))
  ]);
  return { summary, urls, graph: graphRes.graph, graphError: graphRes.graphError };
}

const poller = createRunPoller(
  fetchRunSnapshot,
  (snapshot) => {
    renderSummary(snapshot.summary);
    renderGraph(snapshot);
    renderUrls(snapshot.urls?.urls ?? []);
    setPollStatus(`Polling run ${activeRunId}...`);
    const status = String(snapshot.summary?.status ?? "").toUpperCase();
    if (status === "COMPLETED" || status === "FAILED") {
      setPollStatus(`Run ${activeRunId} is ${status}. Polling stopped.`);
    }
  },
  (err) => {
    setPollStatus(`Polling error: ${err?.message ?? String(err)}`, true);
    el.graphStatus.textContent = `Update failed: ${err?.message ?? String(err)}`;
  },
  1500
);

el.form.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const seedUrl = el.seedUrl.value.trim();
  if (!seedUrl) {
    return;
  }

  el.startBtn.disabled = true;
  el.startStatus.textContent = "Starting crawl...";
  el.startStatus.className = "muted";
  try {
    const run = await startCrawl(seedUrl);
    activeRunId = Number(run.id);
    el.startStatus.textContent = `Crawl started (run ${activeRunId}).`;
    el.startStatus.className = "ok";
    el.urlsLoading.textContent = "Loading URLs...";
    el.graphStatus.textContent = "Loading lineage graph...";
    el.graphNodeInfo.textContent = "Click a node to inspect details.";
    poller.start(activeRunId);
  } catch (err) {
    el.startStatus.textContent = `Start failed: ${err?.message ?? String(err)}`;
    el.startStatus.className = "err";
  } finally {
    el.startBtn.disabled = false;
  }
});
