function normalizeStatus(status) {
  return String(status ?? "").toUpperCase();
}

/**
 * Vis fill colors — one semantic meaning each (matches graph legend).
 * Borders for non-root nodes use borderDefault; root keeps a purple outline for the seed URL.
 */
export const GRAPH_NODE_PALETTE = {
  visited: { bg: "#15803d", borderDefault: "#166534" },
  queued: { bg: "#9ca3af", borderDefault: "#6b7280" },
  inProgress: { bg: "#ea580c", borderDefault: "#c2410c" },
  /** FAILED when worker classified redirect as unexpected (not the same as hard fetch failure). */
  redirect301: { bg: "#2563eb", borderDefault: "#1d4ed8" },
  failed: { bg: "#b91c1c", borderDefault: "#991b1b" }
};

function lastErrorText(row) {
  return String(row?.last_error ?? "");
}

function isRedirect301Failure(row) {
  return normalizeStatus(row.status) === "FAILED" && lastErrorText(row).includes("unexpected_http_301");
}

/**
 * Maps a crawl_urls row to a palette key (failed split into redirect vs hard failure).
 */
export function classifyGraphNodeKind(row) {
  const s = normalizeStatus(row?.status);
  if (s === "FAILED") {
    return isRedirect301Failure(row) ? "redirect301" : "failed";
  }
  if (s === "VISITED" || s === "COMPLETED" || s === "SUCCEEDED") {
    return "visited";
  }
  if (s === "IN_PROGRESS") {
    return "inProgress";
  }
  if (s === "QUEUED") {
    return "queued";
  }
  return "queued";
}

export function colorsForGraphNode(row, isRoot) {
  const kind = classifyGraphNodeKind(row);
  const p = GRAPH_NODE_PALETTE[kind];
  return {
    background: p.bg,
    border: isRoot ? "#4c1d95" : p.borderDefault
  };
}

/**
 * Build graph nodes/edges from /urls + /graph payloads.
 * Keeps transport and rendering separated so polling can be replaced by SSE later.
 */
export function buildLineageGraph(urlsPayload, graphPayload) {
  const rows = urlsPayload?.urls ?? [];
  const edgesRaw = graphPayload?.edges ?? [];
  const nodeMeta = new Map(rows.map((r) => [Number(r.id), r]));

  const nodes = rows.map((r) => {
    const isRoot = r.discovered_from_url_id == null;
    const { background, border } = colorsForGraphNode(r, isRoot);
    return {
      id: Number(r.id),
      label: String(r.id),
      shape: "dot",
      size: isRoot ? 13 : 9,
      color: {
        background,
        border
      },
      font: { color: "#111", size: 11 },
      title: formatNodeInfo(r),
      meta: r
    };
  });

  const edges = edgesRaw.map((e) => ({
    id: `${e.from_url_id}->${e.to_url_id}`,
    from: Number(e.from_url_id),
    to: Number(e.to_url_id),
    arrows: "to",
    color: { color: "#b6b6b6" }
  }));

  return {
    nodes,
    edges,
    nodeMeta,
    edgeCount: edges.length,
    nodeCount: nodes.length
  };
}

export function formatNodeInfo(row) {
  if (!row) {
    return "No node selected";
  }
  return [
    `id: ${row.id}`,
    `url: ${row.normalized_url ?? ""}`,
    `status: ${row.status ?? ""}`,
    `depth: ${row.depth ?? 0}`,
    `discovered_from_url_id: ${row.discovered_from_url_id ?? "-"}`,
    `last_error: ${row.last_error ?? "-"}`,
    `retry_count: ${row.retry_count ?? 0}`
  ].join("\n");
}
