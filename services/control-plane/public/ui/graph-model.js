function normalizeStatus(status) {
  return String(status ?? "").toUpperCase();
}

function statusColor(status, isRoot) {
  if (isRoot) {
    return "#6d28d9";
  }
  const s = normalizeStatus(status);
  if (s === "FAILED") {
    return "#b91c1c";
  }
  if (s === "VISITED" || s === "COMPLETED" || s === "SUCCEEDED") {
    return "#15803d";
  }
  if (s === "IN_PROGRESS") {
    return "#0369a1";
  }
  return "#b45309";
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
    const color = statusColor(r.status, isRoot);
    return {
      id: Number(r.id),
      label: String(r.id),
      shape: "dot",
      size: isRoot ? 13 : 9,
      color: {
        background: color,
        border: isRoot ? "#4c1d95" : "#333333"
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
    edgeCount: Number(graphPayload?.edge_count ?? 0),
    nodeCount: Number(graphPayload?.node_count ?? rows.length)
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
    `discovered_from_url_id: ${row.discovered_from_url_id ?? "-"}`,
    `last_error: ${row.last_error ?? "-"}`,
    `retry_count: ${row.retry_count ?? 0}`
  ].join("\n");
}
