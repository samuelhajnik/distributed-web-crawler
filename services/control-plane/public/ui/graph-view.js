/**
 * Thin vis-network wrapper; rendering stays isolated from fetch/poll logic.
 */
export function createLineageGraphView(containerEl, infoEl) {
  // vis-network UMD global loaded from CDN in index.html
  const visNetwork = window.vis;
  const data = {
    nodes: new visNetwork.DataSet([]),
    edges: new visNetwork.DataSet([])
  };
  const network = new visNetwork.Network(
    containerEl,
    data,
    {
      physics: {
        stabilization: false,
        barnesHut: { gravitationalConstant: -2800, springLength: 120, springConstant: 0.04 }
      },
      layout: { improvedLayout: true },
      edges: { smooth: { type: "continuous" }, arrows: { to: true } },
      interaction: { hover: true, navigationButtons: true, zoomView: true },
      nodes: { borderWidth: 1, borderWidthSelected: 2 }
    }
  );

  network.on("click", (params) => {
    if (!params.nodes?.length) {
      return;
    }
    const node = data.nodes.get(params.nodes[0]);
    if (!node) {
      return;
    }
    infoEl.textContent = node.title ?? "No node metadata";
  });

  function render(model) {
    data.nodes.clear();
    data.edges.clear();
    data.nodes.add(model.nodes);
    data.edges.add(model.edges);
  }

  function clear(message) {
    data.nodes.clear();
    data.edges.clear();
    infoEl.textContent = message ?? "No node selected";
  }

  return { render, clear };
}
