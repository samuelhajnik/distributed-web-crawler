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

  const physicsOptions = {
    enabled: true,
    barnesHut: { gravitationalConstant: -2800, springLength: 120, springConstant: 0.04 }
  };

  const network = new visNetwork.Network(
    containerEl,
    data,
    {
      physics: physicsOptions,
      layout: { improvedLayout: true },
      edges: { smooth: { type: "continuous" }, arrows: { to: true } },
      interaction: { hover: true, navigationButtons: true, zoomView: true },
      nodes: { borderWidth: 1, borderWidthSelected: 2 }
    }
  );

  /** True until the first post-clear render has registered a one-shot freeze after stabilization. */
  let pendingPhysicsFreeze = true;

  function syncDataSet(dataSet, nextItems) {
    const nextIds = new Set(nextItems.map((item) => String(item.id)));
    const existingIds = dataSet.getIds();
    const toRemove = existingIds.filter((id) => !nextIds.has(String(id)));
    if (toRemove.length) {
      dataSet.remove(toRemove);
    }
    if (nextItems.length) {
      dataSet.update(nextItems);
    }
  }

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
    syncDataSet(data.nodes, model.nodes);
    syncDataSet(data.edges, model.edges);

    if (model.nodes.length > 0 && pendingPhysicsFreeze) {
      pendingPhysicsFreeze = false;
      network.setOptions({ physics: physicsOptions });
      network.once("stabilizationIterationsDone", () => {
        network.setOptions({ physics: false });
      });
    }
  }

  function clear(message) {
    pendingPhysicsFreeze = true;
    data.nodes.clear();
    data.edges.clear();
    network.setOptions({ physics: physicsOptions });
    infoEl.textContent = message ?? "No node selected";
  }

  return { render, clear };
}
