/**
 * Thin vis-network wrapper; rendering stays isolated from fetch/poll logic.
 */
export function createLineageGraphView(containerEl, infoEl, hooks = {}) {
  const { onUserViewportInteraction } = hooks;
  let suppressViewportInteraction = false;
  let fitSoonTimer = null;
  let largeGraphMode = false;

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

  const normalNetworkOptions = {
    physics: physicsOptions,
    edges: { smooth: { type: "continuous" }, arrows: { to: true } },
    interaction: { hover: true, navigationButtons: true, zoomView: true }
  };
  const largeNetworkOptions = {
    // Freeze layout for responsiveness once graphs get large.
    physics: false,
    edges: { smooth: false, arrows: { to: true } },
    interaction: { hover: false, navigationButtons: true, zoomView: true }
  };

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

  function notifyViewportInteraction() {
    if (suppressViewportInteraction) {
      return;
    }
    onUserViewportInteraction?.();
  }

  network.on("dragEnd", notifyViewportInteraction);
  network.on("zoom", notifyViewportInteraction);

  /**
   * Fit viewport to all nodes; modest animation. Suppresses viewport “interaction” callbacks during programmatic fit.
   */
  function fit(options = {}) {
    const duration = options.duration ?? 320;
    const easingFunction = options.easingFunction ?? "easeInOutQuad";
    suppressViewportInteraction = true;
    network.fit({
      animation: {
        duration,
        easingFunction
      }
    });
    setTimeout(() => {
      suppressViewportInteraction = false;
    }, duration + 100);
  }

  /**
   * Debounced fit after a short delay so physics can spread new nodes; repeated calls reset the timer.
   */
  function fitSoon(options = {}) {
    const { delayMs = 350, ...fitOpts } = options;
    if (fitSoonTimer !== null) {
      clearTimeout(fitSoonTimer);
    }
    fitSoonTimer = setTimeout(() => {
      fitSoonTimer = null;
      fit(fitOpts);
    }, delayMs);
  }

  function getCounts() {
    return {
      nodeCount: data.nodes.getIds().length,
      edgeCount: data.edges.getIds().length
    };
  }

  function render(model) {
    syncDataSet(data.nodes, model.nodes);
    syncDataSet(data.edges, model.edges);

    return {
      nodeCount: model.nodes.length,
      edgeCount: model.edges.length
    };
  }

  function clear(message) {
    if (fitSoonTimer !== null) {
      clearTimeout(fitSoonTimer);
      fitSoonTimer = null;
    }
    data.nodes.clear();
    data.edges.clear();
    setLargeGraphMode(false);
    infoEl.textContent = message ?? "No node selected";
  }

  function setLargeGraphMode(enabled) {
    const next = Boolean(enabled);
    if (next === largeGraphMode) {
      return;
    }
    largeGraphMode = next;
    network.setOptions(next ? largeNetworkOptions : normalNetworkOptions);
    if (!next) {
      network.startSimulation();
    }
  }

  function isLargeGraphMode() {
    return largeGraphMode;
  }

  function resumeLayout() {
    if (!network || largeGraphMode) {
      return;
    }
    network.startSimulation();
  }

  return { render, clear, fit, fitSoon, getCounts, setLargeGraphMode, isLargeGraphMode, resumeLayout };
}
