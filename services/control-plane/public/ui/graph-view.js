/**
 * Thin vis-network wrapper; rendering stays isolated from fetch/poll logic.
 */
export function createLineageGraphView(containerEl, infoEl, hooks = {}) {
  const { onUserViewportInteraction } = hooks;
  let suppressViewportInteraction = false;
  let fitSoonTimer = null;
  /** One delayed follow-up wake after tab resume (coalesced). */
  let wakeFollowUpTimer = null;
  const WAKE_FOLLOWUP_MS = 200;
  /** Terminal run (COMPLETED/FAILED): freeze layout — static nodes, no hover churn. */
  let completedMode = false;

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

  /** Completed/failed run: static layout, no hover highlights — avoids costly redraw on mousemove vs idle physics. */
  const completedNetworkOptions = {
    physics: false,
    edges: { smooth: { type: "continuous" }, arrows: { to: true } },
    interaction: { hover: false, navigationButtons: true, zoomView: true }
  };

  function getEffectiveNetworkOptions() {
    return completedMode ? completedNetworkOptions : normalNetworkOptions;
  }

  /**
   * Apply mode flags to vis-network. Starts physics only while the crawl is active (non-terminal).
   */
  function applyEffectiveNetworkOptions() {
    network.setOptions(getEffectiveNetworkOptions());
    if (!completedMode) {
      network.startSimulation();
    }
  }

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
    if (wakeFollowUpTimer !== null) {
      clearTimeout(wakeFollowUpTimer);
      wakeFollowUpTimer = null;
    }
    data.nodes.clear();
    data.edges.clear();
    completedMode = false;
    applyEffectiveNetworkOptions();
    infoEl.textContent = message ?? "No node selected";
  }

  /**
   * When true, the crawl is COMPLETED or FAILED — keep the graph static (no physics, no hover).
   */
  function setCompletedMode(enabled) {
    const next = Boolean(enabled);
    if (next === completedMode) {
      return;
    }
    completedMode = next;
    applyEffectiveNetworkOptions();
  }

  function isCompletedMode() {
    return completedMode;
  }

  function resumeLayout() {
    if (!network || completedMode) {
      return;
    }
    network.startSimulation();
  }

  /**
   * After a tab was backgrounded, browsers may freeze physics timers. Re-apply normal-mode
   * options (re-arms physics), restart simulation, optional stabilize, redraw, rAF nudge, and
   * one bounded delayed follow-up — skipped when the graph is frozen (completed run).
   */
  function wakeFromBackgroundStrong() {
    if (!network || completedMode) {
      return;
    }
    if (wakeFollowUpTimer !== null) {
      clearTimeout(wakeFollowUpTimer);
      wakeFollowUpTimer = null;
    }

    network.setOptions(normalNetworkOptions);
    network.startSimulation();
    if (typeof network.stabilize === "function") {
      try {
        network.stabilize(50);
      } catch (_err) {
        /* ignore if engine rejects small stabilize */
      }
    }
    if (typeof network.redraw === "function") {
      network.redraw();
    }

    window.requestAnimationFrame(() => {
      if (!network || completedMode) {
        return;
      }
      network.startSimulation();
    });

    wakeFollowUpTimer = setTimeout(() => {
      wakeFollowUpTimer = null;
      if (!network || completedMode) {
        return;
      }
      network.startSimulation();
      if (typeof network.redraw === "function") {
        network.redraw();
      }
    }, WAKE_FOLLOWUP_MS);
  }

  return {
    render,
    clear,
    fit,
    fitSoon,
    getCounts,
    setCompletedMode,
    isCompletedMode,
    resumeLayout,
    wakeFromBackgroundStrong
  };
}
