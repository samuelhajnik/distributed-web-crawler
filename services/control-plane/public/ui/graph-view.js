/**
 * Thin vis-network wrapper; rendering stays isolated from fetch/poll logic.
 */
export function createLineageGraphView(containerEl, infoEl, hooks = {}) {
  const { onAutoZoomChange, onUserViewportInteraction } = hooks;
  let suppressViewportInteraction = false;
  let fitSoonTimer = null;
  /** One delayed follow-up wake after tab resume (coalesced). */
  let wakeFollowUpTimer = null;
  const WAKE_FOLLOWUP_MS = 200;
  let terminalFinalizeTimer = null;
  let finalizationSeq = 0;
  let finalizationInFlight = null;
  /** Terminal run (COMPLETED/FAILED): freeze layout — static nodes, no hover churn. */
  let completedMode = false;
  /** Auto-zoom controls only automatic fit; manual "Fit graph" always works. */
  let autoZoomEnabled = true;
  /** Last rendered model snapshot for tab-return recovery refreshes. */
  let lastModel = null;

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

  function cloneModel(model) {
    return {
      nodes: model.nodes.map((n) => ({ ...n })),
      edges: model.edges.map((e) => ({ ...e })),
      nodeCount: model.nodeCount,
      edgeCount: model.edgeCount
    };
  }

  function clearPendingTimers() {
    if (fitSoonTimer !== null) {
      clearTimeout(fitSoonTimer);
      fitSoonTimer = null;
    }
    if (wakeFollowUpTimer !== null) {
      clearTimeout(wakeFollowUpTimer);
      wakeFollowUpTimer = null;
    }
    if (terminalFinalizeTimer !== null) {
      clearTimeout(terminalFinalizeTimer);
      terminalFinalizeTimer = null;
    }
  }

  function replaceAllData(model) {
    data.nodes.clear();
    data.edges.clear();
    if (model.nodes.length) {
      data.nodes.add(model.nodes);
    }
    if (model.edges.length) {
      data.edges.add(model.edges);
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
    setAutoZoomEnabled(false);
    onUserViewportInteraction?.();
  }

  network.on("dragEnd", notifyViewportInteraction);
  network.on("zoom", notifyViewportInteraction);

  function setAutoZoomEnabled(enabled) {
    const next = Boolean(enabled);
    if (next === autoZoomEnabled) {
      return;
    }
    autoZoomEnabled = next;
    onAutoZoomChange?.(autoZoomEnabled);
  }

  function isAutoZoomEnabled() {
    return autoZoomEnabled;
  }

  function getAutoCenterScaleMultiplier(nodeCount) {
    if (nodeCount <= 120) {
      return 0.94;
    }
    if (nodeCount <= 300) {
      return 0.97;
    }
    return 1;
  }

  function performFit(options = {}) {
    const requireAutoCenter = options.requireAutoCenter ?? false;
    const duration = options.duration ?? (requireAutoCenter ? 0 : 320);
    const easingFunction = options.easingFunction ?? "easeInOutQuad";
    if (requireAutoCenter && !autoZoomEnabled) {
      return;
    }
    suppressViewportInteraction = true;
    network.fit({
      animation: {
        duration,
        easingFunction
      }
    });
    if (requireAutoCenter && !completedMode) {
      // Early graph states can be framed slightly too tight; apply a small one-shot
      // conservative scale adjustment for smaller graphs instead of delayed choreography.
      const multiplier = getAutoCenterScaleMultiplier(getCounts().nodeCount);
      if (multiplier < 1 && typeof network.getScale === "function" && typeof network.moveTo === "function") {
        const currentScale = Number(network.getScale());
        if (Number.isFinite(currentScale) && currentScale > 0) {
          network.moveTo({ scale: currentScale * multiplier, animation: false });
        }
      }
    }
    setTimeout(() => {
      suppressViewportInteraction = false;
    }, duration + 100);
  }

  /**
   * Fit viewport to all nodes; modest animation. Suppresses viewport “interaction” callbacks during programmatic fit.
   */
  function fit(options = {}) {
    performFit(options);
  }

  /**
   * Debounced fit after a short delay so physics can spread new nodes; repeated calls reset the timer.
   */
  function fitSoon(options = {}) {
    if (!autoZoomEnabled) {
      return;
    }
    const { delayMs = 350, ...fitOpts } = options;
    if (fitSoonTimer !== null) {
      clearTimeout(fitSoonTimer);
    }
    fitSoonTimer = setTimeout(() => {
      fitSoonTimer = null;
      performFit({ ...fitOpts, requireAutoCenter: true });
    }, delayMs);
  }

  function getCounts() {
    return {
      nodeCount: data.nodes.getIds().length,
      edgeCount: data.edges.getIds().length
    };
  }

  function render(model) {
    lastModel = cloneModel(model);
    syncDataSet(data.nodes, model.nodes);
    syncDataSet(data.edges, model.edges);

    return {
      nodeCount: model.nodes.length,
      edgeCount: model.edges.length
    };
  }

  function clear(message) {
    clearPendingTimers();
    finalizationSeq += 1;
    data.nodes.clear();
    data.edges.clear();
    lastModel = null;
    completedMode = false;
    setAutoZoomEnabled(true);
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

  function pauseForHiddenTab() {
    clearPendingTimers();
    if (!network || completedMode) {
      return;
    }
    if (typeof network.stopSimulation === "function") {
      network.stopSimulation();
    }
  }

  /**
   * Hidden tabs heavily throttle timers/animation; restart physics+redraw from a clean paused state.
   * No fit here so the user's viewport remains stable on tab restore.
   */
  function resumeAfterHiddenTab() {
    if (!network || completedMode) {
      return;
    }
    network.setOptions(normalNetworkOptions);
    network.startSimulation();
    if (typeof network.redraw === "function") {
      network.redraw();
    }
    window.requestAnimationFrame(() => {
      if (!network || completedMode) {
        return;
      }
      if (typeof network.redraw === "function") {
        network.redraw();
      }
      network.startSimulation();
    });
  }

  /**
   * Browsers throttle hidden tabs heavily; repeated startSimulation nudges are sometimes not enough
   * on larger graphs. This performs a stronger data/physics re-arm without recreating Network.
   * By default no fit is performed, preserving the user's viewport on tab return.
   */
  function refreshFromModel(model, options = {}) {
    const { fit: shouldFit = false } = options;
    if (!network || !model) {
      return;
    }
    clearPendingTimers();
    const normalized = cloneModel(model);
    lastModel = normalized;

    suppressViewportInteraction = true;
    replaceAllData(normalized);

    if (completedMode) {
      network.setOptions(completedNetworkOptions);
    } else {
      if (typeof network.stopSimulation === "function") {
        network.stopSimulation();
      }
      network.setOptions(normalNetworkOptions);
      network.startSimulation();
    }
    if (typeof network.redraw === "function") {
      network.redraw();
    }
    window.requestAnimationFrame(() => {
      if (!network) {
        return;
      }
      if (typeof network.redraw === "function") {
        network.redraw();
      }
      if (!completedMode) {
        network.startSimulation();
      }
      suppressViewportInteraction = false;
    });

    if (shouldFit) {
      fit();
    }
  }

  function getLastModel() {
    return lastModel;
  }

  /**
   * Terminal runs should settle briefly before freezing; immediate physics stop can preserve
   * a poor intermediate layout. We wait for vis "stabilized" or a bounded timeout, then freeze.
   */
  async function finalizeCompletedLayout(options = {}) {
    const { maxSettleMs = 1800, fit: shouldFit = false } = options;
    if (!network) {
      return;
    }
    if (finalizationInFlight) {
      return finalizationInFlight;
    }

    const seq = ++finalizationSeq;
    const settlePromise = new Promise((resolve) => {
      let done = false;
      let removeStabilizedListener = () => {};
      const onStabilized = () => {
        finish();
      };

      const cleanup = () => {
        removeStabilizedListener();
        if (terminalFinalizeTimer !== null) {
          clearTimeout(terminalFinalizeTimer);
          terminalFinalizeTimer = null;
        }
      };

      const finish = () => {
        if (done) {
          return;
        }
        done = true;
        cleanup();
        if (seq !== finalizationSeq) {
          resolve();
          return;
        }
        completedMode = true;
        applyEffectiveNetworkOptions();
        if (typeof network.redraw === "function") {
          network.redraw();
        }
        resolve();
      };

      completedMode = false;
      network.setOptions(normalNetworkOptions);
      if (typeof network.stopSimulation === "function") {
        network.stopSimulation();
      }
      network.startSimulation();
      if (typeof network.redraw === "function") {
        network.redraw();
      }
      if (shouldFit) {
        fit();
      }

      if (typeof network.once === "function") {
        network.once("stabilized", onStabilized);
      } else if (typeof network.on === "function" && typeof network.off === "function") {
        network.on("stabilized", onStabilized);
        removeStabilizedListener = () => {
          network.off("stabilized", onStabilized);
        };
      }

      terminalFinalizeTimer = setTimeout(() => {
        terminalFinalizeTimer = null;
        finish();
      }, maxSettleMs);

      window.requestAnimationFrame(() => {
        if (done || seq !== finalizationSeq) {
          return;
        }
        if (typeof network.redraw === "function") {
          network.redraw();
        }
        network.startSimulation();
      });
    });

    finalizationInFlight = settlePromise;
    try {
      await settlePromise;
    } finally {
      if (finalizationInFlight === settlePromise) {
        finalizationInFlight = null;
      }
    }
  }

  /**
   * After a tab was backgrounded, browsers may freeze physics timers. Re-apply normal-mode
   * options (re-arms physics), restart simulation, redraw, rAF nudge, and one bounded delayed
   * follow-up startSimulation — skipped when the graph is frozen (completed run).
   */
  function wakeFromBackgroundStrong() {
    resumeAfterHiddenTab();
  }

  return {
    render,
    clear,
    fit,
    fitSoon,
    getCounts,
    getLastModel,
    setAutoZoomEnabled,
    isAutoZoomEnabled,
    refreshFromModel,
    finalizeCompletedLayout,
    setCompletedMode,
    isCompletedMode,
    resumeLayout,
    pauseForHiddenTab,
    resumeAfterHiddenTab,
    wakeFromBackgroundStrong
  };
}
