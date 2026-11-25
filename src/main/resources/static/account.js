(() => {
  const $ = (id) => document.getElementById(id);
  const state = { connector: null, agent: null, publicUrl: location.origin, selectedClusterId: null, account: null };

  function when(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    const seconds = Math.round((Date.now() - d.getTime()) / 1000);
    if (seconds < 60) return "just now";
    if (seconds < 3600) return Math.round(seconds / 60) + " min ago";
    if (seconds < 86400) return Math.round(seconds / 3600) + " h ago";
    return d.toLocaleDateString();
  }
  function cell(text) { const td = document.createElement("td"); td.textContent = text; return td; }
  function stat(value, label, bad) {
    const div = document.createElement("div");
    div.className = "stat" + (bad ? " bad" : "");
    const v = document.createElement("div"); v.className = "v"; v.textContent = value == null ? "–" : Number(value).toLocaleString();
    const k = document.createElement("div"); k.className = "k"; k.textContent = label;
    div.append(v, k);
    return div;
  }

  function renderSnippets() {
    const connector = state.connector || "jrc_read_…your connector token…";
    const agent = state.agent || "jra_…your agent token…";
    const url = state.publicUrl + "/mcp";
    $("snippet-spring").textContent =
`# application.properties
jobrunr.dashboard.mcp.token=${connector}` + (state.publicUrl !== "https://mcp.jobrunr.io" ? `\njobrunr.dashboard.mcp.url=${state.publicUrl}` : "");
    $("snippet-java").textContent =
`// next to JobRunr.configure()...initialize(), with the same storageProvider
new JobRunrMcpConnector("${connector}"` + (state.publicUrl !== "https://mcp.jobrunr.io" ? `, "${state.publicUrl}", storageProvider, JsonMapperFactory.createJsonMapper()` : ", storageProvider") + `).start();`;
    $("snippet-claude").textContent = `claude mcp add --transport http jobrunr ${url} \\\n  --header "Authorization: Bearer ${agent}"`;
    $("snippet-cursor").textContent = JSON.stringify({ mcpServers: { jobrunr: { url, headers: { Authorization: "Bearer " + agent } } } }, null, 2);
    $("snippet-vscode").textContent = JSON.stringify({ servers: { jobrunr: { type: "http", url, headers: { Authorization: "Bearer " + agent } } } }, null, 2);
  }

  function render(account) {
    state.account = account;
    state.publicUrl = account.publicUrl || location.origin;
    $("who").textContent = account.email;
    $("subtitle").textContent = account.name + " · " + account.company;
    $("plan-note").textContent = account.proEnabled
      ? "JobRunr Pro: unlimited connected nodes and clusters. Use list_clusters in your MCP client and pass cluster_id to select a cluster."
      : "Free plan: one connected cluster with up to two live nodes. JobRunr Pro allows unlimited connected nodes and clusters.";

    const snapshots = (account.instances && account.instances.length > 0)
      ? account.instances : (account.instance ? [account.instance] : []);
    const defaultId = account.instance && account.instance.clusterId;
    const knownIds = snapshots.map((item) => item.clusterId);
    state.selectedClusterId = account.proEnabled && knownIds.includes(state.selectedClusterId)
      ? state.selectedClusterId
      : (knownIds.includes(defaultId) ? defaultId : knownIds[0] || null);
    const picker = $("cluster-select");
    picker.replaceChildren();
    snapshots.forEach((snapshot) => {
      const option = document.createElement("option");
      option.value = snapshot.clusterId;
      option.textContent = snapshot.clusterId + (snapshot.clusterId === defaultId ? " (default)" : "");
      picker.append(option);
    });
    if (state.selectedClusterId) picker.value = state.selectedClusterId;
    $("cluster-picker-label").classList.toggle("hidden", !account.proEnabled || snapshots.length < 2);

    const nodes = account.nodes || [];
    const selectedNodes = account.proEnabled && state.selectedClusterId
      ? nodes.filter((node) => node.clusterId === state.selectedClusterId) : nodes;
    const instance = snapshots.find((snapshot) => snapshot.clusterId === state.selectedClusterId) || account.instance;
    $("status-dot").classList.toggle("on", selectedNodes.length > 0);
    if (selectedNodes.length > 0) {
      const n = selectedNodes[0];
      $("status-text").textContent = "Connected" + (selectedNodes.length > 1 ? ` (${selectedNodes.length} instances)` : "");
      $("status-detail").textContent = `JobRunr ${n.jobrunrVersion || "?"} on ${n.framework || "?"}, Java ${n.javaVersion || "?"}. ${n.redaction ? "Connector reports parameter redaction enabled" : "Job parameter values are sent as-is"}. Last seen ${when(n.lastSeenAt)}.`;
    } else {
      $("status-text").textContent = account.proEnabled && nodes.length > 0
        ? "Selected cluster not connected right now" : (instance ? "Not connected right now" : "Waiting for your application");
      $("status-detail").textContent = instance
        ? `Last snapshot ${when(instance.snapshotAt)}. Start your application with the connector token to reconnect.`
        : "Add the connector token to your application and start it. This page updates by itself.";
    }
    const stats = $("stats");
    stats.replaceChildren();
    if (instance) {
      stats.append(
        stat(instance.jobsFailed, "failed jobs", instance.jobsFailed > 0),
        stat(instance.jobsEnqueued, "enqueued"),
        stat(instance.jobsProcessing, "processing"),
        stat(instance.jobsScheduled, "scheduled"),
        stat(instance.jobsSucceeded, "succeeded"),
        stat(instance.servers, "servers"),
        stat(instance.recurringJobs, "recurring jobs"));
      $("stats-age").textContent = "Numbers updated " + when(instance.snapshotAt) + (selectedNodes.length > 0 ? ", refreshed while this page is open." : ".");
    }
    $("stats-age").classList.toggle("hidden", !instance);

    const tokens = account.tokens || [];
    const hasTokens = tokens.length > 0;
    $("tokens-none").classList.toggle("hidden", hasTokens || !!state.agent);
    $("tokens-active").classList.toggle("hidden", !hasTokens || !!state.agent);
    $("issue").textContent = hasTokens ? "Create new tokens" : "Create tokens";
    const rows = $("token-rows");
    rows.replaceChildren();
    tokens.forEach((t) => {
      const tr = document.createElement("tr");
      tr.append(
        cell((t.kind === "connector" ? "Connector" : "Agent") + " …" + t.hint),
        cell(t.kind === "connector" ? (t.scope === "operate" ? "read and operate" : "read-only") : "your MCP client"),
        cell(new Date(t.createdAt).toLocaleDateString()));
      rows.append(tr);
    });

    const calls = account.recentToolCalls || [];
    $("activity-empty").classList.toggle("hidden", calls.length > 0);
    $("activity").classList.toggle("hidden", calls.length === 0);
    const activity = $("activity-rows");
    activity.replaceChildren();
    calls.forEach((c) => {
      const tr = document.createElement("tr");
      tr.append(cell(c.toolName), cell(when(c.occurredAt)), cell(c.success ? "ok" : "failed"));
      activity.append(tr);
    });
    renderSnippets();
  }

  $("cluster-select").addEventListener("change", (event) => {
    state.selectedClusterId = event.target.value;
    if (state.account) render(state.account);
  });

  async function load() {
    const response = await fetch("/api/account", { headers: { Accept: "application/json" } });
    if (response.status === 401) { location.replace("/signup#signin"); return; }
    render(await response.json());
  }

  $("issue").addEventListener("click", async () => {
    const hadTokens = !$("tokens-active").classList.contains("hidden") || !!state.agent;
    if (hadTokens && !confirm("Create new tokens? The current connector and agent tokens stop working right away.")) return;
    $("issue").disabled = true;
    $("tokens-error").classList.remove("show");
    try {
      const response = await fetch("/api/account/tokens", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ operate: $("operate").checked }) });
      const json = await response.json();
      if (!response.ok) throw new Error(json.error || "Could not create tokens.");
      state.connector = json.connectorToken;
      state.agent = json.agentToken;
      $("fresh-connector").textContent = json.connectorToken;
      $("fresh-agent").textContent = json.agentToken;
      $("tokens-fresh").classList.remove("hidden");
      await load();
    } catch (e) {
      $("tokens-error").textContent = e.message;
      $("tokens-error").classList.add("show");
    } finally {
      $("issue").disabled = false;
    }
  });

  document.querySelectorAll(".tabs").forEach((tabs) => {
    tabs.addEventListener("click", (event) => {
      const tab = event.target.closest(".tab");
      if (!tab) return;
      const group = tabs.dataset.group;
      tabs.querySelectorAll(".tab").forEach((t) => t.setAttribute("aria-selected", String(t === tab)));
      document.querySelectorAll(`[data-panel^="${group}:"]`).forEach((panel) => panel.classList.toggle("hidden", panel.dataset.panel !== `${group}:${tab.dataset.tab}`));
    });
  });
  document.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-copy]");
    if (!button) return;
    try {
      await navigator.clipboard.writeText($(button.dataset.copy).textContent);
      button.textContent = "Copied";
      setTimeout(() => { button.textContent = "Copy"; }, 1500);
    } catch (e) { /* clipboard unavailable, the text is selectable */ }
  });
  $("signout").addEventListener("click", async () => {
    await fetch("/api/signout", { method: "POST" });
    location.replace("/signup");
  });

  load();
  setInterval(() => { if (!document.hidden) load(); }, 10000);
})();
