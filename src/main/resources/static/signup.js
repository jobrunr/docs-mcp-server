(() => {
  const $ = (id) => document.getElementById(id);
  const FREE_MAIL = ["gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com", "yahoo.com", "icloud.com", "me.com", "proton.me", "protonmail.com", "gmx.com", "gmx.de", "aol.com", "mail.com", "yandex.com", "telenet.be", "skynet.be"];
  let companyTouched = false;
  let lastFlow = "signup";

  function show(view) {
    ["signup-view", "signin-view", "sent-view"].forEach((v) => $(v).classList.toggle("hidden", v !== view));
  }
  function showError(id, message) {
    const el = $(id);
    el.textContent = message;
    el.classList.add("show");
  }
  function clearError(id) {
    $(id).classList.remove("show");
  }

  $("company").addEventListener("input", () => { companyTouched = true; });
  $("email").addEventListener("input", () => {
    if (companyTouched) return;
    const domain = ($("email").value.split("@")[1] || "").toLowerCase();
    if (!domain.includes(".") || FREE_MAIL.includes(domain)) { $("company").value = ""; return; }
    const name = domain.split(".").slice(-2, -1)[0] || "";
    $("company").value = name.split(/[-_]/).filter(Boolean).map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
  });

  async function post(url, body) {
    const response = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    const json = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(json.error || "Something went wrong. Please try again.");
    return json;
  }

  $("signup-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    clearError("signup-error");
    const form = event.target;
    const data = Object.fromEntries(new FormData(form).entries());
    if (!data.name || !data.email || !data.company) { showError("signup-error", "Please fill in your name, work email and company."); return; }
    $("signup-submit").disabled = true;
    try {
      await post("/api/signup", data);
      lastFlow = "signup";
      $("sent-prefix").textContent = "We sent a confirmation link to";
      $("sent-to").textContent = data.email;
      show("sent-view");
    } catch (e) {
      showError("signup-error", e.message);
    } finally {
      $("signup-submit").disabled = false;
    }
  });

  $("signin-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    clearError("signin-error");
    const email = $("signin-email").value.trim();
    if (!email) { showError("signin-error", "Please enter your email."); return; }
    $("signin-submit").disabled = true;
    try {
      await post("/api/signin", { email });
      lastFlow = "signin";
      $("sent-prefix").textContent = "If an account exists, a sign-in link is on its way to";
      $("sent-to").textContent = email;
      show("sent-view");
    } catch (e) {
      showError("signin-error", e.message);
    } finally {
      $("signin-submit").disabled = false;
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

  $("show-signin").addEventListener("click", () => show("signin-view"));
  $("show-signup").addEventListener("click", () => show("signup-view"));
  $("try-again").addEventListener("click", () => show(lastFlow === "signin" ? "signin-view" : "signup-view"));
  if (location.hash === "#signin") show("signin-view");
})();
