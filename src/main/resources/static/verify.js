(async () => {
  // the secret travels in the fragment, so it never reaches server logs and link scanners cannot use it up
  const token = location.hash.slice(1);
  history.replaceState(null, "", "/verify");
  const fail = (message) => {
    document.getElementById("title").textContent = "This link did not work";
    document.getElementById("message").textContent = message;
    document.getElementById("retry").classList.remove("hidden");
  };
  if (!token) { fail("The link is incomplete. Request a new one."); return; }
  try {
    const response = await fetch("/api/verify", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token }) });
    const json = await response.json().catch(() => ({}));
    if (!response.ok) { fail(json.error || "Please request a new link."); return; }
    location.replace("/account");
  } catch (e) {
    fail("We could not reach the server. Please try again.");
  }
})();
