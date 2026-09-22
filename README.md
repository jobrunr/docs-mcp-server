# jobrunr-docs-mcp

Hosted [Model Context Protocol](https://modelcontextprotocol.io) server that exposes [JobRunr](https://www.jobrunr.io) documentation to AI coding assistants (Claude Code, Cursor, VS Code, Windsurf, ChatGPT desktop, ...).

Add one URL to your IDE config and JobRunr docs are live in every conversation — your agent stops hallucinating APIs and starts citing real `jobrunr.io` pages.

**Live endpoint:** `https://mcp.jobrunr.io/mcp`

Built on Spring Boot 4.1 + Spring AI 2.0's MCP server starter + Apache Lucene (BM25) + a local ONNX MiniLM embedding model. Retrieval is hybrid: BM25 and semantic search combined via Reciprocal Rank Fusion.

### Transports

The server speaks **Streamable HTTP** at `/mcp` — the transport every current MCP client uses, where each
JSON-RPC message is its own HTTP POST. It negotiates protocol versions `2024-11-05` through `2025-11-25`;
newer clients negotiate down to `2025-11-25`.

The deprecated **HTTP+SSE** transport (`GET /sse` + `POST /mcp/message`, protocol `2024-11-05`) still runs
alongside it so clients installed before Streamable HTTP landed keep working. It is deprecated upstream and
will be removed once that traffic stops — new installs should use `/mcp`. Set `MCP_LEGACY_SSE_ENABLED=false`
to switch it off.

## Tools

| Tool | Purpose |
|---|---|
| `search_jobrunr_docs(query, limit?)` | Hybrid search. Returns ranked pages with title, URL, section, tier (`oss` or `pro`), and a highlighted snippet. When any result is `tier: "pro"`, the response also carries a `proTrialHint` pointing the agent at the trial tool. |
| `fetch_jobrunr_doc(path)` | Full markdown for a single page. Use the `path` field from `search_jobrunr_docs` results. |
| `list_jobrunr_doc_sections()` | Grouped TOC. Useful for browsing before searching. |
| `request_jobrunr_pro_trial(email, company, interested_in?, use_case?)` | Submit a free JobRunr Pro trial request on the user's behalf. The agent is instructed to offer this when surfacing Pro features. Always collects email and company from the user — never invented. |

### How the Pro trial flow works

1. User asks about a JobRunr Pro feature (priority queues, real-time scheduling, multi-cluster dashboard, ...).
2. `search_jobrunr_docs` returns the Pro page plus a `proTrialHint` telling the agent a trial is available.
3. The agent offers: "This is a JobRunr Pro feature — want me to request a free trial for you?"
4. If the user agrees, the agent asks for their email and company, then calls `request_jobrunr_pro_trial`.
5. The tool POSTs to a configured n8n webhook with `{email, company, form: "mcp-trial", interested_in?, use_case?, submitted_at}` and returns a success or failure message that the agent relays.

The `form: "mcp-trial"` field is hardcoded server-side so n8n can route MCP-sourced trials separately from website form trials.

## Ops hub: agents that operate your own JobRunr cluster (private beta)

The same server is also the hub of the hosted JobRunr MCP for JobRunr OSS users. A developer signs up at
`/signup`, gets two tokens and their agent can then inspect and operate their own cluster. Plan of record:
`Strategy/plans/jobrunr-mcp-ops-server-plan.md` in the Second Brain repo.

```
 Claude Code / Cursor ──POST /mcp + Bearer jra_…──►  this server (hub)  ◄──long-poll /connector/poll──  JobRunr dashboard
                                                     tools, triage,          + POST /connector/results      (customer JVM)
                                                     previews, accounts      ── outbound only ──            connector: scope check,
                                                                                                             redaction, local API call
```

* **One URL, two servers.** `/mcp` without an agent token is the docs server, unchanged. With
  `Authorization: Bearer jra_…`, `AgentAuthWebFilter` routes the request to a second MCP server (`OpsMcpServerConfig`)
  that serves the docs tools plus the cluster tools in `OpsTools`.
* **The hub does the thinking, the connector relays.** Every tool turns into dashboard REST calls (`/api/jobs`,
  `/api/servers`, …). Aggregation such as `get_cluster_overview`, `triage_failed_jobs` and the two-step
  `requeue_jobs` preview runs here. The connector in JobRunr core (`org.jobrunr.dashboard.mcp.McpHubConnector`,
  branch `feat/mcp-hub-connector` of jobrunr/jobrunr) only long-polls, calls its own dashboard on 127.0.0.1 and posts
  the answer back.
* **Safety lives at the edge.** The connector only reaches `/api/*`, refuses mutating routes unless its token starts
  with `jrc_operate_`, and replaces job parameter values with `<redacted>` before anything leaves the JVM. A
  compromised hub cannot do more than the token in the customer's config allows.
* **Free tier: one cluster per account at a time.** A second cluster connecting while the linked one answers gets a
  409 and a `second_cluster_attempt` lead event. A new cluster whose predecessor is gone (fresh database) takes over.

### Cluster tools

| Tool | What it does |
|---|---|
| `get_cluster_overview` | Version, job counts per state, servers, recurring jobs, problems, hints |
| `list_jobs(state, offset, limit)` | Compact job summaries, failed jobs include their exception |
| `get_job(job_id)` | Full job with history and stack traces (long strings truncated) |
| `triage_failed_jobs(max_jobs)` | Groups failed jobs by signature and exception, with job ids per group |
| `list_recurring_jobs`, `list_servers`, `get_problems` | As named |
| `requeue_job`, `delete_job`, `trigger_recurring_job`, `dismiss_problem` | Operate tools, need an operate connector token |
| `requeue_jobs(job_ids, dry_run, preview_id)` | Bulk requeue, max 100: dry run returns a `preview_id`, execution needs it |
| `list_clusters`, `search_jobs` | Pro hooks: multi-cluster and job search answer with `proFeature: true` and record a lead event |

### Pages and endpoints

| Path | Purpose |
|---|---|
| `GET /signup` (also `/`) | Signup and sign-in page |
| `GET /verify#<secret>` | Sign-in link target; the secret stays in the fragment and is POSTed to `/api/verify` |
| `GET /account` | Tokens, setup snippets, connection status, cluster numbers, recent agent activity |
| `POST /api/signup`, `/api/signin`, `/api/verify`, `/api/signout`, `GET /api/account`, `POST /api/account/tokens` | JSON API behind the pages |
| `GET /connector/poll`, `POST /connector/results/{id}` | Connector endpoints, `Authorization: Bearer jrc_…` |

### What the hub stores

Accounts, SHA-256 hashes of tokens, sign-in links and sessions, one snapshot per account (versions, job counts,
servers), tool call rows (tool name, success, latency, user agent) and lead events. Never tool arguments, tool
results or job data. Ops tool calls do not go to the attribution DB `mcp_query_log`, so the docs usage numbers stay
clean.

### Try it locally

```bash
docker run -d --name mcp-mailpit -p 1025:1025 -p 8025:8025 axllent/mailpit      # catches the sign-in emails
PORT=18080 MCP_LOG_API_URL= TRIAL_WEBHOOK_URL=http://127.0.0.1:1/none \
HUB_PUBLIC_URL=http://localhost:18080 SPRING_MAIL_HOST=localhost SPRING_MAIL_PORT=1025 \
DOCS_URL=file://$PWD/data/docs.json DOCS_MANIFEST_URL=file://$PWD/data/manifest.json \
java -jar target/jobrunr-docs-mcp-0.2.0-SNAPSHOT.jar
```

Keep `MCP_LOG_API_URL` empty locally, otherwise local tool calls land in the production attribution DB. Sign up at
http://localhost:18080/signup, open the email at http://localhost:8025, create tokens, then start
`~/jobrunr-mcp-demo` with the connector token (see its README). Without SMTP settings the sign-in link is written to
the log instead.

### Deploying the ops hub

Not deployed yet. Before it goes to `mcp.jobrunr.io`:

1. **Run one machine.** Connector state is in memory: `fly scale count 1`.
2. **Persist the database.** `fly volumes create hub_data --region ams --size 1`, mount it at `/data`
   (`[mounts] source = "hub_data"`, `destination = "/data"`), `chown 1001:1001 /data` once (the image runs as uid 1001)
   and set `HUB_DB_URL=jdbc:h2:file:/data/hub/hub`. PostgreSQL works too, the schema is portable.
3. **Mail.** `SPRING_MAIL_HOST`, `SPRING_MAIL_PORT`, `SPRING_MAIL_USERNAME`, `SPRING_MAIL_PASSWORD`,
   `SPRING_MAIL_PROPERTIES_MAIL_SMTP_STARTTLS_ENABLE=true`, and `HUB_MAIL_FROM` on a domain with SPF and DKIM.
4. **Leads.** `HUB_LEADS_WEBHOOK_URL` pointing at an n8n workflow that upserts the HubSpot contact. Payload:
   `form` (`mcp-ops-verified`, `mcp-ops-first-connection`, `mcp-ops-pro-feature-attempt`,
   `mcp-ops-second-cluster-attempt`), `email`, `name`, `company`, `role`, `use_case`, `detail`, `source=mcp-ops`.
5. **Privacy page and DPA** before public signups (plan section 5).

## Use it

### Claude Code

```
claude mcp add --transport http jobrunr-docs https://mcp.jobrunr.io/mcp
```

Restart Claude Code, then ask about anything JobRunr-related. Run `/mcp` inside Claude Code to confirm the connection.

If you added this server before August 2026 you'll have it on the old SSE URL. That still works, but re-add it
to move to the current transport: `claude mcp remove jobrunr-docs` then the command above.

### Cursor

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "jobrunr-docs": {
      "url": "https://mcp.jobrunr.io/mcp"
    }
  }
}
```

Restart Cursor. Tools appear under Settings → MCP.

### VS Code / Windsurf / others

Use the same URL — `https://mcp.jobrunr.io/mcp` — with whatever MCP config the client expects. Most accept a one-line `url` entry.

### MCP Inspector (debugging)

```
npx @modelcontextprotocol/inspector --cli https://mcp.jobrunr.io/mcp --transport http --method tools/list
```

Or drop the `--cli …` flags for the browser UI: transport = **Streamable HTTP**, URL = **`https://mcp.jobrunr.io/mcp`**, click Connect.

### Raw curl

```bash
curl https://mcp.jobrunr.io/actuator/health
# {"status":"UP",...}

# Full JSON-RPC handshake — this is exactly what a client's first request looks like:
curl -X POST https://mcp.jobrunr.io/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2025-11-25' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},
        "clientInfo":{"name":"curl","version":"1.0"}}}'
```

The response carries an `Mcp-Session-Id` header; send it back on every subsequent POST (`tools/list`,
`tools/call`, …). Note the `Accept` header must list both types — the spec requires it and the server
returns `400` without it.

## Local development

```
mvn spring-boot:run
```

Defaults to polling `https://www.jobrunr.io/mcp/docs.json` (once the Hugo workflow lands), falling back to the bootstrap URL configured in `application.yml`. Override either via env:

```
DOCS_URL=file:///absolute/path/to/docs.json \
DOCS_MANIFEST_URL=file:///absolute/path/to/manifest.json \
mvn spring-boot:run
```

`file://` URLs are supported for quick local iteration without an HTTP server.

Run tests:

```
mvn test
```

Tests use the bundled `src/test/resources/sample-docs.json` so they don't depend on the network. A deterministic stub embedding model is used to keep the suite fast and offline.

## Architecture

```
   Hugo build (JobRunr website repo)
   ──► public/mcp/docs.json + manifest.json
       │
       ▼
   GitHub Pages: https://www.jobrunr.io/mcp/...
       │
       ▼   (15-min poll + HMAC-signed /admin/reindex webhook)
   Spring Boot app (this repo)
       │
       ├── LuceneIndex          BM25, in-memory RAMDirectory
       ├── VectorIndex          Spring AI TransformersEmbeddingModel (ONNX MiniLM)
       ├── HybridSearch         Reciprocal Rank Fusion (k=60)
       └── DocsTools            @Tool methods → MCP tools/call
            │
            ▼
   Streamable HTTP /mcp   ──►  Claude Code / Cursor / ...
   (+ deprecated /sse)
```

The server **never generates answers** — it surfaces relevant pages and lets the client LLM synthesize. Stateless apart from the in-memory indexes.

## Operations

| Endpoint | Purpose |
|---|---|
| `POST /mcp` | **MCP Streamable HTTP endpoint** — every JSON-RPC message. What clients should use. |
| `GET /mcp` | Standalone SSE stream for server-initiated messages (same session) |
| `GET /sse` | _Deprecated_ HTTP+SSE entry, kept for clients installed before Streamable HTTP |
| `POST /mcp/message?sessionId=…` | _Deprecated_ JSON-RPC messages for the above (advertised over SSE) |
| `GET /actuator/health` | Liveness + readiness |
| `POST /admin/reindex` | Force reload of `docs.json` (HMAC headers required) |

Both transports are served by the same application on the same port and expose the same tools — the legacy
one runs as a second in-process MCP server (`LegacySseTransportConfig`) sharing the primary server's tool
specifications and capabilities. Idle streams are kept alive by protocol-level `ping` messages every 25 s.

`/admin/reindex` requires two headers:

```
X-Reindex-Timestamp: <unix seconds>
X-Reindex-Signature: <hex hmac-sha256 of the timestamp, keyed with MCP_REINDEX_SECRET>
```

The JobRunr website's GitHub Actions deploy step builds these automatically (see `.github/workflows/hugo.yml` in that repo).

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `DOCS_URL` | website `docs.json` | Where to fetch the catalog (HTTP or `file://`) |
| `DOCS_MANIFEST_URL` | website `manifest.json` | Where to fetch the manifest (used to detect changes) |
| `DOCS_POLL_INTERVAL` | `PT15M` | ISO-8601 duration for the scheduled poll |
| `MCP_REINDEX_SECRET` | _(empty)_ | HMAC secret for `/admin/reindex`. If empty, the endpoint returns 503. |
| `TRIAL_WEBHOOK_URL` | n8n webhook | Where `request_jobrunr_pro_trial` POSTs trial submissions. |
| `TRIAL_TIMEOUT` | `PT10S` | Webhook call timeout. |
| `MCP_LEGACY_SSE_ENABLED` | `true` | Serve the deprecated `/sse` transport alongside `/mcp`. Set `false` to retire it. |
| `RATELIMIT_ENABLED` | `false` | Per-IP rate limiting. See the caveat below before enabling. |
| `HUB_PUBLIC_URL` | `https://mcp.jobrunr.io` | Base url in sign-in emails and setup snippets |
| `HUB_DB_URL` | `jdbc:h2:file:./data/hub/hub` | Hub database (H2 file or PostgreSQL) |
| `HUB_MAIL_FROM` | `JobRunr MCP <mcp@jobrunr.io>` | Sender of sign-in emails |
| `SPRING_MAIL_HOST` (+ `_PORT`, `_USERNAME`, `_PASSWORD`) | _(unset)_ | SMTP for sign-in emails; unset logs the link instead |
| `HUB_LEADS_WEBHOOK_URL` | _(empty)_ | n8n webhook for signups, first connections and Pro-feature attempts |
| `HUB_POLL_HOLD` | `PT25S` | How long a connector long-poll is held open |
| `HUB_REQUEST_TIMEOUT` | `PT20S` | How long an agent waits for the connected instance |

> **Rate limiting caveat.** Under Streamable HTTP every tool call is its own POST, whereas the old SSE
> transport counted one long-lived GET per session. The existing 10 requests/minute default is therefore far
> more aggressive than it used to be — retune `RATELIMIT_RPM` before turning `RATELIMIT_ENABLED` on.

## Things still to do before this is "production"

These are deliberate shortcuts taken to get the server live for testing. Track these before opening it up.

1. ~~**Flip `DOCS_URL` back to the canonical website path.**~~ Done — the server loads from
   `https://www.jobrunr.io/mcp/docs.json`. The stale `data/docs.json` and `data/manifest.json` copies are still in
   this repo though; they're only used as local dev fixtures now and can be deleted.
2. **Wire up `MCP_REINDEX_SECRET`.** Generate a long random value and set it on both sides:
   - `flyctl secrets set MCP_REINDEX_SECRET=<value> -a jobrunr-docs-mcp`
   - GitHub repo `jobrunr/jobrunr.io` (or wherever the website lives) → Settings → Secrets → `MCP_REINDEX_SECRET = <same value>`
   - The Hugo deploy workflow will then push fresh docs to the server immediately on each website deploy, instead of
     waiting up to 15 minutes for the scheduled poll.
3. ~~**DNS for `mcp.jobrunr.io`.**~~ Done — that hostname serves the app and is the canonical URL everywhere in
   this README. Note `jobrunr-docs-mcp.fly.dev` no longer resolves at all, so any install instructions still
   pointing there (jobrunr.io website, older blog posts) are broken and need updating.
4. **GitHub Actions auto-deploy.** Still outstanding — the `deploy` job has failed on every push since May with
   `no access token available`, so deploys are manual. Add a `FLY_API_TOKEN` repo secret (generate at
   fly.io/user/personal_access_tokens) and `.github/workflows/deploy.yml` will test, deploy and smoke-test
   each push to `main`.
5. **Shrink the Docker image and drop VM size.** Current image is ~240 MB because Spring AI's `TransformersEmbeddingModel`
   pulls in DJL + a 150 MB libtorch native lib at first run, which is why the VM is 2 GB instead of the planned 512 MB.
   Options:
   - Pre-download the ONNX model + tokenizer into the image at build time (in the `Dockerfile`) so cold starts don't
     fetch them. Saves 30-60 s of startup. Still uses libtorch.
   - Switch to a direct ONNX-Runtime-only embedding pipeline (skip DJL/PyTorch). Drops the image by ~150 MB and the VM
     could likely go back to 512 MB. Means writing a small `EmbeddingModel` implementation backed by
     `com.microsoft.onnxruntime`.
6. **Move the Fly app from `personal` org to a `jobrunr` org.** Currently sits in Nicholas's personal Fly org. Migrate
   when JobRunr's Fly billing is set up. `flyctl apps move jobrunr-docs-mcp --new-org jobrunr`.
7. **Lower the WebClient body buffer.** Bumped to 16 MB in `DocsLoader` because 256 KB default choked on the 330 KB
   `docs.json`. 1-2 MB would be plenty and bounds memory better. Cosmetic.
8. **Pre-build a `VectorIndex` snapshot.** Embedding 301 chunks at startup takes ~30 s. Persisting the vectors to disk
   (volume or baked into the image alongside the model) would make warm restarts near-instant. Only worth doing if cold
   starts become a UX problem.
9. **Telemetry.** No query logging today. Once usage is non-trivial, add a simple `OncePerRequestFilter` that emits
   `tool_name`, hashed IP, query string, top-result path → wherever metrics live (Cloudflare Analytics Engine, Loki,
   ClickHouse, ...). Useful for understanding what devs actually ask before deciding what to improve.
10. **Marketing surface (separate sprint).** Per the original plan: `/en/mcp/` install page on jobrunr.io with copy-paste
    configs for every IDE, sidebar CTAs on every documentation page, launch blog post. Deferred until the server has been
    validated in production for a couple of weeks.
11. **Rate-limit trial submissions if abuse appears.** Currently every `request_jobrunr_pro_trial` call hits n8n; dedupe
    lives on the n8n side. If a misbehaving agent loop spams submissions, add a small in-memory `email → lastSubmittedAt`
    map with a 1-hour cooldown in `TrialTools`, or move rate limiting to Cloudflare once `mcp.jobrunr.io` DNS is wired.

## Deploying to a different environment

If you want to run this somewhere other than the current Fly app:

1. Provision a host that can run a JVM container (Fly.io / Hetzner / DigitalOcean / Cloud Run / anywhere with Docker).
2. Build and push the image: `flyctl deploy` for Fly, or `docker build -t … . && docker push …` for anything else.
3. Set the env vars from the table above. At minimum: `DOCS_URL`, `DOCS_MANIFEST_URL`. Generate and set `MCP_REINDEX_SECRET`.
4. Make sure the host has enough RAM. Currently **2 GB** is the safe minimum because of the libtorch native lib (see TODO #5). 512 MB will OOM during startup; 1 GB is borderline.
5. Open ports 80/443 (or whatever your TLS proxy expects) and route to container port 8080.
6. Point a DNS name at it and add TLS.
7. Smoke test: `curl https://<host>/actuator/health` then a full MCP roundtrip via `mcp-inspector`.

The app is fully stateless and idempotent, so blue/green or rolling deploys work fine. Indexes rebuild from the polled
`docs.json` within a minute of startup.
