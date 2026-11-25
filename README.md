# jobrunr-docs-mcp

Hosted [Model Context Protocol](https://modelcontextprotocol.io) server that exposes [JobRunr](https://www.jobrunr.io) documentation to AI coding assistants (Claude Code, Cursor, VS Code, Windsurf, ChatGPT desktop, ...).

Add one URL to your IDE config and JobRunr docs are live in every conversation — your agent stops hallucinating APIs and starts citing real `jobrunr.io` pages.

**Live endpoint:** `https://mcp.jobrunr.io/mcp`

Built on Spring Boot 4.1 + Spring AI 2.0's MCP server starter + Apache Lucene (BM25) + a local ONNX MiniLM embedding model. Retrieval is hybrid: BM25 and semantic search combined via Reciprocal Rank Fusion.

### Transports

The server speaks **Streamable HTTP** at `/mcp`. Public documentation calls are stateless: they do not
issue or retain MCP session IDs, so anonymous initialization cannot fill a shared session table.
Requests with a valid agent token use a separate stateful operations server with a configurable
session cap and idle expiry. Both negotiate protocol versions through `2025-11-25`.

The retired HTTP+SSE endpoints (`/sse` and `/mcp/message`) are no longer served. Existing clients
configured with `/sse` must be pointed at `/mcp`.

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
 Claude Code / Cursor ──POST /mcp + Bearer jra_…──►  this server (hub)  ◄──long-poll /connector/poll──  jobrunr-mcp plugin
                                                     tools, triage,          + POST /connector/results      (customer JVM)
                                                     previews, accounts      ── outbound only ──            in-process dashboard API
```

* **One URL, two servers.** `/mcp` without an agent token is the stateless docs server. With
  `Authorization: Bearer jra_…`, `AgentAuthWebFilter` routes the request to a second MCP server (`OpsMcpServerConfig`)
  that serves the docs tools plus the cluster tools in `OpsTools`.
* **The hub does the thinking, the connector relays.** Every tool turns into an allowed dashboard API call
  (`/api/jobs`, `/api/servers`, …). Aggregation such as `get_cluster_overview`, `triage_failed_jobs` and
  two-step `requeue_jobs` previews runs here. The `org.jobrunr:jobrunr-mcp` plugin long-polls the hub,
  invokes JobRunr's API handler in-process, and posts the response. No dashboard web port is needed.
* **The connector enforces the route and token scope.** It accepts only routes used by the current MCP tools
  and rejects mutating requests when configured with a read-only connector token. The hub also checks the
  authenticated connector scope before routing mutations and binds results to the selected account and node.
  The OSS connector sends job parameter values as stored, including possible personal data. Pro redaction
  depends on a separate proprietary runtime and is not provided by this OSS connector.
* **Free tier: one cluster and two concurrently live nodes per account.** An extra cluster receives a 409
  while the linked cluster is live. Once the old cluster has disconnected, a new OSS cluster can claim the
  account's one slot. JobRunr Pro accounts can connect unlimited nodes and clusters; pass `cluster_id` from
  `list_clusters` to target an online cluster. The hub never falls back to another cluster for a selected ID.

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
| `list_clusters` | Lists this account's connected clusters. Pro accounts can pass a listed `cluster_id` to every cluster tool; OSS uses its single default cluster. |
| `search_jobs` | Pro feature hook; returns a `proFeature` hint. |

### Pages and endpoints

| Path | Purpose |
|---|---|
| `GET /signup` (also `/`) | Signup and sign-in page |
| `GET /verify#<secret>` | Sign-in link target; the secret stays in the fragment and is POSTed to `/api/verify` |
| `GET /account` | Tokens, setup snippets, connection status, cluster numbers, recent agent activity |
| `POST /api/signup`, `/api/signin`, `/api/verify`, `/api/signout`, `GET /api/account`, `POST /api/account/tokens` | JSON API behind the pages |
| `GET /connector/poll`, `POST /connector/results/{id}` | Connector endpoints, `Authorization: Bearer jrc_…` |

### What the hub stores

Accounts, SHA-256 hashes of tokens, sign-in links and sessions, one snapshot per connected cluster
(versions, job counts, servers), tool call rows (tool name, success, latency, user agent) and lead events.
It does not persist tool arguments, tool results or job data.

Ops tool calls and lead events also go to the attribution DB `mcp_query_log`, marked `source = "ops"` and
`source = "hub"` (tool `hub_<event>`) so the docs usage numbers stay separate. Each row names the account by email
domain, company and cluster id, never the full address. Ops rows carry the shape of the call, not its subject: only
`state`, `offset`, `limit`, `max_jobs`, `dry_run`, `problem_type` and the number of job ids, never job ids, recurring
job ids or search text. Results are reduced to numbers, flags and sizes (job counts, JobRunr version, `proFeature`);
no result body is sent.

### Try it locally

```bash
docker run -d --name mcp-mailpit -p 1025:1025 -p 8025:8025 axllent/mailpit      # catches the sign-in emails
PORT=18080 MCP_LOG_API_URL= TRIAL_WEBHOOK_URL= HUB_LEADS_WEBHOOK_URL= \
HUB_PUBLIC_URL=http://localhost:18080 SPRING_MAIL_HOST=localhost SPRING_MAIL_PORT=1025 \
DOCS_URL=file://$PWD/data/docs.json DOCS_MANIFEST_URL=file://$PWD/data/manifest.json \
java -jar target/jobrunr-docs-mcp-0.2.0-SNAPSHOT.jar
```

Keep `MCP_LOG_API_URL` empty locally, otherwise local tool calls land in the production attribution DB. Sign up at
http://localhost:18080/signup, open the email at http://localhost:8025, create tokens, then start
`~/jobrunr-mcp-demo` with the connector token (see its README). Account signup and sign-in
require SMTP; without it these endpoints return 503 and do not create a usable link.

If `POST /api/signin` returns 200 but no message arrives, the generic response may mean the
address does not match an existing account. Check that the persistent `/app/data` volume and
`HUB_DB_URL` still point to the database where the account was created. A 429 means the mail
cooldown is active; 502 or 503 indicates mail delivery or configuration trouble.

### Deploying the ops hub (Coolify)

The app is deployed by Coolify from `main` (build pack Dockerfile, port 8080, health check `/actuator/health`), the
same way as the finance demo and the audit app. A push to `main` deploys. Before signups can work:

1. **Mail.** Copy the SMTP variables from the finance demo (`finance.demo.jobrunr.io`) to this app in Coolify:
   `SPRING_MAIL_HOST`, `SPRING_MAIL_PORT`, `SPRING_MAIL_USERNAME`, `SPRING_MAIL_PASSWORD` and
   `SPRING_MAIL_PROPERTIES_MAIL_SMTP_STARTTLS_ENABLE=true`. The hub sends as `noreply@jobrunr.io`, like the tour
   (`HUB_MAIL_FROM` overrides it). Without these, `/api/signup` answers 503 before creating or sending a link.
2. **Persistent storage.** Add a volume mount at `/app/data` (Coolify: Storages, volume mount). The image creates
   that directory owned by the app user, so a new named volume takes over the ownership. Without it every deploy
   wipes accounts and tokens, and every customer's connector token stops working.
3. **One replica.** Connector state is in memory.
4. **Leads.** `HUB_LEADS_WEBHOOK_URL` for the n8n workflow that upserts the HubSpot contact. Payload: `form`
   (`mcp-ops-verified`, `mcp-ops-first-connection`, `mcp-ops-pro-feature-attempt`, `mcp-ops-second-cluster-attempt`),
   `email`, `name`, `company`, `role`, `use_case`, `detail`, `source=mcp-ops`.
5. **Optional receivers.** Set `TRIAL_WEBHOOK_URL` to enable Pro trial submissions. Set
   `MCP_LOG_API_URL` and `MCP_LOG_API_SECRET` together to enable attribution logging.
   Source defaults are empty. Rotate any receiver credential that was previously published
   in this repository if it is still accepted by its receiver.
6. **Privacy page and DPA** before public signups (plan section 5).

### Pro account provisioning

Pro entitlement is a server-managed flag, `hub_accounts.is_pro`, defaulting to `FALSE`.
There is no public API to change it. After confirming the customer's Pro entitlement, an
operator can call `AccountStore.setProEnabled(accountId, true)` from a trusted server-side
provisioning workflow or update the account row directly:

```sql
UPDATE hub_accounts SET is_pro = TRUE WHERE id = '<account UUID>';
```

To downgrade, set `is_pro = FALSE`. Existing MCP sessions read the current flag on every
tool call. The original default cluster remains selected; extra Pro clusters are rejected
on their next poll, and an OSS tool call never falls back to them. Pro's redaction option
requires the separate proprietary JobRunr runtime; this OSS connector sends parameters as stored.

`fly.toml` and `.github/workflows/deploy.yml` are left over from the Fly.io days and are not used.

## Use it

### Claude Code

```
claude mcp add --transport http jobrunr-docs https://mcp.jobrunr.io/mcp
```

Restart Claude Code, then ask about anything JobRunr-related. Run `/mcp` inside Claude Code to confirm the connection.

If an older installation still uses the retired `/sse` URL, re-add it with
`claude mcp remove jobrunr-docs` followed by the command above.

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

The public docs response has no `Mcp-Session-Id`; send later docs calls directly to the same URL.
Authenticated operations responses carry `Mcp-Session-Id`; send that header on later operations
calls. The `Accept` header must list both types or the server returns `400`.

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
```

The public docs server surfaces relevant pages and lets the client LLM synthesize answers.
Its MCP transport is stateless; the authenticated operations server keeps bounded sessions.

## Operations

| Endpoint | Purpose |
|---|---|
| `POST /mcp` | MCP Streamable HTTP endpoint: stateless public docs or stateful operations with a valid agent token. |
| `GET /mcp` | Public docs return 405; authenticated operations can open a session stream. |
| `GET /actuator/health` | Liveness + readiness |
| `POST /admin/reindex` | Force reload of `docs.json` (HMAC headers required) |

Authenticated operations streams are kept alive by protocol-level `ping` messages every 25 s.
Connector result bodies are read only after token authentication and are capped at 4 MiB. The hub
rejects malformed connector tokens without a database lookup and permits at most 128 concurrent
connector token lookups. Monitor repeated 401s: a valid-looking random token still requires one
bounded lookup. Source-IP blocking is deliberately avoided because a reverse proxy or NAT may
make unrelated customers share one socket address.

`/admin/reindex` requires two headers:

```
X-Reindex-Timestamp: <unix seconds>
X-Reindex-Signature: <hex hmac-sha256 of the timestamp, keyed with MCP_REINDEX_SECRET>
```

The JobRunr website's GitHub Actions deploy step builds these automatically (see `.github/workflows/hugo.yml` in that repo).
Each valid signature is accepted once inside the five-minute timestamp window; an exact replay
returns 409. Concurrent reindexes return 429.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `DOCS_URL` | website `docs.json` | Where to fetch the catalog (HTTP or `file://`) |
| `DOCS_MANIFEST_URL` | website `manifest.json` | Where to fetch the manifest (used to detect changes) |
| `DOCS_POLL_INTERVAL` | `PT15M` | ISO-8601 duration for the scheduled poll |
| `EMBEDDING_TOKENIZER_URI` | Spring AI tokenizer URL | Optional local `file:` URI for the tokenizer; useful for offline tests. |
| `EMBEDDING_MODEL_URI` | Spring AI ONNX model URL | Optional local `file:` URI for the model; useful for offline tests. |
| `MCP_REINDEX_SECRET` | _(empty)_ | HMAC secret for `/admin/reindex`. If empty, the endpoint returns 503. |
| `TRIAL_WEBHOOK_URL` | _(empty)_ | Required to submit trials; no receiver is embedded in source. |
| `TRIAL_TIMEOUT` | `PT10S` | Webhook call timeout. |
| `MCP_LOG_API_URL` | _(empty)_ | Optional attribution log receiver. Set with `MCP_LOG_API_SECRET` to enable logging. |
| `MCP_LOG_API_SECRET` | _(empty)_ | Secret for the attribution log receiver; never store a live value in source. |
| `MCP_MAX_SESSIONS` | `2000` | Maximum authenticated operations MCP sessions; public docs use no sessions. |
| `MCP_MAX_SESSIONS_PER_ACCOUNT` | `20` | Maximum concurrent operations MCP sessions for one account. |
| `MCP_SESSION_IDLE_TIMEOUT` | `PT30M` | Authenticated operations session idle expiry. |
| `RATELIMIT_ENABLED` | `false` | Per-IP rate limiting. See the caveat below before enabling. |
| `HUB_PUBLIC_URL` | `https://mcp.jobrunr.io` | Base url in sign-in emails and setup snippets |
| `HUB_DB_URL` | `jdbc:h2:file:./data/hub/hub` | Hub database (H2 file or PostgreSQL) |
| `HUB_MAIL_FROM` | `JobRunr <noreply@jobrunr.io>` | Sender of sign-in emails |
| `SPRING_MAIL_HOST` (+ `_PORT`, `_USERNAME`, `_PASSWORD`) | _(unset)_ | SMTP for sign-in emails; unset makes signup/sign-in return 503 |
| `HUB_LEADS_WEBHOOK_URL` | _(empty)_ | n8n webhook for signups, first connections and Pro-feature attempts |
| `HUB_POLL_HOLD` | `PT25S` | How long a connector long-poll is held open |
| `HUB_REQUEST_TIMEOUT` | `PT20S` | How long an agent waits for the connected instance |
| `HUB_MAX_NODES_GLOBAL` | `10000` | Service-wide connector safety ceiling; Pro has no per-account node limit. |

> **Rate limiting caveat.** Under Streamable HTTP every tool call is its own POST. The existing
> 10 requests/minute default can be too aggressive for incident work; retune `RATELIMIT_RPM`
> before turning `RATELIMIT_ENABLED` on.

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
11. ~~**Rate-limit trial submissions if abuse appears.**~~ Done — `TrialTools` now deduplicates
    submissions per email with bounded in-memory state.

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
