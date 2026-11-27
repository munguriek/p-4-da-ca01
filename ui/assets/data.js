// RouteWatch data page. Reads the read-only JSON API and draws the pipeline
// line, the three tabs and the message drawer. Everything the API returns came
// from the internet, so it only ever reaches the page as text (textContent),
// never as HTML.

const NUMBER = new Intl.NumberFormat("en-GB");
// Large tables are summarised from PostgreSQL's estimates: shown as, say, "≈12.3M".
const COMPACT = new Intl.NumberFormat("en-GB", { notation: "compact", maximumSignificantDigits: 3 });
const FOLLOW_MS = 5000;
const STALE_SECONDS = 300;
const SVG_NS = "http://www.w3.org/2000/svg";

const OUTCOMES = {
  cleaned: { label: "Cleaned", hint: "Turned into one row per prefix." },
  empty: { label: "Empty", hint: "An update that named no prefixes." },
  ignored: { label: "Ignored", hint: "A control message, not a route update." },
  rejected: { label: "Rejected", hint: "Could not be cleaned; the reason is kept." },
};
const WAITING = { label: "Waiting", hint: "The cleaner has not reached this message yet." };

const FLAGS = [
  ["prefix_was_normalized", "normalised", "The announced prefix had host bits set, so it is stored as its network address."],
  ["has_as_set", "AS set", "The path contains an AS_SET: a group of ASes merged by route aggregation."],
  ["has_private_asn", "private AS", "The path contains a private-use AS number."],
  ["has_nonconsecutive_repeat", "repeat", "An AS appears twice in the path, but not side by side."],
  ["has_blackhole_community", "blackhole", "Tagged with the BLACKHOLE community 65535:666, a request to drop the traffic.", true],
];

const state = {
  tab: "raw",
  follow: false,
  ticks: 0,
  summary: null,
  summaryError: null,
  summaryErrorCode: null,
  kafka: null,
  kafkaError: null,
  includeInternal: false,
  raw: { limit: 50, before: null, after: null, page: null, seq: 0 },
  clean: { limit: 50, before: null, after: null, page: null, seq: 0, filters: {} },
  peek: null,
  drawerTrigger: null,
};

// --- Small helpers -------------------------------------------------------------

function h(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "text") node.textContent = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? "" : String(value));
  }
  append(node, children);
  return node;
}

function svg(tag, attrs = {}, ...children) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
  append(node, children);
  return node;
}

function append(node, children) {
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
}

const $ = (id) => document.getElementById(id);

const fmt = {
  int(value) {
    return value === null || value === undefined ? "–" : NUMBER.format(value);
  },
  count(value, approximate) {
    if (value === null || value === undefined) return "–";
    return approximate ? `≈${COMPACT.format(value)}` : NUMBER.format(value);
  },
  asn(value) {
    return value === null || value === undefined ? "–" : String(value);
  },
  time(iso, withMillis = false) {
    if (!iso) return "–";
    const text = new Date(iso).toISOString().replace("T", " ");
    return withMillis ? text.slice(0, 23) : text.slice(0, 19);
  },
  ago(iso) {
    const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
    if (seconds < 60) return `${seconds} s ago`;
    const minutes = Math.round(seconds / 60);
    if (minutes < 60) return `${minutes} min ago`;
    const hours = Math.round(minutes / 60);
    if (hours < 48) return `${hours} h ago`;
    return `${Math.round(hours / 24)} days ago`;
  },
  bytes(value) {
    if (value === null || value === undefined) return "–";
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let size = Number(value);
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
      size /= 1024;
      unit += 1;
    }
    const digits = unit === 0 || size >= 10 || Number.isInteger(size) ? 0 : 1;
    return `${size.toFixed(digits)} ${units[unit]}`;
  },
  duration(ms) {
    const value = Number(ms);
    if (!Number.isFinite(value) || value < 0) return "no time limit";
    const hours = value / 3_600_000;
    if (hours > 48 && Number.isInteger(hours / 24)) return `${hours / 24} days`;
    if (hours >= 1) return `${NUMBER.format(hours)} hour${hours === 1 ? "" : "s"}`;
    const minutes = value / 60_000;
    if (minutes >= 1) return `${NUMBER.format(minutes)} minute${minutes === 1 ? "" : "s"}`;
    return `${NUMBER.format(value / 1000)} seconds`;
  },
  pathText(path) {
    if (!Array.isArray(path)) return "";
    return path.map((hop) => (Array.isArray(hop) ? `{${hop.join(", ")}}` : String(hop))).join(" ");
  },
};

class ApiError extends Error {
  constructor(message, code = "error") {
    super(message);
    this.code = code;
  }
}

async function api(path, params = {}) {
  const url = new URL(path, window.location.origin);
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined && value !== "") url.searchParams.set(key, value);
  }
  let response;
  try {
    response = await fetch(url, { headers: { Accept: "application/json" } });
  } catch {
    throw new ApiError("The UI server is not answering. Is scripts/serve_routewatch_ui.py still running?", "ui_down");
  }
  let body = null;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  if (!response.ok) {
    throw new ApiError(
      body && body.message ? body.message : `The server answered ${response.status}.`,
      body && body.error ? body.error : "error",
    );
  }
  return body;
}

function notice(id, message, isError = true) {
  const box = $(id);
  box.textContent = message || "";
  box.classList.toggle("is-error", Boolean(isError));
  box.hidden = !message;
}

function outcomeBadge(status) {
  const outcome = OUTCOMES[status] || WAITING;
  const tone = OUTCOMES[status] ? status : "waiting";
  return h(
    "span",
    { class: `badge tone-${tone}`, title: outcome.hint },
    h("span", { class: "swatch", "aria-hidden": "true" }),
    outcome.label,
  );
}

// --- The AS path as a short route ---------------------------------------------

function pathStrip(path, originAsn) {
  if (!Array.isArray(path) || path.length === 0) {
    return h("span", { class: "muted" }, "–");
  }
  const stops = [];
  for (const hop of path) {
    const last = stops[stops.length - 1];
    if (!Array.isArray(hop) && last && last.asn === hop) last.count += 1;
    else stops.push(Array.isArray(hop) ? { set: hop } : { asn: hop, count: 1 });
  }
  const MAX = 9;
  const shown = stops.length > MAX ? [...stops.slice(0, MAX - 2), { gap: true }, stops[stops.length - 1]] : stops;
  const step = 15;
  const width = Math.max(12, (shown.length - 1) * step + 12);
  const label = `AS path ${fmt.pathText(path)}${originAsn ? `, origin AS ${originAsn}` : ""}`;
  const drawing = svg(
    "svg",
    { class: "path", width, height: 24, viewBox: `0 0 ${width} 24`, role: "img", "aria-label": label },
    svg("title", {}, label),
    svg("line", { class: "path-track", x1: 6, y1: 15, x2: width - 6, y2: 15 }),
  );
  shown.forEach((stop, index) => {
    const x = 6 + index * step;
    const isLast = index === shown.length - 1;
    if (stop.gap) {
      drawing.append(svg("rect", { x: x - 5, y: 11, width: 10, height: 8, fill: "#fff" }));
      drawing.append(svg("text", { class: "path-repeat", x, y: 18, "text-anchor": "middle" }, "…"));
    } else if (stop.set) {
      drawing.append(svg("rect", { class: "path-set", x: x - 5, y: 10, width: 10, height: 10, rx: 2 }));
    } else {
      drawing.append(svg("circle", { class: isLast ? "path-origin" : "path-stop", cx: x, cy: 15, r: 4 }));
      if (stop.count > 1) {
        drawing.append(svg("text", { class: "path-repeat", x, y: 7, "text-anchor": "middle" }, `×${stop.count}`));
      }
    }
  });
  return drawing;
}

function flagList(row) {
  const flags = [];
  for (const [field, label, hint, alert] of FLAGS) {
    if (row[field]) flags.push(h("span", { class: `flag${alert ? " is-alert" : ""}`, title: hint }, label));
  }
  if (row.duplicate_community_count > 0) {
    const n = row.duplicate_community_count;
    flags.push(
      h(
        "span",
        { class: "flag", title: `${n} repeated ${n === 1 ? "community was" : "communities were"} removed.` },
        n === 1 ? "duplicate" : `${n} duplicates`,
      ),
    );
  }
  return flags.length ? h("span", { class: "flags" }, flags) : h("span", { class: "muted" }, "–");
}

function actionTag(type) {
  const announce = type === "ANNOUNCE";
  return h("span", { class: `action ${announce ? "action-announce" : "action-withdraw"}` }, announce ? "Announce" : "Withdraw");
}

// --- The pipeline line ---------------------------------------------------------

function pipelineTopic() {
  if (!state.kafka) return null;
  return state.kafka.topics.find((topic) => topic.name === state.kafka.pipeline_topic) || null;
}

function station({ name, count, quiet = false, detail, detailTone, backlog, down = false }) {
  const classes = ["station"];
  if (backlog) classes.push("has-backlog");
  if (down) classes.push("is-down");
  return h(
    "li",
    { class: classes.join(" ") },
    h("span", { class: "stop", "aria-hidden": "true" }),
    backlog ? h("span", { class: "backlog" }, backlog) : null,
    h("div", { class: "station-name" }, name),
    h("div", { class: `station-count${quiet ? " is-quiet" : ""}` }, count),
    detail ? h("div", { class: `station-detail${detailTone ? ` is-${detailTone}` : ""}` }, detail) : null,
  );
}

function renderLine() {
  const summary = state.summary;
  const topic = pipelineTopic();
  const collector = summary && summary.clean.latest ? summary.clean.latest.collector_host.split(".")[0] : null;

  const stations = [
    station({
      name: "RIS Live",
      count: "Live stream",
      quiet: true,
      detail: collector ? `collector ${collector}` : "collector not seen yet",
    }),
    state.kafkaError
      ? station({ name: "Kafka topic", count: "–", detail: "Kafka is not answering", detailTone: "bad", down: true })
      : station({
          name: "Kafka topic",
          count: topic ? fmt.int(topic.kept) : "…",
          detail: topic ? `messages kept in ${topic.name}` : "reading…",
        }),
  ];

  if (state.summaryError) {
    stations.push(
      station({ name: "Raw table", count: "–", detail: databaseProblem().short, detailTone: "bad", down: true }),
      station({ name: "Cleaner", count: "–", down: true }),
      station({ name: "Clean table", count: "–", down: true }),
    );
  } else {
    const lag = topic && topic.group_lag ? topic.group_lag : 0;
    const pending = summary ? summary.processing.pending : 0;
    const approximate = Boolean(summary && summary.approximate);
    let cleanDetail = "prefix rows";
    if (summary && approximate) {
      cleanDetail = "prefix rows; counts are PostgreSQL's estimates";
    } else if (summary) {
      cleanDetail = summary.receipts_match_rows
        ? "prefix rows; receipts and rows agree"
        : `prefix rows; receipts promise ${fmt.int(summary.processing.promised_events)}`;
    }
    stations.push(
      station({
        name: "Raw table",
        count: summary ? fmt.count(summary.raw.messages, approximate) : "…",
        detail: "messages stored as received",
        backlog: lag ? `${fmt.int(lag)} waiting in Kafka` : null,
      }),
      station({
        name: "Cleaner",
        count: summary ? fmt.count(summary.processing.receipts, approximate) : "…",
        detail: "messages given an outcome",
        backlog: pending ? `${fmt.int(pending)} waiting to be cleaned` : null,
      }),
      station({
        name: "Clean table",
        count: summary ? fmt.count(summary.clean.events, approximate) : "…",
        detail: cleanDetail,
        detailTone: summary && !approximate && !summary.receipts_match_rows ? "bad" : null,
      }),
    );
  }
  $("stations").replaceChildren(...stations);
  renderOutcomes();
  renderFreshness();

  const approximate = Boolean(summary && summary.approximate);
  $("count-raw").textContent = summary ? fmt.count(summary.raw.messages, approximate) : "";
  $("count-clean").textContent = summary ? fmt.count(summary.clean.events, approximate) : "";
  $("count-kafka").textContent = state.kafka ? fmt.int(state.kafka.topics.length) : "";
}

function renderOutcomes() {
  const box = $("outcomes");
  const summary = state.summary;
  if (!summary || state.summaryError || summary.processing.receipts === 0) {
    box.hidden = true;
    return;
  }
  const total = summary.processing.receipts;
  const approximate = Boolean(summary.approximate);
  const parts = Object.keys(OUTCOMES).map((status) => ({ status, ...summary.processing.by_status[status] }));
  const bar = h("div", { class: "split", "aria-hidden": "true" });
  for (const part of parts) {
    if (!part.messages) continue;
    const piece = h("span", { class: `swatch tone-${part.status}` });
    piece.style.flexGrow = String(part.messages);
    piece.style.flexBasis = "0";
    bar.append(piece);
  }
  const legend = h(
    "ul",
    { class: "legend" },
    parts.map((part) =>
      h(
        "li",
        { class: `tone-${part.status}`, title: OUTCOMES[part.status].hint },
        h("span", { class: "swatch", "aria-hidden": "true" }),
        `${OUTCOMES[part.status].label} `,
        h("strong", {}, fmt.count(part.messages, approximate)),
        part.status === "cleaned" ? ` (${fmt.count(part.events, approximate)} rows)` : null,
      ),
    ),
  );
  box.replaceChildren(
    h(
      "p",
      { class: "outcomes-title" },
      `Outcomes of the ${fmt.count(total, approximate)} messages the cleaner has processed`,
    ),
    bar,
    legend,
  );
  box.hidden = false;
}

function databaseProblem() {
  switch (state.summaryErrorCode) {
    case "tables_missing":
      return { short: "tables not created yet", long: "The RouteWatch tables are not set up yet." };
    case "not_allowed":
      return { short: "UI role cannot read it", long: "The UI's database role cannot read the tables." };
    case "ui_down":
      return { short: "UI server not answering", long: "The UI server is not answering." };
    case "postgres_unavailable":
      return { short: "database not answering", long: "The database is not answering." };
    default:
      return { short: "query failed", long: "The database query failed. See the message below." };
  }
}

function renderFreshness() {
  const line = $("freshness");
  line.classList.remove("is-stale", "is-down");
  if (state.summaryError) {
    line.textContent = databaseProblem().long;
    line.classList.add("is-down");
    return;
  }
  const latest = state.summary && state.summary.raw.latest;
  if (!latest) {
    line.textContent = state.summary ? "No messages stored yet." : "Checking the pipeline…";
    return;
  }
  const age = (Date.now() - new Date(latest.stored_at).getTime()) / 1000;
  if (age > STALE_SECONDS) {
    line.textContent = `No new messages for ${fmt.ago(latest.stored_at).replace(" ago", "")}. Is the raw consumer running?`;
    line.classList.add("is-stale");
  } else {
    line.textContent = `Last message stored ${fmt.ago(latest.stored_at)}`;
  }
}

async function loadSummary() {
  try {
    state.summary = await api("/api/summary");
    state.summaryError = null;
    state.summaryErrorCode = null;
  } catch (error) {
    state.summaryError = error.message;
    state.summaryErrorCode = error.code;
  }
  renderLine();
}

async function loadKafkaOverview() {
  try {
    state.kafka = await api("/api/kafka", { internal: state.includeInternal ? "1" : "" });
    state.kafkaError = null;
  } catch (error) {
    state.kafkaError = error.message;
  }
  renderLine();
  if (state.tab === "kafka") renderKafka();
}

// --- Raw tab ---------------------------------------------------------------------

async function loadRaw() {
  const view = state.raw;
  const seq = ++view.seq;
  const table = $("raw-table");
  table.setAttribute("aria-busy", "true");
  try {
    const page = await api("/api/raw", { limit: view.limit, before: view.before, after: view.after });
    if (seq !== view.seq) return;
    if (page.rows.length === 0 && (view.before || view.after)) {
      view.before = view.after = null;
      return loadRaw();
    }
    view.page = page;
    notice("raw-notice", null);
    renderRaw(page);
  } catch (error) {
    if (seq === view.seq) notice("raw-notice", error.message);
  } finally {
    if (seq === view.seq) table.setAttribute("aria-busy", "false");
  }
}

function renderRaw(page) {
  const body = $("raw-table").tBodies[0];
  if (page.rows.length === 0) {
    body.replaceChildren(
      h("tr", { class: "empty-row" }, h("td", { colspan: 6 }, "No raw messages yet. Start the collector and the raw consumer, then refresh.")),
    );
  } else {
    body.replaceChildren(
      ...page.rows.map((row) =>
        h(
          "tr",
          {},
          h(
            "td",
            { class: "num" },
            h(
              "button",
              {
                type: "button",
                class: "link-button",
                title: "Open this message, its outcome and the rows it made",
                onclick: (event) => openMessage({ key: row.key }, event.currentTarget),
              },
              fmt.int(row.offset),
            ),
          ),
          h("td", { class: "mono", title: row.received_at }, fmt.time(row.received_at)),
          h("td", { class: "num" }, fmt.bytes(row.size_bytes)),
          h("td", {}, outcomeBadge(row.status)),
          h("td", { class: "num" }, row.event_count === null ? "–" : fmt.int(row.event_count)),
          h("td", { class: "preview", title: row.preview }, row.preview),
        ),
      ),
    );
  }
  $("raw-newer").disabled = !page.has_newer;
  $("raw-older").disabled = !page.has_older;
  const first = page.rows[0];
  const last = page.rows[page.rows.length - 1];
  $("raw-range").textContent = first
    ? `Offsets ${fmt.int(first.offset)} to ${fmt.int(last.offset)}, newest first. Choose an offset to see the message and the rows it made.`
    : "Each Kafka message as it was received, newest first.";
}

// --- Clean tab -------------------------------------------------------------------

async function loadClean() {
  const view = state.clean;
  const seq = ++view.seq;
  const table = $("clean-table");
  table.setAttribute("aria-busy", "true");
  try {
    const page = await api("/api/clean", {
      limit: view.limit,
      before: view.before,
      after: view.after,
      ...view.filters,
    });
    if (seq !== view.seq) return;
    if (page.rows.length === 0 && (view.before || view.after)) {
      view.before = view.after = null;
      return loadClean();
    }
    view.page = page;
    notice("clean-notice", null);
    renderClean(page);
  } catch (error) {
    if (seq === view.seq) notice("clean-notice", error.message);
  } finally {
    if (seq === view.seq) table.setAttribute("aria-busy", "false");
  }
}

function renderClean(page) {
  const body = $("clean-table").tBodies[0];
  const filtered = Object.keys(state.clean.filters).length > 0;
  if (page.rows.length === 0) {
    body.replaceChildren(
      h(
        "tr",
        { class: "empty-row" },
        h(
          "td",
          { colspan: 11 },
          filtered
            ? "No rows match these filters. Clear a filter, or widen the prefix."
            : "No clean rows yet. Start scripts/clean_routewatch.py --follow, then refresh.",
        ),
      ),
    );
  } else {
    body.replaceChildren(...page.rows.map(cleanRow));
  }
  $("clean-newer").disabled = !page.has_newer;
  $("clean-older").disabled = !page.has_older;
  const count = page.rows.length;
  $("clean-range").textContent = count
    ? `${fmt.int(count)} rows, newest first${filtered ? ", filtered" : ""}. Choose an offset to see the raw message a row came from.`
    : "One row for each prefix a message announced or withdrew, newest first.";
}

function cleanRow(row) {
  const announce = row.action_type === "ANNOUNCE";
  return h(
    "tr",
    {},
    h("td", { class: "mono", title: row.event_ts }, fmt.time(row.event_ts)),
    h("td", {}, actionTag(row.action_type)),
    h("td", { class: "mono" }, row.prefix),
    h("td", { class: "num" }, fmt.asn(row.peer_asn)),
    h("td", {}, announce ? pathStrip(row.as_path, row.origin_asn) : h("span", { class: "muted" }, "withdrawn")),
    h("td", { class: "num" }, fmt.asn(row.origin_asn)),
    h("td", { class: "num" }, row.as_path_length === null ? "–" : fmt.int(row.as_path_length)),
    h("td", { class: "num" }, row.prepending_count === null ? "–" : fmt.int(row.prepending_count)),
    h("td", { class: "num" }, row.community_count === null ? "–" : fmt.int(row.community_count)),
    h("td", { class: "notes" }, flagList(row)),
    h(
      "td",
      { class: "num" },
      h(
        "button",
        {
          type: "button",
          class: "link-button",
          title: "Open the raw message this row came from",
          onclick: (event) => openMessage({ key: row.message_key }, event.currentTarget),
        },
        fmt.int(row.offset),
      ),
    ),
  );
}

function readFilters(form) {
  const data = new FormData(form);
  const filters = {};
  for (const [key, value] of data.entries()) {
    const text = String(value).trim();
    if (text) filters[key] = text;
  }
  return filters;
}

// --- Kafka tab -------------------------------------------------------------------

function configText(config, key) {
  const entry = config[key];
  return entry ? entry.value : null;
}

function retentionText(config) {
  const ms = configText(config, "retention.ms");
  const bytes = configText(config, "retention.bytes");
  if (ms === null && bytes === null) return "–";
  const time = ms === null ? null : Number(ms) < 0 ? "no time limit" : fmt.duration(ms);
  const size = bytes === null ? null : Number(bytes) < 0 ? "no size limit" : fmt.bytes(bytes);
  if (time && size) return `${time} or ${size}, whichever comes first`;
  return time || size;
}

function segmentText(config) {
  const bytes = configText(config, "segment.bytes");
  const ms = configText(config, "segment.ms");
  const parts = [bytes === null ? null : fmt.bytes(bytes), ms === null ? null : fmt.duration(ms)].filter(Boolean);
  return parts.length ? `a new file every ${parts.join(" or ")}` : "–";
}

function groupText(group) {
  if (group.error) return `Kafka could not describe this group: ${group.error}`;
  const members = group.members || 0;
  switch (group.state) {
    case "stable":
      return `Stable: ${members} consumer${members === 1 ? "" : "s"} connected and reading.`;
    case "empty":
      return "Empty: no consumer is connected right now. Kafka keeps the committed offsets, so the raw consumer carries on from where it stopped.";
    case "preparing_rebalance":
    case "completing_rebalance":
      return "Rebalancing: consumers are joining or leaving the group.";
    case "dead":
      return "No consumer has committed offsets under this name yet.";
    default:
      return `State: ${group.state || "unknown"}.`;
  }
}

function meter(partition) {
  const low = partition.earliest_offset;
  const high = partition.next_offset;
  const done = partition.committed_offset;
  if (low === null || high === null || done === null || high <= low) return h("span", { class: "muted" }, "–");
  const span = high - low;
  const donePart = Math.min(Math.max((done - low) / span, 0), 1);
  const bar = h("div", {
    class: "meter",
    role: "img",
    "aria-label": `${fmt.int(done - low)} of ${fmt.int(span)} kept messages stored`,
  });
  const doneBar = h("span", { class: "meter-done" });
  doneBar.style.width = `${(donePart * 100).toFixed(2)}%`;
  const waitBar = h("span", { class: "meter-wait" });
  waitBar.style.left = `${(donePart * 100).toFixed(2)}%`;
  waitBar.style.width = `${((1 - donePart) * 100).toFixed(2)}%`;
  bar.append(doneBar);
  if (high > done) bar.append(waitBar);
  return bar;
}

function renderKafka() {
  const body = $("kafka-body");
  if (state.kafkaError) {
    notice("kafka-notice", state.kafkaError);
    body.replaceChildren();
    $("peek-read").disabled = true;
    return;
  }
  notice("kafka-notice", null);
  const data = state.kafka;
  if (!data) {
    body.replaceChildren(h("p", { class: "muted" }, "Reading Kafka…"));
    return;
  }
  $("peek-read").disabled = false;
  const brokers = data.brokers.map((broker) => `${broker.id} at ${broker.host}:${broker.port}`).join(", ");
  const internal = h("input", {
    type: "checkbox",
    id: "kafka-internal",
    checked: state.includeInternal,
    onchange: (event) => {
      state.includeInternal = event.currentTarget.checked;
      loadKafkaOverview();
    },
  });

  const topics = data.topics.map((topic) => {
    const isPipeline = topic.name === data.pipeline_topic;
    return h(
      "article",
      { class: "topic" },
      h(
        "h3",
        {},
        topic.name,
        isPipeline ? h("span", { class: "tag" }, "Pipeline topic") : null,
        topic.internal ? h("span", { class: "tag" }, "Kafka internal") : null,
      ),
      topic.error ? h("p", { class: "station-detail is-bad" }, topic.error) : null,
      h(
        "dl",
        { class: "facts" },
        fact("Partitions", fmt.int(topic.partitions.length)),
        fact("Messages kept now", fmt.int(topic.kept)),
        fact("Kept for", retentionText(topic.config)),
        fact("Log files", segmentText(topic.config)),
        fact("Largest message", configText(topic.config, "max.message.bytes") ? fmt.bytes(configText(topic.config, "max.message.bytes")) : "–"),
        fact("Clean-up", configText(topic.config, "cleanup.policy") || "–"),
      ),
      h(
        "div",
        { class: "table-wrap" },
        h(
          "table",
          { class: "grid" },
          h("caption", { class: "visually-hidden" }, `Partitions of ${topic.name}`),
          h(
            "thead",
            {},
            h(
              "tr",
              {},
              h("th", { scope: "col", class: "num" }, "Partition"),
              h("th", { scope: "col", class: "num" }, "Leader"),
              h("th", { scope: "col" }, "In sync"),
              h("th", { scope: "col", class: "num" }, "First offset kept"),
              h("th", { scope: "col", class: "num" }, "Next offset"),
              h("th", { scope: "col", class: "num" }, `Committed by ${data.group.group_id}`),
              h("th", { scope: "col", class: "num" }, "Waiting"),
              h("th", { scope: "col" }, "Stored so far"),
            ),
          ),
          h(
            "tbody",
            {},
            topic.partitions.map((partition) =>
              h(
                "tr",
                {},
                h("td", { class: "num" }, fmt.int(partition.partition)),
                h("td", { class: "num" }, fmt.int(partition.leader)),
                h("td", {}, partition.in_sync.join(", ")),
                h("td", { class: "num" }, fmt.int(partition.earliest_offset)),
                h("td", { class: "num" }, fmt.int(partition.next_offset)),
                h("td", { class: "num" }, partition.committed_offset === null ? "none" : fmt.int(partition.committed_offset)),
                h("td", { class: "num" }, partition.lag === null ? "–" : fmt.int(partition.lag)),
                h("td", {}, meter(partition)),
              ),
            ),
          ),
        ),
      ),
    );
  });

  body.replaceChildren(
    h(
      "div",
      { class: "cluster" },
      h(
        "p",
        {},
        "Cluster ",
        h("code", {}, data.cluster_id || "unknown"),
        ` with ${data.brokers.length} broker${data.brokers.length === 1 ? "" : "s"} (${brokers}); controller ${data.controller_id}.`,
      ),
      h("label", { class: "follow" }, internal, " Show Kafka's internal topics"),
    ),
    ...(topics.length ? topics : [h("p", { class: "muted" }, "Kafka has no topics yet. Run scripts/create_routewatch_topics.sh.")]),
    h(
      "section",
      { class: "group", "aria-labelledby": "group-title" },
      h("h3", { id: "group-title" }, `Consumer group ${data.group.group_id}`),
      h("p", {}, groupText(data.group)),
    ),
  );
  fillPeekChoices();
}

function fact(label, value) {
  return h("div", {}, h("dt", {}, label), h("dd", {}, value));
}

function fillPeekChoices() {
  const topicSelect = $("peek-topic");
  const partitionSelect = $("peek-partition");
  const data = state.kafka;
  if (!data) return;
  const visible = data.topics.filter((topic) => !topic.internal);
  const chosen = topicSelect.value || data.pipeline_topic;
  topicSelect.replaceChildren(
    ...visible.map((topic) => h("option", { value: topic.name, selected: topic.name === chosen }, topic.name)),
  );
  const topic = visible.find((item) => item.name === topicSelect.value);
  const chosenPartition = partitionSelect.value;
  partitionSelect.replaceChildren(
    ...(topic ? topic.partitions : []).map((partition) =>
      h(
        "option",
        { value: partition.partition, selected: String(partition.partition) === chosenPartition },
        String(partition.partition),
      ),
    ),
  );
}

async function readPeek() {
  const button = $("peek-read");
  button.disabled = true;
  try {
    const result = await api("/api/kafka/peek", {
      topic: $("peek-topic").value,
      partition: $("peek-partition").value,
      limit: $("peek-limit").value,
    });
    state.peek = result;
    notice("peek-notice", null);
    renderPeek(result);
  } catch (error) {
    notice("peek-notice", error.message);
  } finally {
    button.disabled = false;
  }
}

function renderPeek(result) {
  const wrap = $("peek-wrap");
  const body = $("peek-table").tBodies[0];
  const data = state.kafka;
  const isPipeline = data && result.topic === data.pipeline_topic;
  const topic = data ? data.topics.find((item) => item.name === result.topic) : null;
  const partition = topic ? topic.partitions.find((item) => item.partition === result.partition) : null;
  const committed = partition ? partition.committed_offset : null;

  if (result.records.length === 0) {
    body.replaceChildren(h("tr", { class: "empty-row" }, h("td", { colspan: 6 }, "This partition holds no records right now.")));
  } else {
    body.replaceChildren(
      ...result.records.map((record) => {
        const run = (record.headers.find(([name]) => name === "routewatch_run_id") || [])[1];
        let stored;
        if (!isPipeline) stored = h("span", { class: "muted" }, "not a pipeline topic");
        else if (committed !== null && record.offset < committed) {
          stored = h(
            "button",
            {
              type: "button",
              class: "link-button",
              onclick: (event) =>
                openMessage(
                  {
                    cluster_id: result.cluster_id,
                    topic: result.topic,
                    partition: result.partition,
                    offset: record.offset,
                  },
                  event.currentTarget,
                ),
            },
            "Open stored copy",
          );
        } else stored = h("span", { class: "badge tone-waiting" }, h("span", { class: "swatch", "aria-hidden": "true" }), "Not yet");
        return h(
          "tr",
          {},
          h("td", { class: "num" }, fmt.int(record.offset)),
          h("td", { class: "mono", title: record.timestamp.at || "" }, fmt.time(record.timestamp.at, true)),
          h("td", { class: "num" }, fmt.bytes(record.size_bytes)),
          h("td", { class: "mono", title: run || "" }, run ? run.slice(0, 8) : "–"),
          h("td", {}, stored),
          h(
            "td",
            { class: "wrap" },
            h(
              "details",
              { class: "value" },
              h("summary", {}, record.preview ? record.preview.slice(0, 60) : "(empty)"),
              h("pre", { class: "payload" }, record.value || ""),
            ),
          ),
        );
      }),
    );
  }
  wrap.hidden = false;
}

// --- The message drawer --------------------------------------------------------

async function openMessage(params, trigger) {
  const dialog = $("drawer");
  state.drawerTrigger = trigger || document.activeElement;
  $("drawer-title").textContent = "Raw message";
  $("drawer-body").replaceChildren(h("p", { class: "muted" }, "Loading the message…"));
  if (!dialog.open) dialog.showModal();
  try {
    const result = await api("/api/raw/message", params);
    renderDrawer(result);
  } catch (error) {
    $("drawer-body").replaceChildren(h("div", { class: "notice is-error" }, error.message));
  }
}

function renderDrawer(result) {
  const message = result.message;
  const receipt = result.receipt;
  $("drawer-title").textContent = `Raw message at offset ${fmt.int(message.offset)}`;

  const facts = h(
    "dl",
    { class: "facts" },
    fact("Topic", h("span", { class: "mono" }, message.topic)),
    fact("Partition", fmt.int(message.partition)),
    fact("Offset", fmt.int(message.offset)),
    fact("Kafka cluster", h("span", { class: "mono" }, message.cluster_id)),
    fact("Received (UTC)", fmt.time(message.received_at, true)),
    fact("Stored (UTC)", fmt.time(message.stored_at, true)),
    fact("Kafka time (UTC)", fmt.time(message.kafka_timestamp, true)),
    fact("Collector run", h("span", { class: "mono", title: message.run_id }, String(message.run_id).slice(0, 8))),
    fact("Size", fmt.bytes(message.size_bytes)),
  );

  let receiptBlock;
  if (!receipt) {
    receiptBlock = h("div", { class: "receipt" }, outcomeBadge(null), h("span", {}, WAITING.hint));
  } else {
    receiptBlock = h(
      "div",
      { class: "receipt" },
      outcomeBadge(receipt.status),
      h(
        "span",
        {},
        receipt.status === "cleaned"
          ? `Made ${fmt.int(receipt.event_count)} row${receipt.event_count === 1 ? "" : "s"}`
          : OUTCOMES[receipt.status].hint,
      ),
      h("span", { class: "muted" }, `transform version ${receipt.transform_version}, processed ${fmt.time(receipt.processed_at)} UTC`),
      receipt.error_message ? h("span", { class: "error" }, receipt.error_message) : null,
    );
  }

  const payload = message.payload;
  const pre = h("pre", { class: "payload", tabindex: "0" });
  const views = [];
  if (payload.pretty !== null) views.push(["Formatted", payload.pretty]);
  views.push([payload.encoding === "hex" ? "As stored (hex)" : "As stored", payload.text]);
  const switcher = h("div", { class: "view-switch", role: "group", "aria-label": "Payload view" });
  const show = (index) => {
    pre.textContent = views[index][1];
    [...switcher.children].forEach((button, i) => button.setAttribute("aria-pressed", String(i === index)));
  };
  views.forEach(([label], index) => {
    switcher.append(h("button", { type: "button", onclick: () => show(index) }, label));
  });
  show(0);
  const notes = [];
  if (payload.truncated) notes.push(h("p", { class: "muted" }, "The payload is long, so only its first part is shown."));
  if (payload.encoding === "hex") notes.push(h("p", { class: "muted" }, "These bytes are not valid UTF-8, so they are shown as hex."));

  const events = result.events;
  const rows = events.length
    ? h(
        "div",
        { class: "table-wrap" },
        h(
          "table",
          { class: "grid" },
          h("caption", { class: "visually-hidden" }, "Rows this message made"),
          h(
            "thead",
            {},
            h(
              "tr",
              {},
              h("th", { scope: "col", class: "num" }, "Index"),
              h("th", { scope: "col" }, "Action"),
              h("th", { scope: "col" }, "Prefix"),
              h("th", { scope: "col" }, "AS path"),
              h("th", { scope: "col", class: "num" }, "Origin AS"),
              h("th", { scope: "col", class: "num" }, "Hops"),
              h("th", { scope: "col", class: "num" }, "Prepends"),
              h("th", { scope: "col" }, "Notes"),
            ),
          ),
          h(
            "tbody",
            {},
            events.map((row) =>
              h(
                "tr",
                {},
                h("td", { class: "num" }, fmt.int(row.action_index)),
                h("td", {}, actionTag(row.action_type)),
                h("td", { class: "mono" }, row.prefix),
                h("td", {}, row.action_type === "ANNOUNCE" ? pathStrip(row.as_path, row.origin_asn) : h("span", { class: "muted" }, "withdrawn")),
                h("td", { class: "num" }, fmt.asn(row.origin_asn)),
                h("td", { class: "num" }, row.as_path_length === null ? "–" : fmt.int(row.as_path_length)),
                h("td", { class: "num" }, row.prepending_count === null ? "–" : fmt.int(row.prepending_count)),
                h("td", {}, flagList(row)),
              ),
            ),
          ),
        ),
      )
    : h("p", { class: "muted" }, receipt ? "This message made no rows." : "No rows yet.");

  const firstAnnounce = events.find((row) => row.action_type === "ANNOUNCE" && row.as_path);
  $("drawer-body").replaceChildren(
    h("section", {}, h("h3", {}, "Where it sits"), facts),
    h("section", {}, h("h3", {}, "What the cleaner decided"), receiptBlock),
    h("section", {}, h("h3", {}, "Payload"), views.length > 1 ? switcher : null, pre, notes),
    h(
      "section",
      {},
      h("h3", {}, `Rows it made in the clean table (${fmt.int(events.length)})`),
      firstAnnounce ? h("p", { class: "muted" }, "Path read left to right: ", h("span", { class: "mono" }, fmt.pathText(firstAnnounce.as_path))) : null,
      rows,
      result.events_truncated ? h("p", { class: "muted" }, "Only the first 1,000 rows are listed.") : null,
    ),
  );
}

function closeDrawer() {
  const dialog = $("drawer");
  if (dialog.open) dialog.close();
}

// --- Tabs, follow mode and start-up -----------------------------------------------

const TABS = ["raw", "clean", "kafka"];

function selectTab(name, { focus = false, load = true } = {}) {
  if (!TABS.includes(name)) name = "raw";
  state.tab = name;
  for (const tab of TABS) {
    const button = $(`tab-${tab}`);
    const selected = tab === name;
    button.setAttribute("aria-selected", String(selected));
    button.tabIndex = selected ? 0 : -1;
    $(`panel-${tab}`).hidden = !selected;
    if (selected && focus) button.focus();
  }
  if (window.location.hash !== `#${name}`) history.replaceState(null, "", `#${name}`);
  if (!load) return;
  if (name === "raw") loadRaw();
  if (name === "clean") loadClean();
  if (name === "kafka") renderKafka();
}

function wireTabs() {
  for (const tab of TABS) {
    $(`tab-${tab}`).addEventListener("click", () => selectTab(tab));
  }
  $("tab-raw").parentElement.addEventListener("keydown", (event) => {
    const index = TABS.indexOf(state.tab);
    let next = null;
    if (event.key === "ArrowRight") next = TABS[(index + 1) % TABS.length];
    if (event.key === "ArrowLeft") next = TABS[(index - 1 + TABS.length) % TABS.length];
    if (event.key === "Home") next = TABS[0];
    if (event.key === "End") next = TABS[TABS.length - 1];
    if (next) {
      event.preventDefault();
      selectTab(next, { focus: true });
    }
  });
}

function wirePaging(name, load) {
  const view = state[name];
  $(`${name}-newer`).addEventListener("click", () => {
    if (!view.page) return;
    view.after = view.page.newer_cursor;
    view.before = null;
    load();
  });
  $(`${name}-older`).addEventListener("click", () => {
    if (!view.page) return;
    view.before = view.page.older_cursor;
    view.after = null;
    load();
  });
  $(`${name}-limit`).addEventListener("change", (event) => {
    view.limit = Number(event.currentTarget.value);
    view.before = view.after = null;
    load();
  });
}

function onFirstPage(view) {
  return !view.before && !view.after;
}

async function refreshAll() {
  await Promise.all([loadSummary(), loadKafkaOverview()]);
  if (state.tab === "raw") loadRaw();
  if (state.tab === "clean") loadClean();
}

function followTick() {
  if (!state.follow || document.hidden) return;
  state.ticks += 1;
  loadSummary();
  if (state.ticks % 2 === 0) loadKafkaOverview();
  if (state.tab === "raw" && onFirstPage(state.raw)) loadRaw();
  if (state.tab === "clean" && onFirstPage(state.clean)) loadClean();
}

function start() {
  wireTabs();
  wirePaging("raw", loadRaw);
  wirePaging("clean", loadClean);

  $("refresh").addEventListener("click", refreshAll);
  $("follow").addEventListener("change", (event) => {
    state.follow = event.currentTarget.checked;
    if (state.follow) {
      state.raw.before = state.raw.after = null;
      state.clean.before = state.clean.after = null;
      followTick();
    }
  });
  window.setInterval(followTick, FOLLOW_MS);

  $("clean-filters").addEventListener("submit", (event) => {
    event.preventDefault();
    state.clean.filters = readFilters(event.currentTarget);
    state.clean.before = state.clean.after = null;
    loadClean();
  });
  $("clean-clear").addEventListener("click", () => {
    $("clean-filters").reset();
    state.clean.filters = {};
    state.clean.before = state.clean.after = null;
    loadClean();
  });

  $("peek-topic").addEventListener("change", () => {
    $("peek-partition").value = "";
    fillPeekChoices();
  });
  $("peek-read").addEventListener("click", readPeek);

  $("drawer-close").addEventListener("click", closeDrawer);
  $("drawer").addEventListener("click", (event) => {
    if (event.target === event.currentTarget) closeDrawer();
  });
  $("drawer").addEventListener("close", () => {
    const trigger = state.drawerTrigger;
    state.drawerTrigger = null;
    if (trigger && document.contains(trigger)) trigger.focus();
  });

  window.addEventListener("hashchange", () => selectTab(window.location.hash.slice(1)));
  selectTab(window.location.hash.slice(1) || "raw");
  loadSummary();
  loadKafkaOverview();
}

start();
