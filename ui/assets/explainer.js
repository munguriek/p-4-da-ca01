// RouteWatch explainer. Each step is a short animation drawn as a pure
// function of time, so a single clock gives play, pause, rewind, scrubbing
// and speed. The worked examples use addresses (RFC 5737) and AS numbers
// (RFC 5398) reserved for documentation, so no real data appears here.

const SVG_NS = "http://www.w3.org/2000/svg";
const REDUCED = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

// --- Drawing helpers -------------------------------------------------------------

function s(tag, attrs = {}, ...children) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== null && value !== undefined && value !== false) node.setAttribute(key, String(value));
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

// A line of text; parts can be [text, class] pairs for mixed styles.
function text(x, y, value, cls = "sc-note", anchor = "start") {
  const node = s("text", { x, y, class: cls, "text-anchor": anchor });
  if (Array.isArray(value)) {
    for (const [part, partCls] of value) node.append(s("tspan", { class: partCls || null }, part));
  } else {
    node.textContent = value;
  }
  return node;
}

function card({ x, y, w, title, rows = [], mono = true, lh = 21, cls = "" }) {
  const pad = 12;
  const head = title ? 22 : 0;
  const h = pad * 2 + head + rows.length * lh - (rows.length ? 5 : 0);
  return s(
    "g",
    { transform: `translate(${x} ${y})` },
    s("rect", { class: `sc-card ${cls}`.trim(), width: w, height: h, rx: 8 }),
    title ? text(pad, pad + 13, title, "sc-card-title") : null,
    rows.map((row, i) => text(pad, pad + head + 14 + i * lh, row, mono ? "sc-mono" : "sc-note")),
  );
}

function station(x, y, label, sub, { end = false, r = 13 } = {}) {
  const dot = s("circle", { class: `sc-station${end ? " is-end" : ""}`, cx: x, cy: y, r });
  return {
    dot,
    node: s(
      "g",
      {},
      dot,
      label ? text(x, y + r + 26, label, "sc-label", "middle") : null,
      sub ? text(x, y + r + 46, sub, "sc-sub", "middle") : null,
    ),
  };
}

function token(kind = "msg") {
  let shape;
  if (kind === "row") shape = s("rect", { class: "tok-row", x: -12, y: -8, width: 24, height: 16, rx: 3 });
  else if (kind === "commit") shape = s("path", { class: "tok-commit", d: "M0 -11 L11 0 L0 11 L-11 0 Z" });
  else if (kind === "bad") shape = s("circle", { class: "tok-bad", r: 8 });
  else if (kind === "request") shape = s("circle", { class: "tok-request", r: 9 });
  else shape = s("circle", { class: kind === "ctl" ? "tok-ctl" : "tok-msg", r: 9 });
  return s("g", { opacity: 0 }, shape);
}

function put(node, x, y, opacity = 1) {
  node.setAttribute("transform", `translate(${x.toFixed(1)} ${y.toFixed(1)})`);
  node.setAttribute("opacity", clamp01(opacity).toFixed(3));
}

function hide(node) {
  node.setAttribute("opacity", "0");
}

// Wraps a drawing so it can fade and rise into place.
function fader(child, rise = 10) {
  const node = s("g", { opacity: 0 }, child);
  return {
    node,
    set(p) {
      node.setAttribute("opacity", p.toFixed(3));
      node.setAttribute("transform", `translate(0 ${((1 - p) * rise).toFixed(2)})`);
    },
  };
}

function arrowLine(x1, y1, x2, y2, cls = "sc-line") {
  const angle = Math.atan2(y2 - y1, x2 - x1);
  const size = 11;
  const head = [
    [x2, y2],
    [x2 - size * Math.cos(angle - 0.45), y2 - size * Math.sin(angle - 0.45)],
    [x2 - size * Math.cos(angle + 0.45), y2 - size * Math.sin(angle + 0.45)],
  ];
  return s(
    "g",
    {},
    s("line", { class: cls, x1, y1, x2, y2 }),
    s("path", { class: "sc-route", d: `M${head.map((p) => p.join(" ")).join(" L")} Z` }),
  );
}

// --- Time helpers ------------------------------------------------------------------

const clamp01 = (value) => Math.max(0, Math.min(1, value));
const span = (t, a, b) => clamp01((t - a) / (b - a));
const ease = (p) => (p < 0.5 ? 4 * p * p * p : 1 - (-2 * p + 2) ** 3 / 2);
const lerp = (a, b, p) => a + (b - a) * p;

function along(points, p) {
  const lengths = [];
  let total = 0;
  for (let i = 1; i < points.length; i += 1) {
    const length = Math.hypot(points[i][0] - points[i - 1][0], points[i][1] - points[i - 1][1]);
    lengths.push(length);
    total += length;
  }
  let distance = clamp01(p) * total;
  for (let i = 0; i < lengths.length; i += 1) {
    if (distance <= lengths[i] || i === lengths.length - 1) {
      const q = lengths[i] ? Math.min(1, distance / lengths[i]) : 0;
      return [lerp(points[i][0], points[i + 1][0], q), lerp(points[i][1], points[i + 1][1], q)];
    }
    distance -= lengths[i];
  }
  return points[points.length - 1];
}

// Moves a token along a path between times a and b; hidden outside them.
function travel(node, t, a, b, points, { smooth = true, fade = 0.15 } = {}) {
  if (t < a || t > b) {
    hide(node);
    return;
  }
  const p = span(t, a, b);
  const [x, y] = along(points, smooth ? ease(p) : p);
  put(node, x, y, Math.min(1, (t - a) / fade, (b - t) / fade));
}

// --- Step 1: where the routes come from ------------------------------------------

function buildRoutes(root) {
  const ases = [
    { x: 120, y: 100, name: "AS64496" },
    { x: 78, y: 262, name: "AS64497" },
    { x: 120, y: 424, name: "AS64498" },
    { x: 262, y: 470, name: "AS64499" },
  ];
  const rrc = { x: 400, y: 262 };
  const ris = { x: 640, y: 262 };
  const ours = { x: 872, y: 262 };
  const others = [
    { x: 820, y: 96 },
    { x: 820, y: 430 },
  ];

  const sessions = s(
    "g",
    {},
    ases.map((a) => s("line", { class: "sc-thin", x1: a.x, y1: a.y, x2: rrc.x, y2: rrc.y })),
  );
  const fanout = s(
    "g",
    {},
    others.map((o) => s("line", { class: "sc-thin sc-dash", x1: ris.x, y1: ris.y, x2: o.x, y2: o.y })),
  );
  const line = s("line", { class: "sc-line", x1: rrc.x, y1: rrc.y, x2: ours.x, y2: ours.y });
  const asNodes = ases.map((a) =>
    fader(s("g", {}, s("circle", { class: "sc-node", cx: a.x, cy: a.y, r: 18 }), text(a.x, a.y + 42, a.name, "sc-sub", "middle"))),
  );
  const otherNodes = others.map((o) =>
    fader(
      s(
        "g",
        {},
        s("circle", { class: "sc-node is-faint", cx: o.x, cy: o.y, r: 14 }),
        text(o.x + 24, o.y + 5, "other users", "sc-sub"),
      ),
    ),
  );
  const stations = [
    station(rrc.x, rrc.y, "rrc21", "RIPE route collector"),
    station(ris.x, ris.y, "RIS Live", "public JSON stream"),
    station(ours.x, ours.y, "RouteWatch", "our collector", { end: true }),
  ].map((item) => fader(item.node));
  const sessionsNote = fader(text(150, 200, "BGP sessions", "sc-sub"));
  const filterNote = fader(text(430, 398, "Keepalives stay behind: UPDATE only", "sc-sub"));
  const counter = text(ours.x, 352, "", "sc-note", "middle");
  const update = fader(
    card({
      x: 452,
      y: 52,
      w: 300,
      title: "One BGP UPDATE",
      rows: [
        [["announce  ", "is-muted"], ["198.51.100.0/24"]],
        [["path      ", "is-muted"], ["64496 64500 64510"]],
        [["withdraw  ", "is-muted"], ["192.0.2.128/25"]],
      ],
    }),
  );

  const updates = [0, 1, 2, 3, 0, 2].map((from, k) => ({
    from: ases[from],
    start: 1.0 + k * 1.05,
    node: token("msg"),
    ghosts: others.map(() => token("msg")),
  }));
  const controls = [
    { from: ases[1], start: 1.9 },
    { from: ases[3], start: 4.3 },
  ].map((c) => ({ ...c, node: token("ctl") }));

  root.append(
    sessions,
    fanout,
    line,
    ...asNodes.map((f) => f.node),
    ...otherNodes.map((f) => f.node),
    ...stations.map((f) => f.node),
    sessionsNote.node,
    filterNote.node,
    counter,
    update.node,
    ...controls.map((c) => c.node),
    ...updates.flatMap((u) => [...u.ghosts, u.node]),
  );

  return (t) => {
    sessions.setAttribute("opacity", span(t, 0.2, 0.9).toFixed(3));
    fanout.setAttribute("opacity", (0.8 * span(t, 0.6, 1.2)).toFixed(3));
    line.setAttribute("opacity", span(t, 0.3, 0.9).toFixed(3));
    asNodes.forEach((f, i) => f.set(span(t, i * 0.12, 0.45 + i * 0.12)));
    otherNodes.forEach((f) => f.set(span(t, 0.6, 1.1)));
    stations.forEach((f, i) => f.set(span(t, 0.2 + i * 0.15, 0.6 + i * 0.15)));
    sessionsNote.set(span(t, 0.9, 1.4));
    filterNote.set(span(t, 3.2, 3.7));
    update.set(span(t, 4.6, 5.3));

    let received = 0;
    for (const u of updates) {
      const a = u.start;
      if (t < a + 1.1) travel(u.node, t, a, a + 1.1, [[u.from.x, u.from.y], [rrc.x, rrc.y]]);
      else if (t < a + 2.0) travel(u.node, t, a + 1.1, a + 2.0, [[rrc.x, rrc.y], [ris.x, ris.y]], { smooth: false, fade: 0.01 });
      else travel(u.node, t, a + 2.0, a + 2.8, [[ris.x, ris.y], [ours.x, ours.y]], { smooth: false, fade: 0.12 });
      u.ghosts.forEach((ghost, i) => {
        travel(ghost, t, a + 2.0, a + 2.8, [[ris.x, ris.y], [others[i].x, others[i].y]], { smooth: false });
        if (t >= a + 2.0 && t <= a + 2.8) ghost.setAttribute("opacity", (0.35 * Number(ghost.getAttribute("opacity"))).toFixed(3));
      });
      if (t >= a + 2.8) received += 1;
    }
    for (const c of controls) {
      if (t < c.start + 1.1) travel(c.node, t, c.start, c.start + 1.1, [[c.from.x, c.from.y], [rrc.x, rrc.y]]);
      else put(c.node, rrc.x, rrc.y, 1 - span(t, c.start + 1.1, c.start + 1.6));
    }
    counter.textContent = t > 1 ? `updates received: ${received}` : "";
  };
}

// --- Step 2: subscribing to the stream --------------------------------------------

function buildSubscribe(root) {
  const left = { x: 110, y: 300 };
  const right = { x: 830, y: 300 };
  const line = s("line", { class: "sc-line", x1: left.x, y1: left.y, x2: right.x, y2: right.y });
  const ends = [station(left.x, left.y, "RouteWatch collector"), station(right.x, right.y, "RIS Live", null, { end: true })];
  const request = fader(
    card({
      x: 200,
      y: 64,
      w: 500,
      title: "One request, kept open",
      rows: [
        "GET /v1/stream/?format=json&client=routewatch",
        'X-RIS-Subscribe: {"host": "rrc21", "type": "UPDATE"}',
        "User-Agent: RouteWatch/0.1",
      ],
    }),
  );
  const asking = token("request");
  const askingLabel = text(0, 0, "request", "sc-sub", "middle");
  const replyLabel = fader(text(right.x, 252, "response: one message per line", "sc-sub", "end"));
  const counter = text(left.x, 384, "", "sc-note", "middle");
  const chips = Array.from({ length: 6 }, (_, k) => {
    const node = s(
      "g",
      { opacity: 0 },
      s("rect", { class: "sc-chip", x: -64, y: -15, width: 128, height: 30, rx: 6 }),
      text(-54, 5, '{…"UPDATE"…}', "sc-mono"),
      text(70, 6, "↵", "sc-note is-muted"),
    );
    return { node, start: 2.9 + k * 1.0 };
  });
  const note = fader(
    card({
      x: 250,
      y: 410,
      w: 470,
      title: "Newline-delimited JSON",
      mono: false,
      rows: ["One complete message per line, so the collector", "never waits for the response to end."],
    }),
  );

  root.append(line, ...ends.map((e) => e.node), request.node, replyLabel.node, counter, note.node, asking, askingLabel, ...chips.map((c) => c.node));

  return (t) => {
    request.set(span(t, 0.3, 1.0));
    travel(asking, t, 1.3, 2.5, [[left.x, left.y], [right.x, right.y]]);
    if (t >= 1.3 && t <= 2.5) {
      const [x] = along([[left.x, left.y], [right.x, right.y]], ease(span(t, 1.3, 2.5)));
      askingLabel.setAttribute("x", x.toFixed(1));
      askingLabel.setAttribute("y", "272");
      askingLabel.setAttribute("opacity", asking.getAttribute("opacity"));
    } else {
      askingLabel.setAttribute("opacity", "0");
    }
    replyLabel.set(span(t, 2.6, 3.0));
    let read = 0;
    for (const chip of chips) {
      const a = chip.start;
      const b = a + 3.6;
      if (t < a) {
        hide(chip.node);
        continue;
      }
      const x = lerp(right.x - 40, left.x + 95, span(t, a, b));
      const opacity = t <= b ? span(t, a, a + 0.2) : 1 - span(t, b, b + 0.25);
      put(chip.node, x, left.y, opacity);
      if (t >= b) read += 1;
    }
    counter.textContent = t >= 2.9 ? `messages read: ${read}` : "";
    note.set(span(t, 5.0, 5.6));
  };
}

// --- Step 3: captured, unchanged ---------------------------------------------------

function buildCapture(root) {
  const payload = card({
    x: 0,
    y: 0,
    w: 470,
    title: "Payload, as received",
    rows: [
      '{"type": "ris_message", "data": {',
      '  "type": "UPDATE", "peer_asn": "64496",',
      '  "path": [64496, 64500, 64510], …}}',
    ],
  });
  const payloadGroup = s("g", { opacity: 0 }, payload);
  const pill = (y, label, value) =>
    s(
      "g",
      {},
      s("line", { class: "sc-thin", x1: 560, y1: 104, x2: 600, y2: y + 17 }),
      s("rect", { class: "sc-card is-key", x: 600, y, width: 310, height: 34, rx: 17 }),
      text(616, y + 22, [[label, "is-muted"], [value]], "sc-mono"),
    );
  const runTag = fader(pill(62, "run_id  ", "3f6c2b1e-…"));
  const timeTag = fader(pill(112, "received_at  ", "14:05:12.481 UTC"));
  const beside = fader(text(90, 222, "Both are stored beside the payload, never written into it.", "sc-note"));

  const ris = { x: 150, y: 352 };
  const col = { x: 790, y: 352 };
  const leftLine = s("line", { class: "sc-line", x1: ris.x, y1: ris.y, x2: 470, y2: ris.y });
  const rightLine = s("line", { class: "sc-line", x1: 470, y1: col.y, x2: col.x, y2: col.y });
  const ends = [station(ris.x, ris.y, "RIS Live", null, { r: 11 }), station(col.x, col.y, "collector", null, { r: 11, end: true })];
  const cross = text(470, 360, "×", "sc-label sc-big is-bad", "middle");
  const status = text(470, 318, "", "sc-note", "middle");
  const waitLabel = text(470, 424, "", "sc-note", "middle");
  const track = s("rect", { class: "sc-bar-track", x: 390, y: 436, width: 160, height: 6, rx: 3 });
  const bar = s("rect", { class: "sc-bar", x: 390, y: 436, width: 0, height: 6, rx: 3 });
  const newRun = fader(text(470, 474, [["new connection, new run_id  ", "is-muted"], ["9a41d07c-…"]], "sc-mono", "middle"));
  const doubling = fader(text(470, 508, "The wait doubles each time, up to 30 s, and resets once messages flow.", "sc-sub", "middle"));
  const before = [0.6, 2.0].map((start) => ({ start, node: token("msg") }));
  const after = [8.9, 9.6, 10.3].map((start) => ({ start, node: token("msg") }));

  root.append(
    payloadGroup,
    runTag.node,
    timeTag.node,
    beside.node,
    leftLine,
    rightLine,
    ...ends.map((e) => e.node),
    cross,
    status,
    waitLabel,
    track,
    bar,
    newRun.node,
    doubling.node,
    ...before.map((b) => b.node),
    ...after.map((b) => b.node),
  );

  const waits = [
    { a: 4.6, b: 5.7, label: "waiting 1 s" },
    { a: 5.7, b: 7.0, label: "reconnect failed: waiting 2 s" },
    { a: 7.0, b: 8.4, label: "waiting 4 s" },
  ];

  return (t) => {
    const slide = ease(span(t, 0.2, 1.2));
    payloadGroup.setAttribute("transform", `translate(${lerp(-480, 90, slide).toFixed(1)} 46)`);
    payloadGroup.setAttribute("opacity", span(t, 0.2, 0.6).toFixed(3));
    runTag.set(span(t, 1.6, 2.1));
    timeTag.set(span(t, 2.4, 2.9));
    beside.set(span(t, 3.2, 3.7));

    const broken = t >= 4.2 && t < 8.5;
    const gap = 30 * ease(span(t, 4.2, 4.5)) * (1 - ease(span(t, 8.4, 8.7)));
    leftLine.setAttribute("x2", (470 - gap).toFixed(1));
    rightLine.setAttribute("x1", (470 + gap).toFixed(1));
    leftLine.classList.toggle("sc-broken", broken);
    rightLine.classList.toggle("sc-broken", broken);
    cross.setAttribute("opacity", broken ? "1" : "0");
    if (broken) {
      status.textContent = "connection dropped";
      status.setAttribute("class", "sc-note is-bad");
    } else if (t >= 8.5) {
      status.textContent = "connected again";
      status.setAttribute("class", "sc-note is-good");
    } else {
      status.textContent = "";
    }
    const wait = waits.find((w) => t >= w.a && t < w.b);
    waitLabel.textContent = wait ? wait.label : "";
    track.setAttribute("opacity", wait ? "1" : "0");
    bar.setAttribute("width", wait ? (160 * span(t, wait.a, wait.b)).toFixed(1) : "0");
    newRun.set(span(t, 8.7, 9.2));
    doubling.set(span(t, 9.4, 9.9));
    for (const item of [...before, ...after]) {
      travel(item.node, t, item.start, item.start + 1.6, [[ris.x, ris.y], [col.x, col.y]], { smooth: false });
    }
  };
}

// --- Step 4: into Kafka --------------------------------------------------------------

function buildKafka(root) {
  const producer = { x: 120, y: 104 };
  const cellX = (i) => 175 + i * 66;
  const cellY = 272;
  const prod = station(producer.x, producer.y, "collector", "the producer");
  const header = text(175, 254, [["topic ", "is-muted"], ["routewatch.ris.raw", "is-strong"], [", partition 0", "is-muted"]], "sc-mono");
  const cells = Array.from({ length: 11 }, (_, i) => {
    const rect = s("rect", { class: `sc-cell${i >= 7 ? " is-empty" : ""}`, x: cellX(i), y: cellY, width: 58, height: 46, rx: 5 });
    const label = text(cellX(i) + 29, 340, String(1040 + i), "sc-mono is-muted", "middle");
    return { rect, label, group: s("g", {}, rect, label) };
  });
  const record = fader(
    card({
      x: 430,
      y: 30,
      w: 470,
      title: "A Kafka record",
      rows: [
        [["offset     ", "is-muted"], ["1047"]],
        [["value      ", "is-muted"], ["the payload bytes, unchanged"]],
        [["headers    ", "is-muted"], ["routewatch_run_id,"]],
        [["           ", "is-muted"], ["routewatch_received_at"]],
        [["timestamp  ", "is-muted"], ["received_at"]],
      ],
    }),
  );
  const confirmed = text(producer.x + 30, producer.y + 5, "✓ confirmed", "sc-note is-good");
  const next = text(905, 384, "", "sc-note", "end");
  const removed = fader(
    s(
      "g",
      {},
      text(175, 430, "Records older than 48 hours, or past 2 GiB, are deleted.", "sc-note"),
      text(175, 454, "The rest keep their offsets: an offset is never reused.", "sc-note"),
    ),
  );
  const appends = [0, 1, 2, 3].map((k) => ({ index: 7 + k, start: 1.1 + k * 1.6, node: token("msg") }));

  root.append(prod.node, header, ...cells.map((c) => c.group), record.node, confirmed, next, removed.node, ...appends.map((a) => a.node));

  return (t) => {
    record.set(span(t, 1.4, 2.0));
    let landed = 0;
    let lastLanding = -10;
    for (const a of appends) {
      const x = cellX(a.index) + 29;
      travel(a.node, t, a.start, a.start + 1.0, [[producer.x, producer.y], [x, 196], [x, cellY + 23]]);
      const arrived = t >= a.start + 1.0;
      cells[a.index].rect.setAttribute("class", arrived ? "sc-cell" : "sc-cell is-empty");
      cells[a.index].label.setAttribute("class", arrived ? "sc-mono" : "sc-mono is-muted");
      if (arrived) {
        landed += 1;
        lastLanding = a.start + 1.0;
      }
    }
    confirmed.setAttribute("opacity", (t - lastLanding >= 0 && t - lastLanding < 0.8 ? 1 - span(t, lastLanding + 0.5, lastLanding + 0.8) : 0).toFixed(3));
    next.textContent = `next offset: ${1047 + landed}`;
    [0, 1].forEach((i) => {
      const p = ease(span(t, 8.6 + i * 0.3, 9.6 + i * 0.3));
      cells[i].group.setAttribute("transform", `translate(${(-40 * p).toFixed(1)} 0)`);
      cells[i].group.setAttribute("opacity", (1 - p).toFixed(3));
    });
    removed.set(span(t, 9.2, 9.8));
  };
}

// --- Step 5: database first, offset second ----------------------------------------

function buildCommit(root) {
  const cellX = (i) => 200 + i * 70;
  const consumer = { x: 470, y: 330 };
  const table = { x: 790, y: 330 };
  const header = text(200, 64, [["Kafka ", "is-muted"], ["routewatch.ris.raw", "is-strong"]], "sc-mono");
  const cells = Array.from({ length: 8 }, (_, i) =>
    s(
      "g",
      {},
      s("rect", { class: "sc-cell", x: cellX(i), y: 78, width: 62, height: 40, rx: 5 }),
      text(cellX(i) + 31, 138, String(1047 + i), "sc-mono", "middle"),
    ),
  );
  const marker = s("g", {}, s("path", { class: "tok-commit", d: "M0 -9 L9 6 L-9 6 Z" }), text(0, 30, "", "sc-note is-wait", "middle"));
  const markerLabel = marker.lastChild;
  const cons = station(consumer.x, consumer.y, "raw consumer");
  const tableGlyph = s(
    "g",
    {},
    s("rect", { class: "sc-card", x: table.x - 80, y: table.y - 50, width: 160, height: 100, rx: 8 }),
    s("rect", { class: "sc-head", x: table.x - 79, y: table.y - 49, width: 158, height: 26, rx: 7 }),
    text(table.x, table.y - 30, "routewatch_raw", "sc-card-title", "middle"),
    text(table.x, table.y + 76, "raw table", "sc-label", "middle"),
  );
  const rows = text(table.x, table.y + 22, "", "sc-label", "middle");
  const box = s("rect", { class: "sc-thin sc-dash", x: 500, y: 296, width: 200, height: 68, rx: 10 });
  const boxLabel = text(600, 288, "one transaction", "sc-sub", "middle");
  const stored = text(table.x, table.y - 64, "✓ stored", "sc-note is-good", "middle");
  const skipped = text(table.x, table.y - 64, "already stored: skipped", "sc-note is-wait", "middle");
  const event = text(consumer.x, 436, "", "sc-note", "middle");

  const batch = (cellsFrom, a, c) =>
    [0, 1, 2, 3].map((j) => ({ cell: cellsFrom + j, a: a + j * 0.12, c: c + j * 0.08, node: token("msg") }));
  const first = batch(0, 0.5, 1.7);
  const second = batch(4, 4.2, 5.3);
  const replay = batch(4, 8.0, 9.0);
  const commits = [
    { a: 3.0, b: 3.6, to: 4, node: token("commit") },
    { a: 10.0, b: 10.4, to: 8, node: token("commit") },
  ];

  root.append(
    header,
    ...cells,
    marker,
    box,
    boxLabel,
    cons.node,
    tableGlyph,
    rows,
    stored,
    skipped,
    event,
    ...[...first, ...second, ...replay].map((b) => b.node),
    ...commits.map((c) => c.node),
  );

  const toConsumer = (cell) => [[cellX(cell) + 31, 98], [consumer.x, consumer.y]];
  const toTable = [[consumer.x, consumer.y], [table.x - 80, table.y]];

  return (t) => {
    let markerAt = 0;
    if (t >= 3.6) markerAt = lerp(0, 4, ease(span(t, 3.6, 4.0)));
    if (t >= 10.4) markerAt = lerp(4, 8, ease(span(t, 10.4, 10.8)));
    marker.setAttribute("transform", `translate(${(cellX(0) + 31 + markerAt * 70).toFixed(1)} 156)`);
    markerLabel.textContent = `committed offset ${t >= 10.4 ? 1055 : t >= 3.6 ? 1051 : 1047}`;

    let rowCount = 0;
    for (const item of [...first, ...second]) {
      if (t < item.c) travel(item.node, t, item.a, item.a + 0.9, toConsumer(item.cell));
      else travel(item.node, t, item.c, item.c + 0.8, toTable, { smooth: false });
      if (t >= item.c + 0.8) rowCount += 1;
    }
    for (const item of replay) {
      if (t < item.c) travel(item.node, t, item.a, item.a + 0.9, toConsumer(item.cell));
      else if (t < item.c + 0.8) travel(item.node, t, item.c, item.c + 0.8, toTable, { smooth: false, fade: 0.01 });
      else put(item.node, table.x - 80, table.y - 14 * (1 - span(t, item.c + 0.8, item.c + 1.2)), 1 - span(t, item.c + 0.8, item.c + 1.2));
    }
    for (const c of commits) {
      travel(c.node, t, c.a, c.b, [[consumer.x, consumer.y], [cellX(c.to) + 31, 156]]);
    }
    rows.textContent = `rows: ${rowCount}`;

    const inserting = (t >= 1.6 && t < 2.9) || (t >= 5.2 && t < 6.4) || (t >= 8.9 && t < 10.1);
    box.setAttribute("opacity", inserting ? "1" : "0");
    boxLabel.setAttribute("opacity", inserting ? "1" : "0");
    stored.setAttribute("opacity", (t >= 2.6 && t < 3.4) || (t >= 6.1 && t < 6.9) ? "1" : "0");
    skipped.setAttribute("opacity", t >= 9.6 && t < 11.2 ? "1" : "0");

    const crashed = t >= 6.5 && t < 7.5;
    cons.dot.setAttribute("class", crashed ? "sc-station is-bad" : "sc-station");
    if (crashed) {
      event.textContent = "stopped before committing the offset";
      event.setAttribute("class", "sc-note is-bad");
    } else if (t >= 7.5 && t < 9.9) {
      event.textContent = "restarted: reading again from offset 1051";
      event.setAttribute("class", "sc-note");
    } else if (t >= 10.8) {
      event.textContent = "8 messages read, 8 rows stored";
      event.setAttribute("class", "sc-note is-good");
    } else {
      event.textContent = "";
    }
  };
}

// --- Step 6: the raw table ----------------------------------------------------------

function buildRawTable(root) {
  const x0 = 40;
  const y0 = 82;
  const columns = [
    ["cluster", 120],
    ["topic", 178],
    ["partition", 88],
    ["offset", 74],
    ["payload (bytes)", 190],
    ["run_id", 104],
    ["received_at", 126],
  ];
  const width = columns.reduce((sum, [, w]) => sum + w, 0);
  const starts = [];
  columns.reduce((x, [, w]) => {
    starts.push(x);
    return x + w;
  }, x0);
  const keyWidth = columns.slice(0, 4).reduce((sum, [, w]) => sum + w, 0);
  const times = ["14:05:12.481", "14:05:12.913", "14:05:13.240", "14:05:13.902", "14:05:14.377", "14:05:14.960"];
  const rowH = 38;
  const outline = s("rect", { class: "sc-card", x: x0, y: y0, width, height: 40 + times.length * rowH, rx: 8 });
  const head = s("rect", { class: "sc-head", x: x0 + 1, y: y0 + 1, width: width - 2, height: 39, rx: 7 });
  const labels = columns.map(([name], i) => text(starts[i] + 10, y0 + 26, name, "sc-card-title"));
  const keyBar = s("rect", { class: "sc-route", x: x0, y: y0 - 12, width: keyWidth, height: 4, rx: 2 });
  const keyNote = fader(text(x0, y0 - 22, "primary key: the message's Kafka position", "sc-note is-route"));
  const rows = times.map((time, k) => {
    const y = y0 + 40 + k * rowH;
    const values = ["Mk3OEYBS…", "routewatch.ris.raw", "0", String(1050 + k), '{"type": "ris_…', "9a41d07c…", time];
    return fader(
      s(
        "g",
        {},
        k > 0 ? s("line", { class: "sc-grid", x1: x0, y1: y, x2: x0 + width, y2: y }) : null,
        values.map((value, i) => text(starts[i] + 10, y + 25, value, "sc-mono sc-mid")),
      ),
      8,
    );
  });
  const append = fader(
    card({
      x: x0,
      y: 370,
      w: 430,
      title: "Append-only",
      mono: false,
      rows: ["Rows are added and never updated.", "Cleaning reads them and writes elsewhere."],
    }),
  );
  const rebuild = fader(s("g", {}, arrowLine(510, 430, 900, 430), text(515, 412, "the clean tables can be rebuilt from here", "sc-note")));

  root.append(outline, head, ...labels, keyBar, keyNote.node, ...rows.map((r) => r.node), append.node, rebuild.node);

  return (t) => {
    keyBar.setAttribute("opacity", span(t, 0.4, 0.9).toFixed(3));
    keyNote.set(span(t, 0.6, 1.1));
    rows.forEach((row, k) => row.set(span(t, 1.0 + k * 0.7, 1.4 + k * 0.7)));
    append.set(span(t, 5.4, 6.0));
    rebuild.set(span(t, 6.8, 7.4));
  };
}

// --- Step 7: one message, many rows --------------------------------------------------

function buildClean(root) {
  const msgX = 30;
  const msgY = 48;
  const lines = [
    '"peer_asn": "64496",',
    '"path": [64496, 64500, 64510],',
    '"withdrawals": ["192.0.2.128/25"],',
    '"announcements": [{',
    '  "next_hop": "192.0.2.1",',
    '  "prefixes": ["198.51.100.0/24",',
    '               "203.0.113.0/24"]}]',
  ];
  const lineY = (i) => msgY + 12 + 22 + 14 + i * 21;
  const message = card({ x: msgX, y: msgY, w: 440, title: "One raw message: an UPDATE", rows: lines });
  const highlights = [2, 5, 6].map((i) => {
    const mark = s("rect", { x: 6, y: lineY(i) - msgY - 15, width: 390, height: 20, rx: 3, class: "sc-mark", opacity: 0 });
    message.insertBefore(mark, message.children[1]);
    return mark;
  });

  const tx = 510;
  const ty = 48;
  const tableTitle = text(tx, ty - 10, "routewatch_clean.prefix_events", "sc-card-title");
  const outline = s("rect", { class: "sc-card", x: tx, y: ty, width: 420, height: 36 + 3 * 34, rx: 8 });
  const head = s("rect", { class: "sc-head", x: tx + 1, y: ty + 1, width: 418, height: 35, rx: 7 });
  const cols = [14, 48, 162, 336];
  const headLabels = ["#", "action", "prefix", "origin AS"].map((label, i) => text(tx + cols[i], ty + 23, label, "sc-card-title"));
  const values = [
    ["0", "WITHDRAW", "192.0.2.128/25", "–"],
    ["1", "ANNOUNCE", "198.51.100.0/24", "64510"],
    ["2", "ANNOUNCE", "203.0.113.0/24", "64510"],
  ];
  const rowY = (k) => ty + 36 + k * 34;
  const rows = values.map((row, k) =>
    fader(
      s(
        "g",
        {},
        k > 0 ? s("line", { class: "sc-grid", x1: tx, y1: rowY(k), x2: tx + 420, y2: rowY(k) }) : null,
        row.map((value, i) => text(tx + cols[i], rowY(k) + 22, value, i === 1 ? `sc-mono sc-mid ${k ? "is-route" : "is-muted"}` : "sc-mono sc-mid")),
      ),
      0,
    ),
  );
  const moves = [
    { from: 2, start: 1.8 },
    { from: 5, start: 3.0 },
    { from: 6, start: 4.2 },
  ].map((m, k) => ({ ...m, row: k, node: token("row") }));
  const receipt = fader(
    card({
      x: tx,
      y: 210,
      w: 420,
      title: "routewatch_clean.processing",
      rows: [
        [["status       ", "is-muted"], ["cleaned"]],
        [["event_count  ", "is-muted"], ["3"]],
        [["version      ", "is-muted"], ["1"]],
      ],
    }),
  );

  const lane = fader(text(30, 372, "Every message gets exactly one receipt, whatever happens to it:", "sc-note"));
  const bins = [
    { x: 330, name: "ignored", sub: "a control message" },
    { x: 560, name: "empty", sub: "an UPDATE, no prefixes" },
    { x: 790, name: "rejected", sub: "the reason is kept" },
  ].map((b) => {
    const rect = s("rect", { class: "sc-card", x: b.x - 100, y: 412, width: 200, height: 64, rx: 10 });
    return {
      ...b,
      rect,
      fade: fader(s("g", {}, rect, text(b.x, 438, b.name, `sc-label${b.name === "rejected" ? " is-bad" : ""}`, "middle"), text(b.x, 461, b.sub, "sc-sub", "middle"))),
    };
  });
  const sources = [
    { kind: "ctl", label: "KEEPALIVE", start: 7.0, bin: 0 },
    { kind: "msg", label: "UPDATE", start: 8.4, bin: 1 },
    { kind: "bad", label: "not JSON", start: 9.8, bin: 2 },
  ].map((src) => ({ ...src, node: token(src.kind), tag: text(0, 0, src.label, "sc-sub", "middle") }));

  root.append(
    message,
    tableTitle,
    outline,
    head,
    ...headLabels,
    ...rows.map((r) => r.node),
    receipt.node,
    lane.node,
    ...bins.map((b) => b.fade.node),
    ...moves.map((m) => m.node),
    ...sources.flatMap((src) => [src.node, src.tag]),
  );

  return (t) => {
    moves.forEach((m) => {
      const a = m.start;
      highlights[m.row].setAttribute("opacity", (t >= a - 0.3 ? 0.9 : 0).toString());
      travel(m.node, t, a, a + 1.0, [[msgX + 400, lineY(m.from) - 5], [tx - 30, rowY(m.row) + 17]]);
      rows[m.row].set(span(t, a + 0.9, a + 1.2));
    });
    receipt.set(span(t, 5.8, 6.4));
    lane.set(span(t, 6.6, 7.0));
    bins.forEach((b) => b.fade.set(span(t, 6.7, 7.1)));
    for (const src of sources) {
      const bin = bins[src.bin];
      const a = src.start;
      travel(src.node, t, a, a + 1.2, [[50, 444], [bin.x - 110, 444]]);
      const visible = t >= a && t <= a + 1.2;
      if (visible) {
        const [x] = along([[50, 444], [bin.x - 110, 444]], ease(span(t, a, a + 1.2)));
        src.tag.setAttribute("x", x.toFixed(1));
        src.tag.setAttribute("y", "424");
      }
      src.tag.setAttribute("opacity", visible ? src.node.getAttribute("opacity") : "0");
      const glow = t >= a + 1.2 && t < a + 2.0;
      bin.rect.setAttribute("class", glow ? "sc-card is-key" : "sc-card");
    }
  };
}

// --- Step 8: features for analysis ---------------------------------------------------

function buildFeatures(root) {
  const prefixTitle = text(40, 50, "Prefix", "sc-label");
  const before = text(40, 100, [["198.51.100."], ["7", "is-bad"], ["/24"]], "sc-mono sc-big");
  const arrow = fader(arrowLine(258, 93, 318, 93));
  const after = fader(text(338, 100, [["198.51.100."], ["0", "is-route"], ["/24"]], "sc-mono sc-big"));
  const attrs = [
    [["ip_version  ", "is-muted"], ["4"]],
    [["prefix_length  ", "is-muted"], ["24"]],
    [["prefix_was_normalized  ", "is-muted"], ["true", "is-route"]],
  ].map((row, i) => fader(text(600, 74 + i * 24, row, "sc-mono")));

  const pathTitle = text(40, 192, "AS path, from the peer to the origin", "sc-label");
  const hops = [64500, 64501, 64501, 64501, 64502];
  const xs = [90, 240, 390, 540, 690];
  const py = 252;
  const track = s("line", { class: "sc-line", x1: xs[0], y1: py, x2: xs[4], y2: py });
  const stops = hops.map((asn, i) => {
    const repeat = i > 0 && hops[i - 1] === asn;
    const dot = s("circle", {
      class: `sc-station${i === hops.length - 1 ? " is-end" : ""}`,
      cx: xs[i],
      cy: py,
      r: 12,
      "stroke-dasharray": repeat ? "5 4" : null,
    });
    return s("g", {}, dot, text(xs[i], py + 42, String(asn), "sc-mono", "middle"), repeat ? text(xs[i], py - 24, "repeat", "sc-sub is-wait", "middle") : null);
  });
  const ends = s("g", {}, text(xs[0], py + 64, "peer", "sc-sub", "middle"), text(xs[4], py + 64, "origin", "sc-sub", "middle"));
  const cursor = s("circle", { class: "sc-cursor", r: 21, cx: 0, cy: 0, opacity: 0 });
  const meterNames = ["as_path_length", "unique_as_count", "prepending_count", "origin_asn"];
  const meterValues = meterNames.map((_, i) => text(90 + i * 200, 382, "", "sc-label", "start"));
  const meters = s("g", {}, meterNames.map((name, i) => text(90 + i * 200, 358, name, "sc-mono is-muted")), meterValues);
  const lengths = [1, 2, 3, 4, 5];
  const unique = [1, 2, 2, 2, 3];
  const prepends = [0, 0, 1, 2, 2];

  const commTitle = text(40, 432, "Communities", "sc-label");
  const chipAt = (x, label) =>
    s("g", { transform: `translate(${x} 450)` }, s("rect", { class: "sc-chip", width: 124, height: 34, rx: 6 }), text(12, 23, label, "sc-mono"));
  const chip1 = chipAt(40, "64500:100");
  const chip2 = chipAt(184, "64500:100");
  const chip3 = chipAt(328, "65535:666");
  const chip3Rect = chip3.firstChild;
  const dupNote = fader(text(40, 506, "duplicate removed", "sc-sub"));
  const blackhole = fader(text(390, 506, "BLACKHOLE", "sc-sub is-bad", "middle"));
  const commAttrs = [
    [["community_count  ", "is-muted"], ["2"]],
    [["duplicate_community_count  ", "is-muted"], ["1"]],
    [["has_blackhole_community  ", "is-muted"], ["true", "is-bad"]],
  ].map((row, i) => fader(text(530, 448 + i * 24, row, "sc-mono")));

  root.append(prefixTitle, before, arrow.node, after.node, ...attrs.map((a) => a.node), pathTitle, track, ...stops, ends, cursor, meters, commTitle, chip1, chip2, chip3, dupNote.node, blackhole.node, ...commAttrs.map((a) => a.node));

  return (t) => {
    arrow.set(span(t, 0.6, 1.0));
    after.set(span(t, 0.9, 1.5));
    attrs.forEach((a, i) => a.set(span(t, 1.6 + i * 0.35, 2.0 + i * 0.35)));

    // The cursor rests on each hop, then moves on in the last third of its 0.9 s.
    const walk = (t - 3.0) / 0.9;
    if (walk < 0) {
      cursor.setAttribute("opacity", "0");
      meterValues.forEach((v) => (v.textContent = "–"));
    } else {
      const h = Math.min(4, Math.floor(walk));
      const move = h < 4 ? clamp01((walk - h - 2 / 3) * 3) : 0;
      const x = h < 4 ? lerp(xs[h], xs[h + 1], ease(move)) : xs[4];
      cursor.setAttribute("cx", x.toFixed(1));
      cursor.setAttribute("cy", String(py));
      cursor.setAttribute("opacity", (t > 8.6 ? 1 - span(t, 8.6, 9.0) : 1).toFixed(3));
      meterValues[0].textContent = String(lengths[h]);
      meterValues[1].textContent = String(unique[h]);
      meterValues[2].textContent = String(prepends[h]);
      meterValues[3].textContent = h === 4 ? "64502" : "–";
    }

    const merge = ease(span(t, 8.8, 9.6));
    chip2.setAttribute("transform", `translate(${lerp(184, 40, merge).toFixed(1)} 450)`);
    chip2.setAttribute("opacity", (1 - span(t, 9.3, 9.7)).toFixed(3));
    chip3.setAttribute("transform", `translate(${lerp(328, 184, ease(span(t, 9.6, 10.1))).toFixed(1)} 450)`);
    chip3Rect.setAttribute("class", t >= 10.2 ? "sc-chip is-flag" : "sc-chip");
    dupNote.set(span(t, 9.6, 10.0));
    blackhole.set(span(t, 10.2, 10.6));
    blackhole.node.setAttribute("transform", `translate(${(-144 * ease(span(t, 9.6, 10.1))).toFixed(1)} 0)`);
    commAttrs.forEach((a, i) => a.set(span(t, 10.6 + i * 0.4, 11.0 + i * 0.4)));
  };
}

// --- Step 9: recap and checks --------------------------------------------------------

function buildRecap(root) {
  const xs = [70, 200, 340, 480, 620, 760, 890];
  const y = 190;
  const names = ["RIS Live", "collector", "Kafka", "raw consumer", "raw table", "cleaner", "clean tables"];
  const subs = ["rrc21 updates", "bytes unchanged", "kept 48 hours", "database first", "append-only", "one receipt each", "rows, features"];
  const line = s("line", { class: "sc-line", x1: xs[0], y1: y, x2: xs[6], y2: y });
  const stations = xs.map((x, i) => fader(station(x, y, names[i], subs[i], { end: i === xs.length - 1 }).node, 6));
  const travellers = Array.from({ length: 9 }, (_, k) => ({ start: 0.4 + k * 0.9, node: token("msg"), rows: [token("row"), token("row")] }));
  const audit = fader(
    card({
      x: 240,
      y: 330,
      w: 480,
      title: "The audit query checks two things",
      mono: false,
      rows: [
        [["✓  ", "is-good"], ["every raw message has a receipt, or is waiting for one"]],
        [["✓  ", "is-good"], ["the rows the receipts promise are the rows stored"]],
      ],
    }),
  );

  root.append(line, ...stations.map((f) => f.node), audit.node, ...travellers.flatMap((tr) => [tr.node, ...tr.rows]));

  return (t) => {
    line.setAttribute("opacity", span(t, 0, 0.5).toFixed(3));
    stations.forEach((f, i) => f.set(span(t, 0.1 + i * 0.12, 0.5 + i * 0.12)));
    for (const tr of travellers) {
      const a = tr.start;
      travel(tr.node, t, a, a + 4.2, [[xs[0], y], [xs[5], y]], { smooth: false });
      tr.rows.forEach((row, i) => {
        if (t < a + 4.2 || t > a + 5.0) {
          hide(row);
          return;
        }
        const p = span(t, a + 4.2, a + 5.0);
        put(row, lerp(xs[5], xs[6], p), y + (i ? 11 : -11) * Math.min(1, p * 3), Math.min(1, (a + 5.0 - t) / 0.15));
      });
    }
    audit.set(span(t, 5.4, 6.1));
  };
}

// --- The steps -----------------------------------------------------------------------

const STEPS = [
  {
    short: "Routes",
    tag: "Setup",
    title: "Where the routes come from",
    body: [
      "Networks on the internet, called autonomous systems (ASes), tell their neighbours which blocks of addresses they can reach. Every change is sent as a BGP UPDATE message.",
      "The RIPE NCC runs route collectors that hold BGP sessions with many networks and record every update they receive. RIS Live republishes those updates as JSON, live. RouteWatch listens to one collector, `rrc21`, and only to its UPDATE messages.",
    ],
    callout: {
      label: "Key idea",
      text: "An UPDATE either announces prefixes, with the path of ASes the route took, or withdraws prefixes that can no longer be reached.",
    },
    summary: "Four networks send BGP updates to the route collector rrc21; RIS Live passes the updates on to RouteWatch and other users, while keepalives stay behind.",
    duration: 11,
    build: buildRoutes,
  },
  {
    short: "Subscribe",
    tag: "Technique",
    title: "Subscribing to the stream",
    body: [
      "The collector sends one HTTP request and keeps it open. The `X-RIS-Subscribe` header narrows the stream to UPDATE messages from rrc21, and `client=routewatch` in the address tells the RIPE NCC who is connecting.",
      "The response never ends. Each line holds one complete JSON message, so the collector reads the bytes as they arrive and cuts them at every newline.",
    ],
    callout: {
      label: "Good to know",
      text: "If the stream breaks in the middle of a line, that half message is never stored: the collector drops it and reconnects.",
    },
    summary: "The collector sends one long-lived request to RIS Live; the response streams back one JSON message per line, and the collector counts each message as it cuts the stream at newlines.",
    duration: 12,
    build: buildSubscribe,
  },
  {
    short: "Capture",
    tag: "Technique",
    title: "Captured, unchanged",
    body: [
      "Each message keeps its exact bytes. Beside it, never inside it, the collector records two facts: a `run_id` for the connection it arrived on, and `received_at`, the moment it arrived, in UTC.",
      "Connections drop. When one does, the collector waits and reconnects: 1 second, then 2, then 4, up to 30, or longer if RIS Live asks it to. The wait resets once messages flow again, and each new connection gets a new `run_id`.",
    ],
    callout: {
      label: "Key idea",
      text: "Raw data is never changed. Anything learned about it is stored beside it.",
    },
    summary: "A payload keeps its bytes while run_id and received_at are attached beside it; then the connection drops, the collector waits 1, 2 and 4 seconds, and reconnects with a new run_id.",
    duration: 12,
    build: buildCapture,
  },
  {
    short: "Kafka",
    tag: "Technique",
    title: "Into Kafka",
    body: [
      "Kafka stores messages in an append-only log called a topic. The collector writes each one to `routewatch.ris.raw`: the payload as the record's value, and `run_id` and `received_at` as headers. Kafka gives the record the next number in the log, its offset.",
      "Kafka confirms every write to the producer, and the producer is idempotent, so a retry after a network hiccup cannot store a record twice. Records are kept for 48 hours, or until the log reaches 2 GiB.",
    ],
    callout: {
      label: "Why Kafka",
      text: "The source keeps streaming even when the database is slow or down. Messages wait safely in the log until the consumer catches up.",
    },
    summary: "Four records are appended to the topic at offsets 1047 to 1050; later the two oldest records expire, and the remaining offsets stay the same.",
    duration: 11,
    build: buildKafka,
  },
  {
    short: "Commits",
    tag: "Trade-off",
    title: "Database first, offset second",
    body: [
      "The raw consumer reads a batch of records and stores them in PostgreSQL in one transaction. Only after the database confirms does it commit the offset, telling Kafka how far it has read.",
      "If it stops between the two, Kafka hands the same batch over again when it restarts. Each row is keyed by its Kafka position, so the repeat finds the rows already stored and skips them. Nothing is lost, and nothing is stored twice.",
    ],
    callout: {
      label: "The trade-off",
      text: "Committing the offset first would be simpler, but a crash before the insert would lose that batch for good. Storing first costs one repeated read after a crash, which the keys make harmless.",
      tradeoff: true,
    },
    summary: "The consumer stores a batch, then commits the offset; it stores a second batch but stops before committing, so after a restart the batch is read again and skipped, leaving 8 rows for 8 messages.",
    duration: 12,
    build: buildCommit,
  },
  {
    short: "Raw table",
    tag: "Setup",
    title: "The raw table",
    body: [
      "`routewatch_raw.messages` holds every message as Kafka delivered it, with its metadata. Its key is the message's Kafka position: cluster, topic, partition and offset.",
      "Rows are only ever added. Cleaning never edits them, so if a cleaning rule turns out to be wrong, the clean tables can be rebuilt from here.",
    ],
    callout: {
      label: "Key idea",
      text: "Raw is the evidence. Clean is one reading of it.",
    },
    summary: "Rows are appended to the raw table; the first four columns, the Kafka position, form the primary key.",
    duration: 9,
    build: buildRawTable,
  },
  {
    short: "Clean",
    tag: "Technique",
    title: "One message, many rows",
    body: [
      "The cleaner reads raw messages that have no receipt yet. One UPDATE can withdraw some prefixes and announce others, so it becomes one row per prefix in `routewatch_clean.prefix_events`, withdrawals first.",
      "Every message also gets exactly one receipt in `routewatch_clean.processing`: cleaned, empty, ignored or rejected, and a rejection keeps its reason. Nothing is dropped silently, and a rerun picks up only what is still waiting.",
    ],
    callout: {
      label: "Good to know",
      text: "Receipts carry a transform version. A new cleaning rule runs as a new version beside the old one, from the same raw rows.",
    },
    summary: "One UPDATE becomes three rows, a withdrawal then two announcements, and a receipt saying cleaned with 3 rows; a keepalive, an empty update and a broken message get ignored, empty and rejected receipts.",
    duration: 12,
    build: buildClean,
  },
  {
    short: "Features",
    tag: "Solution",
    title: "Features for analysis",
    body: [
      "Cleaning turns raw fields into columns you can count and compare. A prefix is stored as its network: 198.51.100.7/24 becomes 198.51.100.0/24, flagged as changed, with its IP version and length.",
      "The AS path gives its length, the number of distinct ASes, how often an AS repeats itself to make a route less attractive (prepending), and the origin AS at the end. Communities are de-duplicated and counted, and the BLACKHOLE community 65535:666 is flagged.",
    ],
    callout: {
      label: "Good to know",
      text: "Withdrawals carry no path or attributes, and a check constraint on the table makes sure of it.",
    },
    summary: "A prefix is normalised to its network; a path of five hops with one AS repeated three times gives length 5, three distinct ASes, two prepends and origin AS 64502; a duplicate community is removed and a blackhole community flagged.",
    duration: 12,
    build: buildFeatures,
  },
  {
    short: "Recap",
    tag: "Recap",
    title: "Recap and checks",
    body: [
      "Every hop can stop and restart without losing or duplicating data. Kafka holds what the database has not stored yet, and the cleaner picks up whatever has no receipt.",
      "An audit query checks the result: every raw message has a receipt or is waiting for one, and the rows the receipts promise are the rows in the clean table. The data page shows the same counts, live.",
    ],
    callout: {
      label: "See it with real data",
      text: "The data page shows these tables with the messages RouteWatch has collected.",
      link: { href: "data.html", label: "Open the data page" },
    },
    summary: "Messages travel the whole line from RIS Live to the clean tables, and the audit card confirms every message has a receipt and every promised row is stored.",
    duration: 10,
    build: buildRecap,
  },
];

// --- Page wiring ---------------------------------------------------------------------

const $ = (id) => document.getElementById(id);
const scene = $("scene");
const sceneTitle = $("scene-title");
const scrub = $("ctl-scrub");
const clock = { t: 0, duration: 10, playing: false, speed: 1, last: null, raf: 0 };
let current = 0;
let layer = null;
let draw = () => {};

function rich(value) {
  const paragraph = document.createElement("p");
  value.split("`").forEach((part, i) => {
    if (!part) return;
    if (i % 2) {
      const code = document.createElement("code");
      code.textContent = part;
      paragraph.append(code);
    } else {
      paragraph.append(document.createTextNode(part));
    }
  });
  return paragraph;
}

function seconds(value) {
  const whole = Math.floor(value + 1e-6);
  return `0:${String(whole).padStart(2, "0")}`;
}

function render() {
  draw(clock.t);
  scrub.value = String(Math.round(clock.t * 1000));
  $("ctl-time").textContent = `${seconds(clock.t)} / ${seconds(clock.duration)}`;
}

function showPlaying(playing) {
  const button = $("ctl-play");
  button.querySelector(".icon-pause").toggleAttribute("hidden", !playing);
  button.querySelector(".icon-play").toggleAttribute("hidden", playing);
  button.setAttribute("aria-label", playing ? "Pause" : "Play");
}

function frame(now) {
  if (!clock.playing) return;
  if (clock.last !== null) {
    clock.t = Math.min(clock.duration, clock.t + (Math.min(100, now - clock.last) / 1000) * clock.speed);
  }
  clock.last = now;
  render();
  if (clock.t >= clock.duration) {
    clock.playing = false;
    showPlaying(false);
    return;
  }
  clock.raf = window.requestAnimationFrame(frame);
}

function play() {
  if (clock.t >= clock.duration) clock.t = 0;
  clock.playing = true;
  clock.last = null;
  showPlaying(true);
  window.cancelAnimationFrame(clock.raf);
  clock.raf = window.requestAnimationFrame(frame);
}

function pause() {
  clock.playing = false;
  window.cancelAnimationFrame(clock.raf);
  showPlaying(false);
  render();
}

function toggle() {
  if (clock.playing) pause();
  else play();
}

function replay() {
  clock.t = 0;
  play();
}

function renderNarration(step, index) {
  $("step-count").textContent = `Step ${index + 1} of ${STEPS.length}`;
  $("step-tag").textContent = step.tag;
  $("step-title").textContent = step.title;
  $("step-body").replaceChildren(...step.body.map(rich));
  const callout = $("step-callout");
  callout.classList.toggle("is-tradeoff", Boolean(step.callout.tradeoff));
  $("callout-label").textContent = step.callout.label;
  const calloutText = $("callout-text");
  calloutText.replaceChildren(...rich(step.callout.text).childNodes);
  if (step.callout.link) {
    const link = document.createElement("a");
    link.className = "btn btn-primary";
    link.href = step.callout.link.href;
    link.textContent = step.callout.link.label;
    calloutText.append(document.createElement("br"), link);
  }
  $("nav-back").disabled = index === 0;
  $("nav-next").textContent = index < STEPS.length - 1 ? `Next: ${STEPS[index + 1].title}` : "Start again";
  sceneTitle.textContent = `Step ${index + 1} of ${STEPS.length}: ${step.summary}`;
}

function renderStepper(index) {
  [...$("steps").children].forEach((item, i) => {
    item.classList.toggle("is-done", i < index);
    item.classList.toggle("is-current", i === index);
    const button = item.querySelector("button");
    if (i === index) button.setAttribute("aria-current", "step");
    else button.removeAttribute("aria-current");
  });
}

function go(index, { autoplay = !REDUCED } = {}) {
  current = (index + STEPS.length) % STEPS.length;
  const step = STEPS[current];
  if (layer) layer.remove();
  layer = s("g");
  scene.append(layer);
  draw = step.build(layer);
  clock.duration = step.duration;
  clock.t = autoplay ? 0 : step.duration;
  scrub.max = String(step.duration * 1000);
  renderNarration(step, current);
  renderStepper(current);
  $("announce").textContent = `Step ${current + 1} of ${STEPS.length}: ${step.title}`;
  const hash = `#step-${current + 1}`;
  if (window.location.hash !== hash) window.history.replaceState(null, "", hash);
  scene.classList.remove("is-entering");
  scene.getBoundingClientRect();
  scene.classList.add("is-entering");
  render();
  if (autoplay) play();
  else pause();
}

function stepFromHash() {
  const match = /^#step-(\d+)$/.exec(window.location.hash);
  const number = match ? Number(match[1]) : 1;
  return Math.min(Math.max(number, 1), STEPS.length) - 1;
}

function buildStepper() {
  $("steps").replaceChildren(
    ...STEPS.map((step, i) => {
      const item = document.createElement("li");
      const button = document.createElement("button");
      button.type = "button";
      button.title = step.title;
      const num = document.createElement("span");
      num.className = "num";
      num.textContent = String(i + 1);
      const label = document.createElement("span");
      label.className = "label";
      label.textContent = step.short;
      button.append(num, label);
      button.addEventListener("click", () => go(i));
      item.append(button);
      return item;
    }),
  );
}

function start() {
  buildStepper();
  $("ctl-play").addEventListener("click", toggle);
  $("ctl-replay").addEventListener("click", replay);
  $("ctl-back").addEventListener("click", () => go(current - 1));
  $("ctl-next").addEventListener("click", () => go(current + 1));
  $("nav-back").addEventListener("click", () => go(current - 1));
  $("nav-next").addEventListener("click", () => go(current + 1));
  $("ctl-speed").addEventListener("change", (event) => {
    clock.speed = Number(event.currentTarget.value) || 1;
  });
  scrub.addEventListener("input", () => {
    if (clock.playing) {
      clock.playing = false;
      window.cancelAnimationFrame(clock.raf);
      showPlaying(false);
    }
    clock.t = Number(scrub.value) / 1000;
    render();
  });
  document.addEventListener("keydown", (event) => {
    if (event.altKey || event.ctrlKey || event.metaKey) return;
    const tag = event.target.tagName;
    const typing = tag === "INPUT" || tag === "SELECT" || tag === "TEXTAREA";
    if (typing) return;
    if (event.key === "ArrowRight") {
      event.preventDefault();
      go(current + 1);
    } else if (event.key === "ArrowLeft") {
      event.preventDefault();
      go(current - 1);
    } else if (event.key === " " && tag !== "BUTTON" && tag !== "A") {
      event.preventDefault();
      toggle();
    } else if (event.key === "r" || event.key === "R") {
      replay();
    }
  });
  window.addEventListener("hashchange", () => {
    const index = stepFromHash();
    if (index !== current) go(index);
  });
  go(stepFromHash());
}

start();
