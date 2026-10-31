// xiaozhi-music-bridge (event-driven)
// 桥接：小智 MCP 中继 <-> 上游音乐 MCP WebSocket 服务器（双向转发 JSON-RPC 消息）
// 注意：Deno Deploy 运行时 WebSocket 不支持异步迭代（for await），必须用 onmessage/onclose/onerror 事件驱动。
const RELAY_URL = Deno.env.get("RELAY_URL");
const UPSTREAM_URL = Deno.env.get("UPSTREAM_URL") ?? "wss://xiaozhi-music-railway.fairy-arch.deno.net";
if (!RELAY_URL) {
  console.error("RELAY_URL not set");
  Deno.exit(1);
}

const state = {
  relay: "not-started",
  upstream: "not-started",
  lastError: "",
  attempts: 0,
  lastConnect: 0,
  msgsRelayToUp: 0,
  msgsUpToRelay: 0,
  bootAt: Date.now(),
};

let relayWs = null;
let upWs = null;
let relayOpen = false;
let upOpen = false;
const logLimit = { r2u: 0, u2r: 0 };

function fmt(data) {
  return typeof data === "string" ? data : new TextDecoder().decode(data);
}

function maybeLog(dir, data) {
  if (logLimit[dir] >= 6) return;
  logLimit[dir] += 1;
  let t = fmt(data);
  if (t.length > 500) t = t.slice(0, 500) + "...";
  console.log("[" + dir + "] " + t);
}

function connectRelay() {
  state.relay = "opening";
  state.attempts += 1;
  state.lastConnect = Date.now();
  try {
    relayWs = new WebSocket(RELAY_URL);
  } catch (err) {
    state.relay = "error";
    state.lastError = "relay new WS: " + err;
    console.error("relay create error: " + err);
    setTimeout(connectRelay, 3000);
    return;
  }
  relayWs.onopen = () => {
    relayOpen = true;
    console.log("relay open");
    if (upOpen) console.log("bridge connected: relay + upstream");
  };
  relayWs.onmessage = (e) => {
    state.msgsRelayToUp += 1;
    maybeLog("r2u", e.data);
    if (upWs && upOpen) {
      try { upWs.send(e.data); } catch (err) { console.error("send to upstream: " + err); }
    } else {
      console.error("upstream not open, dropping relay msg");
    }
  };
  relayWs.onclose = (e) => {
    relayOpen = false;
    state.relay = "closed";
    console.error("relay closed code=" + e.code + " reason=" + e.reason);
    setTimeout(connectRelay, 3000);
  };
  relayWs.onerror = (e) => {
    state.relay = "error";
    state.lastError = "relay ws error";
    console.error("relay ws error");
  };
}

function connectUpstream() {
  state.upstream = "opening";
  try {
    upWs = new WebSocket(UPSTREAM_URL);
  } catch (err) {
    state.upstream = "error";
    state.lastError = "upstream new WS: " + err;
    console.error("upstream create error: " + err);
    setTimeout(connectUpstream, 3000);
    return;
  }
  upWs.onopen = () => {
    upOpen = true;
    console.log("upstream open");
    if (relayOpen) console.log("bridge connected: relay + upstream");
  };
  upWs.onmessage = (e) => {
    state.msgsUpToRelay += 1;
    maybeLog("u2r", e.data);
    if (relayWs && relayOpen) {
      try { relayWs.send(e.data); } catch (err) { console.error("send to relay: " + err); }
    } else {
      console.error("relay not open, dropping upstream msg");
    }
  };
  upWs.onclose = (e) => {
    upOpen = false;
    state.upstream = "closed";
    console.error("upstream closed code=" + e.code + " reason=" + e.reason);
    setTimeout(connectUpstream, 3000);
  };
  upWs.onerror = (e) => {
    state.upstream = "error";
    state.lastError = "upstream ws error";
    console.error("upstream ws error");
  };
}

Deno.serve((req) => {
  const url = new URL(req.url);
  if (url.pathname === "/health") {
    return new Response("ok", { headers: { "content-type": "text/plain" } });
  }
  if (url.pathname === "/state") {
    return Response.json(state);
  }
  return new Response("not found", { status: 404 });
});

console.log("bridge starting, relay host=" + new URL(RELAY_URL).host + " upstream=" + UPSTREAM_URL);
connectRelay();
connectUpstream();
