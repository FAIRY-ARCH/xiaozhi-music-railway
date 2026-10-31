// xiaozhi-music-bridge (event-driven)
// 桥接：小智 MCP 中继 <-> 上游音乐 MCP WebSocket 服务器（双向转发 JSON-RPC 消息）
// 注意：Deno Deploy 运行时 WebSocket 不支持异步迭代（for await），必须用 onmessage/onclose/onerror 事件驱动。
// v3: 本地应答 ping、吞掉 notifications/*，避免上游 -32601 错误应答导致中继关闭会话。
// v4: 修复 /state 状态字段在连接打开时仍显示 opening；新增每分钟保活定时任务（免费层实例空闲约1-3分钟即被回收，中继连接随实例销毁，需定时唤醒保持在线）。
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
  if (logLimit[dir] >= 8) return;
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
    state.relay = "open";
    console.log("relay open");
    if (upOpen) console.log("bridge connected: relay + upstream");
  };
  relayWs.onmessage = (e) => {
    state.msgsRelayToUp += 1;
    maybeLog("r2u", e.data);
    const text = fmt(e.data);
    try {
      const obj = JSON.parse(text);
      const method = obj && obj.method;
      const noId = !obj || typeof obj.id === "undefined" || obj.id === null;
      if (method === "ping") {
        relayWs.send(JSON.stringify({ jsonrpc: "2.0", id: obj.id, result: {} }));
        console.log("[r2u-pong] ping answered locally");
        return;
      }
      if (noId && method && method.startsWith("notifications/")) {
        console.log("[r2u-notif] " + method + " swallowed locally");
        return;
      }
    } catch (_) {}
    if (upWs && upOpen) {
      try { upWs.send(text); } catch (err) { console.error("send to upstream: " + err); }
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
    state.upstream = "open";
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

// 保活：免费层实例空闲约1-3分钟即被回收（中继连接随实例销毁），每分钟定时唤醒保持在线
if (typeof Deno.cron === "function") {
  Deno.cron("keepalive", "*/1 * * * *", () => {
    console.log("[keepalive] tick");
  });
}

console.log("bridge starting, relay host=" + new URL(RELAY_URL).host + " upstream=" + UPSTREAM_URL);
connectRelay();
connectUpstream();
