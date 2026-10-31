// xiaozhi-music-bridge
// 桥接：小智 MCP 中继 <-> 上游音乐 MCP WebSocket 服务器（双向转发 JSON-RPC 消息）
const RELAY_URL = Deno.env.get("RELAY_URL");
const UPSTREAM_URL = Deno.env.get("UPSTREAM_URL") ?? "wss://xiaozhi-music-railway.fairy-arch.deno.net";
if (!RELAY_URL) {
  console.error("RELAY_URL not set");
  Deno.exit(1);
}

// 诊断状态
const state = {
  relay: "not-started",
  upstream: "not-started",
  lastError: "",
  attempts: 0,
  lastConnect: 0,
  msgsRelayToUp: 0,
  msgsUpToRelay: 0,
  probeHttp: "",
  probeAt: 0,
};

function open(ws, name, timeoutMs) {
  return new Promise((resolve, reject) => {
    state[name] = "opening";
    const timer = setTimeout(() => {
      state[name] = "timeout";
      state.lastError = name + " open timeout";
      try { ws.close(); } catch {}
      reject(new Error(name + " open timeout"));
    }, timeoutMs);
    ws.onopen = () => { clearTimeout(timer); state[name] = "open"; resolve(); };
    ws.onerror = (e) => { clearTimeout(timer); state[name] = "error"; state.lastError = name + " open failed"; reject(new Error(name + " open failed")); };
  });
}

async function pipe(src, dst, name) {
  for await (const msg of src) {
    if (typeof msg === "string") {
      dst.send(msg);
    } else {
      dst.send(await new Response(msg).text());
    }
    if (name === "relay") state.msgsRelayToUp++; else state.msgsUpToRelay++;
  }
}

// 定时探测中继主机可达性（区分“主机不可达/被墙”与“仅WS握手受阻”）
async function probeRelay() {
  try {
    const t = Date.now();
    const resp = await fetch("https://api.xiaozhi.me/", { signal: AbortSignal.timeout(8000) });
    state.probeHttp = "status=" + resp.status + " took=" + (Date.now() - t) + "ms";
  } catch (e) {
    state.probeHttp = "FAIL " + String(e && e.message ? e.message : e);
  }
  state.probeAt = Date.now();
}

async function runBridge() {
  // 立即探活一次，之后每 30s 一次
  probeRelay();
  setInterval(probeRelay, 30000);
  for (;;) {
    let relay;
    let up;
    try {
      state.attempts++;
      state.lastConnect = Date.now();
      relay = new WebSocket(RELAY_URL);
      up = new WebSocket(UPSTREAM_URL);
      await Promise.all([open(relay, "relay", 12000), open(up, "upstream", 12000)]);
      console.log("bridge connected: relay + upstream");
      await Promise.race([pipe(relay, up, "relay"), pipe(up, relay, "upstream")]);
    } catch (err) {
      state.lastError = String(err && err.message ? err.message : err);
      console.error("bridge error:", err);
    } finally {
      try { relay?.close(); } catch {}
      try { up?.close(); } catch {}
      state.relay = "closed";
      state.upstream = "closed";
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
}

Deno.serve((req) => {
  const url = new URL(req.url);
  if (url.pathname === "/health") return new Response("ok");
  if (url.pathname === "/state") return Response.json(state);
  return new Response("xiaozhi-music-bridge running", { status: 200 });
});
runBridge();
