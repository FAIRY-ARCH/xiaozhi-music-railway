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

// 事件驱动的双向转发（Deno Deploy 运行时 WebSocket 不支持 for await 异步迭代器）
function pipe(src, dst, name) {
  return new Promise((resolve) => {
    src.onmessage = (e) => {
      const data = e.data;
      try {
        if (typeof data === "string") {
          dst.send(data);
        } else if (data instanceof Blob) {
          data.arrayBuffer().then((b) => dst.send(b));
        } else {
          dst.send(data);
        }
        if (name === "relay") state.msgsRelayToUp++; else state.msgsUpToRelay++;
      } catch (err) {
        state.lastError = "pipe send err: " + String(err && err.message ? err.message : err);
      }
    };
    src.onclose = () => resolve();
    src.onerror = () => resolve();
  });
}

async function runBridge() {
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
