// Sanji Control relay — forwards JSON messages between phones (devices) and
// controllers over WebSocket. No auth, no storage, no logs of content.
//
// Multi-phone: each phone registers with a permanent UID (its Android ID).
// Devices are kept in a Map keyed by UID. A controller picks a phone by
// sending {"type":"target","uid":"<uid>"} (or by putting "to":"<uid>" on a
// command). Device messages carry "from":"<uid>" so a panel can show only
// the selected phone. /devices lists the connected phones.
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;
const CONTROL_PAGE = fs.readFileSync(path.join(__dirname, 'public', 'control.html'), 'utf8');
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({ok: true, device: devices.size > 0, devices: [...devices.keys()], deviceCount: devices.size, controllers: controllers.size}));
    return;
  }
  if (req.url === '/devices') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({devices: [...devices.keys()].map(deviceInfo),
      banned: [...banned], aliases: [...aliases.entries()].map(([alias, uid]) => ({alias, uid}))}));
    return;
  }
  if (req.url === '/' || req.url === '/control' || req.url === '/control.html') {
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
    res.end(CONTROL_PAGE);
    return;
  }
  res.writeHead(200, {'Content-Type': 'text/plain'}); res.end('sanji-control-relay ok');
});
const wss = new WebSocket.Server({ server, maxPayload: 50 * 1024 * 1024 });
// uid -> {ws, name}
const devices = new Map();
// controllers: ws -> {targetUid}
const controllers = new Map();
// Owner panel: banned device UIDs (rejected on hello) and custom-UID
// aliases (alias -> real UID). The owner sets them from the owner panel;
// the phone itself keeps its Android-ID UID, but the owner can refer to
// it (and control it) by the custom UID. Both live in memory only — a
// relay restart clears them.
const banned = new Set();
const aliases = new Map(); // alias -> realUid
function aliasOf(realUid) {
  for (const [a, u] of aliases) if (u === realUid) return a;
  return null;
}
function resolveUid(uidOrAlias) {
  return aliases.get(uidOrAlias) || uidOrAlias;
}
function deviceInfo(uid) {
  const d = devices.get(uid);
  return {uid, name: (d && d.name) || uid, alias: aliasOf(uid), banned: banned.has(uid)};
}
function safeSend(ws, data) { try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(data); } catch (e) {} }
function notifyControllers(msg) {
  const data = JSON.stringify(msg);
  for (const [cws, c] of controllers) {
    if (!msg.uid || !c.targetUid || c.targetUid === msg.uid) safeSend(cws, data);
  }
}

// ---- Script runner (panel "Scripts" feature) ----
// A script is plain text, one action per line. Lines starting with '#'
// are comments; blank lines are skipped. Supported actions:
//   open_app <package> | tap <x> <y> | swipe <x1> <y1> <x2> <y2> [ms]
//   type <text...> | back | home | recents | wait <ms> | screenshot
// The relay parses the script, sends each step to the phone one at a
// time, waits for that step's reply (matched by the step's id), and
// reports progress to the controller. A failed step stops the script
// (safer than blindly continuing). No app change needed — a script is
// just a sequence of the commands the app already handles.
function parseScript(text) {
  const steps = [];
  const errors = [];
  String(text || '').split('\n').forEach((raw, i) => {
    const line = raw.trim();
    if (!line || line.startsWith('#')) return;
    const m = line.match(/^(\S+)\s*(.*)$/);
    const action = (m[1] || '').toLowerCase();
    const rest = (m[2] || '').trim();
    const parts = rest.split(/\s+/).filter(Boolean);
    const num = (v) => { const x = parseInt(v, 10); return Number.isFinite(x) ? x : null; };
    const push = (s) => steps.push(Object.assign({line: i + 1, action}, s));
    if (action === 'open_app') {
      if (!parts[0]) errors.push('line ' + (i + 1) + ': open_app needs a package');
      else push({package: parts[0]});
    } else if (action === 'tap') {
      const x = num(parts[0]), y = num(parts[1]);
      if (x === null || y === null) errors.push('line ' + (i + 1) + ': tap needs x y numbers');
      else push({x, y});
    } else if (action === 'swipe') {
      const x1 = num(parts[0]), y1 = num(parts[1]), x2 = num(parts[2]), y2 = num(parts[3]);
      const ms = parts[4] ? num(parts[4]) : 300;
      if (x1 === null || y1 === null || x2 === null || y2 === null) errors.push('line ' + (i + 1) + ': swipe needs x1 y1 x2 y2 numbers');
      else push({x1, y1, x2, y2, durationMs: ms || 300});
    } else if (action === 'type') {
      if (!rest) errors.push('line ' + (i + 1) + ': type needs text');
      else push({text: rest});
    } else if (action === 'back' || action === 'home' || action === 'recents' || action === 'screenshot') {
      push({});
    } else if (action === 'wait') {
      const ms = num(parts[0]);
      if (ms === null || ms < 0) errors.push('line ' + (i + 1) + ': wait needs milliseconds');
      else push({ms});
    } else {
      errors.push('line ' + (i + 1) + ': unknown action "' + parts[0] + '"');
    }
  });
  return {steps, errors};
}
// Pending waits for script steps, keyed by the step's command id:
// id -> {resolve, timer}. A device 'result' or 'screenshot' carrying
// that id resolves it. Live-stream screenshot frames carry no id, so
// they never resolve a wait.
const scriptWaits = new Map();
const scriptRuns = new Map(); // runId -> {stop}
function resolveScriptWait(msg) {
  if (!msg || !msg.id) return;
  const w = scriptWaits.get(msg.id);
  if (!w) return;
  clearTimeout(w.timer);
  scriptWaits.delete(msg.id);
  if (msg.type === 'screenshot') w.resolve({ok: true, info: 'screenshot ok'});
  else w.resolve({ok: !!msg.ok, info: msg.info || ''});
}
function waitForStep(id, timeoutMs) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      scriptWaits.delete(id);
      resolve({ok: false, info: 'timeout — phone did not reply in ' + (timeoutMs / 1000) + 's'});
    }, timeoutMs);
    scriptWaits.set(id, {resolve, timer});
  });
}
async function runScript(controllerWs, toUid, scriptText, runId) {
  const send = (obj) => safeSend(controllerWs, JSON.stringify(obj));
  const {steps, errors} = parseScript(scriptText);
  if (errors.length) { send({type: 'script_done', runId, ok: false, error: errors.join('; ')}); return; }
  if (!steps.length) { send({type: 'script_done', runId, ok: false, error: 'script is empty'}); return; }
  const run = {stop: false};
  scriptRuns.set(runId, run);
  for (let i = 0; i < steps.length; i++) {
    if (run.stop) { send({type: 'script_done', runId, ok: false, error: 'stopped by user', step: i, total: steps.length}); scriptRuns.delete(runId); return; }
    const st = steps[i];
    const dev = devices.get(toUid);
    if (!dev || dev.ws.readyState !== WebSocket.OPEN) {
      send({type: 'script_progress', runId, step: i, total: steps.length, action: st.action, ok: false, info: 'phone not connected'});
      send({type: 'script_done', runId, ok: false, error: 'phone disconnected at step ' + (i + 1), step: i, total: steps.length});
      scriptRuns.delete(runId); return;
    }
    if (st.action === 'wait') {
      await new Promise((r) => setTimeout(r, st.ms));
      send({type: 'script_progress', runId, step: i, total: steps.length, action: 'wait', ok: true, info: 'waited ' + st.ms + 'ms'});
      continue;
    }
    const stepId = 'script-' + runId + '-' + i;
    const cmd = {type: 'cmd', id: stepId, to: toUid, action: st.action};
    if (st.action === 'open_app') cmd.package = st.package;
    if (st.action === 'tap') { cmd.x = st.x; cmd.y = st.y; }
    if (st.action === 'swipe') { cmd.x1 = st.x1; cmd.y1 = st.y1; cmd.x2 = st.x2; cmd.y2 = st.y2; cmd.durationMs = st.durationMs; }
    if (st.action === 'type') cmd.text = st.text;
    safeSend(dev.ws, JSON.stringify(cmd));
    const res = await waitForStep(stepId, 20000);
    send({type: 'script_progress', runId, step: i, total: steps.length, action: st.action, ok: res.ok, info: res.info || ''});
    if (!res.ok) {
      send({type: 'script_done', runId, ok: false, error: 'step ' + (i + 1) + ' (' + st.action + ') failed: ' + (res.info || ''), step: i, total: steps.length});
      scriptRuns.delete(runId); return;
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  send({type: 'script_done', runId, ok: true, step: steps.length, total: steps.length});
  scriptRuns.delete(runId);
}
wss.on('connection', (ws) => {
  let role = null, uid = null;
  // A bad or oversized message must never kill the relay: swallow the
  // socket error (the socket closes) instead of crashing the process.
  // (This was the crash: RangeError 'Max payload size exceeded',
  // WS_ERR_UNSUPPORTED_MESSAGE_LENGTH, unhandled 'error' -> Node.js exit.)
  ws.on('error', () => { try { ws.close(); } catch (e) {} });
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (msg.type === 'hello' && (msg.role === 'device' || msg.role === 'controller')) {
      role = msg.role;
      if (role === 'device') {
        uid = String(msg.uid || msg.device || 'unknown');
        // Banned phones are rejected: no device entry, socket closed.
        if (banned.has(uid)) {
          safeSend(ws, JSON.stringify({type: 'result', id: '', ok: false, info: 'banned'}));
          try { ws.close(); } catch (e) {}
          return;
        }
        devices.set(uid, {ws, name: msg.device || uid});
        notifyControllers({type: 'device_online', uid, alias: aliasOf(uid)});
      } else {
        controllers.set(ws, {targetUid: null});
        safeSend(ws, JSON.stringify({type: 'relay_hello', devices: [...devices.keys()]}));
      }
      return;
    }
    if (role === 'controller' && msg.type === 'target') {
      const c = controllers.get(ws); if (c) c.targetUid = String(msg.uid || '');
      safeSend(ws, JSON.stringify({type: 'target_ok', uid: c ? c.targetUid : ''}));
      return;
    }
    if (role === 'controller' && (msg.type === 'ban' || msg.type === 'unban' || msg.type === 'set_alias' || msg.type === 'remove_alias' || msg.type === 'list_bans' || msg.type === 'list_aliases')) {
      // Owner-panel commands. (The panel only shows them in master
      // mode; the relay itself has no login, so any controller that
      // knows these message types can use them — same trust model as
      // the rest of this relay.)
      if (msg.type === 'ban') {
        const u = String(msg.uid || '');
        if (u) {
          banned.add(u);
          const dev = devices.get(u);
          if (dev) { try { dev.ws.close(); } catch (e) {} devices.delete(u); }
          notifyControllers({type: 'device_offline', uid: u});
          notifyControllers({type: 'banned', uid: u});
          safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: true, info: 'banned ' + u}));
        }
      } else if (msg.type === 'unban') {
        const u = String(msg.uid || '');
        banned.delete(u);
        notifyControllers({type: 'unbanned', uid: u});
        safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: true, info: 'unbanned ' + u}));
      } else if (msg.type === 'set_alias') {
        const u = String(msg.uid || ''), a = String(msg.alias || '').trim();
        if (u && a && !devices.has(a) && !aliases.has(a)) {
          // One alias per phone: drop any old alias for this phone.
          const old = aliasOf(u); if (old) aliases.delete(old);
          aliases.set(a, u);
          notifyControllers({type: 'alias_set', uid: u, alias: a});
          safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: true, info: 'UID changed: ' + u + ' is now ' + a}));
        } else {
          safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: false, info: 'alias not set (empty, already used, or same as a real UID)'}));
        }
      } else if (msg.type === 'remove_alias') {
        const a = String(msg.alias || '').trim();
        const u = aliases.get(a);
        if (u) { aliases.delete(a); notifyControllers({type: 'alias_removed', uid: u, alias: a}); }
        safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: true, info: u ? ('alias removed: ' + a) : 'no such alias'}));
      } else if (msg.type === 'list_bans') {
        safeSend(ws, JSON.stringify({type: 'bans', bans: [...banned]}));
      } else if (msg.type === 'list_aliases') {
        safeSend(ws, JSON.stringify({type: 'aliases', aliases: [...aliases.entries()].map(([alias, uid]) => ({alias, uid}))}));
      }
      return;
    }
    if (role === 'controller' && (msg.type === 'run_script' || msg.type === 'stop_script')) {
      // Script runner: run a multi-step script on the chosen phone.
      if (msg.type === 'stop_script') {
        const run = scriptRuns.get(String(msg.runId || ''));
        if (run) run.stop = true;
        safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: true, info: run ? 'script stop requested' : 'no such script running'}));
        return;
      }
      const toRaw = String(msg.to || (controllers.get(ws) && controllers.get(ws).targetUid) || '');
      const to = resolveUid(toRaw);
      const runId = String(msg.runId || ('r' + Date.now().toString(36)));
      if (!devices.get(to)) {
        safeSend(ws, JSON.stringify({type: 'script_done', runId, ok: false, error: 'phone not connected: ' + (to || '(no phone selected)')}));
        return;
      }
      safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: true, info: 'script started: ' + runId}));
      runScript(ws, to, String(msg.script || ''), runId);
      return;
    }
    if (role === 'controller') {
      // Forward a command to the chosen phone: msg.to wins, else the
      // controller's current target. A custom UID (alias) resolves to
      // the phone's real UID first.
      const c = controllers.get(ws);
      const toRaw = String(msg.to || (c && c.targetUid) || '');
      const to = resolveUid(toRaw);
      const dev = devices.get(to);
      // v7 relay fix: a device entry whose socket is no longer OPEN is
      // STALE (phone's network dropped without a close event — the old
      // code silently swallowed the send and the panel waited forever
      // with no feedback, looking like "the app doesn't work"). Drop the
      // stale entry, tell the controllers the phone is off, and reply so
      // the panel shows the real state instead of hanging.
      if (dev && dev.ws.readyState === WebSocket.OPEN) {
        safeSend(dev.ws, raw.toString());
      } else {
        if (dev) { devices.delete(to); notifyControllers({type: 'device_offline', uid: to}); }
        safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: false, info: 'phone not connected: ' + (to || '(no phone selected)')}));}
      return;
    }
    if (role === 'device') {
      // A reply carrying a script step id resolves that step's wait
      // (the reply is still relayed to controllers below, so the panel
      // log and the screenshot view keep working as before).
      resolveScriptWait(msg);
      // Relay a device message to controllers watching this phone.
      msg.from = uid;
      const a = aliasOf(uid); if (a) msg.alias = a;
      notifyControllers(msg);
      return;
    }
  });
  ws.on('close', () => {
    if (role === 'device' && uid && devices.get(uid) && devices.get(uid).ws === ws) {
      devices.delete(uid);
      notifyControllers({type: 'device_offline', uid});
    }
    if (role === 'controller') controllers.delete(ws);
  });
});
wss.on('error', (e) => console.error('ws server error:', e && e.message));
server.on('error', (e) => console.error('http server error:', e && e.message));
server.listen(PORT, () => console.log('relay listening on ' + PORT));
