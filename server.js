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
