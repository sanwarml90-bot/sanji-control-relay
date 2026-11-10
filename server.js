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
    res.end(JSON.stringify({devices: [...devices.entries()].map(([uid, info]) => ({uid, name: info.name || uid}))}));
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
        devices.set(uid, {ws, name: msg.device || uid});
        notifyControllers({type: 'device_online', uid});
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
    if (role === 'controller') {
      // Forward a command to the chosen phone: msg.to wins, else the
      // controller's current target.
      const c = controllers.get(ws);
      const to = String(msg.to || (c && c.targetUid) || '');
      const dev = devices.get(to);
      if (dev) safeSend(dev.ws, raw.toString());
      else safeSend(ws, JSON.stringify({type: 'result', id: msg.id || '', ok: false, info: 'phone not connected: ' + (to || '(no phone selected)')}));
      return;
    }
    if (role === 'device') {
      // Relay a device message to controllers watching this phone.
      msg.from = uid;
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
