// Sanji Control relay — forwards JSON messages between the phone (device)
// and any number of controllers (this agent, the web control panel) over
// WebSocket. No auth, no storage, no logs of content.
// One device at a time (last one wins); controllers are a set, and device
// messages are broadcast to all of them.
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;
const CONTROL_PAGE = fs.readFileSync(path.join(__dirname, 'public', 'control.html'), 'utf8');
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, {'Content-Type': 'application/json'}); res.end(JSON.stringify({ok: true, device: !!device, controllers: controllers.size})); return; }
  if (req.url === '/' || req.url === '/control' || req.url === '/control.html') {
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
    res.end(CONTROL_PAGE);
    return;
  }
  res.writeHead(200, {'Content-Type': 'text/plain'}); res.end('sanji-control-relay ok');
});
const wss = new WebSocket.Server({ server });
let device = null;
const controllers = new Set();
function safeSend(ws, data) { try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(data); } catch (e) {} }
function broadcastControllers(data) { for (const c of controllers) safeSend(c, data); }
wss.on('connection', (ws) => {
  let role = null;
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (msg.type === 'hello' && (msg.role === 'device' || msg.role === 'controller')) {
      role = msg.role;
      if (role === 'device') { device = ws; broadcastControllers(JSON.stringify({type: 'device_online'})); }
      else { controllers.add(ws); safeSend(ws, JSON.stringify({type: 'relay_hello', device_online: !!device})); }
      return;
    }
    // Forward: controller -> device, device -> all controllers.
    if (role === 'controller') safeSend(device, raw.toString());
    else if (role === 'device') broadcastControllers(raw.toString());
  });
  ws.on('close', () => {
    if (role === 'device' && device === ws) { device = null; broadcastControllers(JSON.stringify({type: 'device_offline'})); }
    if (role === 'controller') controllers.delete(ws);
  });
});
server.listen(PORT, () => console.log('relay listening on ' + PORT));
