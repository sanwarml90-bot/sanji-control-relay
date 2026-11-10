// Sanji Control relay — forwards JSON messages between the phone (device)
// and the controller over WebSocket. No auth, no storage, no logs of content.
// One device + one controller at a time (last one wins per role).
const http = require('http');
const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, {'Content-Type': 'application/json'}); res.end(JSON.stringify({ok: true, device: !!device, controller: !!controller})); return; }
  res.writeHead(200, {'Content-Type': 'text/plain'}); res.end('sanji-control-relay ok');
});
const wss = new WebSocket.Server({ server });
let device = null, controller = null;
function safeSend(ws, data) { try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(data); } catch (e) {} }
wss.on('connection', (ws) => {
  let role = null;
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (msg.type === 'hello' && (msg.role === 'device' || msg.role === 'controller')) {
      role = msg.role;
      if (role === 'device') { device = ws; safeSend(controller, JSON.stringify({type: 'device_online'})); }
      else { controller = ws; safeSend(controller, JSON.stringify({type: 'relay_hello', device_online: !!device})); }
      return;
    }
    // Forward: controller -> device, device -> controller.
    if (role === 'controller') safeSend(device, raw.toString());
    else if (role === 'device') safeSend(controller, raw.toString());
  });
  ws.on('close', () => {
    if (role === 'device' && device === ws) { device = null; safeSend(controller, JSON.stringify({type: 'device_offline'})); }
    if (role === 'controller' && controller === ws) controller = null;
  });
});
server.listen(PORT, () => console.log('relay listening on ' + PORT));
