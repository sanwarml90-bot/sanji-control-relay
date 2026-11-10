# Sanji Control Relay
WebSocket relay between Sanji's phone (device) and the controller.
- `GET /health` → {"ok":true,"device":bool,"controller":bool}
- Device connects and sends {"type":"hello","role":"device"}.
- Controller connects and sends {"type":"hello","role":"controller"}.
- Messages are forwarded verbatim between the two roles. No storage, no auth.
Run: `npm install && npm start` (PORT env, default 8080).
