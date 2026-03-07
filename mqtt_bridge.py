"""
MQTT bridge for CNC pendant.

Subscribes to pendant command topics and translates them to GRBL operations,
sharing the same GRBLController instance as the MCP server.

Topics consumed (prefix = /cova by default):
  {prefix}/cnc/jog    {"axis":"X","dist":0.10,"feed":500}
  {prefix}/cnc/home   (any payload)
  {prefix}/cnc/zero   (any payload)
  {prefix}/cnc/probe  (any payload)

Safety: all commands are silently dropped if the machine is not in Idle state,
except home ($H) which is allowed from Alarm state.
"""

import json
import logging
import threading
import time

import paho.mqtt.client as mqtt

from grbl import GRBLController
import journal as _journal_mod

log = logging.getLogger("mqtt_bridge")


class MQTTBridge:
    def __init__(self, grbl: GRBLController, broker: str,
                 port: int = 1883, prefix: str = "/cova",
                 puck_mm: float = 1.5):
        self._grbl   = grbl
        self._broker = broker
        self._port   = port
        self._prefix = prefix.rstrip("/")
        self._puck_mm = puck_mm
        self._client  = None
        self._thread  = None

    def start(self):
        """Start the MQTT client in a background thread. Non-blocking."""
        self._thread = threading.Thread(
            target=self._run, name="mqtt-bridge", daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self):
        client = mqtt.Client(client_id="cnc-mcp-bridge", protocol=mqtt.MQTTv311)
        client.on_connect    = self._on_connect
        client.on_message    = self._on_message
        client.on_disconnect = self._on_disconnect
        self._client = client

        while True:
            try:
                log.info(f"Connecting to MQTT broker {self._broker}:{self._port}")
                client.connect(self._broker, self._port, keepalive=60)
                client.loop_forever()
            except Exception as e:
                log.warning(f"MQTT connection failed: {e} — retrying in 5s")
                time.sleep(5)

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.warning(f"MQTT connect refused (rc={rc})")
            return
        log.info(f"MQTT bridge connected to {self._broker}")
        p = self._prefix
        for suffix in ("cnc/jog", "cnc/home", "cnc/zero", "cnc/probe"):
            topic = f"{p}/{suffix}"
            client.subscribe(topic)
            log.debug(f"Subscribed: {topic}")

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log.warning(f"MQTT bridge disconnected (rc={rc})")

    def _on_message(self, client, userdata, msg):
        topic   = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace").strip()
        suffix  = topic.removeprefix(self._prefix + "/")

        log.debug(f"MQTT rx {topic}: {payload!r}")

        try:
            if suffix == "cnc/jog":
                self._handle_jog(payload)
            elif suffix == "cnc/home":
                self._handle_home()
            elif suffix == "cnc/zero":
                self._handle_zero()
            elif suffix == "cnc/probe":
                self._handle_probe()
        except Exception as e:
            log.error(f"Error handling {suffix}: {e}")

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _machine_state(self) -> str:
        status = self._grbl.get_status()
        return status.get("state", "Disconnected")

    def _handle_jog(self, payload: str):
        try:
            data = json.loads(payload)
            axis = str(data["axis"]).upper()
            dist = float(data["dist"])
            feed = float(data.get("feed", 500))
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            log.warning(f"Bad jog payload: {payload!r} ({e})")
            return

        state = self._machine_state()
        if state not in ("Idle", "Jog"):
            log.debug(f"Dropping jog — machine state={state}")
            return

        result = self._grbl.jog(axis, dist, feed)
        if not result.get("ok"):
            log.warning(f"Jog failed: {result}")

    def _handle_home(self):
        # Allow home from Idle or Alarm (homing clears alarm)
        state = self._machine_state()
        if state not in ("Idle", "Alarm"):
            log.debug(f"Dropping home — machine state={state}")
            return
        result = self._grbl.home()
        if not result.get("ok"):
            log.warning(f"Home failed: {result}")

    def _handle_zero(self):
        state = self._machine_state()
        if state != "Idle":
            log.debug(f"Dropping zero — machine state={state}")
            return
        result = self._grbl.set_zero()
        if not result.get("ok"):
            log.warning(f"Zero failed: {result}")

    def _handle_probe(self):
        state = self._machine_state()
        if state != "Idle":
            log.debug(f"Dropping probe — machine state={state}")
            return
        result = self._grbl.probe_z(puck_mm=self._puck_mm)
        if not result.get("ok"):
            log.warning(f"Probe failed: {result}")
        else:
            log.info(f"Probe complete: {result['message']}")
            j = _journal_mod.get()
            if j:
                j.log_event("probe", {"puck_mm": self._puck_mm,
                                      "result": result.get("message")})


def start(grbl: GRBLController, broker: str, port: int = 1883,
          prefix: str = "/cova", puck_mm: float = 1.5) -> MQTTBridge:
    """Convenience: create, start, and return the bridge."""
    bridge = MQTTBridge(grbl, broker, port=port, prefix=prefix, puck_mm=puck_mm)
    bridge.start()
    return bridge
