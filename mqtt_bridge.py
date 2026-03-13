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

The MQTT on_message callback never blocks — commands are queued and executed
by a dedicated worker thread so paho's network loop stays responsive.
"""

import json
import logging
import os
import queue
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
        self._q: queue.Queue = queue.Queue()

    def start(self):
        """Start MQTT client and worker threads. Non-blocking."""
        threading.Thread(target=self._run,    name="mqtt-bridge",  daemon=True).start()
        threading.Thread(target=self._worker, name="mqtt-worker",  daemon=True).start()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self):
        client_id = f"cnc-mcp-bridge-{os.getpid()}"
        client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        client.on_connect    = self._on_connect
        client.on_message    = self._on_message
        client.on_disconnect = self._on_disconnect
        self._client = client

        while True:
            try:
                log.info(f"Connecting to MQTT broker {self._broker}:{self._port} as {client_id}")
                client.connect(self._broker, self._port, keepalive=60)
                client.loop_forever()
                # loop_forever() returns normally on disconnect (e.g. session taken over).
                # Always sleep before reconnecting to avoid hammering the broker.
                log.warning("MQTT loop_forever() returned — reconnecting in 5s")
                time.sleep(5)
            except Exception as e:
                log.warning(f"MQTT connection failed: {e} — retrying in 5s")
                time.sleep(5)

    def _worker(self):
        """Execute GRBL commands from the queue — never called from paho thread."""
        while True:
            suffix, data = self._q.get()
            try:
                if suffix == "cnc/jog":
                    # Coalesce: drain any additional jog messages for the same axis
                    # already in the queue so we send one combined move, not a replay.
                    try:
                        first = json.loads(data)
                        axis  = str(first["axis"]).upper()
                        dist  = float(first["dist"])
                        feed  = float(first.get("feed", 500))
                    except (KeyError, ValueError, json.JSONDecodeError) as e:
                        log.warning(f"Bad jog payload: {data!r} ({e})")
                        continue
                    while True:
                        try:
                            s2, d2 = self._q.get_nowait()
                        except queue.Empty:
                            break
                        if s2 != "cnc/jog":
                            self._q.put((s2, d2))  # put non-jog back
                            break
                        try:
                            extra = json.loads(d2)
                            if str(extra["axis"]).upper() == axis:
                                dist += float(extra["dist"])
                            else:
                                self._q.put((s2, d2))  # different axis — keep
                                break
                        except Exception:
                            break
                    self._handle_jog_direct(axis, dist, feed)
                elif suffix == "cnc/home":
                    self._handle_home()
                elif suffix == "cnc/zero":
                    self._handle_zero()
                elif suffix == "cnc/probe":
                    self._handle_probe()
            except Exception as e:
                log.error(f"Error handling {suffix}: {e}")

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
        """Returns immediately — enqueues work for the worker thread."""
        topic   = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace").strip()
        suffix  = topic.removeprefix(self._prefix + "/")
        log.debug(f"MQTT rx {topic}: {payload!r}")
        self._q.put((suffix, payload))

    # ------------------------------------------------------------------
    # Handlers (run in worker thread, blocking is fine here)
    # ------------------------------------------------------------------

    def _machine_state(self) -> str:
        return self._grbl.cached_state()

    def _handle_jog_direct(self, axis: str, dist: float, feed: float):
        state = self._machine_state()
        if state == "Alarm":
            log.info("Alarm state — auto-issuing $X before jog")
            self._grbl.unlock()
        elif state not in ("Idle", "Jog"):
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
