"""
GRBL serial protocol handler.

Design:
- Dedicated reader thread parses all incoming serial data
- Status responses (<...>) update cached MachineStatus
- ok/error responses go into a queue consumed by the streaming thread
- Single write lock prevents concurrent writes from multiple threads
- run_file() streams in a background thread; use get_status() to poll progress
"""

import os
import queue
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import serial
import serial.tools.list_ports


# ---------------------------------------------------------------------------
# G-code safety limits
# ---------------------------------------------------------------------------

# Soft travel limits for the Genmitsu 4030 ProVerXL2 (mm).
# These are sanity bounds — GRBL's own soft limits are the real enforcement,
# but catching obviously wrong values before they hit the machine is safer.
SOFT_LIMITS = {
    "X": (-1.0, 410.0),
    "Y": (-1.0, 310.0),
    "Z": (-1.0, 120.0),
}

# Commands that are always allowed (GRBL system, status, override)
_SAFE_COMMANDS = re.compile(
    r"^(\$[A-Za-z$#=0-9]|[?!~]|\x18)"  # $H, $$, $X, $#, $N=, ?, !, ~, ctrl-X
)

# G-code words that set axis positions — used to check travel limits
_AXIS_WORD = re.compile(r"([XYZ])\s*(-?[\d.]+)", re.IGNORECASE)

# Dangerous commands that should be blocked or warned about
_BLOCKED_PATTERNS = [
    (re.compile(r"\$RST=\*", re.IGNORECASE), "Factory reset ($RST=*) blocked"),
    (re.compile(r"\$RST=#", re.IGNORECASE), "Coordinate reset ($RST=#) blocked"),
]


def validate_gcode(command: str) -> Optional[str]:
    """Validate a G-code command. Returns error string if invalid, None if OK."""
    cmd = command.strip().upper()
    if not cmd:
        return "Empty command"

    # GRBL system commands are always OK (except blocked ones)
    for pattern, msg in _BLOCKED_PATTERNS:
        if pattern.search(cmd):
            return msg

    if _SAFE_COMMANDS.match(cmd):
        return None

    # Check axis values against soft limits
    for match in _AXIS_WORD.finditer(cmd):
        axis = match.group(1).upper()
        try:
            value = float(match.group(2))
        except ValueError:
            continue
        if axis in SOFT_LIMITS:
            lo, hi = SOFT_LIMITS[axis]
            # Allow relative moves (G91) to exceed — GRBL handles those.
            # For absolute coordinates, flag obviously out-of-range values.
            # We use a generous 2x multiplier to avoid false positives on
            # relative moves (we can't always tell G90 vs G91 from one line).
            generous_lo, generous_hi = lo - hi, hi * 2
            if value < generous_lo or value > generous_hi:
                return f"Axis {axis} value {value} far outside machine limits ({lo} to {hi})"

    return None


def validate_serial_port(port: str) -> Optional[str]:
    """Validate that a serial port path looks legitimate. Returns error or None."""
    # Must be a device path, not arbitrary file
    if not port.startswith("/dev/") and not port.startswith("COM"):
        return f"Invalid port: {port} (must be /dev/... or COM...)"
    # Block obvious non-serial paths
    if ".." in port:
        return f"Invalid port path: {port}"
    # Check it's actually a character device (Unix) or exists (Windows)
    if port.startswith("/dev/") and os.path.exists(port):
        if not os.stat(port).st_mode & 0o020000:  # S_IFCHR
            # Not a character device — could be a regular file
            import stat
            if not stat.S_ISCHR(os.stat(port).st_mode):
                return f"Not a character device: {port}"
    return None


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MachineStatus:
    state: str = "Disconnected"
    mpos: dict = field(default_factory=lambda: {"x": 0.0, "y": 0.0, "z": 0.0})
    wpos: dict = field(default_factory=lambda: {"x": 0.0, "y": 0.0, "z": 0.0})
    feed: float = 0.0
    spindle: float = 0.0


@dataclass
class JobStatus:
    running: bool = False
    filepath: str = ""
    total_lines: int = 0
    sent_lines: int = 0
    complete: bool = False
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------------

class GRBLController:
    BAUD_RATE = 115200
    RX_BUFFER_SIZE = 127   # GRBL's RX buffer (bytes)
    STATUS_INTERVAL = 0.2  # seconds between status polls

    def __init__(self):
        self._serial: Optional[serial.Serial] = None
        self._write_lock = threading.Lock()       # serialises writes
        self._status = MachineStatus()
        self._status_lock = threading.Lock()
        self._response_q: queue.Queue = queue.Queue()

        self._connected = False
        self._port: Optional[str] = None

        self._stop_event = threading.Event()
        self._job_stop = threading.Event()
        self._job = JobStatus()

        self._reader_thread: Optional[threading.Thread] = None
        self._poller_thread: Optional[threading.Thread] = None
        self._stream_thread: Optional[threading.Thread] = None
        self._job_complete_cb = None   # callable(total, sent, success, error)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_job_complete_callback(self, cb):
        """Register a callback fired when the current streaming job finishes.
        Signature: cb(total_lines, sent_lines, success: bool, error: str|None)
        One-shot: cleared after each job completes.
        """
        self._job_complete_cb = cb

    def list_ports(self) -> list[str]:
        return [p.device for p in serial.tools.list_ports.comports()]

    def connect(self, port: str) -> dict:
        if self._connected:
            self.disconnect()

        port_err = validate_serial_port(port)
        if port_err:
            return {"ok": False, "error": port_err}

        try:
            ser = serial.Serial(port, self.BAUD_RATE, timeout=1)
            time.sleep(2)          # GRBL resets on DTR; wait for startup
            ser.reset_input_buffer()

            # Collect startup banner (e.g. "Grbl 1.1h ['$' for help]")
            startup_lines = []
            deadline = time.time() + 3
            while time.time() < deadline:
                if ser.in_waiting:
                    line = ser.readline().decode("utf-8", errors="replace").strip()
                    if line:
                        startup_lines.append(line)
                    if any("Grbl" in l for l in startup_lines):
                        break
                time.sleep(0.05)

            self._serial = ser
            self._connected = True
            self._port = port

            self._stop_event.clear()
            self._reader_thread = threading.Thread(
                target=self._reader, name="grbl-reader", daemon=True)
            self._poller_thread = threading.Thread(
                target=self._poller, name="grbl-poller", daemon=True)
            self._reader_thread.start()
            self._poller_thread.start()

            return {"ok": True, "port": port,
                    "startup": " | ".join(startup_lines)}

        except Exception as e:
            return {"ok": False, "error": str(e)}

    def disconnect(self) -> dict:
        self._stop_event.set()
        self._job_stop.set()

        for t in (self._reader_thread, self._poller_thread, self._stream_thread):
            if t and t.is_alive():
                t.join(timeout=2)

        with self._write_lock:
            if self._serial:
                try:
                    self._serial.close()
                except Exception:
                    pass
                self._serial = None

        self._connected = False
        self._port = None
        self._status = MachineStatus()
        self._job = JobStatus()
        return {"ok": True}

    def cached_state(self) -> str:
        """Return machine state from cache — no serial I/O, safe to call from callbacks."""
        if not self._connected:
            return "Disconnected"
        with self._status_lock:
            return self._status.state

    def get_status(self) -> dict:
        if not self._connected:
            return {"connected": False, "state": "Disconnected"}

        # Request a fresh status; reader thread will update cache
        self._write(b"?")
        time.sleep(0.15)

        with self._status_lock:
            s = self._status

        result = {
            "connected": True,
            "port": self._port,
            "state": s.state,
            "mpos": s.mpos,
            "wpos": s.wpos,
            "feed": s.feed,
            "spindle": s.spindle,
        }

        j = self._job
        if j.running or j.complete or j.error:
            pct = (j.sent_lines / j.total_lines * 100) if j.total_lines else 0
            result["job"] = {
                "running": j.running,
                "file": os.path.basename(j.filepath),
                "lines": f"{j.sent_lines}/{j.total_lines}",
                "percent": round(pct, 1),
                "complete": j.complete,
                "error": j.error,
            }

        return result

    def run_file(self, filepath: str) -> dict:
        if not self._connected:
            return {"ok": False, "error": "Not connected"}
        if self._job.running:
            return {"ok": False, "error": "A job is already running"}
        if not os.path.exists(filepath):
            return {"ok": False, "error": f"File not found: {filepath}"}

        # Pre-validate all lines before streaming to the machine
        try:
            with open(filepath) as f:
                for line_num, raw in enumerate(f, 1):
                    line = raw.strip()
                    if ";" in line:
                        line = line[:line.index(";")].strip()
                    if line and not line.startswith("(") and line != "%":
                        err = validate_gcode(line)
                        if err:
                            return {"ok": False,
                                    "error": f"Line {line_num}: {err} — file not sent"}
        except Exception as e:
            return {"ok": False, "error": f"Cannot read file: {e}"}

        self._job_stop.clear()
        self._job = JobStatus(filepath=filepath, running=True)

        self._stream_thread = threading.Thread(
            target=self._stream_file, args=(filepath,),
            name="grbl-stream", daemon=True)
        self._stream_thread.start()

        return {"ok": True,
                "message": f"Streaming started: {os.path.basename(filepath)}"}

    def pause(self) -> dict:
        self._write(b"!")
        return {"ok": True}

    def resume(self) -> dict:
        self._write(b"~")
        return {"ok": True}

    def stop(self) -> dict:
        self._job_stop.set()
        self._write(b"\x18")          # GRBL soft reset
        time.sleep(0.5)
        with self._write_lock:
            if self._serial:
                self._serial.reset_input_buffer()
        # Drain response queue
        while not self._response_q.empty():
            try:
                self._response_q.get_nowait()
            except queue.Empty:
                break
        self._job = JobStatus()
        return {"ok": True}

    def unlock(self) -> dict:
        """Send $X to clear Alarm state. Fire-and-forget, no sleep."""
        if not self._connected:
            return {"ok": False, "error": "Not connected"}
        self._write(b"$X\n")
        return {"ok": True}

    def jog(self, axis: str, distance_mm: float, feed_mmpm: float = 500.0) -> dict:
        if not self._connected:
            return {"ok": False, "error": "Not connected"}
        axis = axis.upper()
        if axis not in ("X", "Y", "Z"):
            return {"ok": False, "error": "Axis must be X, Y, or Z"}
        # Sanity check: jog distance shouldn't exceed full machine travel
        if axis in SOFT_LIMITS:
            lo, hi = SOFT_LIMITS[axis]
            max_travel = hi - lo
            if abs(distance_mm) > max_travel:
                return {"ok": False,
                        "error": f"Jog distance {distance_mm}mm exceeds {axis} travel ({max_travel}mm)"}
        if feed_mmpm <= 0 or feed_mmpm > 10000:
            return {"ok": False, "error": f"Feed rate {feed_mmpm} out of range (0-10000 mm/min)"}
        cmd = f"$J=G91 G21 {axis}{distance_mm:.3f} F{feed_mmpm:.0f}\n"
        self._write(cmd.encode())
        return {"ok": True, "command": cmd.strip()}

    def home(self) -> dict:
        if not self._connected:
            return {"ok": False, "error": "Not connected"}
        self._write(b"$H\n")
        return {"ok": True}

    def set_zero(self, axes: Optional[list[str]] = None) -> dict:
        if not self._connected:
            return {"ok": False, "error": "Not connected"}
        if axes is None:
            axes = ["X", "Y", "Z"]
        parts = " ".join(f"{a.upper()}0" for a in axes)
        cmd = f"G10 L20 P1 {parts}\n"
        self._write(cmd.encode())
        return {"ok": True, "command": cmd.strip()}

    def probe_z(self, puck_mm: float = 1.5, max_travel_mm: float = 20.0,
                feed_mmpm: float = 50.0) -> dict:
        """
        Run a Z-probe cycle and set work Z to the puck thickness.

        Sequence:
          G38.2 Z-{max_travel} F{feed}   — probe toward workpiece
          G92 Z{puck_mm}                  — set Z = puck thickness (tool offset)
          G0 Z5                           — lift clear

        The machine must be connected and idle.
        puck_mm: thickness of the probe puck in mm (sets the Z work offset).
        """
        if not self._connected:
            return {"ok": False, "error": "Not connected"}
        with self._status_lock:
            state = self._status.state
        if state not in ("Idle",):
            return {"ok": False, "error": f"Machine not idle (state={state})"}

        # Probe move — wait for completion (can take several seconds)
        probe_cmd = f"G38.2 Z-{max_travel_mm:.1f} F{feed_mmpm:.0f}\n"
        self._write(probe_cmd.encode())
        try:
            resp = self._response_q.get(timeout=60.0)
        except queue.Empty:
            return {"ok": False, "error": "Probe timed out — no response from GRBL"}
        if resp.startswith("error"):
            return {"ok": False, "error": f"Probe failed: {resp}"}

        # Set Z work coordinate to puck thickness
        offset_cmd = f"G92 Z{puck_mm:.3f}\n"
        self._write(offset_cmd.encode())
        try:
            self._response_q.get(timeout=5.0)
        except queue.Empty:
            pass

        # Lift clear of puck
        self._write(b"G0 Z5\n")
        try:
            self._response_q.get(timeout=10.0)
        except queue.Empty:
            pass

        return {"ok": True, "puck_mm": puck_mm,
                "message": f"Z probed, offset set to {puck_mm}mm, lifted to Z5"}

    def send_command(self, command: str) -> dict:
        if not self._connected:
            return {"ok": False, "error": "Not connected"}
        err = validate_gcode(command)
        if err:
            return {"ok": False, "error": f"Command rejected: {err}"}
        cmd = command.strip() + "\n"
        self._write(cmd.encode())
        time.sleep(0.25)
        # Collect any queued responses
        responses = []
        while not self._response_q.empty():
            try:
                responses.append(self._response_q.get_nowait())
            except queue.Empty:
                break
        return {"ok": True, "responses": responses}

    # ------------------------------------------------------------------
    # Internal threads
    # ------------------------------------------------------------------

    def _write(self, data: bytes):
        with self._write_lock:
            if self._serial and self._connected:
                try:
                    self._serial.write(data)
                except Exception:
                    pass

    def _reader(self):
        """Reads all incoming serial data; routes status vs ok/error."""
        buf = ""
        while not self._stop_event.is_set():
            try:
                with self._write_lock:
                    ser = self._serial
                if ser is None:
                    time.sleep(0.01)
                    continue

                waiting = ser.in_waiting
                if waiting:
                    chunk = ser.read(waiting)
                    buf += chunk.decode("utf-8", errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("<") and line.endswith(">"):
                            parsed = self._parse_status(line)
                            if parsed:
                                with self._status_lock:
                                    self._status = parsed
                        elif line.startswith("ok") or line.startswith("error"):
                            self._response_q.put(line)
                        elif line.startswith("ALARM"):
                            with self._status_lock:
                                self._status.state = "Alarm"
                        # startup / info lines silently dropped
                else:
                    time.sleep(0.005)
            except Exception:
                time.sleep(0.01)

    def _poller(self):
        """Sends '?' periodically to keep status cache fresh."""
        while not self._stop_event.is_set():
            self._write(b"?")
            time.sleep(self.STATUS_INTERVAL)

    def _stream_file(self, filepath: str):
        """Buffer-aware GRBL file streaming (runs in background thread)."""
        try:
            with open(filepath) as f:
                lines = []
                for raw in f:
                    line = raw.strip()
                    if ";" in line:
                        line = line[:line.index(";")].strip()
                    if line and not line.startswith("(") and line != "%":
                        lines.append(line.upper())

            self._job.total_lines = len(lines)
            buf_sizes: list[int] = []   # byte sizes of lines currently in GRBL buffer
            sent_idx = 0
            errors: list[str] = []

            while sent_idx < len(lines) and not self._job_stop.is_set():
                line = lines[sent_idx]
                line_bytes = len(line) + 1   # +1 for \n

                # Drain responses until there's buffer room
                while (sum(buf_sizes) + line_bytes > self.RX_BUFFER_SIZE
                       and not self._job_stop.is_set()):
                    try:
                        resp = self._response_q.get(timeout=10.0)
                        if buf_sizes:
                            buf_sizes.pop(0)
                        if resp.startswith("error"):
                            errors.append(f"line {self._job.sent_lines}: {resp}")
                    except queue.Empty:
                        # Machine stopped responding — abort
                        self._job.error = "Timeout waiting for GRBL response"
                        self._job.running = False
                        return

                if self._job_stop.is_set():
                    break

                self._write((line + "\n").encode())
                buf_sizes.append(line_bytes)
                sent_idx += 1
                self._job.sent_lines = sent_idx

            # Drain remaining acks
            while buf_sizes and not self._job_stop.is_set():
                try:
                    resp = self._response_q.get(timeout=10.0)
                    if buf_sizes:
                        buf_sizes.pop(0)
                    if resp.startswith("error"):
                        errors.append(resp)
                except queue.Empty:
                    break

            if errors:
                self._job.error = "; ".join(errors[:5])
            self._job.complete = not self._job_stop.is_set()
            self._job.running = False

            cb = self._job_complete_cb
            self._job_complete_cb = None
            if cb:
                cb(self._job.total_lines, self._job.sent_lines,
                   self._job.complete and not self._job.error,
                   self._job.error)

        except Exception as e:
            self._job.error = str(e)
            self._job.running = False
            cb = self._job_complete_cb
            self._job_complete_cb = None
            if cb:
                cb(self._job.total_lines, self._job.sent_lines, False, str(e))

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_status(self, line: str) -> Optional[MachineStatus]:
        """Parse <State|MPos:x,y,z|FS:f,s|WCO:x,y,z> response."""
        if not (line.startswith("<") and line.endswith(">")):
            return None
        s = MachineStatus()
        parts = line[1:-1].split("|")
        s.state = parts[0]
        wco = None
        for part in parts[1:]:
            if ":" not in part:
                continue
            key, val = part.split(":", 1)
            coords = val.split(",")
            if key == "MPos" and len(coords) == 3:
                s.mpos = {k: float(v) for k, v in zip("xyz", coords)}
            elif key == "WPos" and len(coords) == 3:
                s.wpos = {k: float(v) for k, v in zip("xyz", coords)}
            elif key == "WCO" and len(coords) == 3:
                wco = [float(v) for v in coords]
            elif key == "FS" and len(coords) >= 2:
                s.feed, s.spindle = float(coords[0]), float(coords[1])
            elif key == "F" and coords:
                s.feed = float(coords[0])
        # WCO mode: wpos = mpos - wco
        if wco and s.mpos:
            s.wpos = {
                "x": round(s.mpos["x"] - wco[0], 3),
                "y": round(s.mpos["y"] - wco[1], 3),
                "z": round(s.mpos["z"] - wco[2], 3),
            }
        return s


# Module-level singleton
_controller = GRBLController()


def get_controller() -> GRBLController:
    return _controller
