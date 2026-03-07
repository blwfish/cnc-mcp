#!/usr/bin/env python3
"""
CNC MCP Server — GRBL machine control via Model Context Protocol.

Usage:
  stdio (dev, Claude Code on same machine):
    python3 cnc_mcp_server.py

  SSE (production, Claude Code connects over LAN):
    python3 cnc_mcp_server.py --transport sse --port 8765
"""

import argparse
import sys

from mcp.server.fastmcp import FastMCP

from grbl import get_controller
import journal as _journal_mod

mcp = FastMCP(
    "cnc-mcp",
    instructions="CNC machine control via GRBL serial protocol",
)
grbl = get_controller()
# Journal is initialised in main() once CLI args are parsed
journal = None


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_serial_ports() -> dict:
    """List serial ports available on this machine. Use to find the CNC port."""
    ports = grbl.list_ports()
    return {"ports": ports}


@mcp.tool()
def connect(port: str) -> dict:  # noqa: F811 (shadows outer name intentionally)
    """
    Connect to the CNC machine on the given serial port.
    Use list_serial_ports first to find the correct port (e.g. /dev/tty.usbserial-...).
    """
    result = grbl.connect(port)
    if result.get("ok"):
        journal.log_event("connect", {"port": port,
                                      "startup": result.get("startup", "")})
    return result


@mcp.tool()
def disconnect() -> dict:
    """Disconnect from the CNC machine and stop all background threads."""
    return grbl.disconnect()


@mcp.tool()
def get_status() -> dict:
    """
    Get current machine status: connection state, position (machine + work coords),
    feed rate, spindle speed, and active job progress if a file is running.
    """
    return grbl.get_status()


@mcp.tool()
def run_file(filepath: str) -> dict:
    """
    Stream a G-code file to the CNC machine.
    Returns immediately — use get_status() to monitor progress.
    The machine must be connected, idle, and the file must exist on this machine.
    """
    status = grbl.get_status()
    wpos = status.get("wpos")
    result = grbl.run_file(filepath)
    if result.get("ok"):
        job_id = journal.job_start(filepath, wpos)
        grbl.set_job_complete_callback(
            lambda total, sent, success, error:
                journal.job_complete(job_id, total, sent, success, error)
        )
    return result


@mcp.tool()
def pause() -> dict:
    """
    Feed hold: pause the running job. The machine decelerates to a stop.
    Use resume() to continue or stop() to cancel.
    """
    return grbl.pause()


@mcp.tool()
def resume() -> dict:
    """Cycle start: resume a feed-held (paused) job."""
    return grbl.resume()


@mcp.tool()
def stop() -> dict:
    """
    Soft reset: immediately stop all motion and cancel the current job.
    Machine will enter Alarm state; may need $X to unlock before next job.
    """
    return grbl.stop()


@mcp.tool()
def jog(axis: str, distance_mm: float, feed_mmpm: float = 500.0) -> dict:
    """
    Move one axis by a relative distance (incremental jog).
    axis: "X", "Y", or "Z"
    distance_mm: positive (away from home) or negative
    feed_mmpm: feed rate in mm/min (default 500)
    """
    return grbl.jog(axis, distance_mm, feed_mmpm)


@mcp.tool()
def home() -> dict:
    """
    Run the homing cycle ($H). Requires homing switches to be installed and enabled.
    Machine moves to home position and sets machine coordinates to zero.
    """
    result = grbl.home()
    if result.get("ok"):
        journal.log_event("home")
    return result


@mcp.tool()
def set_zero(axes: list[str] | None = None) -> dict:
    """
    Set work coordinate zero at the current position for the specified axes.
    axes: list such as ["X", "Y", "Z"] or ["Z"] for Z-only. Defaults to all three.
    This sets the work offset (G10 L20 P1), not the machine coordinates.
    """
    result = grbl.set_zero(axes)
    if result.get("ok"):
        status = grbl.get_status()
        journal.log_event("zero", {
            "axes": axes or ["X", "Y", "Z"],
            "wpos": status.get("wpos"),
        })
    return result


@mcp.tool()
def restart_server() -> dict:
    """
    Restart this MCP server process in-place. Disconnects from the machine first,
    then re-execs the same command. Use when the server is stale or needs a code reload.
    """
    import os
    grbl.disconnect()
    # Brief pause so the MCP response gets sent before we die
    import threading
    def _restart():
        import time
        time.sleep(0.5)
        os.execv(sys.executable, [sys.executable] + sys.argv)
    threading.Thread(target=_restart, daemon=True).start()
    return {"ok": True, "message": "Restarting in 0.5s…"}


@mcp.tool()
def send_command(command: str) -> dict:
    """
    Send an arbitrary G-code or GRBL command and return the response.
    Useful for diagnostics: "$$" (settings), "$#" (offsets), "$X" (unlock alarm),
    "G28" (go to predefined position), etc.
    """
    return grbl.send_command(command)


# ---------------------------------------------------------------------------
# Journal tools
# ---------------------------------------------------------------------------

@mcp.tool()
def journal_list_jobs(limit: int = 20) -> dict:
    """
    List recent CNC jobs from the journal, newest first.
    Returns filename, start/end times, line counts, success/failure, and any notes.
    """
    return {"jobs": journal.list_jobs(limit)}


@mcp.tool()
def journal_get_job(job_id: int) -> dict:
    """
    Get full details for a specific job, including nearby events (home, zero, probe, alarm).
    """
    job = journal.get_job(job_id)
    if job is None:
        return {"ok": False, "error": f"Job {job_id} not found"}
    return {"ok": True, "job": job}


@mcp.tool()
def journal_add_note(job_id: int, note: str) -> dict:
    """
    Add a note to a job. Can be called any time — immediately after a job or days later.
    Notes are appended with a timestamp; existing notes are preserved.
    Use this to record what was cut, material, tool, outcome, or anything else.
    """
    ok = journal.add_note(job_id, note)
    if not ok:
        return {"ok": False, "error": f"Job {job_id} not found"}
    return {"ok": True, "job_id": job_id}


@mcp.tool()
def journal_stats() -> dict:
    """
    Summary statistics: total jobs run, success/failure counts, total machine time.
    """
    return journal.stats()


@mcp.tool()
def journal_list_events(limit: int = 50, event_type: str | None = None) -> dict:
    """
    List recent machine events (connect, home, zero, probe, alarm).
    Optionally filter by type.
    """
    return {"events": journal.list_events(limit, event_type)}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CNC MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "http"],
        default="stdio",
        help="MCP transport: stdio (local), sse, or http (network, for Mini deployment)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind SSE server (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port for SSE server (default: 8765)",
    )
    parser.add_argument(
        "--mqtt",
        metavar="BROKER",
        default=None,
        help="MQTT broker host to enable pendant bridge (e.g. 192.168.1.100)",
    )
    parser.add_argument(
        "--mqtt-port",
        type=int,
        default=1883,
        help="MQTT broker port (default: 1883)",
    )
    parser.add_argument(
        "--mqtt-prefix",
        default="/cova",
        help="MQTT topic prefix (default: /cova)",
    )
    parser.add_argument(
        "--puck-mm",
        type=float,
        default=1.5,
        help="Z-probe puck thickness in mm (default: 1.5)",
    )
    parser.add_argument(
        "--mariadb-host",
        default="localhost",
        help="MariaDB host (default: localhost)",
    )
    parser.add_argument(
        "--mariadb-port",
        type=int,
        default=3306,
        help="MariaDB port (default: 3306)",
    )
    parser.add_argument(
        "--mariadb-db",
        default="mqtt_log",
        help="MariaDB database (default: mqtt_log)",
    )
    parser.add_argument(
        "--mariadb-user",
        default="dashboard",
        help="MariaDB user — password from macOS Keychain (default: dashboard)",
    )
    args = parser.parse_args()

    global journal
    journal = _journal_mod.init(
        host=args.mariadb_host,
        port=args.mariadb_port,
        db=args.mariadb_db,
        user=args.mariadb_user,
    )

    if args.mqtt:
        import mqtt_bridge
        mqtt_bridge.start(grbl, args.mqtt, port=args.mqtt_port,
                          prefix=args.mqtt_prefix, puck_mm=args.puck_mm)
        print(f"MQTT pendant bridge active: {args.mqtt}:{args.mqtt_port} "
              f"prefix={args.mqtt_prefix} puck={args.puck_mm}mm", flush=True)

    if args.transport in ("sse", "http"):
        print(f"Starting CNC MCP server ({args.transport.upper()}) on {args.host}:{args.port}", flush=True)
        from mcp.server.transport_security import TransportSecuritySettings
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        # Disable localhost-only DNS rebinding protection so LAN clients can connect
        mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=False
        )
        transport = "streamable-http" if args.transport == "http" else "sse"
        mcp.run(transport=transport)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
