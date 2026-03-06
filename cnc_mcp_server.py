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

mcp = FastMCP(
    "cnc-mcp",
    instructions="CNC machine control via GRBL serial protocol",
)
grbl = get_controller()


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_serial_ports() -> dict:
    """List serial ports available on this machine. Use to find the CNC port."""
    ports = grbl.list_ports()
    return {"ports": ports}


@mcp.tool()
def connect(port: str) -> dict:
    """
    Connect to the CNC machine on the given serial port.
    Use list_serial_ports first to find the correct port (e.g. /dev/tty.usbserial-...).
    """
    return grbl.connect(port)


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
    return grbl.run_file(filepath)


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
    return grbl.home()


@mcp.tool()
def set_zero(axes: list[str] | None = None) -> dict:
    """
    Set work coordinate zero at the current position for the specified axes.
    axes: list such as ["X", "Y", "Z"] or ["Z"] for Z-only. Defaults to all three.
    This sets the work offset (G10 L20 P1), not the machine coordinates.
    """
    return grbl.set_zero(axes)


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
    args = parser.parse_args()

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
