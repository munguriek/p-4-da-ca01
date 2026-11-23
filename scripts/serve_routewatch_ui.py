import argparse
import ipaddress
import logging
from pathlib import Path

from routewatch.web import load_ui_settings, make_server

UI_DIR = Path(__file__).resolve().parents[1] / "ui"


def is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the RouteWatch web UI.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--transform-version", type=int, default=1)
    parser.add_argument(
        "--allowed-host",
        action="append",
        default=[],
        help="An extra Host name browsers may use, such as a tunnel's name.",
    )
    parser.add_argument(
        "--allow-non-loopback",
        action="store_true",
        help="Listen on a non-loopback address. Put HTTPS and a firewall in front.",
    )
    arguments = parser.parse_args()

    if not is_loopback(arguments.host) and not arguments.allow_non_loopback:
        parser.error(
            f"{arguments.host} is not a loopback address; "
            "reach the UI through an SSH tunnel, or pass --allow-non-loopback"
        )
    if not 1 <= arguments.port <= 65535:
        parser.error("--port must be from 1 to 65535")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        settings = load_ui_settings(
            transform_version=arguments.transform_version,
            static_dir=UI_DIR,
        )
    except ValueError as error:
        parser.error(f"{error} (see .env.example)")
    server = make_server(
        settings,
        host=arguments.host,
        port=arguments.port,
        extra_hosts=arguments.allowed_host,
    )
    shown_host = f"[{arguments.host}]" if ":" in arguments.host else arguments.host
    print(f"RouteWatch UI on http://{shown_host}:{arguments.port}/  (Ctrl-C stops it)")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("RouteWatch UI stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
