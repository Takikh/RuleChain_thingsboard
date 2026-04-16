#!/usr/bin/env python3
import json
import logging
import os
import signal
import socket
import inspect
import threading
import time
import subprocess
import queue
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

import paho.mqtt.client as mqtt

try:
    # pymodbus >= 3.x
    from pymodbus.client import ModbusSerialClient
except Exception:  # pragma: no cover
    # pymodbus <= 2.x fallback
    from pymodbus.client.sync import ModbusSerialClient  # type: ignore


LOG = logging.getLogger("edge_gateway")


def load_config() -> Dict[str, Any]:
    config_from_env = os.getenv("TB_EDGE_IO_CONFIG")
    if config_from_env:
        config_path = Path(config_from_env)
    else:
        config_path = Path(__file__).resolve().parent / "config.json"

    with config_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


class EdgeGateway:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        tb_cfg = config.get("thingsboard", {})
        self.host: str = tb_cfg.get("host", "127.0.0.1")
        self.port: int = int(tb_cfg.get("port", 1883))
        self.access_token: str = tb_cfg["access_token"]
        self.client_id: str = tb_cfg.get("client_id") or socket.gethostname()
        self.telemetry_interval: float = max(float(tb_cfg.get("telemetry_interval_sec", 1)), 0.2)
        self.snapshot_interval: float = max(float(tb_cfg.get("snapshot_interval_sec", 1.0)), 0.2)
        self.gpio_poll_interval: float = max(float(tb_cfg.get("gpio_poll_interval_sec", 0.05)), 0.02)
        self.gpio_output_poll_interval: float = max(float(tb_cfg.get("gpio_output_poll_interval_sec", 0.5)), 0.1)
        self.gpio_read_timeout: float = max(float(tb_cfg.get("gpio_read_timeout_sec", 0.04)), 0.01)
        self.gpio_write_timeout: float = max(float(tb_cfg.get("gpio_write_timeout_sec", 0.25)), 0.05)
        self.max_latency_ms: int = int(tb_cfg.get("max_end_to_end_latency_ms", 100))
        self.buffer_size: int = int(tb_cfg.get("telemetry_buffer_size", 2048))
        self.gpio_workers: int = max(2, int(tb_cfg.get("gpio_workers", 8)))
        self.publish_ack_timeout_sec: float = max(float(tb_cfg.get("publish_ack_timeout_sec", 0.6)), 0.1)
        self.require_publish_ack: bool = bool(tb_cfg.get("require_publish_ack", False))

        self.stop_event = threading.Event()
        self.connected = False
        self.seq = 0
        self.start_monotonic = time.monotonic()
        self.do_state: Dict[str, int] = {}
        self.modbus_error_count = 0
        self.gpio_error_count = 0
        self.publish_error_count = 0
        self.last_gpio_capture_ms = 0
        self.last_modbus_capture_ms = 0
        self.latest_data_lock = threading.Lock()
        self.latest_gpio: Dict[str, Any] = {}
        self.latest_modbus: Dict[str, Any] = {}
        self.last_gpio_change_ms: Dict[str, int] = {}
        self.telemetry_queue: "queue.Queue[Tuple[Dict[str, Any], int, str]]" = queue.Queue(maxsize=self.buffer_size)

        # Persistent session + QoS1 follow official MQTT reliability recommendations.
        self.mqtt_client = mqtt.Client(client_id=self.client_id, clean_session=False)
        self.mqtt_client.username_pw_set(self.access_token)
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
        # Avoid silent drops when publish rate is high or network is unstable.
        self.mqtt_client.max_queued_messages_set(10000)
        self.mqtt_client.max_inflight_messages_set(100)

        self.io_cfg = config.get("io", {})
        self.di_pins: List[str] = list(self.io_cfg.get("onboard_di", []))
        self.do_pins: List[str] = list(self.io_cfg.get("onboard_do", []))
        self.ai_pins: List[str] = list(self.io_cfg.get("onboard_ai", []))
        self.expansion_di_pins: List[str] = list(self.io_cfg.get("expansion_di", []))
        self.expansion_do_pins: List[str] = list(self.io_cfg.get("expansion_do", []))
        self.input_pins: List[str] = self.di_pins + self.expansion_di_pins
        self.output_pins: List[str] = self.do_pins + self.expansion_do_pins
        self.all_gpio_pins: List[str] = self.input_pins + self.output_pins + self.ai_pins
        self.npe_available = shutil.which("npe") is not None
        self.ex_card_available = shutil.which("ex_card") is not None
        self.gpio_executor = ThreadPoolExecutor(max_workers=self.gpio_workers, thread_name_prefix="gpio-read")

        self.modbus_client: Optional[ModbusSerialClient] = None
        self.modbus_device_kw: Optional[str] = None
        self.modbus_connected = False
        self.modbus_cfg = config.get("modbus", {})
        if self.modbus_cfg.get("enabled", False):
            self.modbus_client = ModbusSerialClient(
                port=self.modbus_cfg.get("port", "/dev/ttySC0"),
                baudrate=int(self.modbus_cfg.get("baudrate", 9600)),
                bytesize=int(self.modbus_cfg.get("bytesize", 8)),
                parity=str(self.modbus_cfg.get("parity", "N")),
                stopbits=int(self.modbus_cfg.get("stopbits", 1)),
                timeout=float(self.modbus_cfg.get("timeout", 1)),
            )
            self.modbus_device_kw = self._detect_modbus_device_kw()

    def start(self) -> None:
        LOG.info(
            "Starting gateway client_id=%s target=%s:%s (gpio_poll=%.3fs, snapshot=%.3fs, max_latency=%sms)",
            self.client_id,
            self.host,
            self.port,
            self.gpio_poll_interval,
            self.snapshot_interval,
            self.max_latency_ms,
        )
        self._configure_gpio_interfaces()
        self.mqtt_client.connect_async(self.host, self.port, keepalive=60)
        self.mqtt_client.loop_start()

        threads = [
            threading.Thread(target=self._gpio_worker_loop, name="gpio-worker", daemon=True),
            threading.Thread(target=self._modbus_worker_loop, name="modbus-worker", daemon=True),
            threading.Thread(target=self._publisher_loop, name="publisher-worker", daemon=True),
            threading.Thread(target=self._monitor_loop, name="monitor-worker", daemon=True),
        ]
        for th in threads:
            th.start()

        while not self.stop_event.is_set():
            time.sleep(0.2)

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        LOG.info("Stopping gateway")
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        if self.modbus_client:
            try:
                self.modbus_client.close()
                self.modbus_connected = False
            except Exception:
                LOG.exception("Error while closing modbus client")
        try:
            self.gpio_executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            LOG.exception("Error while stopping GPIO executor")

    def _on_connect(self, client: mqtt.Client, _userdata: Any, _flags: Dict[str, int], rc: int) -> None:
        if rc == 0:
            self.connected = True
            LOG.info("MQTT connected")
            client.subscribe("v1/devices/me/rpc/request/+", qos=1)
            self._publish_attributes({"gateway_online": True, "client_id": self.client_id})
        else:
            self.connected = False
            LOG.error("MQTT connection failed with code=%s", rc)

    def _on_disconnect(self, _client: mqtt.Client, _userdata: Any, rc: int) -> None:
        self.connected = False
        if rc != 0:
            LOG.warning("MQTT disconnected unexpectedly (rc=%s). Auto-reconnect active.", rc)
        else:
            LOG.info("MQTT disconnected")

    def _on_message(self, client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            request = json.loads(msg.payload.decode("utf-8"))
            method = request.get("method", "")
            params = request.get("params", {})
            req_id = msg.topic.split("/")[-1]

            normalized = method.replace("setState", "set").replace("getState", "get")

            if normalized.startswith("set"):
                pin = normalized[3:]
                value = self._parse_bool_param(params)
                ok = self._set_output_pin(pin, value)
                response = {"ok": ok, "method": method, "pin": pin, "value": int(value)}
                if ok:
                    now_ms = self._now_ms()
                    self._enqueue_telemetry({pin: int(value), "src": "rpc_set", "ts_ms": now_ms}, now_ms, "gpio")
            elif normalized.startswith("get"):
                pin = normalized[3:]
                pin_val = self._read_gpio_pin(pin)
                response = {"ok": pin_val is not None, "method": method, "pin": pin, "value": pin_val}
            elif method in ("ping", "getStatus", "getAll"):
                response = {
                    "ok": True,
                    "connected": self.connected,
                    "uptime_sec": int(time.monotonic() - self.start_monotonic),
                    "last_seq": self.seq,
                    "do_state": self.do_state,
                    "modbus_errors": self.modbus_error_count,
                    "gpio_errors": self.gpio_error_count,
                    "queue_size": self.telemetry_queue.qsize(),
                    "last_gpio_capture_ms": self.last_gpio_capture_ms,
                    "last_modbus_capture_ms": self.last_modbus_capture_ms,
                }
            else:
                response = {"ok": False, "error": f"Unsupported method: {method}"}

            client.publish(f"v1/devices/me/rpc/response/{req_id}", json.dumps(response), qos=1)
        except Exception as exc:
            LOG.exception("RPC handling error")
            try:
                req_id = msg.topic.split("/")[-1]
                client.publish(
                    f"v1/devices/me/rpc/response/{req_id}",
                    json.dumps({"ok": False, "error": str(exc)}),
                    qos=1,
                )
            except Exception:
                LOG.exception("Failed to publish RPC error response")

    def _detect_modbus_device_kw(self) -> Optional[str]:
        if not self.modbus_client:
            return None
        try:
            params = inspect.signature(self.modbus_client.read_input_registers).parameters
            if "device_id" in params:
                return "device_id"
            if "slave" in params:
                return "slave"
            if "unit" in params:
                return "unit"
        except Exception:
            LOG.exception("Unable to inspect pymodbus read_input_registers signature")
        return None

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _parse_bool_param(params: Any) -> bool:
        if isinstance(params, bool):
            return params
        if isinstance(params, (int, float)):
            return params != 0
        if isinstance(params, str):
            return params.strip().lower() in ("1", "true", "on", "yes")
        if isinstance(params, dict):
            if "value" in params:
                return EdgeGateway._parse_bool_param(params.get("value"))
            if "state" in params:
                return EdgeGateway._parse_bool_param(params.get("state"))
        return bool(params)

    def _run_command(self, args: List[str], timeout: float) -> Optional[str]:
        try:
            result = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            return result.stdout.strip()
        except Exception:
            self.gpio_error_count += 1
            LOG.debug("Command failed: %s", " ".join(args), exc_info=True)
            return None

    def _configure_gpio_interfaces(self) -> None:
        if self.ex_card_available and self.expansion_di_pins:
            self._run_command(["ex_card", "configure"], timeout=2.0)
            for pin in self.expansion_di_pins:
                self._run_command(["ex_card", "setup", "DIO", pin, "DI"], timeout=self.gpio_write_timeout)
            for pin in self.expansion_do_pins:
                self._run_command(["ex_card", "setup", "DIO", pin, "DO", "0"], timeout=self.gpio_write_timeout)
                self.do_state[pin] = 0

        if not self.npe_available:
            LOG.warning("Command 'npe' not found. Onboard GPIO (DI/DO/AI) may not be readable.")
        if self.expansion_di_pins and not self.ex_card_available:
            LOG.warning("Command 'ex_card' not found. Expansion GPIO (DIO*) may not be readable.")

    def _read_gpio_pin(self, pin: str) -> Optional[Any]:
        if pin.startswith("DI") and not pin.startswith("DIO"):
            if not self.npe_available:
                return None
            out = self._run_command(["npe", f"?{pin}"], timeout=self.gpio_read_timeout)
            if out and out.lstrip("-").isdigit():
                return int(out)
            return None

        if pin.startswith("DO"):
            if not self.npe_available:
                return self.do_state.get(pin)
            out = self._run_command(["npe", f"?{pin}"], timeout=self.gpio_read_timeout)
            if out and out.lstrip("-").isdigit():
                return int(out)
            return self.do_state.get(pin)

        if pin.startswith("AI"):
            if not self.npe_available:
                return None
            out = self._run_command(["npe", f"?{pin}"], timeout=self.gpio_read_timeout)
            if out:
                try:
                    return float(out)
                except ValueError:
                    return None
            return None

        if pin.startswith("DIO"):
            if not self.ex_card_available:
                return self.do_state.get(pin)
            out = self._run_command(["ex_card", "read", "DIO", pin], timeout=self.gpio_read_timeout)
            if out and out.lstrip("-").isdigit():
                return int(out)
            return self.do_state.get(pin)

        return None

    def _set_output_pin(self, pin: str, value: bool) -> bool:
        pin = pin.upper()
        target = 1 if value else 0
        ok = False
        if pin.startswith("DO"):
            if self.npe_available:
                cmd = f"+{pin}" if value else f"-{pin}"
                _ = self._run_command(["npe", cmd], timeout=self.gpio_write_timeout)
                ok = True
        elif pin.startswith("DIO"):
            if self.ex_card_available:
                _ = self._run_command(
                    ["ex_card", "setup", "DIO", pin, "DO", str(target)],
                    timeout=self.gpio_write_timeout,
                )
                ok = True

        if ok:
            self.do_state[pin] = target
            with self.latest_data_lock:
                self.latest_gpio[pin] = target
                self.last_gpio_change_ms[pin] = self._now_ms()
        else:
            self.gpio_error_count += 1
            LOG.warning("Unable to set output pin %s (value=%s)", pin, target)
        return ok

    def _enqueue_telemetry(self, payload: Dict[str, Any], captured_ts_ms: int, source: str) -> None:
        item = (payload, captured_ts_ms, source)
        try:
            self.telemetry_queue.put_nowait(item)
        except queue.Full:
            self.publish_error_count += 1
            try:
                _ = self.telemetry_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self.telemetry_queue.put_nowait(item)
            except queue.Full:
                LOG.error("Telemetry queue saturated; dropping payload from source=%s", source)

    def _read_modbus_value(self, poll: Dict[str, Any]) -> Optional[float]:
        if not self.modbus_client:
            return None

        try:
            function_code = int(poll.get("function_code", 4))
            address = int(poll.get("address", 0))
            slave_id = int(poll.get("slave_id", 1))

            kwargs: Dict[str, Any] = {"count": 1}
            if self.modbus_device_kw:
                kwargs[self.modbus_device_kw] = slave_id

            if function_code == 4:
                response = self.modbus_client.read_input_registers(address, **kwargs)
            elif function_code == 3:
                response = self.modbus_client.read_holding_registers(address, **kwargs)
            else:
                raise ValueError(f"Unsupported function_code={function_code}")

            if response.isError():
                raise RuntimeError(str(response))

            raw = float(response.registers[0])
            divider = float(poll.get("divider", 1) or 1)
            return raw / divider
        except Exception:
            self.modbus_error_count += 1
            LOG.exception("Modbus poll failed for %s", poll.get("key", "unknown"))
            return None

    def _publish_telemetry(self, telemetry: Dict[str, Any]) -> bool:
        payload = json.dumps(telemetry, separators=(",", ":"))
        info = self.mqtt_client.publish("v1/devices/me/telemetry", payload, qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.publish_error_count += 1
            LOG.warning("Telemetry publish failed rc=%s", info.rc)
            return False
        if self.require_publish_ack:
            try:
                info.wait_for_publish(timeout=self.publish_ack_timeout_sec)
                if not info.is_published():
                    self.publish_error_count += 1
                    LOG.warning("Telemetry publish not ACKed within %.2fs", self.publish_ack_timeout_sec)
                    return False
            except Exception:
                self.publish_error_count += 1
                LOG.exception("Telemetry publish ACK wait failed")
                return False
        LOG.debug("Telemetry sent: %s", payload)
        return True

    def _publish_attributes(self, attributes: Dict[str, Any]) -> None:
        payload = json.dumps(attributes, separators=(",", ":"))
        self.mqtt_client.publish("v1/devices/me/attributes", payload, qos=1, retain=False)

    def _gpio_worker_loop(self) -> None:
        next_output_scan = 0.0
        while not self.stop_event.is_set():
            delta_payload: Dict[str, Any] = {}
            updated_values: Dict[str, Any] = {}
            pins_to_read = self.input_pins + self.ai_pins
            if pins_to_read:
                future_map = {self.gpio_executor.submit(self._read_gpio_pin, pin): pin for pin in pins_to_read}
                for future in as_completed(future_map):
                    pin = future_map[future]
                    try:
                        value = future.result()
                    except Exception:
                        self.gpio_error_count += 1
                        LOG.debug("GPIO read worker failed for pin=%s", pin, exc_info=True)
                        continue
                    if value is not None:
                        updated_values[pin] = value

            now_mono = time.monotonic()
            if now_mono >= next_output_scan:
                if self.output_pins:
                    future_map = {self.gpio_executor.submit(self._read_gpio_pin, pin): pin for pin in self.output_pins}
                    for future in as_completed(future_map):
                        pin = future_map[future]
                        try:
                            value = future.result()
                        except Exception:
                            self.gpio_error_count += 1
                            LOG.debug("GPIO output read worker failed for pin=%s", pin, exc_info=True)
                            continue
                        if value is None and pin in self.do_state:
                            value = self.do_state[pin]
                        if value is not None:
                            updated_values[pin] = value
                next_output_scan = now_mono + self.gpio_output_poll_interval

            captured_ms = self._now_ms()
            with self.latest_data_lock:
                for pin, value in updated_values.items():
                    previous = self.latest_gpio.get(pin)
                    self.latest_gpio[pin] = value
                    if previous != value:
                        delta_payload[pin] = value
                        self.last_gpio_change_ms[pin] = captured_ms
                self.last_gpio_capture_ms = captured_ms

            if delta_payload:
                delta_payload.update({"src": "gpio_change", "ts_ms": captured_ms})
                self._enqueue_telemetry(delta_payload, captured_ms, "gpio")

            time.sleep(self.gpio_poll_interval)

    def _modbus_worker_loop(self) -> None:
        next_poll = time.monotonic()
        while not self.stop_event.is_set():
            now = time.monotonic()
            if now < next_poll:
                time.sleep(0.01)
                continue
            next_poll = now + self.telemetry_interval

            captured_ms = self._now_ms()
            modbus_payload: Dict[str, Any] = {}

            if self.modbus_client:
                try:
                    if not self.modbus_connected:
                        self.modbus_connected = bool(self.modbus_client.connect())
                        if not self.modbus_connected:
                            raise RuntimeError("Cannot connect Modbus serial port")
                except Exception:
                    self.modbus_error_count += 1
                    self.modbus_connected = False
                    LOG.exception("Modbus connection failed")
                    continue

            for poll in self.modbus_cfg.get("polls", []):
                key = poll.get("key")
                if not key:
                    continue
                value = self._read_modbus_value(poll)
                if value is not None:
                    modbus_payload[key] = value

            if modbus_payload:
                modbus_payload.update({"src": "modbus_poll", "ts_ms": captured_ms})
                with self.latest_data_lock:
                    self.latest_modbus.update({k: v for k, v in modbus_payload.items() if k not in ("src", "ts_ms")})
                    self.last_modbus_capture_ms = captured_ms
                self._enqueue_telemetry(modbus_payload, captured_ms, "modbus")
            else:
                # If the read cycle failed silently for all polls, force reconnect on next pass.
                self.modbus_connected = False

    def _publisher_loop(self) -> None:
        next_snapshot = time.monotonic()
        while not self.stop_event.is_set():
            now_mono = time.monotonic()
            try:
                payload, captured_ts_ms, source = self.telemetry_queue.get(timeout=0.05)
                if not self.connected:
                    # Keep data until MQTT session is up, while queue max size protects memory.
                    self._enqueue_telemetry(payload, captured_ts_ms, source)
                    time.sleep(0.05)
                    continue
                publish_ts_ms = self._now_ms()
                payload["publish_ts_ms"] = publish_ts_ms
                payload["latency_ms"] = max(0, publish_ts_ms - captured_ts_ms)
                ok = self._publish_telemetry(payload)
                if not ok:
                    # Retry later instead of dropping the sample.
                    self._enqueue_telemetry(payload, captured_ts_ms, source)
                    time.sleep(0.05)
                    continue
                if source == "gpio" and payload["latency_ms"] > self.max_latency_ms:
                    LOG.warning(
                        "Latency breach source=%s latency=%sms threshold=%sms",
                        source,
                        payload["latency_ms"],
                        self.max_latency_ms,
                    )
            except queue.Empty:
                pass

            if now_mono >= next_snapshot:
                # Prioritize event data when queue starts backing up.
                if self.telemetry_queue.qsize() > int(self.buffer_size * 0.8):
                    next_snapshot = now_mono + self.snapshot_interval
                    continue
                self.seq += 1
                snap_ts_ms = self._now_ms()
                with self.latest_data_lock:
                    snapshot = {
                        **self.latest_modbus,
                        **self.latest_gpio,
                    }
                snapshot.update(
                    {
                        "alive": 1,
                        "seq": self.seq,
                        "client_id": self.client_id,
                        "uptime_sec": int(time.monotonic() - self.start_monotonic),
                        "modbus_error_count": self.modbus_error_count,
                        "gpio_error_count": self.gpio_error_count,
                        "publish_error_count": self.publish_error_count,
                        "src": "snapshot",
                        "ts_ms": snap_ts_ms,
                    }
                )
                self._enqueue_telemetry(snapshot, snap_ts_ms, "snapshot")
                next_snapshot = now_mono + self.snapshot_interval

    def _monitor_loop(self) -> None:
        while not self.stop_event.is_set():
            now_ms = self._now_ms()
            gpio_age = now_ms - self.last_gpio_capture_ms if self.last_gpio_capture_ms else None
            modbus_age = now_ms - self.last_modbus_capture_ms if self.last_modbus_capture_ms else None
            LOG.info(
                "monitor connected=%s queue=%s gpio_age_ms=%s modbus_age_ms=%s errors(gpio=%s modbus=%s publish=%s)",
                self.connected,
                self.telemetry_queue.qsize(),
                gpio_age,
                modbus_age,
                self.gpio_error_count,
                self.modbus_error_count,
                self.publish_error_count,
            )
            if gpio_age is not None and gpio_age > 2 * int(self.gpio_poll_interval * 1000):
                LOG.warning("GPIO capture delay detected: %sms", gpio_age)
            time.sleep(5.0)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("TB_EDGE_IO_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    config = load_config()
    gateway = EdgeGateway(config)

    def _signal_handler(_sig: int, _frame: Any) -> None:
        gateway.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        gateway.start()
    finally:
        gateway.stop()


if __name__ == "__main__":
    main()
