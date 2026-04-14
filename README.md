# Circuit Python Edge Gateway

This folder contains a ThingsBoard Edge gateway setup for telemetry collection, RPC fan control, and rule chain import.

## Contents

- `edge_gateway.py`: Python gateway runtime (MQTT + Modbus + IO).
- `config.json`: Runtime configuration (ThingsBoard, IO pins, Modbus polling).
- `root_rule_chain_device.json`: Rule chain JSON to import in ThingsBoard.
- `tb-edge-io.service`: systemd service unit for running the gateway.
- `requirements.txt`: Python dependencies.

## Quick start

1. Create and activate virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure values in `config.json`.
4. Run gateway:

```bash
python3 edge_gateway.py
```

## Run as a service

```bash
sudo cp tb-edge-io.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tb-edge-io.service
```

## Notes

- Keep your ThingsBoard access token private.
- Import `root_rule_chain_device.json` into ThingsBoard Rule Chains.
- Use dashboard RPC controls according to your configured automation mode.
