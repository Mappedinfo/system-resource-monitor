# Anonymous System Resource Monitor

[![CI](https://github.com/mappedinfo/system-resource-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/mappedinfo/system-resource-monitor/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/system-resource-monitor.svg)](https://pypi.org/project/system-resource-monitor/)

This is a host-level Ubuntu monitor for group servers. It samples aggregate CPU,
memory, load, GPU power/utilization/memory/temperature, and best-effort CPU
package power. It does not record PID, username, command line, Docker container,
image, or compose service information.

## Install from PyPI

Recommended with `pipx`:

```bash
sudo apt update
sudo apt install -y pipx
pipx ensurepath
pipx install system-resource-monitor

sudo "$(command -v system-resource-monitor)" install-systemd
```

Upgrade an existing install:

```bash
pipx upgrade system-resource-monitor
```

Then check the timer:

```bash
systemctl list-timers system-resource-monitor.timer
sudo "$(command -v system-resource-monitor)" status
```

## Safety Contract

The `collect`, `watch`, and `report` commands are read-only with respect to
running workloads:

- They do not call `docker`, `kill`, `pkill`, `os.kill`, signal
  APIs, `terminate()`, or `kill()`.
- They do not enumerate `/proc/<pid>` and do not inspect per-process metadata.
- The only external command used by `collect` and `watch` is:

  ```bash
  nvidia-smi --query-gpu=index,uuid,name,power.draw,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits
  ```

- The `install-systemd`, `uninstall-systemd`, and `status` commands may call
  `systemctl`, but only to manage `system-resource-monitor.service` and
  `system-resource-monitor.timer`.
- The systemd unit also drops `CAP_KILL` and denies common signal-sending
  syscalls, so even accidental future signal-sending code should fail under the
  timer service.

## Install from Source

```bash
cd system-resource-monitor

sudo mkdir -p /opt/system-resource-monitor /var/lib/system-resource-monitor
sudo install -m 0755 system_resource_monitor.py /opt/system-resource-monitor/system_resource_monitor.py
sudo install -m 0644 README.md /opt/system-resource-monitor/README.md
sudo install -m 0644 system-resource-monitor.service /etc/systemd/system/system-resource-monitor.service
sudo install -m 0644 system-resource-monitor.timer /etc/systemd/system/system-resource-monitor.timer
sudo chmod 0750 /var/lib/system-resource-monitor

sudo systemctl daemon-reload
sudo systemctl enable --now system-resource-monitor.timer
```

If installed as a Python package, prefer:

```bash
sudo "$(command -v system-resource-monitor)" install-systemd
```

## Check Status

```bash
systemctl list-timers system-resource-monitor.timer
sudo systemctl status system-resource-monitor.service
journalctl -u system-resource-monitor.service -n 50
```

## Collect Once

```bash
sudo "$(command -v system-resource-monitor)" collect
```

## Watch Live Metrics

Refresh the terminal every 5 seconds and automatically save samples from this
watch session to `/tmp/system_resource_watch_<timestamp>.csv`:

```bash
sudo "$(command -v system-resource-monitor)" watch
```

Use a custom interval and CSV path:

```bash
sudo "$(command -v system-resource-monitor)" watch \
  --interval-sec 2 \
  --csv /tmp/locus_gpu_watch.csv
```

Stop with `Ctrl-C`. The CSV contains one `host` row per sample and one `gpu`
row per GPU per sample. It does not contain PID, username, command line, Docker
container, image, or compose service information.

## Export Reports

Recent 24 hours:

```bash
sudo "$(command -v system-resource-monitor)" report \
  --since-hours 24 \
  --out /tmp/system_resource_report_24h.csv
```

Recent 7 days:

```bash
sudo "$(command -v system-resource-monitor)" report \
  --since-hours 168 \
  --out /tmp/system_resource_report_7d.csv
```

## Notes

- GPU metrics use `nvidia-smi --query-gpu`, so NVIDIA drivers must be installed
  for GPU rows to appear.
- CPU package power is read from `/sys/class/powercap/intel-rapl:*` when the
  kernel exposes it. If unavailable, it is stored as empty/NULL.
- Energy estimates use `power_w * interval_sec`; first samples and samples after
  long gaps are not counted toward kWh estimates.
- Data older than 90 days is deleted during each collection.

## Development

Run the local checks used by GitHub Actions:

```bash
python -m py_compile system_resource_monitor.py
python system_resource_monitor.py --db /tmp/system_resource_monitor_dev.sqlite3 collect
python system_resource_monitor.py --db /tmp/system_resource_monitor_dev.sqlite3 watch \
  --samples 1 \
  --no-clear \
  --csv /tmp/system_resource_monitor_watch_dev.csv
python -m build
python -m twine check dist/*
```

## PyPI Release Automation

The repository includes `.github/workflows/publish.yml`, which publishes to PyPI
with OpenID Connect trusted publishing. To enable cloud publishing, configure a
PyPI trusted publisher for:

```text
Owner: mappedinfo
Repository: system-resource-monitor
Workflow: publish.yml
Environment: pypi
```

After that, publishing a GitHub release or manually running the workflow will
build the package in GitHub Actions and publish it to PyPI without storing a
PyPI token in the repository.
