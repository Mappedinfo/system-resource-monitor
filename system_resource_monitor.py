#!/usr/bin/env python3
"""Anonymous host-level CPU/GPU resource and power monitor.

This collector records machine-level metrics only. It does not inspect or
store process IDs, usernames, command lines, Docker containers, or images.
The collect/report paths are read-only with respect to running workloads:
they never call docker, os.kill, signal APIs, terminate(), or kill().
"""

import argparse
import csv
import glob
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_DB = "/var/lib/system-resource-monitor/system_metrics.sqlite3"
DEFAULT_RETENTION_DAYS = 90
DEFAULT_MAX_ENERGY_INTERVAL_SEC = 900.0
DEFAULT_WATCH_INTERVAL_SEC = 5.0
SERVICE_NAME = "system-resource-monitor.service"
TIMER_NAME = "system-resource-monitor.timer"
SYSTEMD_DIR = Path("/etc/systemd/system")
INSTALL_DATA_DIR = Path("/var/lib/system-resource-monitor")

GPU_QUERY_FIELDS = (
    "index",
    "uuid",
    "name",
    "power.draw",
    "utilization.gpu",
    "utilization.memory",
    "memory.used",
    "memory.total",
    "temperature.gpu",
)

REPORT_COLUMNS = [
    "section",
    "period_start_local",
    "period_end_local",
    "sample_count",
    "gpu_index",
    "gpu_uuid",
    "gpu_name",
    "avg_cpu_util_pct",
    "max_cpu_util_pct",
    "avg_mem_used_mib",
    "max_mem_used_mib",
    "avg_mem_used_pct",
    "max_mem_used_pct",
    "avg_load1",
    "max_load1",
    "avg_gpu_total_power_w",
    "max_gpu_total_power_w",
    "avg_known_total_power_w",
    "max_known_total_power_w",
    "estimated_known_kwh",
    "avg_gpu_power_w",
    "max_gpu_power_w",
    "avg_gpu_util_pct",
    "max_gpu_util_pct",
    "avg_gpu_mem_used_mib",
    "max_gpu_mem_used_mib",
    "avg_gpu_mem_util_pct",
    "max_gpu_mem_util_pct",
    "avg_gpu_temp_c",
    "max_gpu_temp_c",
    "estimated_gpu_kwh",
]

WATCH_COLUMNS = [
    "ts_unix",
    "ts_local",
    "row_type",
    "cpu_util_pct",
    "mem_used_mib",
    "mem_total_mib",
    "mem_used_pct",
    "load1",
    "load5",
    "load15",
    "cpu_package_power_w",
    "gpu_total_power_w",
    "known_total_power_w",
    "gpu_count",
    "gpu_error",
    "gpu_index",
    "gpu_uuid",
    "gpu_name",
    "gpu_power_w",
    "gpu_util_pct",
    "gpu_mem_util_pct",
    "gpu_mem_used_mib",
    "gpu_mem_total_mib",
    "gpu_temp_c",
]

SERVICE_TEMPLATE = """[Unit]
Description=Anonymous System Resource Monitor sample
Documentation=https://pypi.org/project/system-resource-monitor/
After=multi-user.target

[Service]
Type=oneshot
User=root
UMask=0027
TimeoutStartSec=30
NoNewPrivileges=true
CapabilityBoundingSet=~CAP_KILL
SystemCallFilter=~kill tgkill tkill pidfd_send_signal rt_sigqueueinfo rt_tgsigqueueinfo
ExecStart={python_executable} -m system_resource_monitor --db {db_path} collect --retention-days {retention_days} --max-energy-interval-sec {max_energy_interval_sec}
"""

TIMER_TEMPLATE = """[Unit]
Description=Run anonymous system resource monitor every 5 minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
Persistent=true
Unit=system-resource-monitor.service

[Install]
WantedBy=timers.target
"""


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    text = str(value).strip()
    if text in {"", "N/A", "[N/A]", "NA", "None", "null", "nan"}:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: Any) -> Optional[int]:
    number = parse_float(value)
    if number is None:
        return None
    return int(number)


def local_time(ts: Optional[int]) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts).isoformat(sep=" ", timespec="seconds")


def round_or_empty(value: Any, digits: int = 3) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        return round(value, digits)
    return value


def format_value(value: Any, suffix: str = "", digits: int = 1) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:.{digits}f}{suffix}"
    return f"{value}{suffix}"


def default_watch_csv_path() -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"/tmp/system_resource_watch_{stamp}.csv"


def init_db(db_path: str) -> None:
    db_file = Path(db_path)
    parent_existed = db_file.parent.exists()
    db_file.parent.mkdir(parents=True, exist_ok=True)

    if not parent_existed or db_file.parent.name == "system-resource-monitor":
        try:
            db_file.parent.chmod(0o750)
        except OSError:
            pass

    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS host_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                interval_sec REAL,

                cpu_util_pct REAL,
                mem_used_mib REAL,
                mem_total_mib REAL,
                mem_used_pct REAL,
                load1 REAL,
                load5 REAL,
                load15 REAL,

                cpu_package_power_w REAL,
                gpu_total_power_w REAL,
                known_total_power_w REAL
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS gpu_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_sample_id INTEGER NOT NULL,
                ts INTEGER NOT NULL,

                gpu_index TEXT,
                gpu_uuid TEXT,
                gpu_name TEXT,

                gpu_power_w REAL,
                gpu_util_pct REAL,
                gpu_mem_util_pct REAL,
                gpu_mem_used_mib REAL,
                gpu_mem_total_mib REAL,
                gpu_temp_c REAL,

                FOREIGN KEY(host_sample_id) REFERENCES host_samples(id)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS collector_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_ts INTEGER NOT NULL
            )
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_host_samples_ts
            ON host_samples(ts)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gpu_samples_ts
            ON gpu_samples(ts)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gpu_samples_uuid_ts
            ON gpu_samples(gpu_uuid, ts)
            """
        )

    try:
        db_file.chmod(0o640)
    except OSError:
        pass


def ensure_systemd_arg_safe(value: str, label: str) -> str:
    if not value or any(char.isspace() for char in value):
        raise ValueError(f"{label} must not be empty or contain whitespace: {value!r}")
    return value


def render_service(
    python_executable: str,
    db_path: str,
    retention_days: int,
    max_energy_interval_sec: float,
) -> str:
    return SERVICE_TEMPLATE.format(
        python_executable=ensure_systemd_arg_safe(
            python_executable,
            "python executable",
        ),
        db_path=ensure_systemd_arg_safe(db_path, "database path"),
        retention_days=retention_days,
        max_energy_interval_sec=max_energy_interval_sec,
    )


def run_command(command: Sequence[str], dry_run: bool = False) -> None:
    if dry_run:
        print("+ " + " ".join(command))
        return

    subprocess.run(command, check=True)


def require_root_for_system_install(dry_run: bool) -> None:
    if dry_run:
        return
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        raise PermissionError("install-systemd must be run as root, for example with sudo")


def install_systemd(
    db_path: str,
    retention_days: int,
    max_energy_interval_sec: float,
    enable_now: bool,
    dry_run: bool,
) -> None:
    require_root_for_system_install(dry_run)

    python_executable = sys.executable
    service_text = render_service(
        python_executable=python_executable,
        db_path=db_path,
        retention_days=retention_days,
        max_energy_interval_sec=max_energy_interval_sec,
    )
    timer_text = TIMER_TEMPLATE

    service_path = SYSTEMD_DIR / SERVICE_NAME
    timer_path = SYSTEMD_DIR / TIMER_NAME
    data_dir = Path(db_path).parent

    if dry_run:
        print(f"Would create data directory: {data_dir}")
        print(f"Would write: {service_path}")
        print(service_text)
        print(f"Would write: {timer_path}")
        print(timer_text)
    else:
        data_dir.mkdir(parents=True, exist_ok=True)
        data_dir.chmod(0o750)
        service_path.write_text(service_text)
        timer_path.write_text(timer_text)
        service_path.chmod(0o644)
        timer_path.chmod(0o644)

    run_command(["systemctl", "daemon-reload"], dry_run=dry_run)
    if enable_now:
        run_command(["systemctl", "enable", "--now", TIMER_NAME], dry_run=dry_run)


def uninstall_systemd(dry_run: bool) -> None:
    require_root_for_system_install(dry_run)

    service_path = SYSTEMD_DIR / SERVICE_NAME
    timer_path = SYSTEMD_DIR / TIMER_NAME

    run_command(["systemctl", "disable", "--now", TIMER_NAME], dry_run=dry_run)

    if dry_run:
        print(f"Would remove: {timer_path}")
        print(f"Would remove: {service_path}")
    else:
        timer_path.unlink(missing_ok=True)
        service_path.unlink(missing_ok=True)

    run_command(["systemctl", "daemon-reload"], dry_run=dry_run)


def show_status() -> None:
    systemctl = shutil.which("systemctl")
    if systemctl is None:
        raise RuntimeError("systemctl not found")

    run_command([systemctl, "list-timers", TIMER_NAME])
    run_command([systemctl, "status", SERVICE_NAME])


def get_state(con: sqlite3.Connection, key: str) -> Optional[str]:
    row = con.execute(
        "SELECT value FROM collector_state WHERE key = ?",
        (key,),
    ).fetchone()
    return row[0] if row else None


def set_state(con: sqlite3.Connection, key: str, value: Any, ts: int) -> None:
    if not isinstance(value, str):
        value = json.dumps(value, separators=(",", ":"))
    con.execute(
        """
        INSERT INTO collector_state(key, value, updated_ts)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_ts = excluded.updated_ts
        """,
        (key, value, ts),
    )


def read_boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return ""


def read_cpu_stat() -> Optional[Dict[str, int]]:
    try:
        line = Path("/proc/stat").read_text().splitlines()[0]
    except (OSError, IndexError):
        return None

    parts = line.split()
    if not parts or parts[0] != "cpu":
        return None

    values = [int(part) for part in parts[1:]]
    while len(values) < 8:
        values.append(0)

    user, nice, system, idle, iowait, irq, softirq, steal = values[:8]
    idle_all = idle + iowait
    non_idle = user + nice + system + irq + softirq + steal

    return {
        "total": idle_all + non_idle,
        "idle": idle_all,
    }


def compute_cpu_util(
    previous: Optional[Dict[str, int]],
    current: Optional[Dict[str, int]],
) -> Optional[float]:
    if not previous or not current:
        return None

    total_delta = current["total"] - previous["total"]
    idle_delta = current["idle"] - previous["idle"]
    if total_delta <= 0 or idle_delta < 0:
        return None

    return max(0.0, min(100.0, 100.0 * (total_delta - idle_delta) / total_delta))


def read_meminfo() -> Dict[str, Optional[float]]:
    values: Dict[str, float] = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            parts = line.split()
            if len(parts) >= 2:
                values[parts[0].rstrip(":")] = float(parts[1])
    except OSError:
        return {
            "mem_used_mib": None,
            "mem_total_mib": None,
            "mem_used_pct": None,
        }

    total_kib = values.get("MemTotal")
    available_kib = values.get("MemAvailable")
    if total_kib is None or available_kib is None or total_kib <= 0:
        return {
            "mem_used_mib": None,
            "mem_total_mib": None,
            "mem_used_pct": None,
        }

    used_kib = max(0.0, total_kib - available_kib)
    return {
        "mem_used_mib": used_kib / 1024.0,
        "mem_total_mib": total_kib / 1024.0,
        "mem_used_pct": 100.0 * used_kib / total_kib,
    }


def read_loadavg() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        load1, load5, load15 = os.getloadavg()
        return load1, load5, load15
    except OSError:
        return None, None, None


def read_rapl_energy() -> Dict[str, Dict[str, int]]:
    packages: Dict[str, Dict[str, int]] = {}
    for path_text in sorted(glob.glob("/sys/class/powercap/intel-rapl:*")):
        path = Path(path_text)
        if path.name.count(":") != 1:
            continue

        energy_path = path / "energy_uj"
        if not energy_path.exists():
            continue

        try:
            energy_uj = int(energy_path.read_text().strip())
        except (OSError, ValueError):
            continue

        max_range_uj = 0
        try:
            max_range_uj = int((path / "max_energy_range_uj").read_text().strip())
        except (OSError, ValueError):
            pass

        packages[path.name] = {
            "energy_uj": energy_uj,
            "max_energy_range_uj": max_range_uj,
        }

    return packages


def compute_rapl_power_w(
    previous: Optional[Dict[str, Dict[str, int]]],
    current: Dict[str, Dict[str, int]],
    interval_sec: Optional[float],
) -> Optional[float]:
    if not previous or not current or interval_sec is None or interval_sec <= 0:
        return None

    total_delta_uj = 0
    matched = 0
    for package_id, current_data in current.items():
        previous_data = previous.get(package_id)
        if not previous_data:
            continue

        current_energy = current_data["energy_uj"]
        previous_energy = previous_data["energy_uj"]
        delta = current_energy - previous_energy
        if delta < 0:
            max_range = current_data.get("max_energy_range_uj") or previous_data.get(
                "max_energy_range_uj"
            )
            if max_range and max_range > previous_energy:
                delta = (max_range - previous_energy) + current_energy

        if delta < 0:
            continue

        total_delta_uj += delta
        matched += 1

    if matched == 0:
        return None

    return total_delta_uj / 1_000_000.0 / interval_sec


def read_gpu_samples() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    command = [
        "nvidia-smi",
        "--query-gpu=" + ",".join(GPU_QUERY_FIELDS),
        "--format=csv,noheader,nounits",
    ]

    try:
        proc = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        return [], "nvidia-smi not found"

    if proc.returncode != 0:
        message = proc.stderr.strip() or "nvidia-smi returned non-zero status"
        return [], message

    rows: List[Dict[str, Any]] = []
    reader = csv.reader(StringIO(proc.stdout))
    for raw_row in reader:
        if not raw_row:
            continue
        if len(raw_row) != len(GPU_QUERY_FIELDS):
            continue

        row = [item.strip() for item in raw_row]
        rows.append(
            {
                "gpu_index": row[0],
                "gpu_uuid": row[1],
                "gpu_name": row[2],
                "gpu_power_w": parse_float(row[3]),
                "gpu_util_pct": parse_float(row[4]),
                "gpu_mem_util_pct": parse_float(row[5]),
                "gpu_mem_used_mib": parse_float(row[6]),
                "gpu_mem_total_mib": parse_float(row[7]),
                "gpu_temp_c": parse_float(row[8]),
            }
        )

    return rows, None


def bounded_interval_sec(
    previous_ts: Optional[int],
    current_ts: int,
    max_energy_interval_sec: float,
) -> Optional[float]:
    if previous_ts is None:
        return None

    interval_sec = float(current_ts - previous_ts)
    if interval_sec <= 0 or interval_sec > max_energy_interval_sec:
        return None
    return interval_sec


def load_json_state(value: Optional[str]) -> Optional[Any]:
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def known_total_power_w(*values: Optional[float]) -> Optional[float]:
    known_values = [value for value in values if value is not None]
    if not known_values:
        return None
    return sum(known_values)


def enforce_retention(con: sqlite3.Connection, ts: int, retention_days: int) -> None:
    cutoff = ts - retention_days * 86400
    con.execute("DELETE FROM gpu_samples WHERE ts < ?", (cutoff,))
    con.execute("DELETE FROM host_samples WHERE ts < ?", (cutoff,))


def collect_once(
    db_path: str,
    retention_days: int,
    max_energy_interval_sec: float,
) -> Dict[str, Any]:
    init_db(db_path)

    ts = int(time.time())
    boot_id = read_boot_id()
    cpu_stat = read_cpu_stat()
    meminfo = read_meminfo()
    load1, load5, load15 = read_loadavg()
    rapl_energy = read_rapl_energy()
    gpu_rows, gpu_error = read_gpu_samples()

    with sqlite3.connect(db_path) as con:
        previous_boot_id = get_state(con, "boot_id")
        previous_ts_value = get_state(con, "last_ts")
        previous_cpu_stat = load_json_state(get_state(con, "cpu_stat"))
        previous_rapl_energy = load_json_state(get_state(con, "rapl_energy"))

        previous_ts = parse_int(previous_ts_value)
        same_boot = bool(boot_id and previous_boot_id == boot_id)
        interval_sec = (
            bounded_interval_sec(previous_ts, ts, max_energy_interval_sec)
            if same_boot
            else None
        )

        cpu_util_pct = compute_cpu_util(
            previous_cpu_stat if same_boot else None,
            cpu_stat,
        )
        cpu_package_power_w = compute_rapl_power_w(
            previous_rapl_energy if same_boot else None,
            rapl_energy,
            interval_sec,
        )

        gpu_power_values = [row["gpu_power_w"] for row in gpu_rows]
        known_gpu_power_values = [value for value in gpu_power_values if value is not None]
        gpu_total_power_w = (
            sum(known_gpu_power_values) if known_gpu_power_values else None
        )
        total_power_w = known_total_power_w(cpu_package_power_w, gpu_total_power_w)

        cur = con.execute(
            """
            INSERT INTO host_samples (
                ts,
                interval_sec,
                cpu_util_pct,
                mem_used_mib,
                mem_total_mib,
                mem_used_pct,
                load1,
                load5,
                load15,
                cpu_package_power_w,
                gpu_total_power_w,
                known_total_power_w
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                interval_sec,
                cpu_util_pct,
                meminfo["mem_used_mib"],
                meminfo["mem_total_mib"],
                meminfo["mem_used_pct"],
                load1,
                load5,
                load15,
                cpu_package_power_w,
                gpu_total_power_w,
                total_power_w,
            ),
        )
        host_sample_id = cur.lastrowid

        con.executemany(
            """
            INSERT INTO gpu_samples (
                host_sample_id,
                ts,
                gpu_index,
                gpu_uuid,
                gpu_name,
                gpu_power_w,
                gpu_util_pct,
                gpu_mem_util_pct,
                gpu_mem_used_mib,
                gpu_mem_total_mib,
                gpu_temp_c
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    host_sample_id,
                    ts,
                    row["gpu_index"],
                    row["gpu_uuid"],
                    row["gpu_name"],
                    row["gpu_power_w"],
                    row["gpu_util_pct"],
                    row["gpu_mem_util_pct"],
                    row["gpu_mem_used_mib"],
                    row["gpu_mem_total_mib"],
                    row["gpu_temp_c"],
                )
                for row in gpu_rows
            ],
        )

        set_state(con, "boot_id", boot_id, ts)
        set_state(con, "last_ts", str(ts), ts)
        if cpu_stat is not None:
            set_state(con, "cpu_stat", cpu_stat, ts)
        set_state(con, "rapl_energy", rapl_energy, ts)
        enforce_retention(con, ts, retention_days)

    return {
        "ts": ts,
        "interval_sec": interval_sec,
        "gpu_count": len(gpu_rows),
        "gpu_error": gpu_error,
        "cpu_util_pct": cpu_util_pct,
        "mem_used_mib": meminfo["mem_used_mib"],
        "mem_total_mib": meminfo["mem_total_mib"],
        "mem_used_pct": meminfo["mem_used_pct"],
        "load1": load1,
        "load5": load5,
        "load15": load15,
        "cpu_package_power_w": cpu_package_power_w,
        "gpu_total_power_w": gpu_total_power_w,
        "known_total_power_w": total_power_w,
        "gpu_rows": gpu_rows,
    }


def fetch_one(con: sqlite3.Connection, query: str, params: Sequence[Any]) -> sqlite3.Row:
    cur = con.execute(query, params)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("query unexpectedly returned no rows")
    return row


def host_report_row(con: sqlite3.Connection, start_ts: int, end_ts: int) -> List[Any]:
    row = fetch_one(
        con,
        """
        SELECT
            COUNT(*) AS sample_count,
            MIN(ts) AS min_ts,
            MAX(ts) AS max_ts,
            ROUND(AVG(cpu_util_pct), 3) AS avg_cpu_util_pct,
            ROUND(MAX(cpu_util_pct), 3) AS max_cpu_util_pct,
            ROUND(AVG(mem_used_mib), 3) AS avg_mem_used_mib,
            ROUND(MAX(mem_used_mib), 3) AS max_mem_used_mib,
            ROUND(AVG(mem_used_pct), 3) AS avg_mem_used_pct,
            ROUND(MAX(mem_used_pct), 3) AS max_mem_used_pct,
            ROUND(AVG(load1), 3) AS avg_load1,
            ROUND(MAX(load1), 3) AS max_load1,
            ROUND(AVG(gpu_total_power_w), 3) AS avg_gpu_total_power_w,
            ROUND(MAX(gpu_total_power_w), 3) AS max_gpu_total_power_w,
            ROUND(AVG(known_total_power_w), 3) AS avg_known_total_power_w,
            ROUND(MAX(known_total_power_w), 3) AS max_known_total_power_w,
            ROUND(
                SUM(
                    CASE
                        WHEN interval_sec IS NOT NULL
                         AND known_total_power_w IS NOT NULL
                        THEN known_total_power_w * interval_sec
                        ELSE 0
                    END
                ) / 3600000.0,
                6
            ) AS estimated_known_kwh
        FROM host_samples
        WHERE ts BETWEEN ? AND ?
        """,
        (start_ts, end_ts),
    )

    return [
        "host",
        local_time(row["min_ts"]),
        local_time(row["max_ts"]),
        row["sample_count"],
        "",
        "",
        "",
        row["avg_cpu_util_pct"],
        row["max_cpu_util_pct"],
        row["avg_mem_used_mib"],
        row["max_mem_used_mib"],
        row["avg_mem_used_pct"],
        row["max_mem_used_pct"],
        row["avg_load1"],
        row["max_load1"],
        row["avg_gpu_total_power_w"],
        row["max_gpu_total_power_w"],
        row["avg_known_total_power_w"],
        row["max_known_total_power_w"],
        row["estimated_known_kwh"],
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]


def gpu_report_rows(con: sqlite3.Connection, start_ts: int, end_ts: int) -> Iterable[List[Any]]:
    cur = con.execute(
        """
        SELECT
            g.gpu_index,
            g.gpu_uuid,
            g.gpu_name,
            COUNT(*) AS sample_count,
            MIN(g.ts) AS min_ts,
            MAX(g.ts) AS max_ts,
            ROUND(AVG(g.gpu_power_w), 3) AS avg_gpu_power_w,
            ROUND(MAX(g.gpu_power_w), 3) AS max_gpu_power_w,
            ROUND(AVG(g.gpu_util_pct), 3) AS avg_gpu_util_pct,
            ROUND(MAX(g.gpu_util_pct), 3) AS max_gpu_util_pct,
            ROUND(AVG(g.gpu_mem_used_mib), 3) AS avg_gpu_mem_used_mib,
            ROUND(MAX(g.gpu_mem_used_mib), 3) AS max_gpu_mem_used_mib,
            ROUND(AVG(g.gpu_mem_util_pct), 3) AS avg_gpu_mem_util_pct,
            ROUND(MAX(g.gpu_mem_util_pct), 3) AS max_gpu_mem_util_pct,
            ROUND(AVG(g.gpu_temp_c), 3) AS avg_gpu_temp_c,
            ROUND(MAX(g.gpu_temp_c), 3) AS max_gpu_temp_c,
            ROUND(
                SUM(
                    CASE
                        WHEN h.interval_sec IS NOT NULL
                         AND g.gpu_power_w IS NOT NULL
                        THEN g.gpu_power_w * h.interval_sec
                        ELSE 0
                    END
                ) / 3600000.0,
                6
            ) AS estimated_gpu_kwh
        FROM gpu_samples g
        LEFT JOIN host_samples h ON h.id = g.host_sample_id
        WHERE g.ts BETWEEN ? AND ?
        GROUP BY g.gpu_index, g.gpu_uuid, g.gpu_name
        ORDER BY estimated_gpu_kwh DESC, g.gpu_index
        """,
        (start_ts, end_ts),
    )

    for row in cur:
        yield [
            "gpu",
            local_time(row["min_ts"]),
            local_time(row["max_ts"]),
            row["sample_count"],
            row["gpu_index"],
            row["gpu_uuid"],
            row["gpu_name"],
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            row["avg_gpu_power_w"],
            row["max_gpu_power_w"],
            row["avg_gpu_util_pct"],
            row["max_gpu_util_pct"],
            row["avg_gpu_mem_used_mib"],
            row["max_gpu_mem_used_mib"],
            row["avg_gpu_mem_util_pct"],
            row["max_gpu_mem_util_pct"],
            row["avg_gpu_temp_c"],
            row["max_gpu_temp_c"],
            row["estimated_gpu_kwh"],
        ]


def write_report(db_path: str, since_hours: float, out_path: Optional[str]) -> None:
    init_db(db_path)

    end_ts = int(time.time())
    start_ts = end_ts - int(since_hours * 3600)

    output = open(out_path, "w", newline="") if out_path else sys.stdout
    try:
        writer = csv.writer(output)
        writer.writerow(REPORT_COLUMNS)

        with sqlite3.connect(db_path) as con:
            con.row_factory = sqlite3.Row
            host_row = host_report_row(con, start_ts, end_ts)
            writer.writerow([round_or_empty(value) for value in host_row])

            for gpu_row in gpu_report_rows(con, start_ts, end_ts):
                writer.writerow([round_or_empty(value) for value in gpu_row])
    finally:
        if out_path:
            output.close()


def watch_csv_rows(sample: Dict[str, Any]) -> Iterable[List[Any]]:
    host_values = [
        sample["ts"],
        local_time(sample["ts"]),
        "host",
        sample["cpu_util_pct"],
        sample["mem_used_mib"],
        sample["mem_total_mib"],
        sample["mem_used_pct"],
        sample["load1"],
        sample["load5"],
        sample["load15"],
        sample["cpu_package_power_w"],
        sample["gpu_total_power_w"],
        sample["known_total_power_w"],
        sample["gpu_count"],
        sample["gpu_error"] or "",
    ]

    yield host_values + ["", "", "", "", "", "", "", "", ""]

    for gpu in sample["gpu_rows"]:
        gpu_values = list(host_values)
        gpu_values[2] = "gpu"
        yield gpu_values + [
            gpu["gpu_index"],
            gpu["gpu_uuid"],
            gpu["gpu_name"],
            gpu["gpu_power_w"],
            gpu["gpu_util_pct"],
            gpu["gpu_mem_util_pct"],
            gpu["gpu_mem_used_mib"],
            gpu["gpu_mem_total_mib"],
            gpu["gpu_temp_c"],
        ]


def render_watch_screen(
    sample: Dict[str, Any],
    csv_path: str,
    sample_count: int,
    started_ts: int,
    interval_sec: float,
) -> str:
    lines = [
        "system-resource-monitor watch",
        f"Started: {local_time(started_ts)}    Samples: {sample_count}    Interval: {interval_sec:g}s",
        f"Now:     {local_time(sample['ts'])}",
        f"CSV:     {csv_path}",
        "",
        "HOST",
        (
            f"  CPU {format_value(sample['cpu_util_pct'], '%')}    "
            f"Mem {format_value(sample['mem_used_mib'], ' MiB')} / "
            f"{format_value(sample['mem_total_mib'], ' MiB')} "
            f"({format_value(sample['mem_used_pct'], '%')})"
        ),
        (
            f"  Load {format_value(sample['load1'])} "
            f"{format_value(sample['load5'])} "
            f"{format_value(sample['load15'])}    "
            f"CPU pkg {format_value(sample['cpu_package_power_w'], ' W')}    "
            f"GPU total {format_value(sample['gpu_total_power_w'], ' W')}    "
            f"Known total {format_value(sample['known_total_power_w'], ' W')}"
        ),
        "",
        "GPU",
    ]

    if sample["gpu_error"]:
        lines.append(f"  GPU metrics unavailable: {sample['gpu_error']}")
    elif not sample["gpu_rows"]:
        lines.append("  No GPUs reported by nvidia-smi.")
    else:
        lines.append(
            "  idx  power    util   mem_used / mem_total     mem_util  temp  name"
        )
        for gpu in sample["gpu_rows"]:
            name = str(gpu["gpu_name"])[:42]
            lines.append(
                "  "
                f"{str(gpu['gpu_index']).rjust(3)}  "
                f"{format_value(gpu['gpu_power_w'], ' W').rjust(8)}  "
                f"{format_value(gpu['gpu_util_pct'], '%').rjust(6)}  "
                f"{format_value(gpu['gpu_mem_used_mib'], ' MiB').rjust(12)} / "
                f"{format_value(gpu['gpu_mem_total_mib'], ' MiB').ljust(12)}  "
                f"{format_value(gpu['gpu_mem_util_pct'], '%').rjust(8)}  "
                f"{format_value(gpu['gpu_temp_c'], ' C').rjust(6)}  "
                f"{name}"
            )

    lines.extend(["", "Press Ctrl-C to stop."])
    return "\n".join(lines)


def run_watch(
    db_path: str,
    csv_path: str,
    interval_sec: float,
    retention_days: int,
    max_energy_interval_sec: float,
    clear_screen: bool,
    samples: int,
) -> None:
    if interval_sec <= 0:
        raise ValueError("--interval-sec must be greater than 0")
    if samples < 0:
        raise ValueError("--samples must be 0 or greater")

    csv_file = Path(csv_path)
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    started_ts = int(time.time())
    sample_count = 0

    with csv_file.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(WATCH_COLUMNS)
        output.flush()

        try:
            while samples == 0 or sample_count < samples:
                sample = collect_once(
                    db_path=db_path,
                    retention_days=retention_days,
                    max_energy_interval_sec=max_energy_interval_sec,
                )
                sample_count += 1

                for row in watch_csv_rows(sample):
                    writer.writerow([round_or_empty(value) for value in row])
                output.flush()

                if clear_screen:
                    print("\033[2J\033[H", end="")
                print(
                    render_watch_screen(
                        sample=sample,
                        csv_path=str(csv_file),
                        sample_count=sample_count,
                        started_ts=started_ts,
                        interval_sec=interval_sec,
                    ),
                    flush=True,
                )

                if samples and sample_count >= samples:
                    break
                time.sleep(interval_sec)

        except KeyboardInterrupt:
            print(f"\nStopped. CSV saved to: {csv_file}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Anonymous host-level CPU/GPU resource and power monitor."
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB,
        help=f"SQLite database path. Default: {DEFAULT_DB}",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="Collect one host-level sample.")
    collect.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Delete samples older than this many days. Default: {DEFAULT_RETENTION_DAYS}",
    )
    collect.add_argument(
        "--max-energy-interval-sec",
        type=float,
        default=DEFAULT_MAX_ENERGY_INTERVAL_SEC,
        help=(
            "Do not use a sample for energy estimates if the previous sample is "
            f"older than this. Default: {DEFAULT_MAX_ENERGY_INTERVAL_SEC}"
        ),
    )

    report = sub.add_parser("report", help="Generate a host/GPU CSV summary.")
    report.add_argument("--since-hours", type=float, default=24.0)
    report.add_argument("--out", default=None)

    watch = sub.add_parser(
        "watch",
        help="Refresh terminal metrics and append raw samples to CSV.",
    )
    watch.add_argument(
        "--interval-sec",
        type=float,
        default=DEFAULT_WATCH_INTERVAL_SEC,
        help=f"Refresh interval in seconds. Default: {DEFAULT_WATCH_INTERVAL_SEC:g}",
    )
    watch.add_argument(
        "--csv",
        default=None,
        help="CSV path. Default: /tmp/system_resource_watch_<timestamp>.csv",
    )
    watch.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
    )
    watch.add_argument(
        "--max-energy-interval-sec",
        type=float,
        default=DEFAULT_MAX_ENERGY_INTERVAL_SEC,
    )
    watch.add_argument(
        "--no-clear",
        action="store_true",
        help="Print samples one after another instead of clearing the screen.",
    )
    watch.add_argument(
        "--samples",
        type=int,
        default=0,
        help="Number of samples to collect. 0 means run until Ctrl-C.",
    )

    install = sub.add_parser(
        "install-systemd",
        help="Install the Ubuntu systemd timer and service.",
    )
    install.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
    )
    install.add_argument(
        "--max-energy-interval-sec",
        type=float,
        default=DEFAULT_MAX_ENERGY_INTERVAL_SEC,
    )
    install.add_argument(
        "--no-enable",
        action="store_true",
        help="Write unit files but do not enable/start the timer.",
    )
    install.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be installed without writing files.",
    )

    uninstall = sub.add_parser(
        "uninstall-systemd",
        help="Remove the Ubuntu systemd timer and service.",
    )
    uninstall.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be removed without changing the system.",
    )

    sub.add_parser("status", help="Show systemd timer/service status.")

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "collect":
        result = collect_once(
            db_path=args.db,
            retention_days=args.retention_days,
            max_energy_interval_sec=args.max_energy_interval_sec,
        )
        print(
            "collected host sample at "
            f"{local_time(result['ts'])}; "
            f"gpu_samples={result['gpu_count']}; "
            f"known_total_power_w={round_or_empty(result['known_total_power_w'])}"
        )
        if result["gpu_error"]:
            print(f"[WARN] GPU metrics unavailable: {result['gpu_error']}", file=sys.stderr)

    elif args.command == "report":
        write_report(
            db_path=args.db,
            since_hours=args.since_hours,
            out_path=args.out,
        )

    elif args.command == "watch":
        run_watch(
            db_path=args.db,
            csv_path=args.csv or default_watch_csv_path(),
            interval_sec=args.interval_sec,
            retention_days=args.retention_days,
            max_energy_interval_sec=args.max_energy_interval_sec,
            clear_screen=not args.no_clear,
            samples=args.samples,
        )

    elif args.command == "install-systemd":
        install_systemd(
            db_path=args.db,
            retention_days=args.retention_days,
            max_energy_interval_sec=args.max_energy_interval_sec,
            enable_now=not args.no_enable,
            dry_run=args.dry_run,
        )

    elif args.command == "uninstall-systemd":
        uninstall_systemd(dry_run=args.dry_run)

    elif args.command == "status":
        show_status()


if __name__ == "__main__":
    main()
