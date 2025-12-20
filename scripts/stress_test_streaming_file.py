#!/usr/bin/env -S uv run -s
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "rich",
# ]
# ///
"""
Stress test for streaming_file input.

Tests:
- High-throughput writes (thousands of lines/sec)
- File rotation simulation
- File truncation
- Concurrent writers
- Bento start/stop cycles
- Mixed workloads

Usage:
    uv run -s scripts/stress_test_streaming_file.py

Or make executable:
    chmod +x scripts/stress_test_streaming_file.py
    ./scripts/stress_test_streaming_file.py
"""

import subprocess
import tempfile
import time
import threading
import os
import signal
import sys
import random
import shutil
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

console = Console()

# Find bento binary
BENTO_BIN = os.environ.get("BENTO_BIN", "./target/bin/bento")
if not Path(BENTO_BIN).exists():
    # Try to find it
    for candidate in ["./target/bin/bento", "./bento", "bento"]:
        if Path(candidate).exists() or shutil.which(candidate):
            BENTO_BIN = candidate
            break


@dataclass
class TestStats:
    """Track test statistics."""
    lines_written: int = 0
    lines_received: int = 0
    rotations: int = 0
    truncations: int = 0
    bento_restarts: int = 0
    errors: list = field(default_factory=list)
    start_time: float = field(default_factory=time.time)

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def write_rate(self) -> float:
        if self.elapsed > 0:
            return self.lines_written / self.elapsed
        return 0

    @property
    def receive_rate(self) -> float:
        if self.elapsed > 0:
            return self.lines_received / self.elapsed
        return 0


class BentoProcess:
    """Manage a bento process."""

    def __init__(self, config_path: str, output_file: str):
        self.config_path = config_path
        self.output_file = output_file
        self.process: Optional[subprocess.Popen] = None
        self._output_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start bento process."""
        self._stop_event.clear()
        # Redirect stdout to a file we can read
        self.output_fh = open(self.output_file, "a")
        self.process = subprocess.Popen(
            [BENTO_BIN, "-c", self.config_path],
            stdout=self.output_fh,
            stderr=subprocess.PIPE,
            text=True,
        )
        time.sleep(0.5)  # Give it time to start

    def stop(self) -> None:
        """Stop bento process gracefully."""
        if self.process:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None
        if hasattr(self, 'output_fh'):
            self.output_fh.close()

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None


def create_config(log_path: str, poll_interval: str = "100ms",
                  disable_fsnotify: bool = True) -> str:
    """Create a bento config file."""
    config = f"""
input:
  streaming_file:
    path: {log_path}
    poll_interval: {poll_interval}
    disable_fsnotify: {str(disable_fsnotify).lower()}
    max_buffer_size: 10000

output:
  stdout:
    codec: lines
"""
    return config


def test_high_throughput(stats: TestStats, log_path: Path, duration: float = 5.0):
    """Test high-throughput writes."""
    console.print("\n[bold blue]Test 1: High Throughput Writes[/]")
    console.print(f"  Writing as fast as possible for {duration}s...")

    end_time = time.time() + duration
    batch_size = 100
    line_num = 0

    while time.time() < end_time:
        lines = [f"high_throughput_line_{line_num + i}\n" for i in range(batch_size)]
        with open(log_path, "a") as f:
            f.writelines(lines)
        line_num += batch_size
        stats.lines_written += batch_size

    console.print(f"  [green]✓[/] Wrote {line_num:,} lines ({stats.write_rate:,.0f} lines/sec)")


def test_rotation(stats: TestStats, log_path: Path, rotations: int = 5):
    """Test file rotation handling."""
    console.print("\n[bold blue]Test 2: File Rotation[/]")

    for i in range(rotations):
        # Write some lines
        with open(log_path, "a") as f:
            for j in range(100):
                f.write(f"pre_rotation_{i}_line_{j}\n")
                stats.lines_written += 1

        time.sleep(0.2)

        # Rotate: rename current file, create new one
        rotated_path = log_path.with_suffix(f".{i}")
        os.rename(log_path, rotated_path)
        stats.rotations += 1

        # Create new file
        with open(log_path, "w") as f:
            for j in range(100):
                f.write(f"post_rotation_{i}_line_{j}\n")
                stats.lines_written += 1

        time.sleep(0.3)
        console.print(f"  [green]✓[/] Rotation {i + 1}/{rotations} complete")

        # Clean up rotated file
        rotated_path.unlink(missing_ok=True)


def test_truncation(stats: TestStats, log_path: Path, truncations: int = 5):
    """Test file truncation handling."""
    console.print("\n[bold blue]Test 3: File Truncation[/]")

    for i in range(truncations):
        # Write a bunch of lines
        with open(log_path, "a") as f:
            for j in range(200):
                f.write(f"pre_truncation_{i}_line_{j}\n")
                stats.lines_written += 1

        time.sleep(0.2)

        # Truncate the file
        with open(log_path, "w") as f:
            f.write(f"truncated_file_{i}_first_line\n")
            stats.lines_written += 1
        stats.truncations += 1

        time.sleep(0.3)
        console.print(f"  [green]✓[/] Truncation {i + 1}/{truncations} complete")


def test_concurrent_writers(stats: TestStats, log_path: Path,
                           num_writers: int = 5, duration: float = 3.0):
    """Test concurrent writers to the same file."""
    console.print(f"\n[bold blue]Test 4: Concurrent Writers ({num_writers} threads)[/]")

    stop_event = threading.Event()
    lock = threading.Lock()

    def writer(writer_id: int):
        line_num = 0
        while not stop_event.is_set():
            line = f"writer_{writer_id}_line_{line_num}\n"
            with lock:
                with open(log_path, "a") as f:
                    f.write(line)
            with lock:
                stats.lines_written += 1
            line_num += 1
            time.sleep(random.uniform(0.001, 0.01))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_writers)]
    for t in threads:
        t.start()

    time.sleep(duration)
    stop_event.set()

    for t in threads:
        t.join()

    console.print(f"  [green]✓[/] Concurrent writes complete")


def test_bento_restarts(stats: TestStats, log_path: Path, config_path: str,
                        output_file: str, restarts: int = 3):
    """Test bento restart cycles."""
    console.print(f"\n[bold blue]Test 5: Bento Restart Cycles ({restarts}x)[/]")

    for i in range(restarts):
        # Start bento
        bento = BentoProcess(config_path, output_file)
        bento.start()
        stats.bento_restarts += 1

        # Write some lines while running
        with open(log_path, "a") as f:
            for j in range(50):
                f.write(f"restart_cycle_{i}_line_{j}\n")
                stats.lines_written += 1

        time.sleep(0.5)

        # Stop bento
        bento.stop()

        # Write lines while bento is stopped
        with open(log_path, "a") as f:
            for j in range(50):
                f.write(f"while_stopped_{i}_line_{j}\n")
                stats.lines_written += 1

        console.print(f"  [green]✓[/] Restart cycle {i + 1}/{restarts} complete")


def test_rapid_small_writes(stats: TestStats, log_path: Path, duration: float = 3.0):
    """Test rapid small writes (simulating line-by-line logging)."""
    console.print(f"\n[bold blue]Test 6: Rapid Small Writes (line-by-line)[/]")

    end_time = time.time() + duration
    line_num = 0

    while time.time() < end_time:
        with open(log_path, "a") as f:
            f.write(f"rapid_line_{line_num}\n")
            f.flush()
        stats.lines_written += 1
        line_num += 1
        # No sleep - as fast as possible, one line at a time

    console.print(f"  [green]✓[/] Wrote {line_num:,} individual lines")


def test_mixed_workload(stats: TestStats, log_path: Path, duration: float = 5.0):
    """Test mixed workload: writes, rotations, truncations all happening."""
    console.print(f"\n[bold blue]Test 7: Mixed Workload (chaos mode)[/]")

    stop_event = threading.Event()
    lock = threading.Lock()

    def continuous_writer():
        line_num = 0
        while not stop_event.is_set():
            with lock:
                try:
                    with open(log_path, "a") as f:
                        f.write(f"mixed_line_{line_num}\n")
                    stats.lines_written += 1
                except:
                    pass
            line_num += 1
            time.sleep(random.uniform(0.001, 0.05))

    def chaos_monkey():
        while not stop_event.is_set():
            time.sleep(random.uniform(0.5, 1.5))
            action = random.choice(["rotate", "truncate", "nothing", "nothing"])

            with lock:
                try:
                    if action == "rotate":
                        rotated = log_path.with_suffix(".old")
                        if log_path.exists():
                            os.rename(log_path, rotated)
                            Path(log_path).touch()
                            rotated.unlink(missing_ok=True)
                            stats.rotations += 1
                    elif action == "truncate":
                        with open(log_path, "w") as f:
                            f.write("truncated\n")
                        stats.truncations += 1
                        stats.lines_written += 1
                except:
                    pass

    writer_thread = threading.Thread(target=continuous_writer)
    chaos_thread = threading.Thread(target=chaos_monkey)

    writer_thread.start()
    chaos_thread.start()

    time.sleep(duration)
    stop_event.set()

    writer_thread.join()
    chaos_thread.join()

    console.print(f"  [green]✓[/] Chaos complete")


def test_fsnotify_vs_polling(stats: TestStats, log_path: Path, config_dir: Path,
                             output_dir: Path):
    """Compare fsnotify vs polling modes."""
    console.print(f"\n[bold blue]Test 8: FSNotify vs Polling Comparison[/]")

    results = {}

    for mode_name, disable_fsnotify in [("polling", True), ("fsnotify", False)]:
        config_content = create_config(str(log_path), "50ms", disable_fsnotify)
        config_path = config_dir / f"config_{mode_name}.yaml"
        config_path.write_text(config_content)

        output_file = output_dir / f"output_{mode_name}.txt"
        output_file.touch()

        # Clear log file
        log_path.write_text("")

        bento = BentoProcess(str(config_path), str(output_file))
        bento.start()
        time.sleep(0.5)

        # Write test lines
        start = time.time()
        num_lines = 1000
        with open(log_path, "a") as f:
            for i in range(num_lines):
                f.write(f"{mode_name}_test_line_{i}\n")
        write_time = time.time() - start

        # Wait for processing
        time.sleep(1.0)
        bento.stop()

        # Count received lines
        received = sum(1 for line in output_file.read_text().splitlines()
                      if mode_name in line)

        results[mode_name] = {
            "write_time": write_time,
            "lines_sent": num_lines,
            "lines_received": received,
        }
        stats.lines_written += num_lines

    # Display comparison
    table = Table(title="FSNotify vs Polling")
    table.add_column("Mode")
    table.add_column("Lines Sent")
    table.add_column("Lines Received")
    table.add_column("Write Time")

    for mode, data in results.items():
        table.add_row(
            mode,
            str(data["lines_sent"]),
            str(data["lines_received"]),
            f"{data['write_time']:.3f}s"
        )

    console.print(table)


def count_output_lines(output_file: Path) -> int:
    """Count non-empty, non-log lines in output."""
    if not output_file.exists():
        return 0
    count = 0
    for line in output_file.read_text().splitlines():
        # Skip bento log lines
        if line.startswith("level=") or not line.strip():
            continue
        count += 1
    return count


def main():
    console.print(Panel.fit(
        "[bold]Streaming File Input Stress Test[/]\n"
        f"Using bento: {BENTO_BIN}",
        title="🔥 Stress Test"
    ))

    # Check bento exists
    if not Path(BENTO_BIN).exists() and not shutil.which(BENTO_BIN):
        console.print(f"[red]Error: bento binary not found at {BENTO_BIN}[/]")
        console.print("Build it with: go build -o ./target/bin/bento ./cmd/bento")
        sys.exit(1)

    stats = TestStats()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        log_path = tmpdir / "test.log"
        config_path = tmpdir / "config.yaml"
        output_file = tmpdir / "output.txt"

        # Create initial log file
        log_path.write_text("initial_line\n")
        stats.lines_written += 1

        # Create config
        config_content = create_config(str(log_path), "100ms", True)
        config_path.write_text(config_content)
        output_file.touch()

        # Start bento
        console.print("\n[yellow]Starting bento...[/]")
        bento = BentoProcess(str(config_path), str(output_file))
        bento.start()

        if not bento.is_running():
            console.print("[red]Failed to start bento![/]")
            sys.exit(1)

        console.print("[green]Bento started successfully[/]")
        time.sleep(0.5)

        try:
            # Run tests
            test_high_throughput(stats, log_path, duration=3.0)
            time.sleep(0.5)

            test_rotation(stats, log_path, rotations=5)
            time.sleep(0.5)

            test_truncation(stats, log_path, truncations=5)
            time.sleep(0.5)

            test_concurrent_writers(stats, log_path, num_writers=5, duration=2.0)
            time.sleep(0.5)

            test_rapid_small_writes(stats, log_path, duration=2.0)
            time.sleep(0.5)

            test_mixed_workload(stats, log_path, duration=3.0)
            time.sleep(0.5)

            # Stop main bento for restart test
            bento.stop()
            time.sleep(0.2)

            test_bento_restarts(stats, log_path, str(config_path),
                               str(output_file), restarts=3)

            # Restart for final test
            bento.start()
            time.sleep(0.5)

            test_fsnotify_vs_polling(stats, log_path, tmpdir, tmpdir)

        finally:
            # Clean up
            console.print("\n[yellow]Stopping bento...[/]")
            bento.stop()

        # Wait a moment for output to flush
        time.sleep(0.5)

        # Count received lines
        stats.lines_received = count_output_lines(output_file)

        # Final summary
        console.print("\n")
        summary = Table(title="📊 Test Summary", show_header=True)
        summary.add_column("Metric", style="cyan")
        summary.add_column("Value", style="green")

        summary.add_row("Total Duration", f"{stats.elapsed:.1f}s")
        summary.add_row("Lines Written", f"{stats.lines_written:,}")
        summary.add_row("Lines Received", f"{stats.lines_received:,}")
        summary.add_row("Write Rate", f"{stats.write_rate:,.0f} lines/sec")
        summary.add_row("File Rotations", str(stats.rotations))
        summary.add_row("File Truncations", str(stats.truncations))
        summary.add_row("Bento Restarts", str(stats.bento_restarts))

        if stats.lines_written > 0:
            capture_rate = (stats.lines_received / stats.lines_written) * 100
            summary.add_row("Capture Rate", f"{capture_rate:.1f}%")

        console.print(summary)

        if stats.errors:
            console.print("\n[red]Errors encountered:[/]")
            for err in stats.errors:
                console.print(f"  - {err}")

        # Final verdict
        if stats.lines_received > 0:
            console.print("\n[bold green]✓ Stress test completed successfully![/]")
        else:
            console.print("\n[bold red]✗ No lines received - check for issues[/]")


if __name__ == "__main__":
    main()
