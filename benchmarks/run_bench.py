"""M4 benchmark: Déjà recording off vs on, under a fixed request rate.

    python benchmarks/run_bench.py --rates 100,250,400 --duration 30 --reps 3
    python benchmarks/run_bench.py --k6 "docker run --rm -i grafana/k6" --base-host host.docker.internal   # no local k6

Each (rate, rep) runs the two modes back to back in alternating order, so slow drift on a shared machine hits both.
Reports medians over reps of: achieved req/s, latency p50/p95/p99, failed/dropped requests, server CPU and memory,
and the recorder's compressed buffer size per request. Needs Postgres (BENCH_PG_DSN) and k6.
"""

import argparse
import json
import os
import shlex
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

import psutil

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
APP_PORT, STUB_PORT = 8900, 8901


def wait_http(url, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                return json.loads(r.read() or b"null")
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"{url} did not come up")


def pin(cmd, cpu):
    """On Linux, pin a process to one CPU so the server and the load generator don't fight over cores."""
    if sys.platform.startswith("linux") and shutil.which("taskset") and cpu is not None:
        return ["taskset", "-c", str(cpu), *cmd]
    return cmd


def start(cmd, env, cpu=None):
    return subprocess.Popen(
        pin(cmd, cpu), cwd=ROOT, env={**os.environ, **env}, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )


def stop(p):
    try:
        for child in psutil.Process(p.pid).children(recursive=True):
            child.kill()
    except psutil.Error:
        pass
    p.kill()
    p.wait(10)


def server_cmd(mode):
    loop = "deja.loop:recording_loop" if mode == "on" else "asyncio"
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "benchmarks.app:app",
        "--port",
        str(APP_PORT),
        "--loop",
        loop,
        "--log-level",
        "warning",
        "--no-access-log",
    ]


def sample(proc, stop_evt, out):
    """Sample the server's CPU% and RSS while the measured load runs."""
    try:
        p = psutil.Process(proc.pid)
        targets = [p] + p.children(recursive=True)
        for t in targets:
            t.cpu_percent(None)
        while not stop_evt.wait(0.5):
            cpu = sum(t.cpu_percent(None) for t in targets)
            rss = sum(t.memory_info().rss for t in targets)
            out.append((cpu, rss))
    except psutil.Error:
        pass


def k6(args, rate, duration, base, summary_path):
    """Run k6 (installed binary, or a docker image when --k6 starts with 'docker') and return its summary."""
    env_args = ["-e", f"RATE={rate}", "-e", f"DURATION={duration}s", "-e", f"BASE_URL={base}"]
    if args.k6.startswith("docker"):
        outdir = tempfile.mkdtemp(prefix="deja-k6-")
        image = shlex.split(args.k6)[-1]
        cmd = [
            "docker",
            "run",
            "--rm",
            "-i",
            "-v",
            f"{outdir}:/out",
            image,
            "run",
            "-q",
            *env_args,
            "--summary-export",
            "/out/summary.json",
            "-",
        ]
        res = subprocess.run(cmd, input=open(os.path.join(HERE, "load.js"), "rb").read(), capture_output=True)
        summary_path = os.path.join(outdir, "summary.json")
    else:
        cmd = [
            *shlex.split(args.k6),
            "run",
            "-q",
            *env_args,
            "--summary-export",
            summary_path,
            os.path.join(HERE, "load.js"),
        ]
        if os.path.exists(summary_path):
            os.remove(summary_path)
        res = subprocess.run(pin(cmd, args.load_cpu), capture_output=True)
    if not os.path.exists(summary_path):
        raise RuntimeError(f"k6 produced no summary: {res.stderr.decode('utf-8', 'replace')[-800:]}")
    return json.load(open(summary_path))


def one_run(args, mode, rate):
    env = {"DEJA": "1" if mode == "on" else "0", "DEJA_DIR": tempfile.mkdtemp(prefix="deja-bench-rec-")}
    server = start(server_cmd(mode), env, cpu=args.server_cpu)
    try:
        wait_http(f"http://127.0.0.1:{APP_PORT}/items/1")
        base = f"http://{args.base_host}:{APP_PORT}"
        k6(args, rate, args.warmup, base, os.path.join(tempfile.gettempdir(), "deja-k6-warm.json"))
        samples, evt = [], threading.Event()
        t = threading.Thread(target=sample, args=(server, evt, samples), daemon=True)
        t.start()
        summary = k6(args, rate, args.duration, base, os.path.join(tempfile.gettempdir(), "deja-k6.json"))
        evt.set()
        t.join()
        size = wait_http(f"http://127.0.0.1:{APP_PORT}/__deja/size") if mode == "on" else None
    finally:
        stop(server)
    m = summary["metrics"]
    dur = m["http_req_duration"]
    reqs = m["http_reqs"]
    failed = m.get("http_req_failed", {})
    cpus = [c for c, _ in samples] or [0]
    return {
        "mode": mode,
        "rate": rate,
        "rps": reqs.get("rate", 0),
        "p50": dur.get("p(50)", dur.get("med")),
        "p95": dur["p(95)"],
        "p99": dur["p(99)"],
        "failed": failed.get("value", failed.get("rate", 0)),
        "dropped": m.get("dropped_iterations", {}).get("count", 0),
        "cpu": statistics.mean(cpus),
        "rss_mb": max((r for _, r in samples), default=0) / 2**20,
        "bytes_per_req": (size["bytes"] / size["requests"]) if size and size["requests"] else None,
    }


def med(rows, key):
    vals = [r[key] for r in rows if r[key] is not None]
    return statistics.median(vals) if vals else None


def pct(on, off):
    return f"{(on / off - 1) * 100:+.1f}%" if on is not None and off else "n/a"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rates", default="100,250,400")
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--warmup", type=int, default=5)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--k6", default="k6", help='k6 binary, or e.g. "docker grafana/k6"')
    p.add_argument("--base-host", default="127.0.0.1")
    p.add_argument("--server-cpu", type=int, default=0, help="Linux: CPU to pin the server to")
    p.add_argument("--load-cpu", type=int, default=1, help="Linux: CPU to pin k6 to")
    p.add_argument("--out", default="bench-results.json")
    a = p.parse_args()
    rates = [int(r) for r in a.rates.split(",")]

    stub = start(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "benchmarks.stub:app",
            "--port",
            str(STUB_PORT),
            "--log-level",
            "warning",
            "--no-access-log",
        ],
        {},
        cpu=None,
    )
    rows = []
    try:
        wait_http(f"http://127.0.0.1:{STUB_PORT}/quote?item=1")
        for rep in range(a.reps):
            for rate in rates:
                order = ("off", "on") if rep % 2 == 0 else ("on", "off")
                for mode in order:
                    r = one_run(a, mode, rate)
                    rows.append(r)
                    print(
                        f"rep {rep + 1} rate {rate:4d} {mode:3s}: {r['rps']:7.1f} req/s  p50 {r['p50']:6.2f}  "
                        f"p95 {r['p95']:6.2f}  p99 {r['p99']:6.2f} ms  cpu {r['cpu']:5.1f}%  "
                        f"rss {r['rss_mb']:5.0f} MB  failed {r['failed']:.3f}  dropped {r['dropped']}",
                        flush=True,
                    )
    finally:
        stop(stub)

    import platform

    lines = [
        f"### Déjà overhead, {platform.system()} {platform.machine()}, Python {platform.python_version()}, "
        f"{psutil.cpu_count()} CPUs, {a.duration}s per run, median of {a.reps}",
        "",
        "| req/s | mode | achieved req/s | p50 ms | p95 ms | p99 ms | CPU % | RSS MB | failed | KB/request |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    summary = {}
    for rate in rates:
        agg = {}
        for mode in ("off", "on"):
            sel = [r for r in rows if r["rate"] == rate and r["mode"] == mode]
            agg[mode] = {
                k: med(sel, k) for k in ("rps", "p50", "p95", "p99", "cpu", "rss_mb", "failed", "bytes_per_req")
            }
            g = agg[mode]
            kb = f"{g['bytes_per_req'] / 1024:.2f}" if g["bytes_per_req"] else "-"
            lines.append(
                f"| {rate} | {mode} | {g['rps']:.1f} | {g['p50']:.2f} | {g['p95']:.2f} | {g['p99']:.2f} | "
                f"{g['cpu']:.1f} | {g['rss_mb']:.0f} | {g['failed']:.3f} | {kb} |"
            )
        on, off = agg["on"], agg["off"]
        lines.append(
            f"| {rate} | **overhead** | {pct(on['rps'], off['rps'])} | {pct(on['p50'], off['p50'])} | "
            f"{pct(on['p95'], off['p95'])} | {pct(on['p99'], off['p99'])} | {pct(on['cpu'], off['cpu'])} | "
            f"{pct(on['rss_mb'], off['rss_mb'])} | | |"
        )
        summary[rate] = agg
    table = "\n".join(lines)
    print("\n" + table)
    json.dump({"rows": rows, "summary": summary}, open(a.out, "w"), indent=1)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(table + "\n")


if __name__ == "__main__":
    main()
