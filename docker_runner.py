# docker_runner.py

# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

import argparse
import sys
import importlib
import time
import json
import datetime
import os
import traceback
import socket

from tools.workload_tools import generate_workload, setup_seed
from tools.dataset_tools import get_sample_ids

def wait_for_port(host, port, timeout=60):
    """
    Blocks until the TCP port is open and listening.
    This is crucial for databases with lazy drivers (like Gremlin)
    that don't throw errors during initialization.
    """
    print(f"[DockerRunner] Waiting for {host}:{port} to be ready (TCP check)...")
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                print(f"[DockerRunner] Port {port} is open!")
                return True
        except (ConnectionRefusedError, OSError, socket.timeout):
            if time.time() - start_time > timeout:
                return False
            time.sleep(1)

def main():
    # 1. Argument Parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--password", default="")
    parser.add_argument("--dataset-path", required=True)
    parser.add_argument("--workload-config", required=True)
    parser.add_argument("--report-file", required=False)

    args = parser.parse_args()

    # 2. Load Configuration
    with open(args.workload_config, 'r') as f:
        workload_config = json.load(f)

    # 3. Setup Report Object
    report = {
        "metadata": {
            "database": args.db,
            "dataset": os.path.basename(args.dataset_path),
            "timestamp": datetime.datetime.now().isoformat(),
            "server_config": workload_config.get("server_config", {})
        },
        "results": []
    }

    # 4. Load Database Driver Dynamically
    module_name = f"e2e_impl.{args.db}_impl"
    class_name = f"{args.db.capitalize()}DB"
    try:
        module = importlib.import_module(module_name)
        DBClass = getattr(module, class_name)
    except Exception as e:
        print(f"[DockerRunner] Critical: Failed to load driver {module_name}.{class_name}: {e}")
        sys.exit(1)

    # 5. Pre-flight Network Check & Connect
    # Step A: Ensure TCP Port is actually listening (Fixes Lazy Connection issues)
    if not wait_for_port(args.host, args.port):
        print(f"[DockerRunner] Error: Port {args.port} was not ready after 60s.")
        sys.exit(1)

    # Step B: Driver Connection with Retry
    print(f"[DockerRunner] Connecting to {class_name} at {args.host}:{args.port}...")
    db = DBClass(host=args.host, port=args.port, password=args.password)

    try:
        # Give the DB application layer a moment to stabilize after TCP is ready
        time.sleep(2)

        for i in range(60):
            try:
                db.connect()
                print("[DockerRunner] Connected successfully.")
                break
            except Exception as e:
                # Print the error so we know why it's retrying
                if i % 5 == 0:
                    print(f"[DockerRunner] Connection attempt {i+1} failed: {e}. Retrying...")
                time.sleep(1)
        else:
            raise Exception("Driver connection timed out after 60s")
    except Exception as e:
        print(f"[DockerRunner] Init Failed: {e}")
        sys.exit(1)

    # 6. Run Benchmark Tasks
    cached_ids = None
    setup_seed(42)

    for task in workload_config.get("tasks", []):
        task_name = task["name"]
        print(f"\n>>> [Task Start] {task_name}")

        method = getattr(db, task_name, None)
        task_result = {
            "task": task_name,
            "config": task,
            "status": "failed",
            "metrics": {}
        }

        if not method:
            print(f"<<< [Task Skipped] Method '{task_name}' not implemented.")
            task_result["error"] = "Method not implemented"
            report["results"].append(task_result)
            continue

        try:
            task_input = None

            if task_name == "load_graph":
                task_input = args.dataset_path
            else:
                if not cached_ids:
                    print("[DockerRunner] Sampling IDs from dataset...")
                    cached_ids = get_sample_ids(args.dataset_path)

                num_ops = task.get("ops", 1000)
                ratios = task.get("ratios", None)

                if "_throughput" in task_name:
                    client_threads = task.get("client_threads", 8)
                    db.current_client_threads = client_threads
                    print(f"[DockerRunner] Configured {client_threads} client threads.")

                print(f"[DockerRunner] Generating {num_ops} abstract ops...")
                task_input = generate_workload(task_name, cached_ids, num_ops, ratios)

            # --- EXECUTE ---
            start_t = time.time()
            metrics = method(task_input)

            if metrics is None:
                metrics = {"duration_s": time.time() - start_t}
            elif isinstance(metrics, float):
                metrics = {"duration_s": metrics}

            task_result["status"] = "success"
            task_result["metrics"] = metrics
            print(f"<<< [Task End] {task_name} Success. Metrics: {metrics}")

        except Exception as e:
            print(f"<<< [Task Failed] {e}")
            traceback.print_exc()
            task_result["error"] = str(e)

        report["results"].append(task_result)

    # 7. Clean up
    try:
        db.close()
    except: pass

    if args.report_file:
        with open(args.report_file, 'w') as f:
            json.dump(report, f, indent=4)
        print(f"[DockerRunner] Report saved to {args.report_file}")

if __name__ == "__main__":
    main()