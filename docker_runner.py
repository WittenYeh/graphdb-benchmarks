# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

import argparse
import sys
import importlib
import time
import json
import datetime
import os
import random
import traceback

def get_sample_ids(filepath, count=5000):
    ids = set()
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if line.startswith('%') or line.startswith('#'): continue
                parts = line.strip().replace(',', ' ').split()
                if len(parts) >= 2:
                    ids.add(parts[0])
                    ids.add(parts[1])
                if len(ids) >= count: break
    except: return ["0", "1"]
    return list(ids)

def generate_workload(task_name, ids, num_ops, ratios=None):
    """
    Generates workload based on task name.
    Supports mixed workloads with configurable ratios.
    """
    workload = []
    if not ids: ids = ["0"]

    # Map config keys to internal OP types
    # user_config_key -> internal_op_type
    KEY_MAP = {
        "read_nbrs": "READ_NBRS",
        "add_nodes": "ADD_NODE",
        "delete_nodes": "DEL_NODE",
        "add_edges": "ADD_EDGE",
        "delete_edges": "DEL_EDGE"
    }

    # Determine operation list
    ops_to_generate = []

    if "mixed" in task_name and ratios:
        # Weighted random generation
        # ratios example: {"read_nbrs": 0.8, "add_edges": 0.2}
        population = []
        weights = []
        for key, weight in ratios.items():
            if key in KEY_MAP:
                population.append(KEY_MAP[key])
                weights.append(weight)

        if not population:
            # Fallback if config is empty
            ops_to_generate = ["READ_NBRS"] * num_ops
        else:
            # Generate all op types at once based on weights
            ops_to_generate = random.choices(population, weights=weights, k=num_ops)

    else:
        # Single task generation
        op_type = "READ_NBRS" # Default
        if "read_nbrs" in task_name: op_type = "READ_NBRS"
        elif "add_nodes" in task_name: op_type = "ADD_NODE"
        elif "delete_nodes" in task_name: op_type = "DEL_NODE"
        elif "add_edges" in task_name: op_type = "ADD_EDGE"
        elif "delete_edges" in task_name: op_type = "DEL_EDGE"

        ops_to_generate = [op_type] * num_ops

    # Create parameter objects for each op
    for op_type in ops_to_generate:
        params = {}

        # 1. READ / DELETE NODE: Need existing ID
        if op_type in ["READ_NBRS", "DEL_NODE"]:
            params = {"id": random.choice(ids)}

        # 2. ADD NODE: Need new unique ID
        elif op_type == "ADD_NODE":
            # Generate a distinct-looking ID to avoid collision with existing dataset
            params = {"id": f"new_{random.randint(1000000, 99999999)}"}

        # 3. EDGES: Need src and dst
        elif op_type in ["ADD_EDGE", "DEL_EDGE"]:
            params = {
                "src": random.choice(ids),
                "dst": random.choice(ids)
            }

        workload.append({"type": op_type, "params": params})

    return workload

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--password", default="")
    parser.add_argument("--dataset-path", required=True)
    parser.add_argument("--workload-config", required=True)
    parser.add_argument("--report-file", required=False)

    args = parser.parse_args()

    # Load Config
    with open(args.workload_config, 'r') as f:
        workload_config = json.load(f)

    # Init Report
    report = {
        "metadata": {
            "database": args.db,
            "dataset": os.path.basename(args.dataset_path),
            "timestamp": datetime.datetime.now().isoformat(),
            "server_config": workload_config.get("server_config", {})
        },
        "results": []
    }

    # Load Module
    module_name = f"impl.{args.db}_impl"
    class_name = f"{args.db.capitalize()}DB"
    try:
        module = importlib.import_module(module_name)
        DBClass = getattr(module, class_name)
    except Exception as e:
        print(f"Error loading module: {e}")
        sys.exit(1)

    # Connect
    print(f"[DockerRunner] Connecting to {args.db}...")
    db = DBClass(host=args.host, port=args.port, password=args.password)
    try:
        for i in range(60):
            try:
                db.connect()
                break
            except: time.sleep(1)
        else: raise Exception("Connection timeout")
    except Exception as e:
        print(f"Init failed: {e}")
        sys.exit(1)

    cached_ids = None

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

        if method:
            try:
                task_input = None
                if task_name == "load_graph":
                    task_input = args.dataset_path
                else:
                    if not cached_ids:
                        print("[DockerRunner] Sampling IDs...")
                        cached_ids = get_sample_ids(args.dataset_path)

                    num_ops = task.get("ops", 1000)
                    ratios = task.get("ratios", None)
                    print(f"[DockerRunner] Generating {num_ops} ops...")
                    task_input = generate_workload(task_name, cached_ids, num_ops, ratios)

                    if "_throughput" in task_name:
                        client_threads = task.get("client_threads", 4)
                        db.current_client_threads = client_threads

                metrics = method(task_input)

                if isinstance(metrics, float):
                    metrics = {"duration_s": metrics}

                task_result["status"] = "success"
                task_result["metrics"] = metrics
                print(f"<<< [Task End] {task_name} Success.")

            except Exception as e:
                print(f"<<< [Task Failed] {e}")
                task_result["error"] = str(e)
                traceback.print_exc()
        else:
            task_result["error"] = "Method not found"

        report["results"].append(task_result)

    try: db.close()
    except: pass

    if args.report_file:
        with open(args.report_file, 'w') as f:
            json.dump(report, f, indent=4)
        print("[DockerRunner] Report saved.")

if __name__ == "__main__":
    main()