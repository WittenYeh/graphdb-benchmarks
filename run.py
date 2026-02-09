# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

import docker
import argparse
import os
import sys
import time
import socket
import json
from contextlib import closing

from tools.db_tools import load_db_config, check_port, configure_concurrency
from tools.docker_tools import build_image, force_remove_container, cleanup_container
from tools.workload_tools import load_workload_config

def run_benchmark(args):
    # Load Configurations
    db_config = load_db_config(args.db_config)
    workload_config = load_workload_config(args.workload_config)

    # Extract Server Threads from Workload Config
    server_threads = workload_config.get("server_config", {}).get("threads", 4)
    print(f"--- Loaded Workload Config: Using {server_threads} Server Threads ---")

    client_docker = docker.from_env()
    db_name = args.db

    if db_name not in db_config:
        print(f"Error: Database '{db_name}' not defined in config.")
        sys.exit(1)

    # Define Image Tags & Paths
    server_img_tag = f"bench-server-{db_name}:latest"
    client_img_tag = f"bench-client-{db_name}:latest"
    server_dockerfile = f"dockerfiles/dockerfile.db.{db_name}"
    client_dockerfile = f"dockerfiles/dockerfile.client.{db_name}"

    # Pre-flight cleanup
    force_remove_container(client_docker, f"bench-target-{db_name}")
    force_remove_container(client_docker, f"bench-client-{db_name}")

    # Build
    build_image(client_docker, server_dockerfile, server_img_tag, pull=not args.use_cache)
    build_image(client_docker, client_dockerfile, client_img_tag, pull=not args.use_cache)

    # Directories
    host_result_dir = os.path.abspath(args.result_dir)
    os.makedirs(host_result_dir, exist_ok=True)
    code_mount = os.path.abspath(os.getcwd())
    data_mount = os.path.abspath(args.dataset_dir)
    workload_config_host_path = os.path.abspath(args.workload_config)

    # Report Filename
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    report_filename = f"bench_{db_name}_{timestamp}.json"
    container_report_path = f"/results/{report_filename}"

    # Configure DB Env
    db_env, db_cmd = configure_concurrency(db_config, db_name, server_threads)
    base_config = db_config[db_name]

    db_container = None
    bench_container = None

    try:
        # --- Step 2: Start Database ---
        print(f"--- [Phase 1] Starting {db_name} Server ---")

        run_kwargs = {
            "image": server_img_tag,
            "name": f"bench-target-{db_name}",
            "environment": db_env,
            "detach": True,
            # CRITICAL CHANGE: Must be False so we can read logs if it crashes
            "remove": False,
            "ports": {f"{base_config['port']}/tcp": base_config['port']}
        }
        if db_cmd:
            if db_name == "arangodb": run_kwargs["command"] = " ".join(db_cmd)

        db_container = client_docker.containers.run(**run_kwargs)

        # Enhanced Health Check: Monitor Container Status + Port
        print(f"Waiting for {db_name} to be ready...")
        start_time = time.time()
        server_ready = False

        while time.time() - start_time < 60:
            # 1. Check if container is still alive
            db_container.reload()
            if db_container.status != 'running':
                exit_code = db_container.attrs['State']['ExitCode']
                raise RuntimeError(f"Server container crashed immediately! Exit Code: {exit_code}")

            # 2. Check Port
            if check_port("localhost", base_config["port"]):
                server_ready = True
                print(f"Port {base_config['port']} is ready!")
                break
            time.sleep(1)

        if not server_ready:
            raise RuntimeError("Database failed to bind port within timeout.")

        # --- Step 3: Start Client ---
        print(f"--- [Phase 2] Starting {db_name} Client ---")

        internal_cmd = [
            "python3", "docker_runner.py",
            "--db", db_name,
            "--host", "localhost",
            "--port", str(base_config["port"]),
            "--password", "password",
            "--dataset-path", f"/data/{args.dataset_filename}",
            "--workload-config", "/app/workload_config.json",
            "--report-file", container_report_path
        ]

        bench_container = client_docker.containers.run(
            client_img_tag,
            name=f"bench-client-{db_name}",
            network_mode=f"container:{db_container.id}",
            environment={"PYTHONUNBUFFERED": "1"},
            volumes={
                code_mount: {'bind': '/app', 'mode': 'rw'},
                data_mount: {'bind': '/data', 'mode': 'ro'},
                host_result_dir: {'bind': '/results', 'mode': 'rw'},
                workload_config_host_path: {'bind': '/app/workload_config.json', 'mode': 'ro'}
            },
            working_dir="/app",
            command=internal_cmd,
            detach=True,
            remove=False # Also set False here to capture exit codes safely
        )

        # Stream logs
        for line in bench_container.logs(stream=True, follow=True):
            print(line.decode().strip())

        result = bench_container.wait()
        exit_code = result.get('StatusCode', 0)

        if exit_code == 0:
            print(f"--- Success. Report: {os.path.join(host_result_dir, report_filename)} ---")
        else:
            raise RuntimeError(f"Client script failed with exit code {exit_code}")

    except KeyboardInterrupt:
        print("\n!!! Interrupted !!!")
    except Exception as e:
        print(f"\n!!! Benchmark Failed: {e} !!!")

        # --- AUTOMATED ERROR LOGGING ---
        print("\n" + "="*30 + " SERVER LOGS (LAST 50 LINES) " + "="*30)
        if db_container:
            try:
                logs = db_container.logs(tail=50).decode(errors='replace')
                print(logs if logs else "[No logs produced]")
            except Exception as log_err:
                print(f"Could not retrieve server logs: {log_err}")
        print("="*85 + "\n")

    finally:
        print("\n--- Cleaning up (Force Removing Containers) ---")
        # cleanup_container handles stop() and remove()
        # Since we set remove=False earlier, we rely on this to keep system clean
        if bench_container:
            cleanup_container(bench_container, "Client")
        if db_container:
            cleanup_container(db_container, "Server")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, choices=["neo4j", "arangodb", "orientdb", "aster"])
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--dataset-filename", required=True)
    parser.add_argument("--db-config", default="db_config.json")
    parser.add_argument("--workload-config", default="workload_config.json")
    parser.add_argument("--result-dir", type=str, default="./results")
    parser.add_argument("--use-cache", action="store_true",
                        help="Use existing Docker images without pulling latest base images")
    args = parser.parse_args()
    run_benchmark(args)