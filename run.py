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

DEFAULT_CONFIG_FILE = "db_config.json"

def load_db_config(config_path):
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON: {e}")
        sys.exit(1)

def check_port(host, port, retries=60, delay=1):
    print(f"Waiting for {host}:{port} to be ready...")
    for i in range(retries):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                print(f"Port {port} is ready!")
                return True
        time.sleep(delay)
    return False

def build_image(client, dockerfile_path, tag_name):
    """
    Builds a docker image from a specific Dockerfile.
    """
    if not os.path.exists(dockerfile_path):
        print(f"Error: Dockerfile not found at {dockerfile_path}")
        sys.exit(1)

    print(f"--- Building Image: {tag_name} from {dockerfile_path} ---")
    try:
        img, logs = client.images.build(
            path=".",
            dockerfile=dockerfile_path,
            tag=tag_name,
            rm=True
        )
        return img
    except docker.errors.BuildError as e:
        print(f"Build Failed for {tag_name}:")
        for line in e.build_log:
            if 'stream' in line:
                print(line['stream'].strip())
        sys.exit(1)

def configure_concurrency(db_config, db_name, threads):
    config = db_config[db_name]
    env = config.get("env", {}).copy()
    command = config.get("command", []).copy()

    if db_name == "neo4j":
        # Neo4j 5.x config format: '.' -> '_' and '_' -> '__'
        env["NEO4J_server_bolt_thread__pool__max__size"] = str(threads)
        env["NEO4J_server_bolt_thread__pool__min__size"] = str(threads)
        env["NEO4J_server_default__listen__address"] = "0.0.0.0"

    elif db_name == "arangodb":
        command.append(f"--server.maximal-threads={threads}")
    elif db_name == "orientdb":
        opts = env.get("JAVA_OPTS", "")
        opts += f" -Dnetwork.maxConcurrentSessions={threads} -Dserver.network.maxThreads={threads}"
        env["JAVA_OPTS"] = opts

    return env, command

def load_workload_config(path):
    if not os.path.exists(path):
        print(f"Error: Workload config file '{path}' not found.")
        sys.exit(1)
    with open(path, 'r') as f:
        return json.load(f)

def force_remove_container(client, container_name):
    """
    Forcefully removes a container by name if it exists.
    Handles 'removal already in progress' race conditions.
    """
    try:
        container = client.containers.get(container_name)
        print(f"--- Found leftover container '{container_name}', removing... ---")
        try:
            container.stop(timeout=1)
        except: pass

        container.remove(force=True)

    except docker.errors.NotFound:
        pass # Good, it's gone
    except docker.errors.APIError as e:
        # Ignore "removal already in progress" errors (HTTP 409)
        if e.response.status_code == 409 or "already in progress" in str(e):
            print(f"--- Container '{container_name}' is already being removed. Skipping. ---")
        else:
            print(f"Warning: Failed to cleanup {container_name}: {e}")
    except Exception as e:
        print(f"Warning: General error cleaning {container_name}: {e}")

def cleanup_container(container, name):
    """Safe cleanup helper for container objects"""
    if container:
        print(f"--- Stopping container {name}... ---")
        try:
            container.stop()
        except docker.errors.NotFound:
            print(f"Container {name} already removed.")
        except Exception as e:
            print(f"Error stopping {name}: {e}")

def run_benchmark(args):
    # Load Configurations
    db_config = load_db_config(args.db_config)
    workload_config = load_workload_config(args.workload_config)

    # Extract Server Threads from Workload Config
    server_threads = workload_config.get("server_config", {}).get("threads", 4)
    print(f"--- Loaded Workload Config: Using {server_threads} Server Threads ---")

    client = docker.from_env()
    db_name = args.db

    if db_name not in db_config:
        print(f"Error: Database '{db_name}' not defined in config.")
        sys.exit(1)

    # Define Image Tags & Paths
    server_img_tag = f"bench-server-{db_name}:latest"
    client_img_tag = f"bench-client-{db_name}:latest"
    server_dockerfile = f"docker/dockerfile.db.{db_name}"
    client_dockerfile = f"docker/dockerfile.client.{db_name}"

    # Pre-flight cleanup
    force_remove_container(client, f"bench-target-{db_name}")
    force_remove_container(client, f"bench-client-{db_name}")

    # Build
    build_image(client, server_dockerfile, server_img_tag)
    build_image(client, client_dockerfile, client_img_tag)

    # Directories
    host_result_dir = os.path.abspath(args.result_dir)
    os.makedirs(host_result_dir, exist_ok=True)
    code_mount = os.path.abspath(os.getcwd())
    data_mount = os.path.abspath(args.dataset_dir)

    # Absolute path to workload config to mount it
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
            "remove": True,
            "ports": {f"{base_config['port']}/tcp": base_config['port']}
        }
        if db_cmd:
            if db_name == "arangodb": run_kwargs["command"] = " ".join(db_cmd)

        db_container = client.containers.run(**run_kwargs)

        if not check_port("localhost", base_config["port"]):
            raise RuntimeError("Database failed to start within timeout.")

        # --- Step 3: Start Client ---
        print(f"--- [Phase 2] Starting {db_name} Client ---")

        # Pass the internal path to the workload config
        internal_cmd = [
            "python3", "docker_runner.py",
            "--db", db_name,
            "--host", "localhost",
            "--port", str(base_config["port"]),
            "--password", "password",
            "--dataset-path", f"/data/{args.dataset_filename}",
            "--workload-config", "/app/workload_config.json", # Fixed internal path
            "--report-file", container_report_path
        ]

        bench_container = client.containers.run(
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
            remove=True
        )

        # Stream logs
        for line in bench_container.logs(stream=True, follow=True):
            print(line.decode().strip())

        result = bench_container.wait()
        if result.get('StatusCode', 0) == 0:
            print(f"--- Success. Report: {os.path.join(host_result_dir, report_filename)} ---")
        else:
            print(f"--- Benchmark Failed ---")
            try: print(db_container.logs(tail=50).decode())
            except: pass

    except KeyboardInterrupt:
        print("\n!!! Interrupted !!!")
    except Exception as e:
        print(f"\n!!! Error: {e} !!!")
        if db_container:
            try: print(db_container.logs(tail=20).decode())
            except: pass
    finally:
        print("\n--- Cleaning up ---")
        cleanup_container(bench_container, "Client")
        cleanup_container(db_container, "Server")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, choices=["neo4j", "arangodb", "orientdb"])
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--dataset-filename", required=True)
    parser.add_argument("--db-config", default="db_config.json")
    parser.add_argument("--workload-config", default="workload_config.json",
                        help="Path to the JSON file defining workload tasks and threads")
    parser.add_argument("--result-dir", type=str, default="./results")
    args = parser.parse_args()
    run_benchmark(args)