# Copyright 2025 Weitang Ye
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import docker
import argparse
import os
import sys
import time
import socket
from contextlib import closing

# Configuration for supported databases
# Defines image tags, default ports, and environment variables
DB_CONFIG = {
    "neo4j": {
        "image": "neo4j:4.4",
        "port": 7687,  # Bolt port
        "env": {"NEO4J_AUTH": "neo4j/password", "NEO4J_dbms_memory_heap_initial__size": "1G"}
    },
    "arangodb": {
        "image": "arangodb:3.8",
        "port": 8529,
        "env": {"ARANGO_ROOT_PASSWORD": "password"}
    },
    "orientdb": {
        "image": "orientdb:3.2",
        "port": 2424, # Binary port
        "env": {"ORIENTDB_ROOT_PASSWORD": "password"}
    }
}

# The client image that contains Python, pandas, and DB drivers
# You need to build this image beforehand (e.g., via a Dockerfile)
CLIENT_IMAGE = "graph-bench-client:latest"

def check_port(host, port, retries=30, delay=1):
    """
    Checks if a TCP port is open. used to wait for DB readiness.
    """
    print(f"Waiting for {host}:{port} to be ready...")
    for i in range(retries):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                print(f"Port {port} is ready!")
                return True
        time.sleep(delay)
    return False

def run_benchmark(args):
    client = docker.from_env()
    db_name = args.db
    config = DB_CONFIG.get(db_name)

    if not config:
        print(f"Error: Database '{db_name}' not defined in DB_CONFIG.")
        sys.exit(1)

    # Define absolute paths for mounting
    # 1. Mount current code directory to /app
    code_mount = os.path.abspath(os.getcwd())
    # 2. Mount dataset directory to /data
    data_mount = os.path.abspath(args.dataset_dir)

    db_container = None
    bench_container = None

    try:
        # ---------------------------------------------------------
        # Step 1: Start the Database Container
        # ---------------------------------------------------------
        print(f"--- [Phase 1] Starting {db_name} container ---")
        db_container = client.containers.run(
            config["image"],
            name=f"bench-target-{db_name}",
            environment=config["env"],
            detach=True,
            remove=True,  # Auto-remove on stop
            # We map ports to host just for health checking,
            # the client container will access via localhost internally.
            ports={f"{config['port']}/tcp": config['port']}
        )

        # Wait for DB to be responsive
        # Since we use 'from_env', localhost refers to the host machine here
        if not check_port("localhost", config["port"]):
            print("Error: Database failed to start in time.")
            sys.exit(1)

        # ---------------------------------------------------------
        # Step 2: Start the Benchmark Client (Sidecar)
        # ---------------------------------------------------------
        print(f"--- [Phase 2] Starting Benchmark Client (Sidecar) ---")

        # Construct the internal command to run inside the client container
        # We assume there is a 'docker_runner.py' that parses these args and calls BaseGraphDB implementation
        # Note: --host is explicitly set to 'localhost' because of shared network namespace
        internal_cmd = [
            "python3", "docker_runner.py",
            "--db", db_name,
            "--host", "localhost",
            "--port", str(config["port"]),
            "--password", "password",
            "--dataset-path", f"/data/{args.dataset_filename}",
            "--tasks", *args.tasks # Unpack list of tasks
        ]

        print(f"Executing internal command: {' '.join(internal_cmd)}")

        bench_container = client.containers.run(
            CLIENT_IMAGE,
            name=f"bench-client-{db_name}",
            # CRITICAL: Share network stack with the DB container
            # This enables 'localhost' access with zero latency
            network_mode=f"container:{db_container.id}",
            volumes={
                code_mount: {'bind': '/app', 'mode': 'rw'}, # Mount code
                data_mount: {'bind': '/data', 'mode': 'ro'} # Mount data
            },
            working_dir="/app",
            command=internal_cmd,
            detach=True,
            remove=True
        )

        # ---------------------------------------------------------
        # Step 3: Stream Logs and Wait for Completion
        # ---------------------------------------------------------
        print("--- [Phase 3] Benchmark Running... Logs output: ---")

        # Stream logs to console
        for line in bench_container.logs(stream=True, follow=True):
            print(line.decode().strip())

        # Wait for container to exit and get status
        result = bench_container.wait()
        exit_code = result.get('StatusCode', 0)

        if exit_code == 0:
            print("--- Benchmark Completed Successfully ---")
        else:
            print(f"--- Benchmark Failed with Exit Code {exit_code} ---")

    except docker.errors.APIError as e:
        print(f"Docker API Error: {e}")
    except KeyboardInterrupt:
        print("\nInterrupted by user. Stopping containers...")
    finally:
        # ---------------------------------------------------------
        # Step 4: Cleanup
        # ---------------------------------------------------------
        print("--- [Phase 4] Cleaning up containers ---")
        if bench_container:
            try:
                bench_container.stop()
            except:
                pass
        if db_container:
            try:
                db_container.stop()
            except:
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Graph Database Benchmark Orchestrator")

    # Required arguments
    parser.add_argument("--db", type=str, required=True, choices=["neo4j", "arangodb", "orientdb"],
                        help="Target database to benchmark")
    parser.add_argument("--dataset-dir", type=str, required=True,
                        help="Host directory containing dataset files")
    parser.add_argument("--dataset-filename", type=str, required=True,
                        help="Filename of the graph data (e.g., social_network.csv)")

    # Task selection (Multiple choices allowed)
    parser.add_argument(
        "--tasks", nargs="+",
        default=["load_graph", "read_nbrs_bench"],
        choices=["load_graph", "append_edges_bench", "read_nbrs_bench",
            "delete_nodes_bench", "delete_edges_bench", "mixed_workload_bench"],
        help="List of benchmark tasks to execute")

    args = parser.parse_args()

    run_benchmark(args)