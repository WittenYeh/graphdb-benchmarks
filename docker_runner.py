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

import argparse
import sys
import importlib
import time
import os

def main():
    """
    Main entry point for the internal benchmark runner.

    This script is intended to be executed inside the Docker container.
    It performs the following steps:
    1. Parses arguments passed from the host orchestrator (run.py).
    2. Dynamically loads the specific database implementation wrapper from the 'impl' package.
    3. Connects to the database (via localhost, thanks to network_mode="container:id").
    4. Executes the requested benchmark tasks sequentially.
    """

    # 1. Parse Command Line Arguments
    parser = argparse.ArgumentParser(description="Internal Benchmark Executor")

    parser.add_argument("--db", type=str, required=True,
                        help="Name of the database (e.g., neo4j, arangodb). Used to load the module.")
    parser.add_argument("--host", type=str, required=True,
                        help="Hostname or IP to connect to (usually 'localhost' in sidecar mode).")
    parser.add_argument("--port", type=int, required=True,
                        help="Port number for the database connection.")
    parser.add_argument("--password", type=str, default="",
                        help="Password for database authentication.")
    parser.add_argument("--dataset-path", type=str, required=True,
                        help="Absolute path to the dataset file inside the container.")
    parser.add_argument("--tasks", nargs="+", required=True,
                        help="List of benchmark methods to execute (e.g., load_graph read_nbrs_bench).")

    args = parser.parse_args()

    # 2. Dynamic Module Loading
    # We expect the wrapper to be located in: impl/{db_name}_impl.py
    # We expect the class name to be: {Db_name_capitalized}DB (e.g., Neo4jDB)
    module_name = f"impl.{args.db}_impl"
    class_name = f"{args.db.capitalize()}DB"

    print(f"[DockerRunner] Loading module: {module_name}, Class: {class_name}")

    try:
        # Import the module dynamically
        module = importlib.import_module(module_name)
        # Get the class reference
        DBClass = getattr(module, class_name)
    except ImportError as e:
        print(f"[DockerRunner] Error: Could not import module '{module_name}'. "
              f"Ensure the file 'impl/{args.db}_impl.py' exists.")
        print(f"[DockerRunner] Detail: {e}")
        sys.exit(1)
    except AttributeError:
        print(f"[DockerRunner] Error: Class '{class_name}' not found in '{module_name}'.")
        sys.exit(1)

    # 3. Instantiate and Connect
    try:
        print(f"[DockerRunner] Connecting to {args.db} at {args.host}:{args.port}...")
        # Initialize the wrapper
        db = DBClass(host=args.host, port=args.port, password=args.password)
        # Establish connection
        db.connect()
        print(f"[DockerRunner] Successfully connected to {args.db}.")
    except Exception as e:
        print(f"[DockerRunner] connection failed: {e}")
        sys.exit(1)

    # 4. Execute Benchmark Tasks
    # We treat the dataset path as the 'workload' object for simplicity here.
    # In a more complex scenario, you might parse the CSV into a specific Workload object.
    workload = args.dataset_path

    for task_name in args.tasks:
        print(f"\n>>> [Task Start] Executing: {task_name}")

        # Check if the method exists in the wrapper class
        method = getattr(db, task_name, None)

        if method and callable(method):
            start_time = time.time()
            try:
                # Execute the method defined in BaseGraphDB interface
                method(workload)
                elapsed = time.time() - start_time
                print(f"<<< [Task End] {task_name} completed in {elapsed:.4f} seconds.")
            except Exception as e:
                print(f"<<< [Task Failed] {task_name} failed with error: {e}")
        else:
            print(f"[DockerRunner] Warning: Method '{task_name}' is not implemented in {class_name}.")

    # 5. Cleanup
    try:
        print("\n[DockerRunner] Closing connection...")
        db.close()
    except Exception as e:
        print(f"[DockerRunner] Error closing connection: {e}")

    print("[DockerRunner] All operations finished.")

if __name__ == "__main__":
    main()