# Copyright 2026 Weitang Ye
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

import os
import sys
import time
import socket
import json
from contextlib import closing

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