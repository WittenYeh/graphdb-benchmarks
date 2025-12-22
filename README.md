# graphdb-benchmarks

**graphdb-benchmarks** is a modular, Docker-based performance benchmarking suite for mainstream graph databases. It currently supports **Neo4j**, **ArangoDB**, and **OrientDB**.

The framework is designed to measure throughput and latency for various graph workloads (loading, traversal, modifications) in a controlled, reproducible environment.

## 🚀 Key Features

*   **Zero-Network-Overhead**: Uses Docker's `container:<id>` network mode to allow the benchmark client to communicate with the database via `localhost`, eliminating network latency.
*   **Isolated Environments**: Utilizes specific Dockerfiles for each database client. This allows mixed Python environments (e.g., Python 3.11 for Neo4j, Python 3.9 for OrientDB) to resolve driver compatibility issues automatically.
*   **Automated Lifecycle**: The orchestrator (`run.py`) handles building images, provisioning containers, executing tests, and cleaning up.
*   **Concurrency Control**: Easily configure the number of server-side threads to test scalability.
*   **Automated Reporting**: Generates detailed JSON performance reports with timestamps.

## 📂 Project Structure

The project uses a dedicated `docker/` directory to manage build contexts for different databases.

```text
.
├── docker/                        # Docker build definitions
│   ├── docker.db.neo4j            # Server image definition for Neo4j
│   ├── docker.client.neo4j        # Client image (Python 3.11 + Driver)
│   ├── docker.db.arrangodb        # Server image definition for ArangoDB
│   ├── docker.client.arrangodb    # Client image (Python 3.11 + Driver)
│   ├── docker.db.orientdb         # Server image definition for OrientDB
│   └── docker.client.orientdb     # Client image (Python 3.9 + Legacy Driver)
├── impl/                          # Python Driver Implementations
│   ├── base.py
│   ├── neo4j_impl.py
│   ├── arangodb_impl.py
│   └── orientdb_impl.py
├── run.py                         # Main Host Orchestrator
├── docker_runner.py               # Internal Runner (executes inside container)
├── db_config.json                 # Port and Environment Variable config
└── README.md
```

## 🏗 Core Architecture & Workflow

The benchmark operates in a fully automated 5-step pipeline orchestrated by `run.py`:

1.  **Image Build Phase**:
    *   The script detects the target DB (e.g., `--db neo4j`).
    *   It locates the corresponding Dockerfiles in `docker/` (e.g., `docker.db.neo4j` and `docker.client.neo4j`).
    *   It builds these images locally, ensuring the correct Python version and driver dependencies are installed.
2.  **Server Provisioning**: Starts the Database Container with specific resource limits and thread configurations.
3.  **Sidecar Attachment**: Starts the Client Container sharing the **same network namespace** as the database container.
4.  **Execution**: The client runs `docker_runner.py`, dynamically loads the specific database driver implementation, and executes workloads against `localhost`.
5.  **Reporting**: Results are written to the host's result directory, and containers are cleaned up.

## 📋 Prerequisites

*   **Docker Engine** installed and running.
*   **Python 3.x** (on the host machine).
*   **docker SDK for Python**:
    ```bash
    python -m pip install docker
    ```

## 🏃 Usage

### Preparing Dataset

```bash
git submodule init
git submodule update
cd graph-dataset
cd ./dataset-subdirectory-that-you-need
make # start downloading
```

### Basic Command

The orchestrator handles everything. You just need to provide the database name and the dataset location.

```bash
# example (connect to docker may need sudo permission)
sudo python3 run.py \
  --db neo4j \
  --dataset-dir ./graph-dataset/coAuthorsDBLP/ \
  --dataset-filename coAuthorsDBLP.mtx \
  --db-config db_config.json \
  --workload-config workload_config.json \
  --result-dir ./reports
```

### Command Line Arguments

| Argument | Required | Default | Description |
| :--- | :---: | :---: | :--- |
| `--db` | Yes | - | Target database. Choices: `neo4j`, `arangodb`, `orientdb`. |
| `--dataset-dir` | Yes | - | Absolute or relative path to the directory containing dataset files on the Host. |
| `--dataset-filename` | Yes | - | The specific filename (e.g., `data.csv`) inside `dataset-dir`. |
| `--result-dir` | No | `./results` | Host directory where the JSON performance report will be saved. |
| `--db-config` | No | `db_config.json` | Path to the config file defining ports and ENV vars. |
| `--workload-config`| No | `workload_config.json` | Path to the config file defining workload details. |

### Supported Tasks

*   `load_graph`: Bulk loads the CSV data.
*   `read_nbrs_bench`: Benchmarks 1-hop neighbor traversal.
*   `add_nodes_bench` / `delete_nodes_bench`: Node modification throughput.
*   `add_edges_bench` / `delete_edges_bench`: Edge modification throughput.
*   `mixed_workload_bench`: Runs a probabilistic mixed read/write workload.

## ⚙️ Configuration

### Dockerfiles
To change the database version or Python driver version, modify the specific file in the `docker/` directory.
*   **Example**: To upgrade Neo4j, edit `docker/docker.db.neo4j`.
*   **Example**: To change the python driver version, edit `docker/docker.client.neo4j`.

### Database Configuration (`db_config.json`)
This file controls the ports, environment variables (passwords), and default commands.

```json
{
  "neo4j": {
    "port": 7687,
    "env": {
      "NEO4J_AUTH": "neo4j/password",
      "NEO4J_server_memory_heap_initial__size": "1G"
    }
  },
  ...
}
```

### Workload Configuration

Edit workload_config.json to define your test scenario.

```json
{
  "server_config": { "threads": 8 },
  "tasks": [
    { "name": "load_graph" },
    { "name": "read_nbrs_latency", "ops": 1000 },
    { "name": "read_nbrs_throughput", "ops": 50000, "client_threads": 16 }
  ]
}
```

## 📊 Output Reports

After execution, a JSON report is generated in the `--result-dir`.

**Sample Filename:** `bench_neo4j_20251221_170530.json`

```json
{
    "metadata": {
        "database": "neo4j",
        "dataset": "social_network.csv",
        "timestamp": "2025-12-21T17:05:30",
        "server_threads": "8"
    },
    "results": [
        {
            "task": "load_graph",
            "status": "success",
            "duration_seconds": 12.503,
            "error": null
        },
        {
            "task": "read_nbrs_bench",
            "status": "success",
            "duration_seconds": 0.452,
            "error": null
        }
    ]
}
```

## 📄 License

Copyright 2025 Weitang Ye. Licensed under the Apache License, Version 2.0.