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

from neo4j import GraphDatabase
import time
import csv
import os
# Assuming BaseGraphDB is in impl/base.py or passed via standard import
from .base import BaseGraphDB

class Neo4jDB(BaseGraphDB):
    """
    Neo4j implementation of the Graph Database Benchmark interface.
    Uses the official neo4j python driver.
    """

    def connect(self):
        """Establishes connection to the Neo4j database."""
        uri = f"bolt://{self.host}:{self.port}"
        # Setting max_connection_lifetime to verify connections in long benchmarks
        self.driver = GraphDatabase.driver(uri, auth=("neo4j", self.password))

        # Verify connectivity
        try:
            self.driver.verify_connectivity()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Neo4j at {uri}: {e}")

    def close(self):
        """Closes the driver connection."""
        if hasattr(self, 'driver'):
            self.driver.close()

    def load_graph(self, graph_file: str):
        """
        Loads graph data from a CSV file using batch insertion via the driver.
        Note: We use driver-side batching instead of LOAD CSV to handle
        file path access issues in containerized environments (Client vs Server).
        """
        batch_size = 1000
        query = """
        UNWIND $rows AS row
        MERGE (s:Node {id: row.source_id})
        MERGE (t:Node {id: row.target_id})
        MERGE (s)-[:REL]->(t)
        """

        batch = []
        print(f"[Neo4j] Loading graph from {graph_file}...")

        with self.driver.session() as session:
            with open(graph_file, 'r') as f:
                reader = csv.reader(f)
                # Skip header if exists, assuming header: source_id, target_id
                # If no header, remove next line or handle logic accordingly
                # headers = next(reader, None)

                for row in reader:
                    if len(row) < 2: continue
                    batch.append({'source_id': row[0], 'target_id': row[1]})

                    if len(batch) >= batch_size:
                        session.run(query, rows=batch)
                        batch = []

                # Insert remaining
                if batch:
                    session.run(query, rows=batch)

        print("[Neo4j] Load complete.")

    def _compile_workload(self, workload):
        """
        Internal Helper: Converts the abstract JSON workload into Neo4j-specific
        Cypher queries and parameters.

        This step is performed BEFORE the timer starts to ensure we measure
        DB execution time, not Python string manipulation time.

        Returns: list of tuples [(query, params), ...]
        """
        compiled_ops = []

        for op in workload:
            op_type = op['type']
            params = op['params']

            if op_type == "READ_NBRS":
                query = "MATCH (n:Node {id: $id})-[:REL]-(m) RETURN m.id"
                p = {"id": params['id']}

            elif op_type == "ADD_NODE":
                query = "CREATE (n:Node {id: $id})"
                p = {"id": params['id']}

            elif op_type == "DEL_NODE":
                # Detach delete removes the node and its incident edges
                query = "MATCH (n:Node {id: $id}) DETACH DELETE n"
                p = {"id": params['id']}

            elif op_type == "ADD_EDGE":
                query = """
                MATCH (s:Node {id: $src}), (t:Node {id: $dst})
                MERGE (s)-[:REL]->(t)
                """
                p = {"src": params['src'], "dst": params['dst']}

            elif op_type == "DEL_EDGE":
                query = """
                MATCH (s:Node {id: $src})-[r:REL]->(t:Node {id: $dst})
                DELETE r
                """
                p = {"src": params['src'], "dst": params['dst']}

            else:
                continue # Skip unknown operations

            compiled_ops.append((query, p))

        return compiled_ops

    def _execute_benchmark(self, workload, label):
        """
        Core execution logic:
        1. Compile workload (Not timed).
        2. Execute queries (Timed).
        """
        # 1. Preparation Phase (Excluded from benchmark time)
        print(f"[Neo4j] Compiling {len(workload)} operations for {label}...")
        prepared_queries = self._compile_workload(workload)

        # 2. Execution Phase (Timed)
        print(f"[Neo4j] Starting execution for {label}...")
        start_time = time.time()

        with self.driver.session() as session:
            for query, params in prepared_queries:
                # Using session.run for raw throughput measurement
                # We consume() the result to ensure the query actually finished on server
                session.run(query, params).consume()

        end_time = time.time()
        duration = end_time - start_time
        ops_per_sec = len(workload) / duration if duration > 0 else 0

        print(f"[Benchmark Result] {label}: {len(workload)} ops in {duration:.4f}s ({ops_per_sec:.2f} ops/s)")
        return duration

    def read_nbrs_bench(self, workload):
        """Performs k-hop neighbor query (k=1 implicit in simple wrapper) or simple neighbor scan."""
        return self._execute_benchmark(workload, "READ_NBRS")

    def add_nodes_bench(self, workload):
        return self._execute_benchmark(workload, "ADD_NODE")

    def delete_nodes_bench(self, workload):
        return self._execute_benchmark(workload, "DEL_NODE")

    def add_edges_bench(self, workload):
        return self._execute_benchmark(workload, "ADD_EDGE")

    def delete_edges_bench(self, workload):
        return self._execute_benchmark(workload, "DEL_EDGE")

    def mixed_workload_bench(self, workload):
        return self._execute_benchmark(workload, "MIXED_WORKLOAD")