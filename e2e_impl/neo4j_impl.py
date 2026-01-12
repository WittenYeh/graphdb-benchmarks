# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

from neo4j import GraphDatabase
import time
import statistics
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple
from e2e_impl.base import BaseGraphDB
import math

class Neo4jDB(BaseGraphDB):

    def connect(self) -> None:
        uri = f"bolt://{self.host}:{self.port}"
        # Configure connection pool to maximize throughput.
        # max_connection_pool_size should be larger than the max worker threads.
        self.driver = GraphDatabase.driver(
            uri,
            auth=("neo4j", self.password),
            max_connection_lifetime=3600,
            max_connection_pool_size=200,
            connection_acquisition_timeout=60
        )
        self.driver.verify_connectivity()

    def close(self) -> None:
        if hasattr(self, 'driver'):
            self.driver.close()

    @staticmethod
    def preprocess_dataset(dataset_path: str, output_dir: str) -> bool:
        """
        Converts the raw dataset into Neo4j-admin compatible CSV files.
        This runs inside the Client container.
        """
        print(f"[Neo4j Impl] Converting {dataset_path} to CSVs in {output_dir}...")
        os.makedirs(output_dir, exist_ok=True)

        nodes_file = os.path.join(output_dir, "nodes.csv")
        edges_file = os.path.join(output_dir, "edges.csv")

        # Check if files already exist to save time
        if os.path.exists(nodes_file) and os.path.exists(edges_file):
             print("[Neo4j Impl] CSVs already exist, skipping conversion.")
             return True

        unique_nodes = set()

        # 1. Parse raw file and generate relationships CSV
        with open(dataset_path, 'r') as f_in, open(edges_file, 'w', newline='') as f_edge:
            writer = csv.writer(f_edge)
            # Define Header required by neo4j-admin
            writer.writerow([":START_ID", ":END_ID"])

            for line in f_in:
                if line.startswith('%') or line.startswith('#'): continue
                parts = line.strip().split()
                if len(parts) >= 2:
                    src, dst = parts[0], parts[1]
                    unique_nodes.add(src)
                    unique_nodes.add(dst)
                    writer.writerow([src, dst])

        # 2. Generate nodes CSV
        with open(nodes_file, 'w', newline='') as f_node:
            writer = csv.writer(f_node)
            writer.writerow(["id:ID"])
            for node in unique_nodes:
                writer.writerow([node])

        print(f"[Neo4j Impl] CSV generation complete. Found {len(unique_nodes)} unique nodes.")
        return True

    def load_graph(self, graph_file: str) -> None:
        # 1. Stream Pre-process
        clean_file = self.clean_dataset(graph_file)

        # Batch size for Bolt Protocol
        BATCH_SIZE = 5000

        print("[Neo4j] Creating Unique Index for Nodes...")
        # Use a single session for setup
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE").consume()
            # Allow index to propagate
            time.sleep(1.0)

        print(f"[Neo4j] Bulk loading from {clean_file}...")
        start_time = time.time()

        local_nodes = set()
        edges_batch = []
        count = 0

        # Reuse a single session for the entire loading process to minimize overhead
        with self.driver.session() as session:
            with open(clean_file, 'r') as f:
                for line in f:
                    parts = line.split()
                    src, dst = parts[0], parts[1]

                    local_nodes.add(src)
                    local_nodes.add(dst)

                    edges_batch.append({"src": src, "dst": dst})
                    count += 1

                    if len(edges_batch) >= BATCH_SIZE:
                        # 1. Upsert Nodes (MERGE is safe with Index)
                        node_list = [{"id": nid} for nid in local_nodes]
                        session.run("UNWIND $batch AS row MERGE (:Node {id: row.id})", batch=node_list).consume()

                        # 2. Insert Edges (CREATE is fastest for initial load)
                        session.run("""
                            UNWIND $batch AS row
                            MATCH (s:Node {id: row.src}), (t:Node {id: row.dst})
                            CREATE (s)-[:REL]->(t)
                        """, batch=edges_batch).consume()

                        local_nodes.clear()
                        edges_batch = []
                        self.print_progress(count, self.total_lines, "Neo4j")

                # Final Flush
                if local_nodes:
                    node_list = [{"id": nid} for nid in local_nodes]
                    session.run("UNWIND $batch AS row MERGE (:Node {id: row.id})", batch=node_list).consume()
                if edges_batch:
                    session.run("""
                        UNWIND $batch AS row
                        MATCH (s:Node {id: row.src}), (t:Node {id: row.dst})
                        CREATE (s)-[:REL]->(t)
                    """, batch=edges_batch).consume()

                self.print_progress(count, self.total_lines, "Neo4j")

        print(f"\n[Neo4j] Load complete in {time.time() - start_time:.2f}s.")

    def _compile_workload(self, workload: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
        """Pre-compiles workload to Cypher queries."""
        compiled_ops = []
        for op in workload:
            t, p = op['type'], op['params']
            if t == "READ_NBRS":
                compiled_ops.append(("MATCH (n:Node {id: $id})-[:REL]->(m) RETURN m.id", {"id": p['id']}))
            elif t == "ADD_NODE":
                compiled_ops.append(("CREATE (n:Node {id: $id})", {"id": p['id']}))
            elif t == "DEL_NODE":
                compiled_ops.append(("MATCH (n:Node {id: $id}) DETACH DELETE n", {"id": p['id']}))
            elif t == "ADD_EDGE":
                # MERGE ensures idempotency, avoiding duplicates if workload is re-run
                compiled_ops.append(("MATCH (s:Node {id: $src}), (t:Node {id: $dst}) MERGE (s)-[:REL]->(t)", {"src": p['src'], "dst": p['dst']}))
            elif t == "DEL_EDGE":
                compiled_ops.append(("MATCH (s:Node {id: $src})-[r:REL]->(t:Node {id: $dst}) DELETE r", {"src": p['src'], "dst": p['dst']}))
        return compiled_ops

    def _execute_latency(self, workload: List[Dict[str, Any]], label: str) -> Dict[str, Any]:
        """
        Executes operations sequentially to measure latency.
        Optimized to reuse a single session, minimizing TCP handshake overhead.
        """
        print(f"[Neo4j] Compiling {len(workload)} ops for {label} (Latency)...")
        prepared = self._compile_workload(workload)

        print(f"[Neo4j] Executing {label} sequentially with single session...")
        latencies = []

        # Optimization: Open ONE session for the entire loop.
        # This isolates query execution time from connection establishment time.
        with self.driver.session() as session:
            for q, p in prepared:
                start = time.time()
                # session.run() is auto-commit.
                # .consume() ensures the driver actually waits for the result (synchronous).
                session.run(q, p).consume()
                latencies.append(time.time() - start)

        avg_lat = statistics.mean(latencies)
        p99_lat = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else 0

        print(f"[Result] {label}: Avg: {avg_lat*1000:.2f}ms, P99: {p99_lat*1000:.2f}ms")
        return {
            "metric_type": "latency",
            "avg_latency_ms": avg_lat * 1000,
            "p99_latency_ms": p99_lat * 1000,
            "total_ops": len(workload)
        }

    def _execute_throughput(self, workload: List[Dict[str, Any]], label: str) -> Dict[str, Any]:
        """
        Executes operations concurrently to measure throughput.
        Optimized using 'Chunking' + 'Session Reuse' to avoid creating a new session per op.
        """
        client_threads = getattr(self, 'current_client_threads', 8)
        print(f"[Neo4j] Compiling {len(workload)} ops for {label} (Throughput, {client_threads} threads)...")
        prepared = self._compile_workload(workload)

        print(f"[Neo4j] Executing {label} concurrently with session reuse strategy...")

        # Split workload into chunks so each thread processes a batch of operations
        # within a single session context.
        chunk_size = math.ceil(len(prepared) / client_threads)
        chunks = [prepared[i:i + chunk_size] for i in range(0, len(prepared), chunk_size)]

        start_time = time.time()

        def process_chunk(ops_chunk):
            # Critical Optimization:
            # Create ONE session per thread, run ALL operations in that session.
            # This massively reduces overhead compared to creating a session per op.
            count = 0
            with self.driver.session() as session:
                for q, p in ops_chunk:
                    # Execute auto-commit transaction
                    session.run(q, p).consume()
                    count += 1
            return count

        total_ops = 0
        with ThreadPoolExecutor(max_workers=client_threads) as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
            for future in as_completed(futures):
                total_ops += future.result()

        duration = time.time() - start_time
        ops_per_sec = len(workload) / duration if duration > 0 else 0

        print(f"[Result] {label}: {ops_per_sec:.2f} ops/s")
        return {
            "metric_type": "throughput",
            "ops_per_sec": ops_per_sec,
            "duration_s": duration,
            "total_ops": len(workload)
        }

    # Interface Mapping
    def read_nbrs_latency(self, w): return self._execute_latency(w, "READ_NBRS")
    def add_nodes_latency(self, w): return self._execute_latency(w, "ADD_NODE")
    def delete_nodes_latency(self, w): return self._execute_latency(w, "DEL_NODE")
    def add_edges_latency(self, w): return self._execute_latency(w, "ADD_EDGE")
    def delete_edges_latency(self, w): return self._execute_latency(w, "DEL_EDGE")
    def mixed_workload_latency(self, w): return self._execute_latency(w, "MIXED")

    def read_nbrs_throughput(self, w): return self._execute_throughput(w, "READ_NBRS")
    def add_nodes_throughput(self, w): return self._execute_throughput(w, "ADD_NODE")
    def delete_nodes_throughput(self, w): return self._execute_throughput(w, "DEL_NODE")
    def add_edges_throughput(self, w): return self._execute_throughput(w, "ADD_EDGE")
    def delete_edges_throughput(self, w): return self._execute_throughput(w, "DEL_EDGE")
    def mixed_workload_throughput(self, w): return self._execute_throughput(w, "MIXED")