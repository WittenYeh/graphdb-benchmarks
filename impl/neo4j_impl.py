# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

from neo4j import GraphDatabase
import time
import statistics
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Tuple
from .base import BaseGraphDB

class Neo4jDB(BaseGraphDB):
    def connect(self) -> None:
        uri = f"bolt://{self.host}:{self.port}"
        # Configure pool size to support high concurrency in throughput tests
        self.driver = GraphDatabase.driver(
            uri,
            auth=("neo4j", self.password),
            max_connection_lifetime=3600,
            max_connection_pool_size=200
        )
        self.driver.verify_connectivity()

    def close(self) -> None:
        if hasattr(self, 'driver'):
            self.driver.close()

    def load_graph(self, graph_file: str) -> None:
        # 1. Stream Pre-process (Remove comments, standardize delimiters)
        clean_file = self.preprocess_dataset(graph_file)

        # Batch size for Bolt Protocol
        BATCH_SIZE = 5000

        print("[Neo4j] Creating Unique Index for Nodes...")
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE")
            # Allow index to propagate
            time.sleep(1.0)

        print(f"[Neo4j] Bulk loading from {clean_file}...")
        start_time = time.time()

        local_nodes = set()
        edges_batch = []
        count = 0

        # Use a single session for loading to minimize overhead
        with self.driver.session() as session:
            with open(clean_file, 'r') as f:
                for line in f:
                    parts = line.split()
                    src, dst = parts[0], parts[1]

                    # Deduplicate nodes within this batch locally
                    local_nodes.add(src)
                    local_nodes.add(dst)

                    # Edges are assumed unique globally (per user config), use direct list
                    edges_batch.append({"src": src, "dst": dst})
                    count += 1

                    if len(edges_batch) >= BATCH_SIZE:
                        # 1. Upsert Nodes (MERGE is safe and fast with Index)
                        node_list = [{"id": nid} for nid in local_nodes]
                        session.run("UNWIND $batch AS row MERGE (:Node {id: row.id})", batch=node_list).consume()

                        # 2. Insert Edges (CREATE is fastest, assumes no dupes in dataset)
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
                # MERGE used in benchmarks to be safe against running same workload twice,
                # but CREATE is also fine if workload is distinct. Using MERGE for consistency.
                compiled_ops.append(("MATCH (s:Node {id: $src}), (t:Node {id: $dst}) MERGE (s)-[:REL]->(t)", {"src": p['src'], "dst": p['dst']}))
            elif t == "DEL_EDGE":
                compiled_ops.append(("MATCH (s:Node {id: $src})-[r:REL]->(t:Node {id: $dst}) DELETE r", {"src": p['src'], "dst": p['dst']}))
        return compiled_ops

    def _execute_latency(self, workload: List[Dict[str, Any]], label: str) -> Dict[str, Any]:
        print(f"[Neo4j] Compiling {len(workload)} ops for {label} (Latency)...")
        prepared = self._compile_workload(workload)

        print(f"[Neo4j] Executing {label} sequentially...")
        latencies = []

        with self.driver.session() as session:
            for q, p in prepared:
                start = time.time()
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
        client_threads = getattr(self, 'current_client_threads', 8)
        print(f"[Neo4j] Compiling {len(workload)} ops for {label} (Throughput, {client_threads} threads)...")
        prepared = self._compile_workload(workload)

        print(f"[Neo4j] Executing {label} concurrently...")
        start_time = time.time()

        def run_op(op_tuple):
            with self.driver.session() as session:
                session.run(op_tuple[0], op_tuple[1]).consume()

        with ThreadPoolExecutor(max_workers=client_threads) as executor:
            list(executor.map(run_op, prepared))

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