# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

import socket
import time
import statistics
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, Cardinality

from e2e_impl.base import BaseGraphDB

class AsterDB(BaseGraphDB):

    def connect(self) -> None:
        """
        Connects to the Aster/Gremlin Server using WebSocket.
        """
        # Construct the Gremlin Server WebSocket URL
        self.url = f'ws://{self.host}:{self.port}/gremlin'

        # Initialize the DriverRemoteConnection
        # pool_size should be configured to handle the concurrency defined in workload_config
        self.connection = DriverRemoteConnection(
            self.url,
            'g',
            pool_size=64,  # Ensure enough connections for threads
            message_serializer=None
        )

        # Create the Traversal Source
        self.g = traversal().withRemote(self.connection)
        print(f"[AsterDB] Connected to {self.url}")

    def close(self) -> None:
        """
        Closes the underlying WebSocket connection.
        """
        if hasattr(self, 'connection'):
            self.connection.close()

    def load_graph(self, graph_file: str) -> None:
        """
        Loads the graph data using simplified batching.
        Assumptions:
        1. The database is empty (or cleared).
        2. The input dataset contains unique edges (no duplicates).
        """
        clean_file = self.clean_dataset(graph_file)

        # Batch size can likely be increased since the query complexity is lower
        BATCH_SIZE = 5000

        print("[AsterDB] Clearing existing graph for clean load...")
        try:
            self.g.V().drop().iterate()
        except Exception as e:
            print(f"[AsterDB] Warning during drop: {e}")
            exit(1)

        print(f"[AsterDB] Bulk loading from {clean_file}...")
        start_time = time.time()

        local_nodes = set() # Python-side deduplication for nodes is still needed
        edges_batch = []
        count = 0

        with open(clean_file, 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2: continue

                src, dst = parts[0], parts[1]

                # We still need to track unique nodes locally because
                # a node ID appears multiple times in an edge list file.
                local_nodes.add(src)
                local_nodes.add(dst)

                edges_batch.append((src, dst))
                count += 1

                if len(edges_batch) >= BATCH_SIZE:
                    self._batch_insert_nodes(local_nodes)
                    self._batch_insert_edges(edges_batch)

                    local_nodes.clear()
                    edges_batch = []
                    self.print_progress(count, self.total_lines, "AsterDB")

            # Final Flush
            if local_nodes:
                self._batch_insert_nodes(local_nodes)
            if edges_batch:
                self._batch_insert_edges(edges_batch)

            self.print_progress(count, self.total_lines, "AsterDB")

        print(f"\n[AsterDB] Load complete in {time.time() - start_time:.2f}s.")

    def _batch_insert_nodes(self, nodes: set):
        """
        Optimized Node Insertion.
        Directly chains addV() calls without checking for existence.
        """
        if not nodes: return
        try:
            # We create a new traversal chain
            # g.addV('Node').property(T.id, 'A').addV('Node').property(T.id, 'B')...
            t = self.g
            first = True

            for nid in nodes:
                if first:
                    t = t.addV('Node').property(T.id, nid)
                    first = False
                else:
                    # Chain subsequent addV calls
                    t = t.addV('Node').property(T.id, nid)

            # Execute the chain
            t.iterate()
        except Exception as e:
            print(f"[AsterDB] Error inserting nodes batch: {e}")

    def _batch_insert_edges(self, edges: list):
        """
        Optimized Edge Insertion.
        Directly chains addE() calls.
        """
        if not edges: return
        try:
            # Chain logic:
            # g.V(src1).addE('REL').to(__.V(dst1)) \
            #  .V(src2).addE('REL').to(__.V(dst2)) ...
            t = self.g
            first = True

            for src, dst in edges:
                if first:
                    t = t.V(src).addE('REL').to(__.V(dst))
                    first = False
                else:
                    # Re-select the next source node and add edge
                    t = t.V(src).addE('REL').to(__.V(dst))

            t.iterate()
        except Exception as e:
            print(f"[AsterDB] Error inserting edges batch: {e}")

    def _run_op(self, g, op_type: str, params: Dict[str, Any]):
        """
        Executes a single Gremlin operation based on the benchmark opcode.
        """
        try:
            if op_type == "READ_NBRS":
                # Get IDs of outgoing neighbors
                # g.V(id).out().id()
                return g.V(params['id']).out().id().toList()

            elif op_type == "ADD_NODE":
                # g.addV('Node').property(T.id, id)
                return g.addV('Node').property(T.id, params['id']).next()

            elif op_type == "DEL_NODE":
                # g.V(id).drop()
                return g.V(params['id']).drop().iterate()

            elif op_type == "ADD_EDGE":
                # g.V(src).addE('REL').to(__.V(dst))
                return g.V(params['src']).addE('REL').to(__.V(params['dst'])).next()

            elif op_type == "DEL_EDGE":
                # g.V(src).outE().where(inV().hasId(dst)).drop()
                return g.V(params['src']).outE('REL').where(__.inV().hasId(params['dst'])).drop().iterate()

        except Exception as e:
            # Gremlin might throw if elements don't exist, swallow for bench stability
            # or re-raise if strict.
            pass

    def _execute_latency(self, workload: List[Dict[str, Any]], label: str) -> Dict[str, Any]:
        """
        Sequential execution for latency measurement.
        """
        print(f"[AsterDB] Executing {len(workload)} ops for {label} (Latency)...")
        latencies = []

        # Reuse the main traversal source
        g = self.g

        for op in workload:
            t_type = op['type']
            params = op['params']

            start = time.time()
            self._run_op(g, t_type, params)
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
        Concurrent execution for throughput measurement.
        """
        client_threads = getattr(self, 'current_client_threads', 8)
        print(f"[AsterDB] Executing {len(workload)} ops for {label} (Throughput, {client_threads} threads)...")

        chunk_size = math.ceil(len(workload) / client_threads)
        chunks = [workload[i:i + chunk_size] for i in range(0, len(workload), chunk_size)]

        start_time = time.time()

        def process_chunk(ops_chunk):
            # In Gremlin Python, 'g' is thread-safe if the underlying driver connection is.
            # However, creating a lightweight spawn of g is good practice.
            local_g = self.g.clone()
            count = 0
            for op in ops_chunk:
                self._run_op(local_g, op['type'], op['params'])
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