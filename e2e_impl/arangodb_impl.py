# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

from arango import ArangoClient
try: from arango.exceptions import DocumentInsertError
except ImportError: from arango import DocumentInsertError
import time
import statistics
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Tuple

from e2e_impl.base import BaseGraphDB

class ArangodbDB(BaseGraphDB):
    def connect(self) -> None:
        self.client = ArangoClient(hosts=f"http://{self.host}:{self.port}")
        sys_db = self.client.db('_system', username='root', password=self.password)
        if not sys_db.has_database("bench_db"): sys_db.create_database("bench_db")
        self.db = self.client.db("bench_db", username='root', password=self.password)
        self.v_coll = "Nodes"
        self.e_coll = "Edges"
        self.graph_name = "bench_graph"

    def close(self) -> None: pass

    def load_graph(self, graph_file: str) -> None:
        clean_file = self.clean_dataset(graph_file)

        BATCH_SIZE = 20000

        if self.db.has_graph(self.graph_name):
            self.db.delete_graph(self.graph_name, drop_collections=True)
        graph = self.db.create_graph(self.graph_name)
        v_proxy = graph.create_vertex_collection(self.v_coll)
        e_proxy = graph.create_edge_definition(edge_collection=self.e_coll, from_vertex_collections=[self.v_coll], to_vertex_collections=[self.v_coll])

        print(f"[ArangoDB] Bulk loading from {clean_file}...")
        start_time = time.time()

        local_nodes = set()
        edges_batch = []
        count = 0

        with open(clean_file, 'r') as f:
            for line in f:
                parts = line.split()
                src, dst = parts[0], parts[1]

                local_nodes.add(src)
                local_nodes.add(dst)

                # Edges in Arango need _from and _to ids
                edges_batch.append({'_from': f"{self.v_coll}/{src}", '_to': f"{self.v_coll}/{dst}"})
                count += 1

                if len(edges_batch) >= BATCH_SIZE:
                    # 1. Insert Nodes (Ignore duplicates)
                    node_list = [{'_key': n} for n in local_nodes]
                    v_proxy.import_bulk(node_list, on_duplicate='ignore')

                    # 2. Insert Edges
                    e_proxy.import_bulk(edges_batch, on_duplicate='ignore')

                    local_nodes.clear()
                    edges_batch = []
                    self.print_progress(count, self.total_lines, "ArangoDB")

        # Final Flush
        if local_nodes:
            node_list = [{'_key': n} for n in local_nodes]
            v_proxy.import_bulk(node_list, on_duplicate='ignore')
        if edges_batch:
            e_proxy.import_bulk(edges_batch, on_duplicate='ignore')

        self.print_progress(count, self.total_lines, "ArangoDB")
        print(f"\n[ArangoDB] Load complete in {time.time() - start_time:.2f}s.")

    def _compile_workload(self, workload: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
        ops = []
        for op in workload:
            t, p = op['type'], op['params']
            if t == "READ_NBRS":
                ops.append((f"FOR v IN 1..1 OUTBOUND @s GRAPH '{self.graph_name}' RETURN v._key", {'s': f"{self.v_coll}/{p['id']}"}))
            elif t == "ADD_NODE":
                ops.append((f"INSERT {{_key:@k}} INTO {self.v_coll} OPTIONS {{ignoreErrors:true}}", {'k': p['id']}))
            elif t == "DEL_NODE":
                ops.append((f"REMOVE @k IN {self.v_coll} OPTIONS {{ignoreErrors:true}}", {'k': p['id']}))
            elif t == "ADD_EDGE":
                ops.append((f"INSERT {{_from:@s, _to:@d}} INTO {self.e_coll} OPTIONS {{ignoreErrors:true}}", {'s': f"{self.v_coll}/{p['src']}", 'd': f"{self.v_coll}/{p['dst']}"}))
            elif t == "DEL_EDGE":
                ops.append((f"FOR e IN {self.e_coll} FILTER e._from==@s AND e._to==@d REMOVE e IN {self.e_coll}", {'s': f"{self.v_coll}/{p['src']}", 'd': f"{self.v_coll}/{p['dst']}"}))
        return ops

    def _execute_latency(self, workload, label):
        print(f"[ArangoDB] Compiling {len(workload)} ops for {label} (Latency)...")
        prepared = self._compile_workload(workload)

        print(f"[ArangoDB] Executing {label} sequentially...")
        latencies = []
        for q, b in prepared:
            start = time.time()
            cursor = self.db.aql.execute(q, bind_vars=b)
            # Drain cursor to ensure execution
            for _ in cursor: pass
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

    def _execute_throughput(self, workload, label):
        client_threads = getattr(self, 'current_client_threads', 8)
        print(f"[ArangoDB] Compiling {len(workload)} ops for {label} (Throughput, {client_threads} threads)...")
        prepared = self._compile_workload(workload)

        print(f"[ArangoDB] Executing {label} concurrently...")
        start_time = time.time()

        def run_op(op_tuple):
            cursor = self.db.aql.execute(op_tuple[0], bind_vars=op_tuple[1])
            for _ in cursor: pass

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