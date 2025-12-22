# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

import pyorient
import time
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any
from .base import BaseGraphDB

class OrientdbDB(BaseGraphDB):
    def connect(self) -> None:
        # Initial connection for setup and main thread operations
        self.client = pyorient.OrientDB(self.host, self.port)
        self.client.connect("root", self.password)
        if not self.client.db_exists("bench_db"):
            self.client.db_create("bench_db", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
        self.client.db_open("bench_db", "root", self.password)

        # Thread-local storage for throughput tests (pyorient is not thread-safe)
        self.thread_local = threading.local()

    def close(self) -> None:
        if hasattr(self, 'client') and self.client:
            self.client.close()

    def _get_thread_connection(self):
        """Ensures each thread has its own connection."""
        if not hasattr(self.thread_local, 'client'):
            client = pyorient.OrientDB(self.host, self.port)
            client.connect("root", self.password)
            client.db_open("bench_db", "root", self.password)
            self.thread_local.client = client
        return self.thread_local.client

    def load_graph(self, graph_file: str) -> None:
        clean_file = self.preprocess_dataset(graph_file)

        BATCH_SIZE = 500

        try: self.client.command("DELETE VERTEX V UNSAFE")
        except: pass
        try: self.client.command("DROP CLASS Node UNSAFE")
        except: pass
        try: self.client.command("DROP CLASS REL UNSAFE")
        except: pass

        self.client.command("CREATE CLASS Node EXTENDS V")
        self.client.command("CREATE PROPERTY Node.id STRING")
        self.client.command("CREATE INDEX Node.id UNIQUE")
        self.client.command("CREATE CLASS REL EXTENDS E")

        print(f"[OrientDB] Bulk loading from {clean_file}...")
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
                edges_batch.append((src, dst))

                count += 1

                if len(edges_batch) >= BATCH_SIZE:
                    cmds = ["BEGIN"]
                    # 1. Upsert Nodes (Ensure existence)
                    for n in local_nodes:
                        cmds.append(f"UPDATE Node SET id = '{n}' UPSERT WHERE id = '{n}'")
                    cmds.append("COMMIT")
                    self.client.batch(";".join(cmds))

                    # 2. Create Edges
                    cmds = ["BEGIN"]
                    for s, t in edges_batch:
                        cmds.append(f"CREATE EDGE REL FROM (SELECT FROM Node WHERE id = '{s}') TO (SELECT FROM Node WHERE id = '{t}')")
                    cmds.append("COMMIT")
                    self.client.batch(";".join(cmds))

                    local_nodes.clear()
                    edges_batch = []
                    self.print_progress(count, self.total_lines, "OrientDB")

        # Final Flush
        if edges_batch:
            cmds = ["BEGIN"]
            for n in local_nodes:
                cmds.append(f"UPDATE Node SET id = '{n}' UPSERT WHERE id = '{n}'")
            cmds.append("COMMIT")
            self.client.batch(";".join(cmds))

            cmds = ["BEGIN"]
            for s, t in edges_batch:
                cmds.append(f"CREATE EDGE REL FROM (SELECT FROM Node WHERE id = '{s}') TO (SELECT FROM Node WHERE id = '{t}')")
            cmds.append("COMMIT")
            self.client.batch(";".join(cmds))

        self.print_progress(count, self.total_lines, "OrientDB")
        print(f"\n[OrientDB] Load complete in {time.time() - start_time:.2f}s.")

    def _compile_workload(self, workload: List[Dict[str, Any]]) -> List[str]:
        cmds = []
        for op in workload:
            t, p = op['type'], op['params']
            if t == "READ_NBRS": cmds.append(f"SELECT expand(out('REL')) FROM Node WHERE id = '{p['id']}'")
            elif t == "ADD_NODE": cmds.append(f"CREATE VERTEX Node SET id = '{p['id']}'")
            elif t == "DEL_NODE": cmds.append(f"DELETE VERTEX Node WHERE id = '{p['id']}' UNSAFE")
            elif t == "ADD_EDGE": cmds.append(f"CREATE EDGE REL FROM (SELECT FROM Node WHERE id = '{p['src']}') TO (SELECT FROM Node WHERE id = '{p['dst']}')")
            elif t == "DEL_EDGE": cmds.append(f"DELETE EDGE REL WHERE out.id = '{p['src']}' AND in.id = '{p['dst']}'")
        return cmds

    def _execute_latency(self, workload, label):
        print(f"[OrientDB] Compiling {len(workload)} ops for {label} (Latency)...")
        cmds = self._compile_workload(workload)

        print(f"[OrientDB] Executing {label} sequentially...")
        latencies = []
        for cmd in cmds:
            start = time.time()
            self.client.command(cmd)
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
        print(f"[OrientDB] Compiling {len(workload)} ops for {label} (Throughput, {client_threads} threads)...")
        cmds = self._compile_workload(workload)

        print(f"[OrientDB] Executing {label} concurrently...")
        start_time = time.time()

        def run_op(cmd):
            # Get thread-local connection
            conn = self._get_thread_connection()
            conn.command(cmd)

        with ThreadPoolExecutor(max_workers=client_threads) as executor:
            list(executor.map(run_op, cmds))

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