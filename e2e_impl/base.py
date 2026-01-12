# Copyright 2025 Weitang Ye
# Licensed under the Apache License, Version 2.0

from abc import ABC, abstractmethod
import os
import sys

class BaseGraphDB(ABC):

    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password
        self.total_lines = 0

    def clean_dataset(self, original_path: str) -> str:
        """
        Reads the raw dataset, strips comments, standardizes delimiter to space.
        """
        clean_path = "/tmp/clean_graph.csv"
        print(f"[Pre-process] Cleaning dataset: {original_path} -> {clean_path}")

        line_count = 0
        with open(original_path, 'r') as f_in, open(clean_path, 'w') as f_out:
            for line in f_in:
                if line.startswith('%') or line.startswith('#'):
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    f_out.write(f"{parts[0]} {parts[1]}\n")
                    line_count += 1

        print(f"[Pre-process] Done. Total edges to load: {line_count}")
        self.total_lines = line_count
        return clean_path

    def print_progress(self, current, total, label="Loading"):
        """
        Prints progress every 5% or at least every 10k items to avoid log spam
        but ensure visibility in Docker.
        """
        if total == 0: return

        # Print roughly every 5% or every 10k records, whichever is smaller
        interval = min(total // 20, 10000)

        if current % interval == 0 or current == total:
            percent = (current / total) * 100
            print(f"[{label}] Progress: {current}/{total} ({percent:.1f}%)")
            sys.stdout.flush()

    @staticmethod
    def preprocess_dataset(original_path: str) -> bool:
        """
        Optional hook for databases that require offline data generation (e.g., CSVs).
        Returns True if pre-processing occurred.
        """
        return False

    @abstractmethod
    def connect(self): pass

    @abstractmethod
    def close(self): pass

    @abstractmethod
    def load_graph(self, graph_file: str): pass

    def offline_load_graph(self, graph_file: str) -> list:
        """
        Executes offline preparation (e.g., CSV generation).
        Returns a command list (e.g., neo4j-admin import ...) to be executed by the server,
        or None if no server-side command is needed.

        This method is called BEFORE db.connect().
        """
        return None

    def check_offline_load(self, graph_file: str):
        """
        Verifies that the offline load was successful (e.g., check node count, create indexes).
        Called AFTER db.connect().
        """
        pass

    @abstractmethod
    def read_nbrs_latency(self, workload): pass

    @abstractmethod
    def add_nodes_latency(self, workload): pass

    @abstractmethod
    def delete_nodes_latency(self, workload): pass

    @abstractmethod
    def add_edges_latency(self, workload): pass

    @abstractmethod
    def delete_edges_latency(self, workload): pass

    @abstractmethod
    def mixed_workload_latency(self, workload): pass

    @abstractmethod
    def read_nbrs_throughput(self, workload): pass

    @abstractmethod
    def add_nodes_throughput(self, workload): pass

    @abstractmethod
    def delete_nodes_throughput(self, workload): pass

    @abstractmethod
    def add_edges_throughput(self, workload): pass

    @abstractmethod
    def delete_edges_throughput(self, workload): pass

    @abstractmethod
    def mixed_workload_throughput(self, workload): pass