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


import random
import json

class WorkloadGenerator:
    """
    Generates synthetic workloads for graph database benchmarks.
    The output is a list of operations in a standardized JSON-compatible dictionary format.

    Standard Operation Format:
    {
        "type": "OP_NAME",  # e.g., ADD_NODE, READ_NBRS
        "params": {         # Arguments specific to the operation
            "id": "...",
            "src": "...",
            "dst": "..."
        }
    }
    """

    def __init__(self, node_count=1000, seed=42):
        """
        Initialize the generator.
        :param node_count: The range of node IDs to generate (0 to node_count-1).
        :param seed: Random seed for reproducibility.
        """
        self.node_count = node_count
        random.seed(seed)

    def _get_random_node_id(self):
        return str(random.randint(0, self.node_count - 1))

    def generate_read_nbrs(self, num_ops):
        """Generates 'READ_NBRS' operations."""
        workload = []
        for _ in range(num_ops):
            op = {
                "type": "READ_NBRS",
                "params": {"id": self._get_random_node_id()}
            }
            workload.append(op)
        return workload

    def generate_add_nodes(self, num_ops):
        """Generates 'ADD_NODE' operations with new unique IDs."""
        workload = []
        base_id = self.node_count
        for i in range(num_ops):
            op = {
                "type": "ADD_NODE",
                "params": {"id": str(base_id + i)}
            }
            workload.append(op)
        return workload

    def generate_delete_nodes(self, num_ops):
        """Generates 'DEL_NODE' operations."""
        workload = []
        for _ in range(num_ops):
            op = {
                "type": "DEL_NODE",
                "params": {"id": self._get_random_node_id()}
            }
            workload.append(op)
        return workload

    def generate_add_edges(self, num_ops):
        """Generates 'ADD_EDGE' operations between random nodes."""
        workload = []
        for _ in range(num_ops):
            op = {
                "type": "ADD_EDGE",
                "params": {
                    "src": self._get_random_node_id(),
                    "dst": self._get_random_node_id()
                }
            }
            workload.append(op)
        return workload

    def generate_delete_edges(self, num_ops):
        """Generates 'DEL_EDGE' operations."""
        workload = []
        for _ in range(num_ops):
            op = {
                "type": "DEL_EDGE",
                "params": {
                    "src": self._get_random_node_id(),
                    "dst": self._get_random_node_id()
                }
            }
            workload.append(op)
        return workload

    def generate_mixed(self, num_ops, ratios):
        """
        Generates a mixed workload based on probability ratios.
        :param ratios: Dictionary defining probabilities, e.g.,
                       {'read': 0.8, 'insert': 0.2}
        """
        workload = []
        ops_map = {
            'read': self.generate_read_nbrs,
            'insert_node': self.generate_add_nodes,
            'delete_node': self.generate_delete_nodes,
            'insert_edge': self.generate_add_edges,
            'delete_edge': self.generate_delete_edges
        }

        # Normalize ratios
        total = sum(ratios.values())
        normalized_ratios = {k: v/total for k, v in ratios.items()}

        keys = list(normalized_ratios.keys())
        probs = list(normalized_ratios.values())

        # Determine operation types for all steps
        chosen_types = random.choices(keys, weights=probs, k=num_ops)

        for op_type in chosen_types:
            # Generate 1 operation of the chosen type
            op = ops_map[op_type](1)[0]
            workload.append(op)

        return workload

    def save_to_file(self, workload, filename):
        with open(filename, 'w') as f:
            json.dump(workload, f)