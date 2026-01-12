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

'''
FilePath: /yeweitang/graphdb-benchmarks/tools/workload_generator.py
Author: Chandler (Weitang Ye) <weitang.ye@ntu.edu.sg>
Description:
'''

import json
import os
import random
import sys

# Mapping from task name prefixes to internal Operation Types.
# This allows handling both "_latency" and "_throughput" variants automatically.
TASK_PREFIX_MAP = {
    "read_nbrs": "READ_NBRS",
    "add_nodes": "ADD_NODE",
    "delete_nodes": "DEL_NODE",
    "add_edges": "ADD_EDGE",
    "delete_edges": "DEL_EDGE"
}

# Mapping from configuration keys (in mixed workload ratios) to Operation Types.
MIXED_CONFIG_KEY_MAP = {
    "read": "READ_NBRS",
    "read_nbrs": "READ_NBRS",
    "insert_node": "ADD_NODE",
    "add_node": "ADD_NODE",
    "delete_node": "DEL_NODE",
    "insert_edge": "ADD_EDGE",
    "add_edge": "ADD_EDGE",
    "delete_edge": "DEL_EDGE"
}

def setup_seed(seed=42):
    """Initialize random seed for reproducibility."""
    random.seed(seed)

def _get_existing_id(existing_ids):
    """Helper: Select a random ID from the existing dataset samples."""
    if not existing_ids:
        return "0"
    return random.choice(existing_ids)

def _get_new_unique_id():
    """Helper: Generate a distinct-looking ID to avoid collision with existing dataset."""
    # Using a large range prefix to distinguish from dataset IDs
    return f"new_{random.randint(1000000, 99999999)}"

def _create_op(op_type, existing_ids):
    """Helper: Create a single operation dictionary based on type."""
    params = {}

    # 1. READ / DELETE NODE: Need existing ID
    if op_type in ["READ_NBRS", "DEL_NODE"]:
        params = {"id": _get_existing_id(existing_ids)}

    # 2. ADD NODE: Need new unique ID
    elif op_type == "ADD_NODE":
        params = {"id": _get_new_unique_id()}

    # 3. EDGES: Need src and dst (both existing)
    elif op_type in ["ADD_EDGE", "DEL_EDGE"]:
        params = {
            "src": _get_existing_id(existing_ids),
            "dst": _get_existing_id(existing_ids)
        }

    return {"type": op_type, "params": params}

def generate_single_task_workload(op_type, existing_ids, num_ops):
    """Generates a homogeneous workload (one type of operation)."""
    return [_create_op(op_type, existing_ids) for _ in range(num_ops)]

def generate_mixed_workload(existing_ids, num_ops, ratios):
    """
    Generates a mixed workload based on probability ratios.
    :param ratios: dict e.g. {"read_nbrs": 0.8, "add_edges": 0.2}
    """
    population = []
    weights = []

    # Parse the ratios dictionary
    for key, weight in ratios.items():
        norm_key = key.lower()
        matched_op = None

        # Try to find the operation type from the config key
        if norm_key in MIXED_CONFIG_KEY_MAP:
             matched_op = MIXED_CONFIG_KEY_MAP[norm_key]
        else:
            # Fallback check (substring match)
            for k_map, v_map in MIXED_CONFIG_KEY_MAP.items():
                if k_map in norm_key:
                    matched_op = v_map
                    break

        if matched_op:
            population.append(matched_op)
            weights.append(weight)

    if not population:
        # Default fallback if config is empty or invalid
        return generate_single_task_workload("READ_NBRS", existing_ids, num_ops)

    # Weighted random selection of operation types
    chosen_ops = random.choices(population, weights=weights, k=num_ops)

    # Generate actual ops
    return [_create_op(op, existing_ids) for op in chosen_ops]

def generate_workload(task_name, existing_ids, num_ops, ratios=None):
    """
    Main Entry Point.
    Parses task_name (e.g., 'read_nbrs_latency') to decide generation strategy.
    """
    if existing_ids is None:
        existing_ids = ["0"]

    # 1. Handle Mixed Workload
    if "mixed" in task_name:
        if not ratios:
            ratios = {"read_nbrs": 0.8, "add_edges": 0.2}
        return generate_mixed_workload(existing_ids, num_ops, ratios)

    # 2. Handle Single Task
    # Check if task_name starts with any known prefix (ignoring _latency/_throughput)
    matched_op_type = None
    for prefix, op_code in TASK_PREFIX_MAP.items():
        if task_name.startswith(prefix):
            matched_op_type = op_code
            break

    if matched_op_type:
        return generate_single_task_workload(matched_op_type, existing_ids, num_ops)

    # 3. Fallback / Unknown Task
    print(f"[WorkloadGenerator] Warning: Unknown task type '{task_name}'. Defaulting to READ_NBRS.")
    return generate_single_task_workload("READ_NBRS", existing_ids, num_ops)

def load_workload_config(path):
    if not os.path.exists(path):
        print(f"Error: Workload config file '{path}' not found.")
        sys.exit(1)
    with open(path, 'r') as f:
        return json.load(f)