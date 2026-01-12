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

def get_sample_ids(filepath, count=5000):
    """
    Parses the dataset file to extract a list of existing Node IDs.
    Essential for ensuring READ and DELETE operations are valid.
    """
    ids = set()
    try:
        with open(filepath, 'r') as f:
            for line in f:
                # Skip comments
                if line.startswith('%') or line.startswith('#'): continue

                parts = line.strip().replace(',', ' ').split()
                if len(parts) >= 2:
                    ids.add(parts[0])
                    ids.add(parts[1])
                if len(ids) >= count: break
    except Exception as e:
        print(f"[DockerRunner] Warning: Failed to read dataset ({e}). Using dummy IDs.")
        return ["0", "1"]
    return list(ids)
