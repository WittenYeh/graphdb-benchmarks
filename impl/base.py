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

from abc import ABC, abstractmethod

class BaseGraphDB(ABC):

    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def load_graph(self, graph_file: str):
        '''r
        Load graph data into the database from the specified file.
        The file is expected to be in CSV format with edges defined as:
        source_node_id,target_node_id
        graph_file: str - path to the CSV file containing graph edges
        Returns: None
        '''
        pass

    @abstractmethod
    def read_nbrs_bench(self, workload):
        '''r
        Perform neighbor reading benchmark based on the provided workload.
        workload: json-lik structure defining the read operations
        '''
        pass

    @abstractmethod
    def add_nodes_bench(self, workload):
        '''r
        Perform node addition benchmark based on the provided workload.
        workload: json-like structure defining the add operations
        '''
        pass

    @abstractmethod
    def delete_nodes_bench(self, workload):
        '''r
        Perform node deletion benchmark based on the provided workload.
        workload: json-like structure defining the delete operations
        '''
        pass

    @abstractmethod
    def add_edges_bench(self, workload):
        '''r
        Perform edge addition benchmark based on the provided workload.
        workload: json-like structure defining the add operations
        '''
        pass

    @abstractmethod
    def delete_edges_bench(self, workload):
        '''r
        Perform edge deletion benchmark based on the provided workload.
        workload: json-like structure defining the delete operations
        '''
        pass

    @abstractmethod
    def mixed_workload_bench(self, workload):
        '''r
        Perform mixed workload benchmark based on the provided workload.
        workload: json-like structure defining the mixed operations
        '''
        pass