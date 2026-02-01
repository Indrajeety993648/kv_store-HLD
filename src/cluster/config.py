"""
Cluster Configuration for Distributed KV-Cache
Defines shard ownership and replica mapping for 3-node cluster.
"""
import os
from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass
class NodeConfig:
    """Configuration for a single node in the cluster."""
    node_id: int
    host: str
    port: int
    
    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass 
class ShardConfig:
    """Configuration for a single shard."""
    shard_id: int
    primary_node: int
    replica_node: int


class ClusterConfig:
    """
    Cluster configuration for 3-node distributed KV-Cache.
    
    Shard Distribution:
    | Shard | Primary | Replica |
    |-------|---------|---------|
    |   0   | Node 1  | Node 3  |
    |   1   | Node 2  | Node 1  |
    |   2   | Node 3  | Node 2  |
    """
    
    NUM_SHARDS = 3
    NUM_NODES = 3
    BASE_PORT = 7171
    
    def __init__(self, current_node_id: int = None):
        """
        Initialize cluster configuration.
        
        Args:
            current_node_id: ID of the current node (1, 2, or 3).
                           If None, reads from NODE_ID environment variable.
        """
        if current_node_id is None:
            current_node_id = int(os.environ.get('NODE_ID', '1'))
        
        self.current_node_id = current_node_id
        self._setup_nodes()
        self._setup_shards()
    
    def _setup_nodes(self):
        """Configure all nodes in the cluster."""
        self.nodes: Dict[int, NodeConfig] = {
            1: NodeConfig(
                node_id=1,
                host=os.environ.get('NODE1_HOST', 'node1'),
                port=int(os.environ.get('NODE1_PORT', self.BASE_PORT))
            ),
            2: NodeConfig(
                node_id=2,
                host=os.environ.get('NODE2_HOST', 'node2'),
                port=int(os.environ.get('NODE2_PORT', self.BASE_PORT))
            ),
            3: NodeConfig(
                node_id=3,
                host=os.environ.get('NODE3_HOST', 'node3'),
                port=int(os.environ.get('NODE3_PORT', self.BASE_PORT))
            ),
        }
    
    def _setup_shards(self):
        """
        Configure shard ownership.
        
        Distribution pattern ensures each node is:
        - Primary for some shards
        - Replica for other shards
        """
        self.shards: Dict[int, ShardConfig] = {
            0: ShardConfig(shard_id=0, primary_node=1, replica_node=3),
            1: ShardConfig(shard_id=1, primary_node=2, replica_node=1),
            2: ShardConfig(shard_id=2, primary_node=3, replica_node=2),
        }
    
    def get_shard_for_key(self, key: str) -> int:
        """
        Determine which shard a key belongs to using consistent hashing.
        
        Args:
            key: The key to hash
            
        Returns:
            Shard ID (0, 1, or 2)
        """
        # Simple hash function: sum of ASCII values mod NUM_SHARDS
        # This is deterministic and works well for distribution
        hash_value = sum(ord(c) for c in key)
        return hash_value % self.NUM_SHARDS
    
    def get_primary_node_for_key(self, key: str) -> NodeConfig:
        """Get the primary node that owns the given key."""
        shard_id = self.get_shard_for_key(key)
        primary_node_id = self.shards[shard_id].primary_node
        return self.nodes[primary_node_id]
    
    def get_replica_node_for_key(self, key: str) -> NodeConfig:
        """Get the replica node for the given key."""
        shard_id = self.get_shard_for_key(key)
        replica_node_id = self.shards[shard_id].replica_node
        return self.nodes[replica_node_id]
    
    def is_primary_for_key(self, key: str) -> bool:
        """Check if current node is the primary for given key."""
        shard_id = self.get_shard_for_key(key)
        return self.shards[shard_id].primary_node == self.current_node_id
    
    def is_replica_for_key(self, key: str) -> bool:
        """Check if current node is the replica for given key."""
        shard_id = self.get_shard_for_key(key)
        return self.shards[shard_id].replica_node == self.current_node_id
    
    def get_current_node(self) -> NodeConfig:
        """Get configuration for the current node."""
        return self.nodes[self.current_node_id]
    
    def get_other_nodes(self) -> List[NodeConfig]:
        """Get configurations for all other nodes in the cluster."""
        return [
            node for node_id, node in self.nodes.items()
            if node_id != self.current_node_id
        ]
    
    def get_shards_as_primary(self) -> List[int]:
        """Get list of shard IDs where current node is primary."""
        return [
            shard_id for shard_id, shard in self.shards.items()
            if shard.primary_node == self.current_node_id
        ]
    
    def get_shards_as_replica(self) -> List[int]:
        """Get list of shard IDs where current node is replica."""
        return [
            shard_id for shard_id, shard in self.shards.items()
            if shard.replica_node == self.current_node_id
        ]
    
    def __repr__(self) -> str:
        return (
            f"ClusterConfig(current_node={self.current_node_id}, "
            f"primary_shards={self.get_shards_as_primary()}, "
            f"replica_shards={self.get_shards_as_replica()})"
        )