"""
Tests for Distributed KV-Cache with Sharding and Replication.

These tests verify:
1. Consistent hashing correctly routes keys to shards
2. Request forwarding works between nodes
3. Synchronous replication writes to both primary and replica
"""
import pytest
import asyncio
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cluster.config import ClusterConfig, NodeConfig, ShardConfig
from src.cluster.distributed_server import (
    KVStore, 
    ProtocolParser, 
    Command, 
    CommandType,
    Response,
    ResponseStatus
)


# ============================================================================
# Cluster Configuration Tests
# ============================================================================

class TestClusterConfig:
    """Test cluster configuration and shard mapping."""
    
    def test_cluster_has_three_nodes(self):
        """Verify cluster has exactly 3 nodes."""
        config = ClusterConfig(current_node_id=1)
        assert len(config.nodes) == 3
        assert set(config.nodes.keys()) == {1, 2, 3}
    
    def test_cluster_has_three_shards(self):
        """Verify cluster has exactly 3 shards."""
        config = ClusterConfig(current_node_id=1)
        assert len(config.shards) == 3
        assert set(config.shards.keys()) == {0, 1, 2}
    
    def test_shard_distribution(self):
        """Verify correct shard to node mapping."""
        config = ClusterConfig(current_node_id=1)
        
        # Shard 0: Primary=Node1, Replica=Node3
        assert config.shards[0].primary_node == 1
        assert config.shards[0].replica_node == 3
        
        # Shard 1: Primary=Node2, Replica=Node1
        assert config.shards[1].primary_node == 2
        assert config.shards[1].replica_node == 1
        
        # Shard 2: Primary=Node3, Replica=Node2
        assert config.shards[2].primary_node == 3
        assert config.shards[2].replica_node == 2
    
    def test_each_node_is_primary_for_one_shard(self):
        """Each node should be primary for exactly one shard."""
        config = ClusterConfig(current_node_id=1)
        
        for node_id in [1, 2, 3]:
            config_for_node = ClusterConfig(current_node_id=node_id)
            primary_shards = config_for_node.get_shards_as_primary()
            assert len(primary_shards) == 1, f"Node {node_id} should be primary for exactly 1 shard"
    
    def test_each_node_is_replica_for_one_shard(self):
        """Each node should be replica for exactly one shard."""
        config = ClusterConfig(current_node_id=1)
        
        for node_id in [1, 2, 3]:
            config_for_node = ClusterConfig(current_node_id=node_id)
            replica_shards = config_for_node.get_shards_as_replica()
            assert len(replica_shards) == 1, f"Node {node_id} should be replica for exactly 1 shard"


# ============================================================================
# Consistent Hashing Tests
# ============================================================================

class TestConsistentHashing:
    """Test consistent hashing for key routing."""
    
    def test_hash_is_deterministic(self):
        """Same key should always hash to same shard."""
        config = ClusterConfig(current_node_id=1)
        
        key = "user:alice"
        shard1 = config.get_shard_for_key(key)
        shard2 = config.get_shard_for_key(key)
        shard3 = config.get_shard_for_key(key)
        
        assert shard1 == shard2 == shard3
    
    def test_hash_returns_valid_shard(self):
        """Hash should return shard ID in valid range (0-2)."""
        config = ClusterConfig(current_node_id=1)
        
        test_keys = ["key1", "key2", "key3", "user:bob", "session:123", ""]
        for key in test_keys:
            shard = config.get_shard_for_key(key)
            assert 0 <= shard < 3, f"Shard {shard} out of range for key '{key}'"
    
    def test_keys_distributed_across_shards(self):
        """Different keys should be distributed across multiple shards."""
        config = ClusterConfig(current_node_id=1)
        
        # Generate many keys and check distribution
        shards_seen = set()
        for i in range(100):
            key = f"key_{i}"
            shard = config.get_shard_for_key(key)
            shards_seen.add(shard)
        
        # Should hit all 3 shards with 100 random keys
        assert len(shards_seen) == 3, "Keys should be distributed across all shards"
    
    def test_get_primary_node_for_key(self):
        """Verify correct primary node is returned for keys."""
        config = ClusterConfig(current_node_id=1)
        
        # Find a key that hashes to each shard
        shard_to_key = {}
        for i in range(100):
            key = f"test_key_{i}"
            shard = config.get_shard_for_key(key)
            if shard not in shard_to_key:
                shard_to_key[shard] = key
            if len(shard_to_key) == 3:
                break
        
        # Verify primary nodes
        for shard_id, key in shard_to_key.items():
            primary_node = config.get_primary_node_for_key(key)
            expected_primary = config.shards[shard_id].primary_node
            assert primary_node.node_id == expected_primary
    
    def test_is_primary_for_key(self):
        """Test is_primary_for_key returns correct boolean."""
        # Node 1 is primary for shard 0
        config1 = ClusterConfig(current_node_id=1)
        
        # Find a key that hashes to shard 0
        for i in range(100):
            key = f"key_{i}"
            if config1.get_shard_for_key(key) == 0:
                assert config1.is_primary_for_key(key) == True
                break
        
        # Find a key that hashes to shard 1 (Node 2 is primary)
        for i in range(100):
            key = f"key_{i}"
            if config1.get_shard_for_key(key) == 1:
                assert config1.is_primary_for_key(key) == False
                break


# ============================================================================
# Protocol Parser Tests (Extended for Replica Commands)
# ============================================================================

class TestProtocolParserDistributed:
    """Test protocol parser with replica commands."""
    
    def setup_method(self):
        self.parser = ProtocolParser()
    
    def test_parse_replica_put(self):
        """Parse REPLICA PUT command."""
        cmd = self.parser.parse_request("REPLICA PUT mykey myvalue 60")
        assert cmd.type == CommandType.REPLICA_PUT
        assert cmd.key == "mykey"
        assert cmd.value == "myvalue"
        assert cmd.ttl == 60
    
    def test_parse_replica_delete(self):
        """Parse REPLICA DELETE command."""
        cmd = self.parser.parse_request("REPLICA DELETE mykey")
        assert cmd.type == CommandType.REPLICA_DELETE
        assert cmd.key == "mykey"
    
    def test_replica_prefix_case_insensitive(self):
        """REPLICA prefix should be case insensitive."""
        cmd1 = self.parser.parse_request("replica put key val")
        cmd2 = self.parser.parse_request("REPLICA PUT key val")
        cmd3 = self.parser.parse_request("Replica Put key val")
        
        assert cmd1.type == CommandType.REPLICA_PUT
        assert cmd2.type == CommandType.REPLICA_PUT
        assert cmd3.type == CommandType.REPLICA_PUT
    
    def test_normal_put_not_replica(self):
        """Normal PUT should not be treated as replica command."""
        cmd = self.parser.parse_request("PUT mykey myvalue")
        assert cmd.type == CommandType.PUT
        assert cmd.type != CommandType.REPLICA_PUT


# ============================================================================
# KV Store Tests (Same as Assignment 2)
# ============================================================================

class TestKVStoreDistributed:
    """Test KVStore functionality in distributed context."""
    
    def setup_method(self):
        self.store = KVStore(max_size=100)
    
    def test_put_and_get(self):
        """Basic put and get operations."""
        assert self.store.put("key1", "value1") == True
        assert self.store.get("key1") == "value1"
    
    def test_put_overwrite(self):
        """Put should overwrite existing value."""
        self.store.put("key1", "value1")
        self.store.put("key1", "value2")
        assert self.store.get("key1") == "value2"
    
    def test_get_nonexistent(self):
        """Get nonexistent key returns None."""
        assert self.store.get("nonexistent") is None
    
    def test_delete(self):
        """Delete removes key."""
        self.store.put("key1", "value1")
        assert self.store.delete("key1") == True
        assert self.store.get("key1") is None
    
    def test_delete_nonexistent(self):
        """Delete nonexistent key returns False."""
        assert self.store.delete("nonexistent") == False
    
    def test_exists(self):
        """Exists returns correct boolean."""
        self.store.put("key1", "value1")
        assert self.store.exists("key1") == True
        assert self.store.exists("nonexistent") == False
    
    def test_ttl_expiration(self):
        """TTL should expire keys."""
        import time
        self.store.put("key1", "value1", ttl=1)
        assert self.store.get("key1") == "value1"
        time.sleep(1.1)
        assert self.store.get("key1") is None
    
    def test_lru_eviction(self):
        """LRU eviction when cache is full."""
        small_store = KVStore(max_size=3)
        small_store.put("key1", "value1")
        small_store.put("key2", "value2")
        small_store.put("key3", "value3")
        
        # Access key1 to make it recently used
        small_store.get("key1")
        
        # Add key4, should evict key2 (LRU)
        small_store.put("key4", "value4")
        
        assert small_store.get("key1") == "value1"  # Recently used, not evicted
        assert small_store.get("key2") is None      # LRU, evicted
        assert small_store.get("key3") == "value3"
        assert small_store.get("key4") == "value4"


# ============================================================================
# Integration Tests (Require Running Cluster)
# ============================================================================

class TestDistributedIntegration:
    """
    Integration tests for the distributed cluster.
    
    These tests require the 3-node cluster to be running:
        docker-compose up -d
    
    Run with: pytest tests/test_distributed.py -v -k "Integration"
    """
    
    @pytest.fixture
    def nodes(self):
        """Return node addresses."""
        return [
            ("localhost", 7171),  # Node 1
            ("localhost", 7172),  # Node 2
            ("localhost", 7173),  # Node 3
        ]
    
    @pytest.mark.asyncio
    async def test_basic_put_get_any_node(self, nodes):
        """PUT to one node, GET from another."""
        import socket
        
        # Skip if cluster not running
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect(nodes[0])
            s.close()
        except (ConnectionRefusedError, socket.timeout):
            pytest.skip("Cluster not running. Start with: docker-compose up -d")
        
        # PUT to Node 1
        reader, writer = await asyncio.open_connection(*nodes[0])
        writer.write(b"PUT integration_test_key integration_test_value\n")
        await writer.drain()
        response = await reader.readline()
        writer.close()
        await writer.wait_closed()
        
        assert response.decode().strip() == "OK stored"
        
        # GET from Node 2 (will be forwarded)
        reader, writer = await asyncio.open_connection(*nodes[1])
        writer.write(b"GET integration_test_key\n")
        await writer.drain()
        response = await reader.readline()
        writer.close()
        await writer.wait_closed()
        
        assert response.decode().strip() == "OK integration_test_value"
        
        # GET from Node 3 (will be forwarded)
        reader, writer = await asyncio.open_connection(*nodes[2])
        writer.write(b"GET integration_test_key\n")
        await writer.drain()
        response = await reader.readline()
        writer.close()
        await writer.wait_closed()
        
        assert response.decode().strip() == "OK integration_test_value"
        
        # Cleanup
        reader, writer = await asyncio.open_connection(*nodes[0])
        writer.write(b"DELETE integration_test_key\n")
        await writer.drain()
        await reader.readline()
        writer.close()
        await writer.wait_closed()


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])