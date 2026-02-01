"""
Node Client for inter-node communication in Distributed KV-Cache.
Handles request forwarding and replication between nodes.
"""
import asyncio
import logging
from typing import Optional, Tuple
from .config import NodeConfig, ClusterConfig

logger = logging.getLogger(__name__)


class NodeClient:
    """
    Client for communicating with other nodes in the cluster.
    Used for:
    1. Request forwarding (when key belongs to another node)
    2. Replication (sending writes to replica node)
    """
    
    def __init__(self, timeout: float = 5.0):
        """
        Initialize node client.
        
        Args:
            timeout: Connection and read timeout in seconds
        """
        self.timeout = timeout
        self._connections: dict = {}  # Cache connections for reuse
    
    async def send_command(
        self, 
        node: NodeConfig, 
        command: str,
        is_replication: bool = False
    ) -> Tuple[bool, str]:
        """
        Send a command to another node and get the response.
        
        Args:
            node: Target node configuration
            command: Raw command string (e.g., "PUT key value 60")
            is_replication: If True, marks as internal replication (add REPLICA prefix)
            
        Returns:
            Tuple of (success: bool, response: str)
        """
        try:
            # Add REPLICA prefix for replication requests so receiving node
            # knows not to replicate again (avoid infinite loop)
            if is_replication:
                command = f"REPLICA {command}"
            
            # Ensure command ends with newline
            if not command.endswith('\n'):
                command += '\n'
            
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node.host, node.port),
                timeout=self.timeout
            )
            
            try:
                # Send command
                writer.write(command.encode())
                await writer.drain()
                
                # Read response
                response = await asyncio.wait_for(
                    reader.readline(),
                    timeout=self.timeout
                )
                
                response_str = response.decode().strip()
                success = response_str.startswith('OK')
                
                logger.debug(
                    f"Node {node.node_id} response for '{command.strip()}': {response_str}"
                )
                
                return success, response_str
                
            finally:
                writer.close()
                await writer.wait_closed()
                
        except asyncio.TimeoutError:
            logger.error(f"Timeout connecting to node {node.node_id} at {node.address}")
            return False, "ERROR connection timeout"
        except ConnectionRefusedError:
            logger.error(f"Connection refused to node {node.node_id} at {node.address}")
            return False, "ERROR connection refused"
        except Exception as e:
            logger.error(f"Error communicating with node {node.node_id}: {e}")
            return False, f"ERROR {str(e)}"
    
    async def forward_request(
        self, 
        node: NodeConfig, 
        command: str
    ) -> Tuple[bool, str]:
        """
        Forward a request to the correct primary node.
        Used when this node receives a request for a key it doesn't own.
        
        Args:
            node: Target primary node
            command: Original command from client
            
        Returns:
            Tuple of (success: bool, response: str)
        """
        logger.info(f"Forwarding request to node {node.node_id}: {command.strip()}")
        return await self.send_command(node, command, is_replication=False)
    
    async def replicate_write(
        self, 
        node: NodeConfig, 
        command: str
    ) -> Tuple[bool, str]:
        """
        Replicate a write operation to the replica node.
        Used after successful write on primary.
        
        Args:
            node: Target replica node
            command: Write command (PUT or DELETE)
            
        Returns:
            Tuple of (success: bool, response: str)
        """
        logger.info(f"Replicating to node {node.node_id}: {command.strip()}")
        return await self.send_command(node, command, is_replication=True)


class ReplicationManager:
    """
    Manages synchronous replication for write operations.
    Ensures writes are committed to both primary and replica before returning success.
    """
    
    def __init__(self, cluster_config: ClusterConfig):
        """
        Initialize replication manager.
        
        Args:
            cluster_config: Cluster configuration
        """
        self.cluster_config = cluster_config
        self.node_client = NodeClient()
    
    async def replicate_if_needed(
        self, 
        key: str, 
        command: str,
        local_success: bool
    ) -> Tuple[bool, str]:
        """
        Replicate a write operation if this node is the primary.
        
        Args:
            key: The key being written
            command: The write command (PUT or DELETE)
            local_success: Whether the local write succeeded
            
        Returns:
            Tuple of (overall_success: bool, message: str)
        """
        # If local write failed, don't replicate
        if not local_success:
            return False, "local write failed"
        
        # Only replicate if we're the primary for this key
        if not self.cluster_config.is_primary_for_key(key):
            # We're the replica, don't replicate further
            return True, "stored (replica)"
        
        # Get replica node and replicate
        replica_node = self.cluster_config.get_replica_node_for_key(key)
        
        success, response = await self.node_client.replicate_write(
            replica_node, command
        )
        
        if success:
            logger.info(f"Replication successful to node {replica_node.node_id}")
            return True, "stored"
        else:
            logger.error(f"Replication failed to node {replica_node.node_id}: {response}")
            # For synchronous replication, we fail if replica fails
            return False, f"replication failed: {response}"