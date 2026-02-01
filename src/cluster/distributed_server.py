"""
Distributed TCP Server for KV-Cache with Sharding and Replication.
Extends the base TCP server to support:
1. Consistent hashing for key routing
2. Request forwarding to correct primary node
3. Synchronous replication to replica node
"""
import asyncio
import logging
import time
import os
import argparse
from asyncio import StreamReader, StreamWriter
from collections import OrderedDict
from typing import Optional, Tuple
from enum import Enum
from dataclasses import dataclass

from .config import ClusterConfig, NodeConfig
from .node_client import NodeClient, ReplicationManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Node%(node_id)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Protocol Definitions (matching original Assignment 2)
# ============================================================================

class CommandType(Enum):
    PUT = "PUT"
    GET = "GET"
    DELETE = "DELETE"
    EXISTS = "EXISTS"
    QUIT = "QUIT"
    REPLICA_PUT = "REPLICA_PUT"    # Internal: replication PUT
    REPLICA_DELETE = "REPLICA_DELETE"  # Internal: replication DELETE
    UNKNOWN = "UNKNOWN"


class ResponseStatus(Enum):
    OK = "OK"
    ERROR = "ERROR"


@dataclass
class Command:
    type: CommandType
    key: Optional[str] = None
    value: Optional[str] = None
    ttl: int = 0
    raw_command: str = ""  # Original command string for forwarding


@dataclass
class Response:
    status: ResponseStatus
    message: str = ""
    value: Optional[str] = None


# ============================================================================
# Protocol Parser
# ============================================================================

class ProtocolParser:
    """Parse text protocol commands and format responses."""
    
    MAX_KEY_LENGTH = 256
    MAX_VALUE_LENGTH = 256
    
    def parse_request(self, data: str) -> Command:
        """Parse a raw command string into a Command object."""
        raw_command = data.strip()
        parts = raw_command.split()
        
        if not parts:
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        
        cmd = parts[0].upper()
        
        # Handle REPLICA prefix (internal replication command)
        is_replica = False
        if cmd == "REPLICA":
            is_replica = True
            parts = parts[1:]  # Remove REPLICA prefix
            if not parts:
                return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
            cmd = parts[0].upper()
            # Rebuild raw_command without REPLICA prefix for storage
            raw_command = ' '.join(parts)
        
        try:
            if cmd == "PUT":
                return self._parse_put(parts, raw_command, is_replica)
            elif cmd == "GET":
                return self._parse_get(parts, raw_command)
            elif cmd == "DELETE":
                return self._parse_delete(parts, raw_command, is_replica)
            elif cmd == "EXISTS":
                return self._parse_exists(parts, raw_command)
            elif cmd == "QUIT":
                return Command(type=CommandType.QUIT, raw_command=raw_command)
            else:
                return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        except Exception as e:
            logger.error(f"Error parsing command: {e}")
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
    
    def _parse_put(self, parts: list, raw_command: str, is_replica: bool = False) -> Command:
        """Parse PUT command: PUT <key> <value> [ttl]"""
        if len(parts) < 3:
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        
        key = parts[1]
        value = parts[2]
        ttl = 0
        
        if len(parts) >= 4:
            try:
                ttl = int(parts[3])
                if ttl < 0:
                    ttl = 0
            except ValueError:
                pass
        
        # Validate constraints
        if len(key) > self.MAX_KEY_LENGTH or len(value) > self.MAX_VALUE_LENGTH:
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        
        if ' ' in key or ' ' in value:
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        
        cmd_type = CommandType.REPLICA_PUT if is_replica else CommandType.PUT
        return Command(type=cmd_type, key=key, value=value, ttl=ttl, raw_command=raw_command)
    
    def _parse_get(self, parts: list, raw_command: str) -> Command:
        """Parse GET command: GET <key>"""
        if len(parts) < 2:
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        return Command(type=CommandType.GET, key=parts[1], raw_command=raw_command)
    
    def _parse_delete(self, parts: list, raw_command: str, is_replica: bool = False) -> Command:
        """Parse DELETE command: DELETE <key>"""
        if len(parts) < 2:
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        cmd_type = CommandType.REPLICA_DELETE if is_replica else CommandType.DELETE
        return Command(type=cmd_type, key=parts[1], raw_command=raw_command)
    
    def _parse_exists(self, parts: list, raw_command: str) -> Command:
        """Parse EXISTS command: EXISTS <key>"""
        if len(parts) < 2:
            return Command(type=CommandType.UNKNOWN, raw_command=raw_command)
        return Command(type=CommandType.EXISTS, key=parts[1], raw_command=raw_command)
    
    def format_response(self, response: Response) -> str:
        """Format a Response object into protocol string."""
        if response.status == ResponseStatus.OK:
            if response.value is not None:
                return f"OK {response.value}\n"
            return f"OK {response.message}\n"
        return f"ERROR {response.message}\n"


# ============================================================================
# KV Store with TTL and LRU Eviction
# ============================================================================

class KVStore:
    """
    In-memory Key-Value store with TTL and LRU eviction.
    Uses OrderedDict for O(1) LRU operations.
    """
    
    def __init__(self, max_size: int = 10000):
        """
        Initialize KV store.
        
        Args:
            max_size: Maximum number of keys before LRU eviction
        """
        self._store: OrderedDict[str, Tuple[str, float]] = OrderedDict()
        self.max_size = max_size
    
    def put(self, key: str, value: str, ttl: int = 0) -> bool:
        """
        Store a key-value pair with optional TTL.
        
        Args:
            key: The key to store
            value: The value to store
            ttl: Time-to-live in seconds (0 = no expiration)
            
        Returns:
            True if stored successfully
        """
        expires_at = time.time() + ttl if ttl > 0 else 0
        
        # If key exists, remove it first (for LRU reordering)
        if key in self._store:
            del self._store[key]
        elif len(self._store) >= self.max_size:
            # Evict LRU (first item in OrderedDict)
            self._store.popitem(last=False)
        
        self._store[key] = (value, expires_at)
        return True
    
    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value by key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found and not expired, None otherwise
        """
        if key not in self._store:
            return None
        
        value, expires_at = self._store[key]
        
        # Check TTL expiration
        if expires_at > 0 and time.time() > expires_at:
            del self._store[key]
            return None
        
        # Update LRU order (move to end = most recently used)
        self._store.move_to_end(key)
        return value
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from the store.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key existed and was deleted, False otherwise
        """
        if key not in self._store:
            return False
        
        # Check if expired (treat as not found)
        value, expires_at = self._store[key]
        if expires_at > 0 and time.time() > expires_at:
            del self._store[key]
            return False
        
        del self._store[key]
        return True
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists and is not expired.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists and is not expired
        """
        if key not in self._store:
            return False
        
        value, expires_at = self._store[key]
        
        # Check TTL expiration
        if expires_at > 0 and time.time() > expires_at:
            del self._store[key]
            return False
        
        return True
    
    def size(self) -> int:
        """Return the number of keys in the store."""
        return len(self._store)


# ============================================================================
# Distributed KV Server
# ============================================================================

class DistributedKVServer:
    """
    Distributed KV-Cache Server with sharding and replication.
    
    Features:
    - Consistent hashing for key routing
    - Request forwarding to correct primary node
    - Synchronous replication to replica node
    """
    
    def __init__(
        self,
        host: str = '0.0.0.0',
        port: int = 7171,
        max_cache_size: int = 10000,
        node_id: int = None
    ):
        """
        Initialize distributed KV server.
        
        Args:
            host: Host to bind to
            port: Port to listen on
            max_cache_size: Maximum keys in local cache
            node_id: This node's ID (1, 2, or 3)
        """
        self.host = host
        self.port = port
        self.store = KVStore(max_size=max_cache_size)
        self.parser = ProtocolParser()
        
        # Cluster configuration
        self.cluster_config = ClusterConfig(current_node_id=node_id)
        self.node_client = NodeClient()
        self.replication_manager = ReplicationManager(self.cluster_config)
        
        # For logging with node context
        self.log_extra = {'node_id': self.cluster_config.current_node_id}
        
        logger.info(
            f"Node {self.cluster_config.current_node_id} initialized on {host}:{port}",
            extra=self.log_extra
        )
        logger.info(
            f"Primary for shards: {self.cluster_config.get_shards_as_primary()}",
            extra=self.log_extra
        )
        logger.info(
            f"Replica for shards: {self.cluster_config.get_shards_as_replica()}",
            extra=self.log_extra
        )
    
    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        """Handle a single client connection."""
        addr = writer.get_extra_info('peername')
        logger.debug(f"New connection from {addr}", extra=self.log_extra)
        
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                
                request = data.decode().strip()
                if not request:
                    continue
                
                logger.debug(f"Received: {request}", extra=self.log_extra)
                
                command = self.parser.parse_request(request)
                
                if command.type == CommandType.QUIT:
                    break
                
                response = await self._execute_command(command)
                response_str = self.parser.format_response(response)
                
                logger.debug(f"Response: {response_str.strip()}", extra=self.log_extra)
                
                writer.write(response_str.encode())
                await writer.drain()
                
        except ConnectionResetError:
            logger.debug(f"Connection reset by {addr}", extra=self.log_extra)
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}", extra=self.log_extra)
        finally:
            writer.close()
            await writer.wait_closed()
            logger.debug(f"Connection closed: {addr}", extra=self.log_extra)
    
    async def _execute_command(self, command: Command) -> Response:
        """
        Execute a command with sharding and replication logic.
        
        For writes (PUT/DELETE):
        1. If we're the primary: execute locally, then replicate
        2. If we're not primary: forward to primary
        
        For reads (GET/EXISTS):
        1. If we're the primary: execute locally
        2. If we're not primary: forward to primary
        """
        if command.type == CommandType.UNKNOWN:
            return Response(status=ResponseStatus.ERROR, message="unknown command")
        
        # Handle REPLICA commands (internal replication - no further replication)
        if command.type == CommandType.REPLICA_PUT:
            return self._execute_replica_put(command)
        
        if command.type == CommandType.REPLICA_DELETE:
            return self._execute_replica_delete(command)
        
        # For write operations
        if command.type in (CommandType.PUT, CommandType.DELETE):
            return await self._handle_write(command)
        
        # For read operations
        if command.type in (CommandType.GET, CommandType.EXISTS):
            return await self._handle_read(command)
        
        return Response(status=ResponseStatus.ERROR, message="unknown command")
    
    def _execute_replica_put(self, command: Command) -> Response:
        """Execute a replica PUT (no further replication)."""
        success = self.store.put(command.key, command.value, command.ttl)
        if success:
            logger.info(
                f"Replica stored: {command.key}",
                extra=self.log_extra
            )
            return Response(status=ResponseStatus.OK, message="stored")
        return Response(status=ResponseStatus.ERROR, message="store failed")
    
    def _execute_replica_delete(self, command: Command) -> Response:
        """Execute a replica DELETE (no further replication)."""
        # For replica deletes, we delete if exists, but don't fail if not found
        # (the key might have expired or been evicted locally)
        self.store.delete(command.key)
        logger.info(
            f"Replica deleted: {command.key}",
            extra=self.log_extra
        )
        return Response(status=ResponseStatus.OK, message="deleted")
    
    async def _handle_write(self, command: Command) -> Response:
        """Handle write operations (PUT/DELETE) with forwarding and replication."""
        key = command.key
        
        # Check if we're the primary for this key
        if not self.cluster_config.is_primary_for_key(key):
            # Forward to the correct primary node
            primary_node = self.cluster_config.get_primary_node_for_key(key)
            shard_id = self.cluster_config.get_shard_for_key(key)
            
            logger.info(
                f"Forwarding {command.type.value} for key '{key}' (shard {shard_id}) to Node {primary_node.node_id}",
                extra=self.log_extra
            )
            
            success, response = await self.node_client.forward_request(
                primary_node, command.raw_command
            )
            
            # Parse and return the response from primary
            if response.startswith('OK'):
                msg = response[3:].strip() if len(response) > 3 else "stored"
                return Response(status=ResponseStatus.OK, message=msg)
            else:
                msg = response[6:].strip() if len(response) > 6 else "forward failed"
                return Response(status=ResponseStatus.ERROR, message=msg)
        
        # We're the primary - execute locally first
        shard_id = self.cluster_config.get_shard_for_key(key)
        logger.info(
            f"Primary handling {command.type.value} for key '{key}' (shard {shard_id})",
            extra=self.log_extra
        )
        
        if command.type == CommandType.PUT:
            local_success = self.store.put(key, command.value, command.ttl)
            if not local_success:
                return Response(status=ResponseStatus.ERROR, message="store failed")
        else:  # DELETE
            local_success = self.store.delete(key)
            if not local_success:
                return Response(status=ResponseStatus.ERROR, message="key not found")
        
        # Replicate to replica node (synchronous replication)
        replica_node = self.cluster_config.get_replica_node_for_key(key)
        
        logger.info(
            f"Replicating {command.type.value} for key '{key}' to Node {replica_node.node_id}",
            extra=self.log_extra
        )
        
        repl_success, repl_response = await self.node_client.replicate_write(
            replica_node, command.raw_command
        )
        
        if repl_success:
            logger.info(
                f"Replication successful for key '{key}'",
                extra=self.log_extra
            )
            if command.type == CommandType.PUT:
                return Response(status=ResponseStatus.OK, message="stored")
            else:
                return Response(status=ResponseStatus.OK, message="deleted")
        else:
            # For synchronous replication, fail if replica fails
            logger.error(
                f"Replication failed for key '{key}': {repl_response}",
                extra=self.log_extra
            )
            return Response(status=ResponseStatus.ERROR, message=f"replication failed")
    
    async def _handle_read(self, command: Command) -> Response:
        """Handle read operations (GET/EXISTS) - reads go to primary only."""
        key = command.key
        
        # Check if we're the primary for this key
        if not self.cluster_config.is_primary_for_key(key):
            # Forward to the correct primary node
            primary_node = self.cluster_config.get_primary_node_for_key(key)
            shard_id = self.cluster_config.get_shard_for_key(key)
            
            logger.debug(
                f"Forwarding {command.type.value} for key '{key}' (shard {shard_id}) to Node {primary_node.node_id}",
                extra=self.log_extra
            )
            
            success, response = await self.node_client.forward_request(
                primary_node, command.raw_command
            )
            
            # Parse and return the response
            if response.startswith('OK'):
                parts = response.split(' ', 1)
                if len(parts) > 1:
                    return Response(status=ResponseStatus.OK, value=parts[1])
                return Response(status=ResponseStatus.OK, message="1")
            else:
                msg = response[6:].strip() if len(response) > 6 else "not found"
                return Response(status=ResponseStatus.ERROR, message=msg)
        
        # We're the primary - execute locally
        if command.type == CommandType.GET:
            value = self.store.get(key)
            if value is not None:
                return Response(status=ResponseStatus.OK, value=value)
            return Response(status=ResponseStatus.ERROR, message="key not found")
        
        else:  # EXISTS
            exists = self.store.exists(key)
            return Response(status=ResponseStatus.OK, value="1" if exists else "0")
    
    async def start(self):
        """Start the distributed KV server."""
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        logger.info(f"Server listening on {addrs}", extra=self.log_extra)
        
        async with server:
            await server.serve_forever()


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Main entry point for the distributed KV server."""
    parser = argparse.ArgumentParser(description='Distributed KV-Cache Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=7171, help='Port to listen on')
    parser.add_argument('--node-id', type=int, help='Node ID (1, 2, or 3)')
    parser.add_argument('--max-size', type=int, default=10000, help='Max cache size')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get node ID from args or environment
    node_id = args.node_id or int(os.environ.get('NODE_ID', '1'))
    
    server = DistributedKVServer(
        host=args.host,
        port=args.port,
        max_cache_size=args.max_size,
        node_id=node_id
    )
    
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\nServer shutting down...")


if __name__ == '__main__':
    main()