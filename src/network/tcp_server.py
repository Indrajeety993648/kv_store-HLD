"""
Async TCP Server Module (Task 3)

This module implements the asynchronous TCP server for KV-Cache.

Students must implement:
- handle_client(): Handle a single client connection
- start(): Start the server and accept connections

Key asyncio concepts needed:
- asyncio.start_server(): Create a TCP server
- StreamReader.readline(): Read a line from client
- StreamWriter.write() / drain(): Send data to client
- Proper connection cleanup with writer.close() / wait_closed()
"""

import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from typing import Optional

from src.cache.store import KVStore
from src.config.settings import settings
from src.protocol.commands import CommandType, Response
from src.protocol.parser import ProtocolParser

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KVServer:
    """
    Asynchronous TCP server for the KV-Cache service.

    This server handles multiple concurrent clients using asyncio.
    Each client connection is handled in a separate coroutine,
    allowing for high concurrency without threading.

    Features:
    - Non-blocking I/O with asyncio
    - Persistent connections (multiple commands per connection)
    - Graceful error handling and connection cleanup
    - Shared KVStore across all connections

    Usage:
        server = KVServer(host='0.0.0.0', port=7171)
        await server.start()  # Runs forever

    Attributes:
        host: Server bind address (e.g., '0.0.0.0')
        port: Server port number (e.g., 7171)
        store: The KVStore instance shared by all connections
        parser: The ProtocolParser for parsing commands
    """

    def __init__(
            self,
            host: str = None,
            port: int = None,
            store: KVStore = None,
    ):
        """
        Initialize the server.

        Args:
            host: Bind address (default from settings)
            port: Port number (default from settings)
            store: KVStore instance (creates new one if not provided)
        """
        self.host = host if host is not None else settings.HOST
        self.port = port if port is not None else settings.PORT
        self.store = store if store is not None else KVStore()
        self.parser = ProtocolParser()

        # Server state
        self._server: Optional[asyncio.Server] = None
        self._running = False
        self._connection_count = 0
        self._total_requests = 0

    async def handle_client(
            self,
            reader: StreamReader,
            writer: StreamWriter
    ) -> None:
        """
        Handle a single client connection.

        This coroutine is called for each new client connection.
        It reads commands from the client, processes them, and sends
        responses until the client disconnects or sends QUIT.

        Args:
            reader: StreamReader for reading from the client
            writer: StreamWriter for writing to the client

        Protocol flow:
            1. Read a line (command) from the client
            2. Parse the command using ProtocolParser
            3. Execute the command on the KVStore
            4. Format and send the response
            5. Repeat until QUIT or client disconnects

        Implementation requirements:
        - Get client address for logging (writer.get_extra_info('peername'))
        - Loop reading lines until empty data (disconnect) or QUIT
        - Handle decode errors gracefully
        - Always close the writer in a finally block
        - Handle ConnectionResetError and other exceptions
        """
        # Get client address for logging
        peername = writer.get_extra_info('peername')
        client_addr = f"{peername[0]}:{peername[1]}" if peername else "unknown"
        
        # Track connection
        self._connection_count += 1
        logger.debug(f"Client connected: {client_addr}")

        try:
            while True:
                # Read a line from the client
                try:
                    data = await reader.readline()
                except ConnectionResetError:
                    logger.debug(f"Client {client_addr} reset connection")
                    break

                # Empty data means client disconnected
                if not data:
                    logger.debug(f"Client {client_addr} disconnected")
                    break

                # Decode the data
                try:
                    request = data.decode('utf-8')
                except UnicodeDecodeError:
                    # Handle decode errors gracefully
                    response = Response.error("invalid encoding")
                    response_str = self.parser.format_response(response)
                    writer.write(response_str.encode('utf-8'))
                    await writer.drain()
                    continue

                # Track requests
                self._total_requests += 1
                logger.debug(f"Received from {client_addr}: {request.strip()}")

                # Parse the command
                command = self.parser.parse_request(request)

                # Handle QUIT command - close connection without response
                if command.type == CommandType.QUIT:
                    logger.debug(f"Client {client_addr} sent QUIT")
                    break

                # Execute the command and get response
                response = self._execute_command(command)

                # Format and send response
                response_str = self.parser.format_response(response)
                writer.write(response_str.encode('utf-8'))
                await writer.drain()

                logger.debug(f"Sent to {client_addr}: {response_str.strip()}")

        except ConnectionResetError:
            logger.debug(f"Client {client_addr} reset connection")
        except Exception as e:
            logger.error(f"Error handling client {client_addr}: {e}")
        finally:
            # Always close the writer
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug(f"Connection closed: {client_addr}")

    def _execute_command(self, command) -> Response:
        """
        Execute a parsed command on the store.

        This is a helper method that routes commands to the appropriate
        KVStore method and returns the formatted response.

        Args:
            command: The Command object to execute

        Returns:
            Response object with the result
        """
        # Handle UNKNOWN/invalid commands
        if command.type == CommandType.UNKNOWN or not command.is_valid:
            return Response.error("invalid command")

        # Route to appropriate store method
        if command.type == CommandType.PUT:
            # PUT key value [ttl]
            self.store.put(command.key, command.value, command.ttl)
            return Response.stored()

        elif command.type == CommandType.GET:
            # GET key
            value = self.store.get(command.key)
            if value is None:
                return Response.key_not_found()
            return Response.value_response(value)

        elif command.type == CommandType.DELETE:
            # DELETE key
            deleted = self.store.delete(command.key)
            if not deleted:
                return Response.key_not_found()
            return Response.deleted()

        elif command.type == CommandType.EXISTS:
            # EXISTS key
            exists = self.store.exists(command.key)
            return Response.exists_response(exists)

        else:
            # Should not reach here, but handle gracefully
            return Response.error("unknown command")

    async def start(self) -> None:
        """
        Start the server and begin accepting connections.

        This method creates the asyncio server and runs forever
        (or until cancelled). It should be called from asyncio.run()
        or within an existing event loop.

        Implementation:
        - Use asyncio.start_server() with self.handle_client as callback
        - Log the server address when started
        - Use server.serve_forever() to run indefinitely

        Example:
            server = KVServer(port=7171)
            asyncio.run(server.start())
        """
        # Create the server
        self._server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        self._running = True
        
        # Get the actual address we're listening on
        addrs = ', '.join(str(sock.getsockname()) for sock in self._server.sockets)
        logger.info(f"KV-Cache server started on {addrs}")

        # Run the server forever
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """
        Stop the server gracefully.

        Closes the server and waits for it to fully shut down.
        """
        if self._server is not None:
            self._running = False
            self._server.close()
            await self._server.wait_closed()
            logger.info("KV-Cache server stopped")

    def is_running(self) -> bool:
        """Check if the server is currently running."""
        return self._running

    def get_stats(self) -> dict:
        """
        Get server statistics.

        Returns:
            Dictionary with server stats including connection counts,
            request counts, and store statistics.
        """
        return {
            "running": self._running,
            "host": self.host,
            "port": self.port,
            "total_connections": self._connection_count,
            "total_requests": self._total_requests,
            "store_stats": self.store.get_stats(),
        }


async def run_server(host: str = None, port: int = None) -> None:
    """
    Convenience function to create and run the server.

    Args:
        host: Bind address (default from settings)
        port: Port number (default from settings)

    Usage:
        asyncio.run(run_server(port=7171))
    """
    server = KVServer(host=host, port=port)

    try:
        await server.start()
    except asyncio.CancelledError:
        logger.info("Server shutdown requested")
    finally:
        await server.stop()