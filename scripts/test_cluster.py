#!/usr/bin/env python3
"""
Test script for Distributed KV-Cache cluster.

Usage:
    1. Start the cluster: docker-compose up -d
    2. Run this script: python scripts/test_cluster.py
"""
import socket
import sys
import time


def send_command(host: str, port: int, command: str) -> str:
    """Send a command to a node and return the response."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((host, port))
            s.sendall(f"{command}\n".encode())
            response = s.recv(1024).decode().strip()
            return response
    except Exception as e:
        return f"ERROR: {e}"


def test_basic_operations():
    """Test basic PUT/GET/DELETE operations."""
    print("\n" + "="*60)
    print("TEST: Basic Operations")
    print("="*60)
    
    nodes = [
        ("localhost", 7171, "Node1"),
        ("localhost", 7172, "Node2"),
        ("localhost", 7173, "Node3"),
    ]
    
    # Test PUT on Node 1
    print(f"\n[Node1:7171] PUT test_key test_value")
    response = send_command("localhost", 7171, "PUT test_key test_value")
    print(f"  Response: {response}")
    assert "OK" in response, f"PUT failed: {response}"
    
    # Test GET from all nodes
    for host, port, name in nodes:
        print(f"\n[{name}:{port}] GET test_key")
        response = send_command(host, port, "GET test_key")
        print(f"  Response: {response}")
        assert response == "OK test_value", f"GET failed on {name}: {response}"
    
    # Test EXISTS from all nodes
    for host, port, name in nodes:
        print(f"\n[{name}:{port}] EXISTS test_key")
        response = send_command(host, port, "EXISTS test_key")
        print(f"  Response: {response}")
        assert response == "OK 1", f"EXISTS failed on {name}: {response}"
    
    # Test DELETE
    print(f"\n[Node2:7172] DELETE test_key")
    response = send_command("localhost", 7172, "DELETE test_key")
    print(f"  Response: {response}")
    assert "OK" in response, f"DELETE failed: {response}"
    
    # Verify deletion
    print(f"\n[Node3:7173] GET test_key (after delete)")
    response = send_command("localhost", 7173, "GET test_key")
    print(f"  Response: {response}")
    assert "ERROR" in response or "not found" in response.lower(), f"Key should be deleted: {response}"
    
    print("\n✅ Basic operations test PASSED")


def test_sharding():
    """Test that keys are distributed across shards."""
    print("\n" + "="*60)
    print("TEST: Sharding Distribution")
    print("="*60)
    
    # Store multiple keys and check they're accessible from all nodes
    test_keys = [f"shard_test_{i}" for i in range(10)]
    
    print("\nStoring 10 keys via Node 1...")
    for key in test_keys:
        response = send_command("localhost", 7171, f"PUT {key} value_{key}")
        if "OK" not in response:
            print(f"  ❌ Failed to store {key}: {response}")
            return
    
    print("Verifying all keys accessible from all nodes...")
    for port, name in [(7171, "Node1"), (7172, "Node2"), (7173, "Node3")]:
        success = 0
        for key in test_keys:
            response = send_command("localhost", port, f"GET {key}")
            if f"OK value_{key}" in response:
                success += 1
        print(f"  {name}: {success}/{len(test_keys)} keys accessible")
        assert success == len(test_keys), f"Not all keys accessible from {name}"
    
    # Cleanup
    for key in test_keys:
        send_command("localhost", 7171, f"DELETE {key}")
    
    print("\n✅ Sharding distribution test PASSED")


def test_replication():
    """Test that writes are replicated."""
    print("\n" + "="*60)
    print("TEST: Replication")
    print("="*60)
    
    # Write to Node 1
    print("\n[Node1] PUT repl_test_key repl_test_value")
    response = send_command("localhost", 7171, "PUT repl_test_key repl_test_value")
    print(f"  Response: {response}")
    
    # Both primary and replica should have the data
    # The key goes to a specific shard, but we can verify via any node
    print("\nVerifying replication by reading from all nodes...")
    for port, name in [(7171, "Node1"), (7172, "Node2"), (7173, "Node3")]:
        response = send_command("localhost", port, "GET repl_test_key")
        print(f"  [{name}] GET repl_test_key -> {response}")
        assert "OK repl_test_value" in response, f"Replication verification failed on {name}"
    
    # Cleanup
    send_command("localhost", 7171, "DELETE repl_test_key")
    
    print("\n✅ Replication test PASSED")


def test_ttl():
    """Test TTL works across the cluster."""
    print("\n" + "="*60)
    print("TEST: TTL (Time-To-Live)")
    print("="*60)
    
    # Set key with 2 second TTL
    print("\n[Node1] PUT ttl_test_key ttl_test_value 2")
    response = send_command("localhost", 7171, "PUT ttl_test_key ttl_test_value 2")
    print(f"  Response: {response}")
    
    # Verify key exists
    response = send_command("localhost", 7172, "GET ttl_test_key")
    print(f"\n[Node2] GET ttl_test_key (immediately) -> {response}")
    assert "OK ttl_test_value" in response
    
    # Wait for TTL to expire
    print("\nWaiting 3 seconds for TTL to expire...")
    time.sleep(3)
    
    # Verify key expired
    response = send_command("localhost", 7173, "GET ttl_test_key")
    print(f"\n[Node3] GET ttl_test_key (after TTL) -> {response}")
    assert "ERROR" in response or "not found" in response.lower(), "Key should have expired"
    
    print("\n✅ TTL test PASSED")


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("   DISTRIBUTED KV-CACHE CLUSTER TEST")
    print("="*60)
    
    # Check if cluster is running
    print("\nChecking cluster connectivity...")
    for port, name in [(7171, "Node1"), (7172, "Node2"), (7173, "Node3")]:
        try:
            response = send_command("localhost", port, "EXISTS __health_check__")
            print(f"  ✅ {name} (port {port}) is reachable")
        except Exception as e:
            print(f"  ❌ {name} (port {port}) is NOT reachable")
            print(f"\nPlease start the cluster first:")
            print("  docker-compose up -d")
            sys.exit(1)
    
    # Run tests
    try:
        test_basic_operations()
        test_sharding()
        test_replication()
        test_ttl()
        
        print("\n" + "="*60)
        print("   🎉 ALL TESTS PASSED!")
        print("="*60 + "\n")
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()