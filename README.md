# 🚀 KV-Cache: High-Performance In-Memory Key-Value Store

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Tests](https://img.shields.io/badge/Tests-170%20Passed-brightgreen.svg)
![Performance](https://img.shields.io/badge/Performance-11%2C117%20req%2Fs-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**A high-performance, in-memory key-value cache server built with Python asyncio**

[Features](#-features) • [Architecture](#-architecture) • [Installation](#-installation) • [Usage](#-usage) • [Performance](#-performance-results) • [Implementation](#-implementation-details)

</div>

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Usage](#-usage)
- [Protocol Specification](#-protocol-specification)
- [Implementation Details](#-implementation-details)
  - [Task 1: Core KV Store](#task-1-core-kv-store)
  - [Task 2: Protocol Parser](#task-2-protocol-parser)
  - [Task 3: TCP Server](#task-3-async-tcp-server)
  - [Task 4: TTL Support](#task-4-ttl-time-to-live)
  - [Task 5: LRU Eviction](#task-5-lru-eviction)
- [Test Results](#-test-results)
- [Performance Results](#-performance-results)
- [Project Structure](#-project-structure)
- [Technical Decisions](#-technical-decisions)
- [What I Learned](#-what-i-learned)

---

## 🎯 Project Overview

This project implements a **Redis-like in-memory key-value cache server** from scratch using Python. It demonstrates core concepts of:

- **Systems Programming**: Low-level TCP socket handling
- **Concurrent Programming**: Async I/O with Python's asyncio
- **Data Structures**: OrderedDict for O(1) LRU operations
- **Protocol Design**: Text-based request/response protocol
- **Cache Systems**: TTL expiration and LRU eviction policies

### 🏆 Achievement Summary

| Metric | Target | Achieved | Status |
|--------|--------|----------|--------|
| Test Cases | Pass All | **170/170** | ✅ |
| Throughput | ≥ 5,000 req/s | **11,117 req/s** | ✅ 2.2x |
| Mean Latency | ≤ 10ms | **4.44ms** | ✅ |
| P99 Latency | ≤ 20ms | **5.93ms** | ✅ |
| Error Rate | 0% | **0%** | ✅ |

---

## ✨ Features

- ⚡ **High Performance**: 11,000+ requests/second
- 🔄 **Async I/O**: Non-blocking concurrent client handling
- ⏰ **TTL Support**: Automatic key expiration with lazy + active cleanup
- 📊 **LRU Eviction**: Least Recently Used eviction when cache is full
- 🐳 **Docker Ready**: Containerized deployment
- 🧪 **Well Tested**: 170 comprehensive test cases
- 📝 **Simple Protocol**: Human-readable text protocol

---

## 🏗 Architecture

### High-Level System Architecture

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/654e148b-0887-4b75-b07d-1f2820ef9d01" />

### Request-Response Flow

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/1c04089c-8957-40d4-b5ad-fb4cb7bcb462" />


### Data Structure: OrderedDict for LRU

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/e881478c-d09d-4f9e-bdf7-c8ae75e618f9" />


### TTL (Time-To-Live) Mechanism

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/9e23fc27-2f7c-4655-b491-2ed68d9ae7c4" />


## 📦 Installation

### Prerequisites

- Python 3.10 or higher
- pip (Python package manager)
- Docker (optional, for containerized deployment)

### Setup Steps

```bash
# 1. Clone the repository
git clone https://github.com/AgarwalPragy/kv-cache.git
cd kv-cache

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Install dependencies
pip install -r requirements.txt
pip install -e .

# 4. Verify installation
python -c "from src.config.settings import Settings; print('✅ Setup successful!')"
```

### Docker Installation

```bash
# Build Docker image
docker build -t kv-cache .

# Run container
docker run -p 7171:7171 kv-cache
```

---

## 🚀 Usage

### Starting the Server

```bash
# Method 1: Direct Python
python -m src.server

# Method 2: With custom port
python -m src.server --port 7171

# Method 3: Docker
docker run -p 7171:7171 kv-cache
```

### Client Examples

#### Using the Interactive Client

```bash
$ python scripts/client.py
KV-Cache Client
===============
Connecting to localhost:7171...
Connected! Type 'help' for commands.

>>> PUT username indrajeet
OK stored

>>> PUT city bangalore
OK stored

>>> GET username
OK indrajeet

>>> EXISTS username
OK 1

>>> DELETE username
OK deleted

>>> GET username
ERROR key not found

>>> QUIT
Goodbye!
```

#### Using Python Socket

```python
import socket

# Connect to server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 7171))

# Send commands
s.send(b'PUT mykey myvalue\n')
print(s.recv(1024).decode())  # OK stored

s.send(b'GET mykey\n')
print(s.recv(1024).decode())  # OK myvalue

s.close()
```

#### Using netcat

```bash
$ nc localhost 7171
PUT name Indra
OK stored
GET name
OK Indra
QUIT
```

---

## 📡 Protocol Specification

### Request Format

```
<COMMAND> [ARGUMENTS]\n
```

### Commands

| Command | Format | Success Response | Error Response |
|---------|--------|------------------|----------------|
| **PUT** | `PUT <key> <value> [ttl]` | `OK stored\n` | `ERROR <msg>\n` |
| **GET** | `GET <key>` | `OK <value>\n` | `ERROR key not found\n` |
| **DELETE** | `DELETE <key>` | `OK deleted\n` | `ERROR key not found\n` |
| **EXISTS** | `EXISTS <key>` | `OK 1\n` or `OK 0\n` | `ERROR <msg>\n` |
| **QUIT** | `QUIT` | *(connection closes)* | - |

### Constraints

| Parameter | Constraint |
|-----------|------------|
| Key Length | 1-256 ASCII characters, no whitespace |
| Value Length | 1-256 ASCII characters, no whitespace |
| TTL | 0-2147483647 seconds (0 = no expiration) |

### Examples

```
Request:  PUT session abc123 3600
Response: OK stored

Request:  GET session
Response: OK abc123

Request:  EXISTS session
Response: OK 1

Request:  DELETE session
Response: OK deleted

Request:  GET session
Response: ERROR key not found
```

---

## 🔧 Implementation Details

### Task 1: Core KV Store

**File**: `src/cache/store.py`

**Objective**: Implement basic key-value operations with O(1) time complexity.

```python
class KVStore:
    def __init__(self, max_size: int):
        self._store: OrderedDict[str, Tuple[str, float]] = OrderedDict()
        self.max_size = max_size
    
    def put(self, key: str, value: str, ttl: int = 0) -> bool:
        """Store key-value pair with optional TTL"""
        expires_at = time.time() + ttl if ttl > 0 else 0
        
        if key in self._store:
            del self._store[key]  # Remove for LRU reordering
        elif len(self._store) >= self.max_size:
            self._store.popitem(last=False)  # Evict LRU
        
        self._store[key] = (value, expires_at)
        return True
    
    def get(self, key: str) -> Optional[str]:
        """Retrieve value, check TTL, update LRU order"""
        if key not in self._store:
            return None
        
        value, expires_at = self._store[key]
        
        # Check expiration
        if expires_at > 0 and time.time() > expires_at:
            del self._store[key]
            return None
        
        self._store.move_to_end(key)  # Update LRU
        return value
```

**Key Design Decisions**:
- Used `OrderedDict` for O(1) operations with order tracking
- Storage format: `key → (value, expiration_timestamp)`
- Combined TTL and LRU in single data structure

---

### Task 2: Protocol Parser

**File**: `src/protocol/parser.py`

**Objective**: Parse text commands into Command objects, format responses.

```python
class ProtocolParser:
    def parse_request(self, data: str) -> Command:
        """Parse 'PUT key value 60' into Command object"""
        parts = data.strip().split()
        if not parts:
            return Command(type=CommandType.UNKNOWN)
        
        cmd = parts[0].upper()
        
        if cmd == "PUT":
            return self._parse_put(parts, data)
        elif cmd == "GET":
            return self._parse_get(parts, data)
        # ... etc
    
    def format_response(self, response: Response) -> str:
        """Format Response object to 'OK stored\n'"""
        if response.status == ResponseStatus.OK:
            if response.value is not None:
                return f"OK {response.value}\n"
            return f"OK {response.message}\n"
        return f"ERROR {response.message}\n"
```

**Key Design Decisions**:
- Case-insensitive command parsing (`PUT`, `put`, `Put` all work)
- Validates key/value length constraints
- Returns `UNKNOWN` command type for invalid input

---

### Task 3: Async TCP Server

**File**: `src/network/tcp_server.py`

**Objective**: Handle multiple concurrent clients using asyncio.

```python
class KVServer:
    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        """Handle single client connection"""
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break  # Client disconnected
                
                command = self.parser.parse_request(data.decode())
                
                if command.type == CommandType.QUIT:
                    break
                
                response = self._execute_command(command)
                writer.write(self.parser.format_response(response).encode())
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def start(self):
        """Start the server"""
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        async with server:
            await server.serve_forever()
```

**Key Design Decisions**:
- `asyncio.start_server()` for non-blocking I/O
- Each client handled in separate coroutine
- Proper cleanup in `finally` block

---

### Task 4: TTL (Time-To-Live)

**Objective**: Automatic key expiration after specified time.

**Implementation Strategy**: Lazy Expiration + Active Cleanup

```
┌────────────────────────────────────────────────────────────┐
│                   TTL Implementation                       │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  1. LAZY EXPIRATION (On Access)                            │
│     ─────────────────────────────                          │
│     When get()/exists() is called:                         │
│     • Check if time.time() > expires_at                    │
│     • If expired: delete key, return None/False            │
│                                                            │
│  2. ACTIVE CLEANUP (Periodic)                              │
│     ─────────────────────────────                          │
│     cleanup_expired() method:                              │
│     • Scan all keys                                        │
│     • Remove keys where time.time() > expires_at           │
│     • Can be called by background task                     │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

---

### Task 5: LRU Eviction

**File**: `src/cache/eviction.py`

**Objective**: Evict least recently used items when cache is full.

```python
class LRUEvictionPolicy:
    def __init__(self, max_size: int):
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self.max_size = max_size
    
    def get(self, key: str) -> Optional[Any]:
        """Get value and mark as recently used"""
        if key not in self._cache:
            return None
        self._cache.move_to_end(key)  # Move to MRU
        return self._cache[key]
    
    def put(self, key: str, value: Any) -> Optional[str]:
        """Put value, evict LRU if full"""
        evicted = None
        
        if key in self._cache:
            del self._cache[key]
        elif len(self._cache) >= self.max_size:
            evicted, _ = self._cache.popitem(last=False)  # Evict LRU
        
        self._cache[key] = value
        return evicted
```

**LRU Operations Complexity**:

| Operation | Time Complexity | Method Used |
|-----------|-----------------|-------------|
| Get | O(1) | `move_to_end()` |
| Put | O(1) | `popitem(last=False)` |
| Delete | O(1) | `del dict[key]` |
| Evict LRU | O(1) | `popitem(last=False)` |

---

## 🧪 Test Results

### Test Summary

```
============================================== test session starts ==============================================
platform linux -- Python 3.14.2, pytest-9.0.2
collected 170 items

tests/test_store.py      ✅ 35 passed
tests/test_protocol.py   ✅ 55 passed  
tests/test_server.py     ✅ 21 passed
tests/test_ttl.py        ✅ passed
tests/test_eviction.py   ✅ passed
tests/test_integration.py ✅ passed

============================================= 170 passed in 25.74s ==============================================
```

### Test Categories

| Test File | Tests | Description |
|-----------|-------|-------------|
| `test_store.py` | 35 | KVStore operations (put, get, delete, exists) |
| `test_protocol.py` | 55 | Protocol parsing and response formatting |
| `test_server.py` | 21 | TCP server, concurrency, edge cases |
| `test_ttl.py` | ~20 | TTL expiration, lazy/active cleanup |
| `test_eviction.py` | ~25 | LRU eviction policy |
| `test_integration.py` | ~14 | End-to-end system tests |

---

## 📊 Performance Results

### Load Test Configuration

```bash
python scripts/load_test.py \
  --host localhost \
  --port 7171 \
  --connections 50 \
  --requests 1000
```

### Results

```
============================================================
                    LOAD TEST RESULTS
============================================================
Total Requests:     50,000
Successful:         50,000 (100.00%)
Failed:             0 (0.00%)
------------------------------------------------------------
Total Time:         4.50 seconds
Requests/Second:    11,117.09
Latency (ms):
  Min:              2.05
  Max:              15.81
  Mean:             4.44
  Median:           4.40
  P95:              5.37
  P99:              5.93
Operations:
  PUT:              25,141 (success: 25,141)
  GET:              24,859 (success: 24,859)
  Cache Hits:       15,108 (60.77%)
  Cache Misses:     9,751
============================================================
Performance Assessment:
  ✓ Throughput: 11117 req/s (target: ≥5,000)
  ✓ Mean latency: 4.44ms (target: ≤10ms)
  ✓ P99 latency: 5.93ms (target: ≤20ms)
  ✓ Error rate: 0.00% (target: 0%)
  🎉 All performance targets met!
```

### Performance Visualization

```
    Throughput: 11,117 req/s
    ████████████████████████████████████████████░░░░░░░░ 222% of target

    Mean Latency: 4.44ms  
    ████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 44% of limit ✓

    P99 Latency: 5.93ms
    ████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 30% of limit ✓

    Error Rate: 0.00%
    ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Perfect ✓
```

---

## 📁 Project Structure

```
kv-cache/
├── src/
│   ├── __init__.py
│   ├── server.py              # Entry point
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── store.py           # ✅ Task 1 & 4: KVStore with TTL
│   │   └── eviction.py        # ✅ Task 5: LRU Eviction Policy
│   ├── protocol/
│   │   ├── __init__.py
│   │   ├── commands.py        # Command/Response data classes
│   │   └── parser.py          # ✅ Task 2: Protocol Parser
│   ├── network/
│   │   ├── __init__.py
│   │   └── tcp_server.py      # ✅ Task 3: Async TCP Server
│   └── config/
│       ├── __init__.py
│       └── settings.py        # Configuration settings
├── tests/
│   ├── conftest.py            # Pytest fixtures
│   ├── test_store.py          # Task 1 tests
│   ├── test_protocol.py       # Task 2 tests
│   ├── test_server.py         # Task 3 tests
│   ├── test_ttl.py            # Task 4 tests
│   ├── test_eviction.py       # Task 5 tests
│   └── test_integration.py    # Integration tests
├── scripts/
│   ├── client.py              # Interactive client
│   └── load_test.py           # Performance testing
├── Dockerfile
├── requirements.txt
├── setup.py
├── pytest.ini
└── README.md
```

---

## 🧠 Technical Decisions

### Why OrderedDict?

| Alternative | Pros | Cons | Decision |
|------------|------|------|----------|
| `dict` | O(1) ops | No order tracking | ❌ |
| `list` | Order preserved | O(n) search | ❌ |
| `OrderedDict` | O(1) ops + order | Slightly more memory | ✅ |
| Custom DLL + HashMap | Most flexible | Complex implementation | ❌ |

**Conclusion**: `OrderedDict` provides the best balance of performance and simplicity.

### Why asyncio over threading?

| Aspect | Threading | Asyncio |
|--------|-----------|---------|
| Memory per connection | ~1MB stack | ~1KB coroutine |
| GIL contention | Yes | No |
| Race conditions | Possible | Not in single thread |
| I/O Performance | Good | Excellent |
| Code complexity | Higher | Lower |

**Conclusion**: For I/O-bound operations like network servers, asyncio is more efficient.

### Why Text Protocol over Binary?

| Aspect | Text | Binary |
|--------|------|--------|
| Debugging | Easy (human-readable) | Difficult |
| Performance | Good | Better |
| Implementation | Simple | Complex |
| Compatibility | Universal | Platform-dependent |

**Conclusion**: Text protocol chosen for simplicity and debuggability, similar to Redis RESP.

---

## 📚 What I Learned

### Systems Programming Concepts

1. **Socket Programming**: TCP connection handling, read/write buffers
2. **Async I/O**: Event loops, coroutines, non-blocking operations
3. **Protocol Design**: Request/response patterns, error handling

### Data Structure Applications

1. **OrderedDict**: Maintaining insertion order with O(1) operations
2. **LRU Cache**: Eviction policies, cache management
3. **TTL Implementation**: Time-based expiration strategies

### Software Engineering Practices

1. **Test-Driven Development**: Writing tests before implementation
2. **Separation of Concerns**: Network, Protocol, Storage layers
3. **Performance Testing**: Load testing, latency measurement

---

## 🔗 References

- [Python asyncio Documentation](https://docs.python.org/3/library/asyncio.html)
- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [LRU Cache - Wikipedia](https://en.wikipedia.org/wiki/Cache_replacement_policies#LRU)
- [OrderedDict Documentation](https://docs.python.org/3/library/collections.html#collections.OrderedDict)

---

## 👨‍💻 Author

Indrajeet Yadav  
HLD-101 Course Assignment

---

<div align="center">

*"There are only two hard things in Computer Science: cache invalidation and naming things."*  
— **Phil Karlton**

*This project tackles one of them.* 🚀

</div>
