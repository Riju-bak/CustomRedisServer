# Custom Redis Server


This is an implementation of a Redis TCP/IP server written in C#

NOTE: This is just for learning purpose and might not be suitable to use directly in a professional codebase

The server has the following capabilities - 

- Thread-pool to handle upto 50 redis-client requests concurrently
- In-memory key value store with data persistence support
- Single master replication for redundancy
- Redis-style transactions for atomic operations

## Usage

```
git clone https://github.com/Riju-bak/CustomRedisServer.git
./CustomRedis/run.sh
```



