# java-redis-client

Low level Redis client (but you really won't need anything more than this)

## Usage
Either package the library using whatever package manager you fancy, or just copy it 
into your source tree.

Then create a socket connection and start speaking Redis. 

```java
Redis r = new Redis(new Socket("127.0.0.1", 6379));
r.call("SET", "foo", "123");
r.call("INCRBY", "foo", "456");
System.out.println(r.call("GET", "foo")); // will print '579'
```

## How should I manage my connections?
However you wish. This library is a protocol implementation only. In my opinion, managing
connection pools or stuff like that is not at all Redis-specific. So either find another
library for it, or write something simple using a BlockinDeque or simply create connections
on the fly whenever you need them.
