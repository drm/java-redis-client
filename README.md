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
However you wish. This library is a protocol implementation only. Managing a connection pool
is not at all Redis-specific, so it doesn't belong here.
 

Having said that, this mostly depends on your use case. Typically
if you have a webserver with 20 threads, you can have a socket per thread managed somewhere
(i.e. ThreadLocal) and if you run a threadpool with 20 workers you can have one socket per
thread there as well. This keeps things simple and practical to reason about and you don't
have to worry who needs to create the socket at what point in time. You do need some error
handling which, in case of managing the sockets centrally is a tiny bit easier.

Summarizing, this is a trade-off which you can decide on for your own. Generally speaking:
don't overcomplicate stuff without good reason.
