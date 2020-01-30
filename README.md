# java-redis-client
Low level Redis client (but you really won't need anything more than this!)

## Usage
Either package the library using `./build.sh` or whatever package manager you
fancy, or just copy it into your source tree.

Then create a socket connection and start speaking Redis. 

```java
nl.melp.redis.Redis r = new nl.melp.redis.Redis(new Socket("127.0.0.1", 6379));
r.call("SET", "foo", "123");
r.call("INCRBY", "foo", "456");
System.out.println(r.call("GET", "foo")); // will print '579'
```

## How data is parsed

* Error responses are translated to an Exception (`nl.melp.redis.Redis.Parser.ServerError`)
* Strings become `byte[]`
* Numbers become `Long`
* Arrays become List<Object>, where the entry can be any of `String`, 
  `Long` or `List<Object>`.

Since `call` uses a template return value, return type is statically inferred:

```java
redis.call("LPUSH", "mylist", "A", "B", "C", "D");
List<Object> l = redis.call("LRANGE", "mylist", "0", "200");
System.out.println("Size: " + l.size()); // prints "Size: 4"
System.out.println((String)l.get(0)); // prints "A"
```

You will have to do some casting yourself in case of List responses. The
reasoning here is that you know what data to expect so you're responsible for
applying the correct casts in the correct context.  

Refer to the Redis documentation for the return types of all calls and
`nl.melp.redis.RedisTest` for some working examples.

## Pipelining
The idea of pipelining is that you can keep writing commands without having
read the response.  This is an inherent feature of the protocol. The `call()`
method by default returns the response immediately, but if you wish to keep
sending commands without reading the response, you can use the pipeline() call.
This is typically useful for a MULTI/EXEC call where the responses aren't
directly useful.

```java
redis.pipeline()
    .call("MULTI")
    .call("INCR", "A")
    .call("INCR", "A")
    .call("INCR", "A")
    .call("INCR", "A")
    .call("EXEC")
    .read()
;
```

This will result in a `List<Object>` containing a list for each of the responses.

## Is the connection thread safe?
No. You need to make sure that the connection is accessed atomically. However,
there is a `Redis.run()` method which you can use to do some simple redis
operations in isolation. It creates a connection and closes it directly after:

```java
nl.melp.redis.Redis.run((redis) -> redis.call("INCR", "mycounter"));
```

## How should I manage my connections?
However you wish. This library is a protocol implementation only. Managing a
connection pool is not at all Redis-specific, so it doesn't belong here.
 
Having said that, this mostly depends on your use case. Typically if you have a
webserver with 20 threads, you can have a socket per thread managed somewhere
(e.g. ThreadLocal) and if you run a threadpool with 20 workers you can have one
socket per thread there as well. This keeps things simple and practical to
reason about and you don't have to worry who needs to create the socket at what
point in time. You do need some error handling which, in case of managing the
sockets centrally is a tiny bit easier. On the other hand, if you expect to be
creating a lot of threads and not a lot of these threads need the connection,
you probably want to implement a pool. 

Summarizing, this is a trade-off which you can decide on for your own.
Generally speaking: don't overcomplicate stuff without good reason. 

In terms of performance: creating socket connections on-the-fly is comparable
to reusing connections. See the performance tests inside RedisTest for more 
information.

## See also

See [drm/java-redis-collections](https://github.com/drm/java-redis-collections)
for collection implementations which can be used to manage your application
runtime data in redis.

## Questions? Issues?
Feel free to report issues here.
