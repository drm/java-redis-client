import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class RedisTest {
	private static void assertEqual(String a, String b) {
		if (!a.equals(b)) {
			throw new RuntimeException("Assertion failed; " + a + " is not equal to " + b);
		}
	}

	private static void assertEqual(long a, long b) {
		if (a != b) {
			throw new RuntimeException("Assertion failed; " + a + " is not equal to " + b);
		}
	}

	private static void assertTrue(boolean something) {
		if (!something) {
			throw new RuntimeException("Assert failed");
		}
	}

	public static void main(String[] args) throws IOException {
		testParse();
		integrationTest();
	}

	public static void testParse() throws IOException {
		assertEqual((String)new Redis.Parser(new ByteArrayInputStream("+OK\r\n".getBytes())).parse(), "OK");
		assertEqual((Long)new Redis.Parser(new ByteArrayInputStream(":1000\r\n".getBytes())).parse(), 1000);
		assertEqual((String)new Redis.Parser(new ByteArrayInputStream("+OK\r\n".getBytes())).parse(), "OK");
		assertTrue(
			new Redis.Parser(new ByteArrayInputStream("$-1\r\n".getBytes())).parse() == null
		);
		assertEqual(
			(String)new Redis.Parser(new ByteArrayInputStream("$10\r\n0123456789\r\n".getBytes())).parse(), "0123456789"
		);

		List arr = (List)new Redis.Parser(new ByteArrayInputStream("*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n:5\r\n".getBytes())).parse();
		assertEqual(arr.size(), 5);
		assertEqual((Long)arr.get(0), 1);
		assertEqual((Long)arr.get(1), 2);
		assertEqual((Long)arr.get(2), 3);
		assertEqual((Long)arr.get(3), 4);
		assertEqual((Long)arr.get(4), 5);
		System.out.println("Tests passed successfully: testParse");
	}

	public static void integrationTest() throws IOException {
		Socket s = new Socket("127.0.0.1", 6379);
		Redis redis = new Redis(s);
		String keyName = RedisTest.class.getCanonicalName();
		redis.call("SET", keyName, "0");
		redis.call("INCR", keyName);
		assertEqual("1", redis.call("GET", keyName));
		redis.call("INCR", keyName);
		assertEqual("2", redis.call("GET", keyName));
		s.close();

		Redis r = new Redis(new Socket("127.0.0.1", 6379));
		r.call("SET", "foo", "123");
		r.call("INCRBY", "foo", "456");
		System.out.println((String)r.call("GET", "foo")); // will print '579'

		s = new Socket("127.0.0.1", 6379);
		redis = new Redis(s);

		System.out.println("Now waiting for messages on " + keyName + ":queue");
		final Redis finalRedis = redis;
		(new Thread(() -> {
			try {
				finalRedis.call("RPUSH", keyName + ":queue", "A");
			} catch (IOException e) {
				e.printStackTrace();
			}
		})).run();

		assertEqual("A", (String)((List)redis.call("BLPOP", keyName + ":queue", "0")).get(1));
	}
}
