package nl.melp.redis;

import nl.melp.redis.protocol.Parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RedisTest {
	private static final String REDIS_HOST = System.getProperty("redis.host", "localhost");
	private static final int REDIS_PORT = Integer.parseInt(System.getProperty("redis.port", "6379"));
	private static final int numThreads = Integer.parseInt(System.getProperty("redis.test.num-threads", "200"));
	private static final int numMessages = Integer.parseInt(System.getProperty("redis.test.num-messages", "25000"));

	// 16 bits buffer size (2 ^ 16, 1<<16) seems to be the most efficient for any value size.
	// tweak these numbers to check if this is the case for you.
	private static final int numBitsFrom = Integer.parseInt(System.getProperty("redis.test.num-bits-from", "16"));
	private static final int numBitsTo = Integer.parseInt(System.getProperty("redis.test.num-bits-to", "16"));

	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length == 0) {
			testParse();
			binaryTest();
			managedTest();
			integrationTest();
			bufferSizePerformanceTest();
			socketManagementPerformanceTest();
			subscribeTest();

			System.out.println("\nEverything seems to be alright.");
		}
	}

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

	private static void testParse() throws IOException {
		assertEqual(new String((byte[])new Parser(new ByteArrayInputStream("+OK\r\n".getBytes())).parse()), "OK");
		assertEqual((Long) new Parser(new ByteArrayInputStream(":1000\r\n".getBytes())).parse(), 1000);
		assertEqual(new String((byte[])new Parser(new ByteArrayInputStream("+OK\r\n".getBytes())).parse()), "OK");
		assertTrue(
			new Parser(new ByteArrayInputStream("$-1\r\n".getBytes())).parse() == null
		);
		assertEqual(
			new String((byte[]) new Parser(new ByteArrayInputStream("$10\r\n0123456789\r\n".getBytes())).parse()),
			"0123456789"
		);

		assertEqual(
			new String((byte[]) new Parser(new ByteArrayInputStream("$12\r\n01234\r\n56789\r\n".getBytes())).parse()),
			"01234\r\n56789"
		);

		List<?> arr = (List<?>) new Parser(new ByteArrayInputStream("*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n:5\r\n".getBytes())).parse();
		assertEqual(arr.size(), 5);
		assertEqual((Long) arr.get(0), 1);
		assertEqual((Long) arr.get(1), 2);
		assertEqual((Long) arr.get(2), 3);
		assertEqual((Long) arr.get(3), 4);
		assertEqual((Long) arr.get(4), 5);

		List<?> arr2 = (List<?>) new Parser(new ByteArrayInputStream("*1\r\n$5\r\n12345\r\n".getBytes())).parse();
		assertEqual(arr2.size(), 1);
		assertEqual(new String((byte[])arr2.get(0)), "12345");
		System.out.println("Tests passed successfully: testParse");
	}

	private static void integrationTest() throws InterruptedException {
		final String keyName = RedisTest.class.getCanonicalName();
		Consumer<Redis.FailableConsumer<Redis, IOException>> exec = (consumer) -> {
			try {
				Redis.run(consumer, "127.0.0.1", REDIS_PORT);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		};

		exec.accept((redis) -> {
			redis.call("SET", keyName, "0");
			redis.call("INCR", keyName);
			assertEqual("1", new String((byte[])redis.call("GET", keyName)));
			redis.call("INCR", keyName);
			assertEqual("2", new String((byte[])redis.call("GET", keyName)));
			redis.call("DEL", keyName);
		});
		exec.accept(
			(redis) -> {
				redis.call("SET", keyName, msg);
				assertEqual(msg, new String((byte[])redis.call("GET", keyName)));
			}
		);
		exec.accept(
			(redis) -> {
				redis.call("SET", keyName, msg.replace("\n", "\r\n"));
				assertEqual(msg.replace("\n", "\r\n"), new String((byte[])redis.call("GET", keyName)));
			}
		);
		exec.accept(
			(redis) -> {
				redis.call("SET", keyName, "123");
				redis.call("INCRBY", keyName, "456");
				assertEqual("579", new String((byte[])redis.call("GET", keyName)));
				redis.call("DEL", keyName);
			}
		);

		exec.accept(
			(redis) -> {
				redis.call("LPUSH", keyName, "A", "B", "C", "D");
				List<Object> l = redis.call("LRANGE", keyName, "0", "200");
				assertEqual(l.size(), 4);
				redis.call("DEL", keyName);
			}
		);

		exec.accept(
			(redis) -> {
				redis = new Redis(new Socket("127.0.0.1", REDIS_PORT));
				List<Object> result = redis.pipeline()
					.call("INCR", keyName)
					.call("INCR", keyName)
					.call("INCR", keyName)
					.call("DEL", keyName)
					.read();
				assertEqual(result.size(), 4);
				assertEqual(1, (Long) result.get(0));
				assertEqual(2, (Long) result.get(1));
				assertEqual(3, (Long) result.get(2));
			}
		);

		exec.accept(
			(redis) -> {
				redis.call("DEL", keyName);
				List<Object> result = redis.pipeline()
					.call("MULTI")
					.call("INCR", keyName)
					.call("INCR", keyName)
					.call("INCR", keyName)
					.call("INCR", keyName)
					.call("EXEC")
					.read();

				assertEqual(result.size(), 6);
				assertEqual("OK", new String((byte[])result.get(0)));
				assertEqual("QUEUED", new String((byte[])result.get(1)));
				assertEqual("QUEUED", new String((byte[])result.get(2)));
				assertEqual("QUEUED", new String((byte[])result.get(3)));
				assertEqual("QUEUED", new String((byte[])result.get(4)));
				assertEqual(1, (Long) ((List<?>) result.get(5)).get(0));
				assertEqual(2, (Long) ((List<?>) result.get(5)).get(1));
				assertEqual(3, (Long) ((List<?>) result.get(5)).get(2));
				assertEqual(4, (Long) ((List<?>) result.get(5)).get(3));
				redis.call("DEL", keyName);
			}
		);

		exec.accept(
			(redis) -> redis.call("INFO")
		);

		// This test checks if `call` will return null from a disconnected connection.
		ScheduledExecutorService r = Executors.newSingleThreadScheduledExecutor();
		r.schedule(
			() -> exec.accept(
				(redis) -> redis.call("CLIENT", "KILL", "TYPE", "normal")
			),
			500,
			TimeUnit.MILLISECONDS
		);
		AtomicBoolean b = new AtomicBoolean(false);
		exec.accept(
			(redis) -> {
				assertTrue(null == redis.call("BLPOP", "blocking", "2"));
				b.set(true);
			}
		);
		r.shutdown();
		r.awaitTermination(2, TimeUnit.SECONDS);
		assertTrue(b.get());

		exec.accept((redis) -> {
			redis.call("SET", "foo", "val");
			redis.call("PEXPIRE", "foo", "100");
			try {
				Thread.sleep(101);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			assertTrue(null == redis.call("GET", "foo"));
		});
	}

	private static void bufferSizePerformanceTest() throws IOException, InterruptedException {
		// Test in- and output buffer sizes.
		for (Map.Entry<MessageProducerTypeName, Supplier<String>> type : messageProducerTypes.entrySet()) {
			for (int ab = 0; ab <= 1; ab++) {
				// simple trick to repeat test with different configuration
				final boolean isInputBufferTest = (ab == 0);

				for (int i = numBitsFrom; i <= numBitsTo; i++) {
					int bufSize = 1 << i;
					performanceTest(
						String.format(
							isInputBufferTest
								? "%s : Input buffer size test: 0x%x"
								: "%s : Output buffer size test: 0x%x",
							type.getKey(),
							bufSize
						),
						numThreads,
						numMessages,
						() -> {
							try {
								if (isInputBufferTest) {
									return new Redis(new Socket("127.0.0.1", REDIS_PORT), bufSize, 1 << 16);
								} else {
									return new Redis(new Socket("127.0.0.1", REDIS_PORT), 1 << 16, bufSize);
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
							return null;
						},
						type.getValue()
					);
				}
			}
		}
	}

	private static void socketManagementPerformanceTest() throws IOException, InterruptedException {
		final int numThreads = 200;
		final int numMessages = 25000;

		// Test thread local vs direct connect
		for (int ab = 0; ab <= 1; ab++) {
			// simple trick to repeat test with different configuration
			final boolean isThreadLocalTest = (ab == 0);
			ThreadLocal<Redis> redis = new ThreadLocal<>();

			performanceTest(
				isThreadLocalTest ? "Thread local" : "Direct connect",
				numThreads,
				numMessages,
				() -> {
					try {
						if (isThreadLocalTest) {
							if (redis.get() == null) {
								redis.set(new Redis(new Socket("127.0.0.1", REDIS_PORT)));
							}
							return redis.get();
						}
						return new Redis(new Socket("127.0.0.1", REDIS_PORT));
					} catch (IOException e) {
						e.printStackTrace();
					}
					return null;
				},
				messageProducerTypes.get(MessageProducerTypeName.VARIABLE_SIZE)
			);
		}
	}

	private static void performanceTest(String description, int numThreads, int numMessages, Supplier<Redis> connector, Supplier<String> dataProducer) throws IOException, InterruptedException {
		String queueKeyName = RedisTest.class.getCanonicalName() + ":queue";
		connector.get().call("DEL", queueKeyName);

		ExecutorService pool = Executors.newFixedThreadPool(numThreads);
		AtomicLong count = new AtomicLong(0);
		AtomicLong size = new AtomicLong(0);

		LocalDateTime start = LocalDateTime.now();
		for (int i = 0; i < numThreads; i++) {
			pool.submit(
				() -> {
					try {
						Redis redis2 = connector.get();
						for (int n = 0; n < numMessages / numThreads; n++) {
							redis2.call("RPUSH", queueKeyName, dataProducer.get());
						}
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			);
		}
		for (int i = 0; i < numThreads; i++) {
			pool.submit(
				() -> {
					try {
						Redis redis2 = connector.get();
						for (int n = 0; n < numMessages / numThreads; n++) {
							List<?> value = redis2.call("BLPOP", queueKeyName, "0");
							assertTrue(value.get(1) instanceof byte[]);
							size.addAndGet(((byte[])value.get(1)).length);
							count.incrementAndGet();
						}
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			);
		}

		pool.shutdown();
		pool.awaitTermination(2, TimeUnit.MINUTES);

		assertEqual(count.get(), numMessages);
		float t = start.until(LocalDateTime.now(), ChronoUnit.MILLIS) / 1000f;
		System.out.printf(
			"%s:\n%d messages of avg %d string length passed in %s ms, total %.2f mB throughput, avg %.2f msg/s\n",
			description,
			count.get(),
			size.get() / count.get(),
			t,
			size.get() / 1024f / 1024f,
			count.get() / t
		);
	}


	public static void binaryTest() throws IOException {
		byte[] bytes = new byte[1024];
		new Random().nextBytes(bytes);

		Redis.run((redis) -> {
			redis.call("SET", "foo", bytes);
			assertEqual(1024, redis.<byte[]>call("GET", "foo").length);
		}, REDIS_HOST, REDIS_PORT);
	}

	public static void managedTest() throws IOException {
		Supplier<Integer> countClients = () -> {
			AtomicInteger numClients = new AtomicInteger(0);
			try {
				Redis.run((r) -> {
					String response = new String(r.<byte[]>call("CLIENT", "LIST", "TYPE", "normal"));
					numClients.set(response.split("\n").length);
				}, REDIS_HOST, REDIS_PORT);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			final int ret = numClients.get();
			System.out.printf("Connected clients now: %d\n", ret);
			return ret;
		};

		int before = countClients.get();
		try (Redis.Managed r = Redis.connect(REDIS_HOST, REDIS_PORT)) {
			if (countClients.get() <= before) {
				throw new IllegalStateException("Expected number of connected clients to be less than " + before);
			}
		}
		if (countClients.get() != before) {
			throw new IllegalStateException("Expected number of connected clients to be equal to " + before);
		}
	}


	public static void subscribeTest() throws IOException, InterruptedException {
		List<String> events =  Collections.synchronizedList(new LinkedList<>());

		ExecutorService s = Executors.newFixedThreadPool(2);
		Redis.run(
			redis -> redis.call("CONFIG", "SET", "notify-keyspace-events", "AKE"), REDIS_HOST, REDIS_PORT
		);
		AtomicInteger n = new AtomicInteger(0);

		s.submit(() -> {
			try {
				System.out.println("Subscription starting");
				Socket timeoutSocket = new Socket(REDIS_HOST, REDIS_PORT);
				timeoutSocket.setSoTimeout(1500);
				try {
					Redis.run(redis -> {
						redis.call("PSUBSCRIBE", "__keyevent@0__:*", "*");
						LinkedList<Object> result;
						try {
							while ((result = redis.read()) != null) {
								try {
									System.out.println("Received event " + String.join(", ", result.stream().map(r -> new String((byte[])r)).toArray(String[]::new)));
								} catch (ClassCastException e) {
									continue;
								}
								if (
									result.get(0) instanceof byte[] && Arrays.equals((byte[])result.get(0), "pmessage".getBytes())
									&& result.get(1) instanceof byte[] && Arrays.equals((byte[])result.get(1), "__keyevent@0__:*".getBytes())
								) {
									events.add(new String((byte[])result.get(2)).replace("__keyevent@0__:", "") + ":" + new String((byte[])result.get(3)));
								}
							}
						} catch (SocketTimeoutException ignored) {
						} catch (Exception e) {
							e.printStackTrace();
						}
					}, timeoutSocket);
				} catch (SocketTimeoutException ignored) {
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			n.incrementAndGet();
		});
		Thread.sleep(1000);

		s.submit(() -> {
			try {
				Redis.run(redis -> {
					redis.call("SET", "a", "b");
					redis.call("PEXPIRE", "a", "100");
					try {
						// so the key will expire
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}, REDIS_HOST, REDIS_PORT);
			} catch (IOException e) {
				e.printStackTrace();
			}
			n.incrementAndGet();
		});

		while (n.get() < 2) {
			System.out.println("Waiting for both threads to finish");
			Thread.sleep(100);
		}
		s.shutdown();
		s.awaitTermination(1000, TimeUnit.MILLISECONDS);

		assertTrue(events.get(0).equals("set:a"));
		assertTrue(events.get(1).equals("expire:a"));
		assertTrue(events.get(2).equals("expired:a"));
	}


	private static String msg = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse in purus in dui cursus dignissim id at neque. Duis porta ullamcorper aliquam. Suspendisse hendrerit urna id felis aliquet rutrum. Fusce ultricies magna elit, id volutpat risus dictum et. Sed pretium elementum arcu, vitae aliquet ligula. Phasellus viverra vel arcu vel dictum. Fusce ac purus fringilla neque dapibus sollicitudin sit amet et felis. Nulla gravida fringilla ex sit amet faucibus. Etiam sit amet nisl id est dictum porttitor eget nec risus. Vivamus et ultrices arcu, vitae accumsan lectus. Phasellus tempus tortor lectus, vitae consequat enim dictum auctor. Ut elementum sapien eu diam tempus condimentum.\n" +
		"\n" +
		"Vestibulum ultricies bibendum arcu ut commodo. Morbi tristique dui quis commodo consectetur. Praesent venenatis augue justo, sed placerat lectus aliquam eget. Duis malesuada lobortis quam id congue. Fusce mollis faucibus arcu. Aliquam consectetur leo eu luctus accumsan. Nulla nec diam non ex eleifend fringilla sit amet et ante. Mauris posuere est ut turpis pellentesque hendrerit.\n" +
		"\n" +
		"Curabitur vel ipsum at neque luctus malesuada. Donec tincidunt nunc ac lacus interdum malesuada eu ut nisi. Duis vulputate elementum magna, vitae interdum odio eleifend et. Vivamus et massa neque. Proin eget tellus porttitor, iaculis risus hendrerit, lacinia velit. Phasellus et mi aliquam sapien convallis imperdiet. Nunc at massa ut neque tristique tempor at at risus. Morbi mattis, orci vitae euismod fermentum, libero ipsum tincidunt diam, nec sagittis diam quam et magna. Phasellus efficitur dolor at neque ornare dictum. Donec a leo vel sem dictum rhoncus. Sed hendrerit dolor non ex ultrices, a feugiat tortor mattis. Aliquam non malesuada neque. Nullam id lobortis justo.\n" +
		"\n" +
		"Praesent porta nibh sed felis aliquet, at molestie mi accumsan. Maecenas varius, justo vitae dapibus auctor, purus massa fringilla lectus, eu semper enim odio pharetra arcu. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Vivamus mollis sit amet lacus sit amet tempor. Integer volutpat mattis velit, id hendrerit diam auctor in. Phasellus at porta eros, at molestie ex. Aliquam et massa at odio vestibulum interdum euismod a nibh. Pellentesque tristique laoreet nunc, nec faucibus turpis tempor et. Phasellus eget felis pellentesque, tempus metus id, laoreet justo. Nullam nec finibus ipsum, id tristique orci. Nulla ut molestie ante, et faucibus felis. Proin eu nunc consequat, vulputate mauris vel, fermentum neque. Vestibulum pretium sapien sit amet massa egestas pulvinar. Cras pellentesque luctus nulla quis auctor. Nulla varius volutpat dolor eget elementum.\n" +
		"\n" +
		"Mauris euismod nisi arcu, quis dictum purus euismod placerat. Duis ut justo felis. Cras ligula lacus, tristique bibendum enim vel, consectetur porttitor massa. Pellentesque nec iaculis nulla, sit amet semper lacus. Praesent et tellus maximus, tincidunt felis et, lobortis mauris. Aenean blandit tortor eu nisl tristique pulvinar. Maecenas nunc risus, venenatis a urna sed, blandit commodo nibh. Aliquam porta nisl eu porta egestas. In at lacus ipsum. Curabitur malesuada nisi vitae nibh tincidunt lacinia. In tempus consectetur egestas. Vestibulum ipsum massa, ultrices non pretium at, placerat sit amet arcu. Praesent purus lorem, scelerisque sed dui sit amet, rhoncus fermentum ligula. Donec sed bibendum ante.\n" +
		"\n" +
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi tincidunt eu risus sit amet feugiat. Curabitur mattis ipsum sed urna fermentum dignissim. Nullam laoreet fringilla ligula a feugiat. Duis laoreet vel est a scelerisque. Proin aliquam elit nisi, eu sollicitudin nisl euismod vel. Donec vel nisi odio. Nunc non ornare lectus, non porttitor libero.\n" +
		"\n" +
		"Vivamus tellus elit, tempus nec tincidunt quis, porttitor eu velit. Mauris sagittis ipsum ac ipsum rutrum accumsan. Pellentesque convallis porttitor erat ac pulvinar. Praesent id sagittis nulla. In ante nibh, suscipit at sem vitae, ornare efficitur eros. Nullam rutrum est leo, sit amet sollicitudin ipsum dapibus a. Vivamus consectetur arcu id sollicitudin semper. Aliquam suscipit eu arcu ac laoreet. Vestibulum blandit arcu vitae neque hendrerit, vitae suscipit ex commodo. Vestibulum ac tellus dignissim, varius arcu ut, interdum sem.\n" +
		"\n" +
		"Donec nibh velit, gravida at tortor quis, pharetra placerat justo. Integer dictum consequat magna ut gravida. Etiam fermentum semper tempus. Morbi scelerisque nulla magna, nec maximus est pretium ut. Etiam fringilla venenatis dapibus. Sed quis ullamcorper justo, dapibus semper dolor. Donec mollis luctus tempus. Suspendisse convallis egestas orci, nec venenatis eros gravida ac.\n" +
		"\n" +
		"Donec sagittis eros et nunc tristique, in aliquam ex gravida. Maecenas cursus lacinia diam finibus interdum. Fusce id orci at quam convallis mattis. Quisque non ipsum urna. Suspendisse vel facilisis leo. Curabitur in ornare mi. Nullam sed arcu finibus, vestibulum quam molestie, commodo odio. Cras nec posuere arcu. Donec purus nisl, aliquam ac facilisis sed, accumsan eu nulla. Phasellus efficitur venenatis massa at pellentesque.\n" +
		"\n" +
		"Donec in risus vitae est cursus aliquam gravida et nulla. Nunc non pulvinar metus. Donec vestibulum arcu enim, ultrices eleifend lacus dapibus quis. Sed lobortis ex vel mauris imperdiet, vel lacinia mauris tincidunt. Cras nisl massa, aliquet lobortis pretium eget, posuere sit amet ante. Cras aliquam pulvinar nisl, quis imperdiet risus suscipit id. Cras pellentesque lacus sed turpis tempor, rhoncus ultricies dolor semper. Etiam et nisi porta, ullamcorper ligula vitae, efficitur dui.\n" +
		"\n" +
		"Praesent varius ipsum at purus venenatis faucibus. Phasellus iaculis velit nibh, nec facilisis ante ultricies vel. Quisque mi massa, dignissim mollis tellus vel, dictum tempor erat. Maecenas malesuada lacinia sem. Sed at dui tempus, convallis dui non, pulvinar lectus. In laoreet, erat ut suscipit egestas, enim diam pellentesque quam, eu malesuada ante arcu a urna. Morbi risus enim, porttitor sit amet euismod accumsan, placerat pharetra dui. Aliquam erat volutpat. Donec cursus nisi ut nisi euismod efficitur. Nullam a diam vitae enim rutrum sagittis a et elit. Nunc sed volutpat risus. Proin porttitor leo nec lectus bibendum vestibulum. Ut mi orci, pretium et vulputate vel, rhoncus sit amet enim. Sed dictum dolor at odio porttitor dignissim.\n" +
		"\n" +
		"Aliquam porta ante eget tellus pretium porttitor. In viverra justo et tristique mollis. Aenean scelerisque orci nec augue placerat, et aliquet arcu molestie. Donec vitae sem diam. Suspendisse venenatis justo mi, quis pharetra metus congue sed. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Phasellus blandit vitae enim et luctus. Sed hendrerit lacus a turpis molestie, sed ullamcorper lorem pharetra. Cras vel finibus ipsum, ut euismod nibh. Maecenas eu orci lorem. Donec sodales quis sem ac facilisis. Integer cursus consequat nibh, et sodales dolor posuere eleifend. Praesent blandit nibh est, in sagittis libero tincidunt vel. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.\n" +
		"\n" +
		"Donec sagittis tortor non erat pellentesque pulvinar. Quisque id libero enim. Vivamus mi ante, euismod nec luctus et, rhoncus ac nunc. Pellentesque libero nisi, dignissim eu tellus quis, hendrerit ornare ipsum. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Donec dapibus pulvinar ipsum, non feugiat lorem mollis ut. Proin cursus venenatis dolor in ullamcorper. In hac habitasse platea dictumst. Aliquam convallis rhoncus lorem, eu iaculis dui fringilla id. Maecenas finibus mi id maximus cursus.\n" +
		"\n" +
		"Nulla posuere neque a sapien ultrices, et finibus tortor mollis. Duis arcu dolor, tempus vulputate dictum nec, tincidunt a libero. Aliquam sagittis sem risus, ac luctus justo congue eget. Donec sed porta eros. Nulla porta ipsum eu quam porttitor, vel aliquet ligula sagittis. Aenean molestie rhoncus ipsum a elementum. Phasellus eu eros elit. Aliquam erat volutpat. Nullam volutpat felis quis tortor lacinia fermentum. Donec ac massa mi. Ut efficitur vitae ex nec pulvinar.\n" +
		"\n" +
		"Nulla mollis vulputate risus non efficitur. Maecenas dapibus eget est vel sodales. Curabitur massa enim, hendrerit vitae elit nec, suscipit consequat augue. Nam condimentum felis a lacus interdum, sed posuere nisi luctus. Nunc tortor tellus, cursus sed sagittis non, molestie eu tortor. Curabitur ac auctor urna. Vestibulum at ante ultricies, finibus eros imperdiet, vulputate lorem. Nullam a elit fringilla diam ullamcorper sagittis. Aenean semper urna ut orci mattis, sit amet pretium orci iaculis. Aenean ullamcorper blandit est a lobortis. Suspendisse sed congue massa, ac vulputate mauris. Praesent in imperdiet lacus.\n" +
		"\n" +
		"Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nulla non nibh in libero ultricies malesuada. Maecenas vitae volutpat sem. Aenean interdum lacus id ipsum posuere eleifend. Morbi eu massa arcu. Fusce justo eros, pellentesque sed accumsan at, hendrerit dapibus urna. Cras a pulvinar leo. Fusce consequat non odio vitae mollis. Quisque sed tortor eu magna hendrerit volutpat non ut dui. Proin tincidunt urna neque, scelerisque aliquam tellus iaculis et. Quisque in malesuada lectus, a hendrerit orci. Proin vel lectus non sem vestibulum vestibulum.\n" +
		"\n" +
		"Donec nibh lorem, malesuada et eleifend at, hendrerit quis risus. Nunc dolor tellus, aliquet vel sollicitudin a, malesuada non metus. Suspendisse vulputate lectus quis libero tincidunt convallis. Cras consequat eros sit amet est tempus, ut venenatis ex rhoncus. Cras condimentum eget nisl vel mattis. Nulla facilisi. Fusce ultricies purus non arcu varius bibendum. Sed ex est, auctor vel lacus ut, malesuada consectetur sem. Donec eu hendrerit metus, vitae maximus eros. Nullam auctor dolor non metus mattis posuere. Mauris gravida, nisl in maximus porta, ligula diam ornare leo, sodales cursus neque nisi rhoncus ex.\n" +
		"\n" +
		"Ut eget pulvinar sapien. Donec in feugiat sem. Aenean sodales aliquam lorem sit amet feugiat. Nullam sed tincidunt felis. Nullam pretium enim ac diam feugiat molestie. Ut accumsan leo a purus mattis iaculis. Aenean vulputate lobortis magna a mollis. Praesent varius dolor sem, vehicula semper elit pharetra nec. Nullam at consequat justo. Donec eget sapien vel nisl scelerisque ornare. Nullam pellentesque enim vel mollis congue. Maecenas tincidunt justo sit amet porta mattis. Praesent pretium elit non tortor pretium, at ullamcorper odio sagittis. Nam sit amet arcu turpis.\n" +
		"\n" +
		"Suspendisse fermentum ante nec lobortis rhoncus. Curabitur a sapien at lacus luctus dictum eget in quam. Aliquam sit amet interdum nisi. Vivamus vel sem arcu. Vivamus quis enim nunc. In vel dui pharetra, egestas elit quis, sollicitudin ligula. Ut suscipit malesuada urna sit amet eleifend. Duis dolor nisl, convallis nec quam vel, mattis sodales neque. Pellentesque eleifend condimentum metus in congue. Aliquam pellentesque eu leo efficitur tempus. Fusce tristique blandit ex, sodales feugiat magna molestie a. Maecenas in tempus mauris, eleifend hendrerit metus. Vestibulum a nisi accumsan, mollis purus dapibus, rutrum tortor. Praesent tempor enim eget dui rutrum condimentum. Etiam maximus eu arcu faucibus aliquam.\n" +
		"\n" +
		"Fusce quis dui in nisi sollicitudin placerat. Mauris finibus nunc eu mi ultrices, in commodo justo tincidunt. Proin vulputate dui lectus, eu lacinia felis bibendum ac. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Cras et ornare felis, sit amet ornare sem. Mauris vel arcu et tellus iaculis consequat in sit amet turpis. Fusce scelerisque, dolor sed tincidunt tempor, nibh leo condimentum urna, ac bibendum lectus dolor quis quam.\n" +
		"\n" +
		"Nulla dapibus dolor id magna vehicula congue. Nulla tristique est et rutrum ornare. Integer rutrum pellentesque orci. Nullam dapibus, nisl ut gravida bibendum, purus odio gravida mi, a semper leo quam in odio. Nulla iaculis felis quis tortor suscipit pulvinar. Proin in orci eros. Ut fermentum risus justo, porttitor lacinia lacus elementum in. Sed id tempor est. Vestibulum id imperdiet dolor.\n" +
		"\n" +
		"Phasellus vel sem pulvinar, ornare leo maximus, tempor felis. Curabitur a sollicitudin tellus, sed ornare velit. Nullam porttitor mauris augue, et laoreet nibh suscipit nec. Curabitur porta tempus posuere. Curabitur commodo sollicitudin convallis. Fusce at neque leo. Vestibulum ut justo ex. Aliquam erat volutpat. Integer et euismod ex. In viverra ipsum at nibh semper consectetur.\n" +
		"\n" +
		"Praesent in erat iaculis, gravida purus tincidunt, tempus justo. Pellentesque ac auctor lectus, eget maximus tellus. Sed eu venenatis nulla. Etiam scelerisque, tortor et volutpat scelerisque, ex turpis auctor elit, et luctus ex dui ac urna. Proin dictum ante lectus, sit amet facilisis diam tristique non. Maecenas ac consectetur mauris. Duis ut risus at dui dictum lacinia. Cras et augue eu tortor rhoncus posuere. Nullam pharetra libero vitae arcu placerat semper. Fusce efficitur nulla purus, quis mollis mauris pulvinar nec. Mauris velit enim, interdum ut mauris eget, tincidunt malesuada ex. Fusce sodales viverra orci a congue. Sed congue est nunc, eget ultricies diam accumsan mattis. Suspendisse sed nulla rutrum, consectetur nisl non, feugiat nisl.\n" +
		"\n" +
		"Integer sit amet ipsum quis velit gravida porta. Vestibulum eget sapien turpis. Ut volutpat, libero egestas pulvinar dignissim, arcu urna interdum nisl, quis egestas nibh neque eu risus. Suspendisse iaculis neque ut pellentesque mollis. Sed feugiat mollis nibh et egestas. In hac habitasse platea dictumst. Ut eget enim et augue eleifend dignissim. Duis orci risus, aliquam ut velit molestie, accumsan efficitur nibh. Phasellus nec hendrerit libero. Nullam bibendum pulvinar nisl eu auctor. Fusce volutpat, justo in ullamcorper egestas, sem neque scelerisque felis, ac faucibus urna justo nec neque. Aenean consectetur felis iaculis enim cursus maximus. Integer gravida libero ut sem sollicitudin, eu congue nisl aliquet.\n" +
		"\n" +
		"Nullam a efficitur justo. Vivamus accumsan nec diam a blandit. Sed sagittis sapien lacus, ut suscipit erat hendrerit id. Sed ipsum dui, consectetur id massa sed, laoreet mollis magna. Proin porttitor sed lectus ac dictum. Fusce nec nibh commodo, pharetra sem eu, pretium lacus. Morbi egestas ultricies mauris. Cras mollis diam est. Mauris id luctus nisi. Sed ullamcorper ac velit in lobortis.\n" +
		"\n" +
		"Curabitur eget varius enim. Suspendisse potenti. Phasellus vestibulum eros lectus, quis blandit orci faucibus quis. Etiam tempus eu mauris at posuere. Donec sit amet dolor dictum, volutpat erat non, porta diam. Quisque eu metus tincidunt, porta lectus in, cursus elit. Nunc sit amet erat quam. Sed sit amet arcu sed augue vestibulum euismod a vitae velit. Donec ut bibendum diam. Curabitur quis nibh vel orci laoreet lobortis. Pellentesque dictum tincidunt orci id hendrerit.\n" +
		"\n" +
		"Pellentesque ac fermentum est, id posuere magna. Integer sit amet nisi ornare, accumsan turpis ut, rutrum quam. Etiam at venenatis orci. Nullam porta molestie magna lacinia porta. Ut maximus pellentesque arcu sit amet laoreet. Vestibulum ac faucibus mauris, eu tincidunt sem. Sed ac massa vel eros ultricies tincidunt. Etiam aliquet velit in libero sodales lobortis. Ut quis quam nec eros blandit tincidunt. Maecenas elementum consequat eros, eget consectetur nulla malesuada id. Vivamus eu neque neque.\n" +
		"\n" +
		"Morbi sed nisi ut sapien elementum molestie ut vitae nibh. Phasellus nec porta sem. Aenean ut massa ac felis suscipit tempor vitae quis tellus. Sed convallis, metus sit amet sagittis malesuada, lacus odio egestas mi, non tempor lacus lorem nec diam. Fusce pretium, enim sed commodo tempor, leo diam laoreet lacus, ut bibendum massa mauris in metus. Morbi quis diam elementum, tincidunt nulla quis, pharetra tortor. Vestibulum eget ante sit amet ante mattis fermentum quis quis augue. Vestibulum commodo, tortor quis malesuada laoreet, quam massa scelerisque nunc, nec sagittis arcu dolor eget ligula. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos.\n" +
		"\n" +
		"Pellentesque convallis nec urna ut tristique. Donec mi magna, lacinia vitae fringilla quis, facilisis quis turpis. In scelerisque rhoncus augue, a scelerisque ante faucibus at. Suspendisse ipsum massa, mattis quis ipsum eget, laoreet sagittis felis. Nullam facilisis imperdiet sodales. In eu scelerisque lorem, at pretium arcu. Vivamus lacinia blandit odio vel consectetur. Aenean euismod metus a tempor malesuada. Nunc vulputate nibh eget leo tincidunt, ac sodales tellus tincidunt. Sed porttitor tellus at congue porta. Morbi a ipsum vel erat venenatis porttitor non et felis.\n" +
		"\n" +
		"Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Donec nec justo et lectus laoreet faucibus eget sit amet libero. Ut rutrum dui in dui pellentesque ullamcorper. Suspendisse non varius eros. Proin rutrum at erat in auctor. Proin non hendrerit ante, eget pulvinar eros. Fusce congue augue quam, at rhoncus nunc rutrum eu. Cras rhoncus odio nec nunc lacinia, nec aliquam ligula pretium. Nam dignissim placerat mollis. Aliquam erat volutpat. Quisque turpis tortor, faucibus vitae odio et, euismod dapibus eros. Interdum et malesuada fames ac ante ipsum primis in faucibus. Donec viverra mauris sed sollicitudin feugiat. Donec et mauris eu elit sollicitudin maximus. Maecenas vitae eros malesuada lacus vulputate bibendum eget non felis.";

	enum MessageProducerTypeName {
		CONSTANT_SIZE_SMALL,
		CONSTANT_SIZE_LARGER,
		CONSTANT_SIZE_LARGE,
		VARIABLE_SIZE
	}

	private static final Map<MessageProducerTypeName, Supplier<String>> messageProducerTypes = new LinkedHashMap<>();
	private static final String[] strings;

	static {
		final String small = msg.substring(0, 256);
		final String larger = msg;
		final AtomicInteger rotate = new AtomicInteger(0);
		StringBuilder tmp = new StringBuilder();
		for (int i = 0; i < 10; i++) {
			tmp.append(msg);
		}
		final String large = (tmp.toString());

		strings = new String[]{
			small, larger, large
		};

		messageProducerTypes.put(MessageProducerTypeName.CONSTANT_SIZE_SMALL, () -> small);
		messageProducerTypes.put(MessageProducerTypeName.CONSTANT_SIZE_LARGER, () -> larger);
		messageProducerTypes.put(MessageProducerTypeName.CONSTANT_SIZE_LARGE, () -> large);
		messageProducerTypes.put(
			MessageProducerTypeName.VARIABLE_SIZE,
			() -> strings[rotate.getAndIncrement() % strings.length]
		);
	}
}
