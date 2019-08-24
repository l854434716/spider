package manke.spider.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class LettuceRedisClient {


    private RedisURI redisURI;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisCommands<String, String> redisCommands;


    public LettuceRedisClient(String host, int port) {

        redisURI = RedisURI.builder().withHost(host).withPort(port).build();

    }


    public synchronized void init() {

        if (redisClient == null) {
            redisClient = RedisClient.create(redisURI);
            connection = redisClient.connect();
            redisCommands = connection.sync();
        }

    }


    public synchronized void close() {

        if (redisClient != null) {

            connection.close();
            redisClient.shutdown();
            redisClient = null;
        }
    }


    public Long sadd(String key, String... values) {

        return redisCommands.sadd(key, values);

    }


    public boolean sismember(String key, String value) {

        return redisCommands.sismember(key, value);
    }
}
