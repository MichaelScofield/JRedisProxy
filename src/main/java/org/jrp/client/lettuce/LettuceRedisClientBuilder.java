package org.jrp.client.lettuce;

public class LettuceRedisClientBuilder {

    static final String DEFAULT_HOST = "127.0.0.1";
    static final int DEFAULT_PORT = 6379;
    static final int DEFAULT_TIMEOUT_MILLIS = 500;
    static final int DEFAULT_REQUEST_QUEUE_SIZE = 2048;

    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private char[] password = null;
    private int timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    private int requestQueueSize = DEFAULT_REQUEST_QUEUE_SIZE;

    public LettuceRedisClientBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public LettuceRedisClientBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public LettuceRedisClientBuilder withPassword(char[] password) {
        this.password = password;
        return this;
    }

    public LettuceRedisClientBuilder withTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    public LettuceRedisClientBuilder withRequestQueueSize(int requestQueueSize) {
        this.requestQueueSize = requestQueueSize;
        return this;
    }

    public LettuceRedisClient build() {
        return new LettuceRedisClient(this);
    }

    String getHost() {
        return host;
    }

    int getPort() {
        return port;
    }

    char[] getPassword() {
        return password;
    }

    int getTimeoutMillis() {
        return timeoutMillis;
    }

    int getRequestQueueSize() {
        return requestQueueSize;
    }
}
