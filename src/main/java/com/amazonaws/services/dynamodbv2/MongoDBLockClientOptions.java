package com.amazonaws.services.dynamodbv2;

import com.mongodb.client.MongoClient;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class MongoDBLockClientOptions {
    protected static final Long DEFAULT_LEASE_DURATION = 20L;
    protected static final Long DEFAULT_HEARTBEAT_PERIOD = 5L;
    protected static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    protected static final Boolean DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD = true;
    protected static final Boolean DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE = false;

    private final MongoClient mongoClient;
    private final String ownerName;
    private final Long leaseDuration;
    private final Long heartbeatPeriod;
    private final TimeUnit timeUnit;
    private final Boolean createHeartbeatBackgroundThread;
    private final Function<String, ThreadFactory> namedThreadCreator;
    private final Boolean holdLockOnServiceUnavailable;


    /**
     * A builder for setting up a MongoDBLockClientOptions object. By default, it is setup to have a lease duration of 20 seconds
     * and a default heartbeat period of 5 seconds. These defaults can be overriden.
     */
    public static class MongoDBLockClientOptionsBuilder {
        private MongoClient mongoClient;
        private String ownerName;
        private Long leaseDuration;
        private Long heartbeatPeriod;
        private TimeUnit timeUnit;
        private Boolean createHeartbeatBackgroundThread;
        private Boolean holdLockOnServiceUnavailable;
        private Function<String, ThreadFactory> namedThreadCreator;

        MongoDBLockClientOptionsBuilder(final MongoClient mongoClient) {
            this(mongoClient,
                    /* By default, tries to set ownerName to the localhost */
                generateOwnerNameFromLocalhost(),
                namedThreadCreator());
        }

        private static final String generateOwnerNameFromLocalhost() {
            try {
                return Inet4Address.getLocalHost().getHostName() + UUID.randomUUID().toString();
            } catch (final UnknownHostException e) {
                return UUID.randomUUID().toString();
            }
        }

        private static Function<String, ThreadFactory> namedThreadCreator() {
            return (String threadName) -> (Runnable runnable) -> new Thread(runnable, threadName);
        }

        MongoDBLockClientOptionsBuilder(final MongoClient mongoClient, final String ownerName,
            final Function<String, ThreadFactory> namedThreadCreator) {
            this.mongoClient = mongoClient;
            this.heartbeatPeriod = DEFAULT_HEARTBEAT_PERIOD;
            this.timeUnit = DEFAULT_TIME_UNIT;
            this.createHeartbeatBackgroundThread = DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD;
            this.ownerName = ownerName == null ? generateOwnerNameFromLocalhost() : ownerName;
            this.namedThreadCreator = namedThreadCreator == null ? namedThreadCreator() : namedThreadCreator;
            this.holdLockOnServiceUnavailable = DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE;
        }

        /**
         * @param ownerName The person that is acquiring the lock (for example, box.amazon.com)
         * @return a reference to this builder for fluent method chaining
         */
        public MongoDBLockClientOptionsBuilder withOwnerName(final String ownerName) {
            this.ownerName = ownerName;
            return this;
        }

        /**
         * @param leaseDuration The length of time that the lease for the lock will be
         *                      granted for. If this is set to, for example, 30 seconds,
         *                      then the lock will expire if the heartbeat is not sent for
         *                      at least 30 seconds (which would happen if the box or the
         *                      heartbeat thread dies, for example.)
         * @return a reference to this builder for fluent method chaining
         */
        public MongoDBLockClientOptionsBuilder withLeaseDuration(final Long leaseDuration) {
            this.leaseDuration = leaseDuration;
            return this;
        }

        /**
         * @param heartbeatPeriod How often to update MongoDB to note that the instance is
         *                        still running (recommendation is to make this at least 3
         *                        times smaller than the leaseDuration -- for example
         *                        heartBeatPeriod=1 second, leaseDuration=10 seconds could
         *                        be a reasonable configuration, make sure to include a
         *                        buffer for network latency.)
         * @return a reference to this builder for fluent method chaining
         */
        public MongoDBLockClientOptionsBuilder withHeartbeatPeriod(final Long heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        /**
         * @param timeUnit What time unit to use for all times in this object, including
         *                 heartbeatPeriod and leaseDuration.
         * @return a reference to this builder for fluent method chaining
         */
        public MongoDBLockClientOptionsBuilder withTimeUnit(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        /**
         * @param createHeartbeatBackgroundThread Whether or not to create a thread to automatically
         *                                        heartbeat (if false, you must call sendHeartbeat manually)
         * @return a reference to this builder for fluent method chaining
         */
        public MongoDBLockClientOptionsBuilder withCreateHeartbeatBackgroundThread(final Boolean createHeartbeatBackgroundThread) {
            this.createHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
            return this;
        }

        /**
         * Builds an MongoDBLockClientOptions. If required parametes are
         * not set, will throw an IllegalArgumentsException.
         *
         * @return Returns the MongoDBLockClientOptions which can be
         * passed to the lock client constructor.
         */
        public MongoDBLockClientOptions build() {
            Objects.requireNonNull(this.ownerName, "Owner Name must not be null");
            return new MongoDBLockClientOptions(this.mongoClient, this.ownerName, this.leaseDuration,
                this.heartbeatPeriod, this.timeUnit, this.createHeartbeatBackgroundThread, this.namedThreadCreator, this.holdLockOnServiceUnavailable);
        }

        @Override
        public String toString() {
            return "MongoDBLockClientOptionsBuilder(mongopDBClient=" + this.mongoClient
                + " ownerName=" + this.ownerName + ", leaseDuration=" + this.leaseDuration + ", heartbeatPeriod=" + this.heartbeatPeriod
                + ", timeUnit=" + this.timeUnit + ", createHeartbeatBackgroundThread=" + this.createHeartbeatBackgroundThread
                + ", holdLockOnServiceUnavailable=" + this.holdLockOnServiceUnavailable + ")";
        }
    }

    /**
     * Creates an MongoDBLockClientOptions builder object, which can be
     * used to create an MongoDBLockClient. The only required parameters
     * are the client and the table name.
     *
     * @param mongoClient The client for talking to MongoDB.
     * @return A builder which can be used for creating a lock client.
     */
    public static MongoDBLockClientOptionsBuilder builder(final MongoClient mongoClient) {
        return new MongoDBLockClientOptionsBuilder(mongoClient);
    }

    private MongoDBLockClientOptions(final MongoClient mongoClient, final String ownerName, final Long leaseDuration, final Long heartbeatPeriod, final TimeUnit timeUnit, final Boolean createHeartbeatBackgroundThread,
        final Function<String, ThreadFactory> namedThreadCreator, final Boolean holdLockOnServiceUnavailable) {
        this.mongoClient = mongoClient;
        this.ownerName = ownerName;
        this.leaseDuration = leaseDuration;
        this.heartbeatPeriod = heartbeatPeriod;
        this.timeUnit = timeUnit;
        this.createHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
        this.namedThreadCreator = namedThreadCreator;
        this.holdLockOnServiceUnavailable = holdLockOnServiceUnavailable;
    }

    /**
     * @return MongoDB client that the lock client will use.
     */
    MongoClient getMongoDBClient() {
        return this.mongoClient;
    }

    /**
     * @return The person that is acquiring the lock (for example, box.amazon.com.)
     */
    String getOwnerName() {
        return this.ownerName;
    }

    /**
     * @return The length of time that the lease for the lock will be granted
     * for. If this is set to, for example, 30 seconds, then the lock
     * will expire if the heartbeat is not sent for at least 30 seconds
     * (which would happen if the box or the heartbeat thread dies, for
     * example.)
     */
    Long getLeaseDuration() {
        return this.leaseDuration;
    }

    /**
     * @return How often the lock client will update MongoDB to note that the
     * instance is still running.
     */
    Long getHeartbeatPeriod() {
        return this.heartbeatPeriod;
    }

    /**
     * @return The TimeUnit that is used for all time values in this object.
     */
    TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    /**
     * @return Whether or not the Lock Client will create a background thread.
     */
    Boolean getCreateHeartbeatBackgroundThread() {
        return this.createHeartbeatBackgroundThread;
    }

    /**
     * @return A function that takes in a thread name and outputs a ThreadFactory that creates threads with the given name.
     */
    Function<String, ThreadFactory> getNamedThreadCreator() {
        return this.namedThreadCreator;
    }

    /**
     * @return Whether or not to hold the lock if MongoDB Service is unavailable
     */
    Boolean getHoldLockOnServiceUnavailable() {
        return this.holdLockOnServiceUnavailable;
    }
    
}
