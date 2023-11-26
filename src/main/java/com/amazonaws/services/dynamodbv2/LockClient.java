/**
 * Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * <p>
 * http://aws.amazon.com/asl/
 * <p>
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.LockTableDoesNotExistException;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;

/**
 * <p>
 * Provides a simple library for using DynamoDB's consistent read/write feature to use it for managing distributed locks.
 * </p>
 * <p>
 * In order to use this library, the client must create a table in DynamoDB, although the library provides a convenience method
 * for creating that table (createLockTableInDynamoDB.)
 * </p>
 * <p>
 * Here is some example code for how to use the lock client for leader election to work on a resource called "database-3" (it
 * assumes you already have a DynamoDB table named lockTable, which can be created with the static
 * {@code createLockTableInDynamoDB} helper method):
 * </p>
 * <pre>
 * {@code
 *  AmazonDynamoDBLockClient lockClient = new AmazonDynamoDBLockClient(
 *      AmazonDynamoDBLockClientOptions.builder(dynamoDBClient, "lockTable").build();
 *  try {
 *      // Attempt to acquire the lock indefinitely, polling DynamoDB every 2 seconds for the lock
 *      LockItem lockItem = lockClient.acquireLock(
 *          AcquireLockOptions.builder("database-3")
 *              .withRefreshPeriod(120L)
 *              .withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2L)
 *              .withTimeUnit(TimeUnit.MILLISECONDS)
 *              .build());
 *      if (!lockItem.isExpired()) {
 *          // do business logic, you can call lockItem.isExpired() to periodically check to make sure you still have the lock
 *          // the background thread will keep the lock valid for you by sending heartbeats (default is every 5 seconds)
 *      }
 *  } catch (LockNotGrantedException x) {
 *      // Should only be thrown if the lock could not be acquired for Long.MAX_VALUE / 2L milliseconds.
 *  }
 * }
 * </pre>
 * <p>
 * Here is an example that involves a bunch of workers getting customer IDs from a queue, taking a lock on that Customer ID, then
 * releasing that lock when complete:
 * </p>
 * <pre>
 * {@code
 *  AmazonDynamoDBLockClient lockClient = new AmazonDynamoDBLockClient(
 *      AmazonDynamoDBLockClient.builder(dynamoDBClient, "lockTable").build();
 *  while (true) {
 *      // Somehow find out about what work needs to be done
 *      String customerID = getCustomerIDFromQueue();
 *
 *     try {
 *          // Don't try indefinitely -- if someone else has a lock on this Customer ID, just move onto the next customer
 *          // (note that, if there is a lock on this customer ID, this method will still wait at least 20 seconds in order to be
 *          // able to determine
 *          // if that lock is stale)
 *          LockItem lockItem = lockClient.acquireLock(AcquireLockOptions.builder(customerID).build());
 *          if (!lockItem.isExpired()) {
 *              // Perform operation on this customer
 *          }
 *          lockItem.close();
 *      } catch (LockNotGrantedException x) {
 *          logger.info("We failed to acquire the lock for customer " + customerID, x);
 *      }
 *  }
 * }
 * </pre>
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a>
 */
@ThreadSafe
public abstract class LockClient implements Runnable, Closeable {
    protected static final Log logger = LogFactory.getLog(LockClient.class);
    protected long leaseDurationInMilliseconds;
    protected long heartbeatPeriodInMilliseconds;
    protected boolean holdLockOnServiceUnavailable;
    protected String ownerName;
    protected ConcurrentHashMap<String, LockItem> locks;
    protected ConcurrentHashMap<String, Thread> sessionMonitors;
    protected Optional<Thread> backgroundThread;
    protected Function<String, ThreadFactory> namedThreadCreator;
    protected volatile boolean shuttingDown = false;

    /* These are the keys that are stored in the DynamoDB table to keep track of the locks */
    protected static final String DATA = "data";
    protected static final String OWNER_NAME = "ownerName";
    protected static final String LEASE_DURATION = "leaseDuration";
    protected static final String RECORD_VERSION_NUMBER = "recordVersionNumber";
    protected static final String IS_RELEASED = "isReleased";
    protected static final String IS_RELEASED_VALUE = "1";
    protected static volatile AtomicInteger lockClientId = new AtomicInteger(0);
    protected static final Boolean IS_RELEASED_INDICATOR = true;
    
    protected Optional<ByteBuffer> newLockData = Optional.empty();
    protected Optional<LockItem> existingLock = null;
    protected String key;
    protected Optional<String> sortKey;
    protected boolean deleteLockOnRelease;
    protected boolean replaceData;
    protected GetLockOptions getLockOptions;
    protected Map<String, Object> item;
    protected Optional<SessionMonitor> sessionMonitor;

    /*
     * Used as a default buffer for how long extra to wait when querying DynamoDB for a lock in acquireLock (can be overriden by
     * specifying a timeout when calling acquireLock)
     */
    private static final long DEFAULT_BUFFER_MS = 1000;

    /**
     * Checks whether the lock table exists.
     *
     * @return true if the table exists, false otherwise.
     */
    public abstract boolean lockTableExists();

    /**
     * Creates a lock table with the right schema for it to be used by this locking library. If the table already exists, this will throw an exception.
     * <p>
     *
     * @param tableOptions The options for the lock client
     */
    public abstract void createLockTable(Object tableOptions);

    protected abstract List<String> getKeys();
    protected abstract List<String> getKeyValues();

    /**
     * Retrieves the lock item. Note that this will return a
     * LockItem even if it was released -- do NOT use this method if your goal
     * is to acquire a lock for doing work.
     *
     * @param options The options such as the key, etc.
     * @return The LockItem, or absent if it is not present. Note that the item
     * can exist in the table even if it is released, as noted by
     * isReleased().
     */
    protected abstract Optional<LockItem> getLock(GetLockOptions lockOptions);
    
    protected abstract LockItem buildLockItem(AcquireLockOptions options, String recordVersionNumber);
    
    protected abstract boolean releaseLock(final ReleaseLockOptions options);
    
    protected abstract LockItem upsertAndMonitorExpiredLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer> newLockData, Map<String, Object> item, String recordVersionNumber);

    protected abstract LockItem upsertAndMonitorReleasedLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean
            deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer>
            newLockData, Map<String, Object> item, String recordVersionNumber);

    protected abstract LockItem updateItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
           Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber);

    protected abstract LockItem putLockItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber);

    /**
     * This method puts a new lock item in the lock table and returns an optionally monitored LockItem object
     * @param options a wrapper of RequestMetricCollector and an "additional attributes" map
     * @param key the partition key of the lock to write
     * @param sortKey the optional sort key of the lock to write
     * @param deleteLockOnRelease whether or not to delete the lock when releasing it
     * @param sessionMonitor the optional session monitor to start for this lock
     * @param newLockData the new lock data
     * @param item the lock item to write to the lock table
     * @param recordVersionNumber the rvn to condition the PutItem call on.
     * @return a new monitored LockItem
     */
    protected abstract LockItem upsertAndMonitorNewLock(AcquireLockOptions options, String key, Optional<String> sortKey,
        boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
        Optional<ByteBuffer> newLockData, Map<String, Object> item, String recordVersionNumber);

    /**
     * <p>
     * Retrieves the locks with partition_key = {@code key}.
     * </p>
     * <p>
     * Not that this may return a lock item even if it was released.
     * </p>
     *
     * @param key the partition key
     * @param deleteOnRelease Whether or not the {@link LockItem} should delete the item
     *                        when {@link LockItem#close()} is called on it.
     * @return A non parallel {@link Stream} of {@link LockItem}s that has the partition key in
     * DynamoDB. Note that the item can exist in the table even if it is
     * released, as noted by {@link LockItem#isReleased()}.
     */
    public abstract Stream<LockItem> getLocksByPartitionKey(String key, final boolean deleteOnRelease);

    /**
     * <p>
     * Retrieves all the lock items from DynamoDB.
     * </p>
     * <p>
     * Not that this will may return a lock item even if it was released.
     * </p>
     *
     * @param deleteOnRelease Whether or not the {@link LockItem} should delete the item
     *                        when {@link LockItem#close()} is called on it.
     * @return A non parallel {@link Stream} of all the {@link LockItem}s in
     * DynamoDB. Note that the item can exist in the table even if it is
     * released, as noted by {@link LockItem#isReleased()}.
     */
    public abstract Stream<LockItem> getAllLocksFromDB(final boolean deleteOnRelease);

    /**
     * <p>
     * Sends a heartbeat to indicate that the given lock is still being worked on. If using
     * {@code createHeartbeatBackgroundThread}=true when setting up this object, then this method is unnecessary, because the
     * background thread will be periodically calling it and sending heartbeats. However, if
     * {@code createHeartbeatBackgroundThread}=false, then this method must be called to instruct DynamoDB that the lock should
     * not be expired.
     * </p>
     * <p>
     * This method will also set the lease duration of the lock to the given value.
     * </p>
     * <p>
     * This will also either update or delete the data from the lock, as specified in the options
     * </p>
     *
     * @param options a set of optional arguments for how to send the heartbeat
     */
    public abstract void sendHeartbeat(final SendHeartbeatOptions options);

    /**
     * Asserts that the lock table exists. You can use this method
     * during application initialization to ensure that the lock client will be
     * usable. Since this is a no-arg assertion as opposed to a check that
     * returns a value, this method is also suitable as an init-method for a
     * Spring bean.
     *
     * @throws LockTableDoesNotExistException if the table doesn't exist.
     */
    public void assertLockTableExists() throws LockTableDoesNotExistException {
        boolean exists;
        try {
            exists = this.lockTableExists();
        } catch (final Exception e) {
            throw new LockTableDoesNotExistException("Lock table does not exist", e);
        }
        if (!exists) {
            throw new LockTableDoesNotExistException("Lock table does not exist");
        }
    }

    /**
     * <p>
     * Attempts to acquire a lock until it either acquires the lock, or a specified {@code additionalTimeToWaitForLock} is
     * reached. This method will poll DynamoDB based on the {@code refreshPeriod}. If it does not see the lock in DynamoDB, it
     * will immediately return the lock to the caller. If it does see the lock, it will note the lease expiration on the lock. If
     * the lock is deemed stale, (that is, there is no heartbeat on it for at least the length of its lease duration) then this
     * will acquire and return it. Otherwise, if it waits for as long as {@code additionalTimeToWaitForLock} without acquiring the
     * lock, then it will throw a {@code LockNotGrantedException}.
     * </p>
     * <p>
     * Note that this method will wait for at least as long as the {@code leaseDuration} in order to acquire a lock that already
     * exists. If the lock is not acquired in that time, it will wait an additional amount of time specified in
     * {@code additionalTimeToWaitForLock} before giving up.
     * </p>
     * <p>
     * See the defaults set when constructing a new {@code AcquireLockOptions} object for any fields that you do not set
     * explicitly.
     * </p>
     *
     * @param options A combination of optional arguments that may be passed in for acquiring the lock
     * @return the lock
     * @throws InterruptedException in case the Thread.sleep call was interrupted while waiting to refresh.
     */
    @SuppressWarnings("resource") // LockItem.close() does not need to be called until the lock is acquired, so we suppress the warning here.
    public LockItem acquireLock(final AcquireLockOptions options) throws LockNotGrantedException, InterruptedException {
        Objects.requireNonNull(options, "Cannot acquire lock when options is null");
        Objects.requireNonNull(options.getPartitionKey(), "Cannot acquire lock when key is null");

        key = options.getPartitionKey();
        sortKey = options.getSortKey();

        if (options.getReentrant() && hasLock(key, sortKey)) { // Call hasLock() to avoid making a db call when the client does not own the lock.
            Optional<LockItem> lock = getLock(key, sortKey);
            if (lock.isPresent() && !lock.get().isExpired()) {
                return lock.get();
            }
        }

        if (options.getAdditionalAttributes().containsKey(this.getKeys().get(0)) || options.getAdditionalAttributes().containsKey(OWNER_NAME) || options
            .getAdditionalAttributes().containsKey(LEASE_DURATION) || options.getAdditionalAttributes().containsKey(RECORD_VERSION_NUMBER) || options
            .getAdditionalAttributes().containsKey(DATA) || this.getKeys().size() > 1 && options.getAdditionalAttributes().containsKey(this.getKeys().get(1))) {
            throw new IllegalArgumentException(String
                .format("Additional attribute cannot be one of the following types: " + "%s, %s, %s, %s, %s", this.getKeys().toString().replace("[", "").replace("]", ""), OWNER_NAME, LEASE_DURATION,
                    RECORD_VERSION_NUMBER, DATA));
        }

        long millisecondsToWait = DEFAULT_BUFFER_MS;
        if (options.getAdditionalTimeToWaitForLock() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "timeUnit must not be null if additionalTimeToWaitForLock is non-null");
            millisecondsToWait = options.getTimeUnit().toMillis(options.getAdditionalTimeToWaitForLock());
        }

        long refreshPeriodInMilliseconds = DEFAULT_BUFFER_MS;
        if (options.getRefreshPeriod() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "timeUnit must not be null if refreshPeriod is non-null");
            refreshPeriodInMilliseconds = options.getTimeUnit().toMillis(options.getRefreshPeriod());
        }

        deleteLockOnRelease = options.getDeleteLockOnRelease();
        replaceData = options.getReplaceData();

        sessionMonitor = options.getSessionMonitor();
        if (sessionMonitor.isPresent()) {
            sessionMonitorArgsValidate(sessionMonitor.get().getSafeTimeMillis(), this.heartbeatPeriodInMilliseconds, this.leaseDurationInMilliseconds);
        }
        final long currentTimeMillis = LockClientUtils.INSTANCE.millisecondTime();

        /*
         * This is the lock we are trying to acquire. If it already exists, then we can try to steal it if it does not get updated
         * after its LEASE_DURATION expires.
         */
        LockItem lockTryingToBeAcquired = null;
        boolean alreadySleptOnceForOneLeasePeriod = false;

        getLockOptions = new GetLockOptions.GetLockOptionsBuilder(key)
                .withSortKey(sortKey.orElse(null))
                .withDeleteLockOnRelease(deleteLockOnRelease)
                .build();

        while (true) {
            try {
                try {
                    logger.trace("Call GetItem to see if the lock for " + this.getKeyValues().toString() + " exists in the table");
                    existingLock = this.getLock(getLockOptions);

                    if (options.getAcquireOnlyIfLockAlreadyExists() && !existingLock.isPresent()) {
                        throw new LockNotGrantedException("Lock does not exist.");
                    }

                    if (options.shouldSkipBlockingWait() && existingLock.isPresent() && !existingLock.get().isExpired()) {
                        /*
                         * The lock is being held by some one and is still not expired. And the caller explicitly said not to perform a blocking wait;
                         * We will throw back a lock not grant exception, so that the caller can retry if needed.
                         */
                        throw new LockCurrentlyUnavailableException("The lock being requested is being held by another client.");
                    }

                    if (replaceData) {
                        newLockData = options.getData();
                    } else if (existingLock.isPresent()) {
                        newLockData = existingLock.get().getData();
                    }

                    if (!newLockData.isPresent()) {
                        newLockData = options.getData(); // If there is no existing data, we write the input data to the lock.
                    }

                    final String recordVersionNumber = this.generateRecordVersionNumber();
                    LockItem lockItem = this.buildLockItem(options, recordVersionNumber);
                    if (lockItem != null) 
                        return lockItem;

                    // we know that we didnt enter the if block above because it returns at the end.
                    // we also know that the existingLock.isPresent() is true
                    if (lockTryingToBeAcquired == null) {
                        //this branch of logic only happens once, in the first iteration of the while loop
                        //lockTryingToBeAcquired only ever gets set to non-null values after this point.
                        //so it is impossible to get in this
                        /*
                         * Someone else has the lock, and they have the lock for LEASE_DURATION time. At this point, we need
                         * to wait at least LEASE_DURATION milliseconds before we can try to acquire the lock.
                         */
                        lockTryingToBeAcquired = existingLock.get();
                        if (!alreadySleptOnceForOneLeasePeriod) {
                            alreadySleptOnceForOneLeasePeriod = true;
                            millisecondsToWait += existingLock.get().getLeaseDuration();
                        }
                    } else {
                        if (lockTryingToBeAcquired.getRecordVersionNumber().equals(existingLock.get().getRecordVersionNumber())) {
                            /* If the version numbers match, then we can acquire the lock, assuming it has already expired */
                            if (lockTryingToBeAcquired.isExpired()) {
                                return upsertAndMonitorExpiredLock(options, key, sortKey, deleteLockOnRelease, sessionMonitor, existingLock, newLockData, item,
                                    recordVersionNumber);
                            }
                        } else {
                            /*
                             * If the version number changed since we last queried the lock, then we need to update
                             * lockTryingToBeAcquired as the lock has been refreshed since we last checked
                             */
                            lockTryingToBeAcquired = existingLock.get();
                        }
                    }
                } catch (final ConditionalCheckFailedException conditionalCheckFailedException) {
                    /* Someone else acquired the lock while we tried to do so, so we throw an exception */
                    logger.debug("Someone else acquired the lock", conditionalCheckFailedException);
                    throw new LockNotGrantedException("Could not acquire lock because someone else acquired it: ", conditionalCheckFailedException);
                } catch (ProvisionedThroughputExceededException provisionedThroughputExceededException) {
                    /* Request exceeded maximum allowed provisioned throughput for the table
                     * or for one or more global secondary indexes.
                     */
                    logger.debug("Maximum allowed provisioned throughput for the table exceeded", provisionedThroughputExceededException);
                    throw new LockNotGrantedException("Could not acquire lock because provisioned throughput for the table exceeded", provisionedThroughputExceededException);
                } catch (final SdkClientException sdkClientException) {
                    /* This indicates that we were unable to successfully connect and make a service call to DDB. Often
                     * indicative of a network failure, such as a socket timeout. We retry if still within the time we
                     * can wait to acquire the lock.
                     */
                    logger.warn("Could not acquire lock because of a client side failure in talking to DDB", sdkClientException);
                }
            } catch (final LockNotGrantedException x) {
                if (LockClientUtils.INSTANCE.millisecondTime() - currentTimeMillis > millisecondsToWait) {
                    logger.debug("This client waited more than millisecondsToWait=" + millisecondsToWait
                        + " ms since the beginning of this acquire call.", x);
                    throw x;
                }
            }
            if (LockClientUtils.INSTANCE.millisecondTime() - currentTimeMillis > millisecondsToWait) {
                throw new LockNotGrantedException("Didn't acquire lock after sleeping for " + (LockClientUtils.INSTANCE.millisecondTime() - currentTimeMillis) + " milliseconds");
            }
            logger.trace("Sleeping for a refresh period of " + refreshPeriodInMilliseconds + " ms");
            Thread.sleep(refreshPeriodInMilliseconds);
        }
    }

    /**
     * Returns true if the client currently owns the lock with @param key and @param sortKey. It returns false otherwise.
     *
     * @param key     The partition key representing the lock.
     * @param sortKey The sort key if present.
     * @return true if the client owns the lock. It returns false otherwise.
     */
    public boolean hasLock(final String key, final Optional<String> sortKey) {
        Objects.requireNonNull(sortKey, "Sort Key must not be null (can be Optional.empty())");
        final LockItem localLock = this.locks.get(key + sortKey.orElse(""));
        return localLock != null && !localLock.isExpired();
    }

    /**
     * Attempts to acquire lock. If successful, returns the lock. Otherwise,
     * returns Optional.empty(). For more details on behavior, please see
     * {@code acquireLock}.
     *
     * @param options The options to use when acquiring the lock.
     * @return the lock if successful.
     * @throws InterruptedException in case this.acquireLock was interrupted.
     */
    public Optional<LockItem> tryAcquireLock(final AcquireLockOptions options) throws InterruptedException {
        try {
            return Optional.of(this.acquireLock(options));
        } catch (final LockNotGrantedException x) {
            return Optional.empty();
        }
    }

    /**
     * Releases the given lock if the current user still has it, returning true if the lock was successfully released, and false
     * if someone else already stole the lock. Deletes the lock item if it is released and deleteLockItemOnClose is set.
     *
     * @param lockItem The lock item to release
     * @return true if the lock is released, false otherwise
     */
    public boolean releaseLock(final LockItem lockItem) {
        return this.releaseLock(ReleaseLockOptions.builder(lockItem).withDeleteLock(lockItem.getDeleteLockItemOnClose()).build());
    }

    /**
     * Releases all the locks currently held by the owner specified when creating this lock client
     */
    private void releaseAllLocks() {
        final Map<String, LockItem> locks = new HashMap<>(this.locks);
        synchronized (locks) {
            for (final Entry<String, LockItem> lockEntry : locks.entrySet()) {
                this.releaseLock(lockEntry.getValue()); // TODO catch exceptions and report failure separately
            }
        }
    }

    /**
     * Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
     * given lock. If the client currently has the lock, it will return the lock, and operations such as releaseLock will work.
     * However, if the client does not have the lock, then operations like releaseLock will not work (after calling getLock, the
     * caller should check lockItem.isExpired() to figure out if it currently has the lock.)
     *
     * @param key     The partition key representing the lock.
     * @param sortKey The sort key if present.
     * @return A LockItem that represents the lock, if the lock exists.
     */
    public Optional<LockItem> getLock(final String key, final Optional<String> sortKey) {
        Objects.requireNonNull(sortKey, "Sort Key must not be null (can be Optional.empty())");
        final LockItem localLock = this.locks.get(key + sortKey.orElse(""));
        if (localLock != null) {
            return Optional.of(localLock);
        }
        final Optional<LockItem> lockItem =
            this.getLock(new GetLockOptions.GetLockOptionsBuilder(key).withSortKey(sortKey.orElse(null)).withDeleteLockOnRelease(false).build());

        if (lockItem.isPresent()) {
            if (lockItem.get().isReleased()) {
                // Return empty if a lock was released but still left in the table
                return Optional.empty();
            } else {
                /*
                 * Clear out the record version number so that the caller cannot accidentally perform updates on this lock (since
                 * the caller has not acquired the lock)
                 */
                lockItem.get().updateRecordVersionNumber("", 0, lockItem.get().getLeaseDuration());
            }
        }

        return lockItem;
    }

    /**
     * <p>
     * Sends a heartbeat to indicate that the given lock is still being worked on. If using
     * {@code createHeartbeatBackgroundThread}=true when setting up this object, then this method is unnecessary, because the
     * background thread will be periodically calling it and sending heartbeats. However, if
     * {@code createHeartbeatBackgroundThread}=false, then this method must be called to instruct DynamoDB that the lock should
     * not be expired.
     * </p>
     * <p>
     * The lease duration of the lock will be set to the default specified in the constructor of this class.
     * </p>
     *
     * @param lockItem the lock item row to send a heartbeat and extend lock expiry.
     */
    public void sendHeartbeat(final LockItem lockItem) {
        this.sendHeartbeat(SendHeartbeatOptions.builder(lockItem).build());
    }

    /**
     * Loops forever, sending hearbeats for all the locks this thread needs to keep track of.
     */
    @Override
    public void run() {
        while (true) {
            try {
                if (this.shuttingDown) {
                    throw new InterruptedException(); // sometimes libraries wrap interrupted and other exceptions
                }
                final long timeWorkBegins = LockClientUtils.INSTANCE.millisecondTime();
                final Map<String, LockItem> workingCopyOfLocks = new HashMap<>(this.locks);
                for (final Entry<String, LockItem> lockEntry : workingCopyOfLocks.entrySet()) {
                    try {
                        this.sendHeartbeat(lockEntry.getValue());
                    } catch (final LockNotGrantedException x) {
                        logger.debug("Heartbeat failed for " + lockEntry, x);
                    } catch (final RuntimeException x) {
                        logger.warn("Exception sending heartbeat for " + lockEntry, x);
                    }
                }
                final long timeElapsed = LockClientUtils.INSTANCE.millisecondTime() - timeWorkBegins;

                if (this.shuttingDown) {
                    throw new InterruptedException(); // sometimes libraries wrap interrupted and other exceptions
                }

                /* If we want to hearbeat every 9 seconds, and it took 3 seconds to send the heartbeats, we only sleep 6 seconds */
                Thread.sleep(Math.max(this.heartbeatPeriodInMilliseconds - timeElapsed, 0));
            } catch (final InterruptedException e) {
                logger.info("Heartbeat thread recieved interrupt, exiting run() (possibly exiting thread)", e);
                return;
            } catch (final RuntimeException x) {
                logger.warn("Exception sending heartbeat", x);
            }
        }
    }

    /**
     * Releases all of the locks by calling releaseAllLocks()
     */
    @Override
    public void close() throws IOException {
        // release the locks before interrupting the heartbeat thread to avoid partially updated/stale locks
        this.releaseAllLocks();
        if (this.backgroundThread.isPresent()) {
            this.shuttingDown = true;
            this.backgroundThread.get().interrupt();
            try {
                this.backgroundThread.get().join();
            } catch (final InterruptedException e) {
                logger.warn("Caught InterruptedException waiting for background thread to exit, interrupting current thread");
                Thread.currentThread().interrupt();
            }
        }
    }

    /* Helper method that starts a background heartbeating thread */
    protected Thread startBackgroundThread() {
        final Thread t = namedThreadCreator
            .apply("dynamodb-lock-client-" + lockClientId.addAndGet(1))
            .newThread(this);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /*
     * Generates a UUID for the record version number. Note that using something like an increasing sequence ID for the record
     * version number doesn't work, because it introduces race conditions into the logic, which could allow different threads to
     * steal each other's locks.
     */
    protected String generateRecordVersionNumber() {
        return UUID.randomUUID().toString();
    }

    protected void tryAddSessionMonitor(final String lockName, final LockItem lock) {
        if (lock.hasSessionMonitor() && lock.hasCallback()) {
            final Thread monitorThread = lockSessionMonitorChecker(lockName, lock);
            monitorThread.setDaemon(true);
            monitorThread.start();
            this.sessionMonitors.put(lockName, monitorThread);
        }
    }

    protected void removeKillSessionMonitor(final String monitorName) {
        if (this.sessionMonitors.containsKey(monitorName)) {
            final Thread monitor = this.sessionMonitors.remove(monitorName);
            monitor.interrupt();
            try {
                monitor.join();
            } catch (final InterruptedException e) {
                logger.warn("Caught InterruptedException waiting for session monitor thread to exit, ignoring");
            }
        }
    }

    /*
     * Validates the arguments to ensure that they are safe to register a
     * SessionMonitor on the lock to be acquired.
     *
     * @param safeTimeWithoutHeartbeatMillis the amount of time (in milliseconds) a lock can go without
     * heartbeating before it is declared to be in the "danger zone"
     *
     * @param heartbeatPeriodMillis the heartbeat period (in milliseconds)
     *
     * @param leaseDurationMillis the lease duration (in milliseconds)
     *
     * @throws IllegalArgumentException when the safeTimeWithoutHeartbeat is
     * less than the heartbeat frequency or greater than the lease duration
     */
    private static void sessionMonitorArgsValidate(final long safeTimeWithoutHeartbeatMillis, final long heartbeatPeriodMillis, final long leaseDurationMillis)
        throws IllegalArgumentException {
        if (safeTimeWithoutHeartbeatMillis <= heartbeatPeriodMillis) {
            throw new IllegalArgumentException("safeTimeWithoutHeartbeat must be greater than heartbeat frequency");
        } else if (safeTimeWithoutHeartbeatMillis >= leaseDurationMillis) {
            throw new IllegalArgumentException("safeTimeWithoutHeartbeat must be less than the lock's lease duration");
        }
    }

    private Thread lockSessionMonitorChecker(final String monitorName, final LockItem lock) {
        return namedThreadCreator.apply(monitorName + "-sessionMonitor").newThread(() -> {
            while (true) {
                try {
                    final long millisUntilDangerZone = lock.millisecondsUntilDangerZoneEntered();
                    if (millisUntilDangerZone > 0) {
                        Thread.sleep(millisUntilDangerZone);
                    } else {
                        lock.runSessionMonitor();
                        sessionMonitors.remove(monitorName);
                        return;
                    }
                } catch (final InterruptedException e) {
                    return;
                }
            }
        });
    }
}
