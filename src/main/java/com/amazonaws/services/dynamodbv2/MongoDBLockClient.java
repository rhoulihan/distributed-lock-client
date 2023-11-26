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

import static com.mongodb.client.model.Filters.eq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.bson.Document;
import org.bson.types.*;

import com.amazonaws.services.dynamodbv2.GetLockOptions.GetLockOptionsBuilder;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.core.SdkBytes;

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
public class MongoDBLockClient extends LockClient {
    protected final MongoClient mongoDB;
    protected final String databaseName;
    protected final String collectionName;
    private final String lockKeyName;
    private final Optional<String> sortKeyName;

    /**
     * Initializes an AmazonDynamoDBLockClient using the lock client options
     * specified in the AmazonDynamoDBLockClientOptions object.
     *
     * @param mongoDBLockClientOptions The options to use when initializing the client, i.e. the
     *                                        table name, sort key value, etc.
     */
    public MongoDBLockClient(final MongoDBLockClientOptions mongoDBLockClientOptions) {
        Objects.requireNonNull(mongoDBLockClientOptions.getMongoDBClient(), "MongoDB client object cannot be null");
        Objects.requireNonNull(mongoDBLockClientOptions.getOwnerName(), "Owner name cannot be null");
        Objects.requireNonNull(mongoDBLockClientOptions.getTimeUnit(), "Time unit cannot be null");
        Objects.requireNonNull(mongoDBLockClientOptions.getNamedThreadCreator(), "Named thread creator cannot be null");
        this.mongoDB = mongoDBLockClientOptions.getMongoDBClient();
        this.databaseName = System.getenv("LOCK_DATABASE_NAME") != null ? System.getenv("LOCK_DATABASE_NAME") : "lockdb";
        this.collectionName = System.getenv("LOCK_COLLECTION_NAME") != null ? System.getenv("LOCK_COLLECTION_NAME") : "locks";
        this.locks = new ConcurrentHashMap<>();
        this.sessionMonitors = new ConcurrentHashMap<>();
        this.ownerName = mongoDBLockClientOptions.getOwnerName();
        this.leaseDurationInMilliseconds = mongoDBLockClientOptions.getTimeUnit().toMillis(mongoDBLockClientOptions.getLeaseDuration());
        this.heartbeatPeriodInMilliseconds = mongoDBLockClientOptions.getTimeUnit().toMillis(mongoDBLockClientOptions.getHeartbeatPeriod());
        this.lockKeyName = "_id";
        this.sortKeyName = Optional.empty();
        this.namedThreadCreator = mongoDBLockClientOptions.getNamedThreadCreator();
        this.holdLockOnServiceUnavailable = mongoDBLockClientOptions.getHoldLockOnServiceUnavailable();

        if (mongoDBLockClientOptions.getCreateHeartbeatBackgroundThread()) {
            if (this.leaseDurationInMilliseconds < 2 * this.heartbeatPeriodInMilliseconds) {
                throw new IllegalArgumentException("Heartbeat period must be no more than half the length of the Lease Duration, "
                    + "or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example "
                    + "4+ times greater)");
            }
            this.backgroundThread = Optional.of(this.startBackgroundThread());
        } else {
            this.backgroundThread = Optional.empty();
        }
    }

    /**
     * Creates a MongoDB collection to be used by this locking library. 
     * <p>
     *
     * @param options The options for the lock client
     */
    private void createLockTableInMongoDB(Object options) {
        this.mongoDB.getDatabase(this.databaseName).createCollection(this.collectionName);
    }

    /**
     * Retrieves the lock item from MongoDB. Note that this will return a
     * LockItem even if it was released -- do NOT use this method if your goal
     * is to acquire a lock for doing work.
     *
     * @param options The options such as the key, etc.
     * @return The LockItem, or absent if it is not present. Note that the item
     * can exist in the table even if it is released, as noted by
     * isReleased().
     */
    public Optional<LockItem> getLockFromMongoDB(final GetLockOptions options) {
        Objects.requireNonNull(options, "AcquireLockOptions cannot be null");
        Objects.requireNonNull(options.getPartitionKey(), "Cannot lookup null key");

        MongoCursor<Document> cursor = this.mongoDB.getDatabase(databaseName).getCollection(collectionName).find(eq("_id", options.getPartitionKey())).limit(1).cursor();

        if (cursor.hasNext()) {
            return Optional.of(this.createLockItem(options, cursor.next()));
        }

        return Optional.empty();
    }

    private LockItem createLockItem(final GetLockOptions options, Document document) {
        final Optional<ByteBuffer> data = Optional.ofNullable(document.get(DATA)).map(dataValue -> {
            document.remove(DATA);
            return ByteBuffer.wrap(((Binary)dataValue).getData());
        });

        final String ownerName = document.remove(OWNER_NAME).toString();
        final Long leaseDuration = (Long)document.remove(LEASE_DURATION);
        final String recordVersionNumber = document.remove(RECORD_VERSION_NUMBER).toString();

        final boolean isReleased = document.containsKey(IS_RELEASED);
        document.remove(IS_RELEASED);
        document.remove(this.lockKeyName);

        /*
         * The person retrieving the lock should err on the side of
         * not expiring the lock, so they don't start counting until after the
         * call succeeds
         */
        final long lookupTime = LockClientUtils.INSTANCE.millisecondTime();
        final LockItem lockItem =
            new MongoDBLockItem(this,
                options.getPartitionKey(), data,
                options.isDeleteLockOnRelease(),
                ownerName,
                leaseDuration, lookupTime,
                recordVersionNumber, isReleased, Optional.empty(), document);
        return lockItem;
    }

    /**
     * <p>
     * Retrieves all the lock items from MongoDB.
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
    public Stream<LockItem> getAllLocksFromMongoDB(final boolean deleteOnRelease) {
        MongoCursor<Document> cursor = this.mongoDB.getDatabase(databaseName).getCollection(collectionName).find().cursor();
        final MongoDBLockItemIterator iterator = new MongoDBLockItemIterator(cursor, item -> {
            final String key = item.get(this.lockKeyName).toString();
            return getLockItem(key, deleteOnRelease, item);
        });

        final Iterable<LockItem> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false /*isParallelStream*/);
    }

    private LockItem getLockItem(String key, boolean deleteOnRelease, Document item) {
        GetLockOptionsBuilder options = GetLockOptions.builder(key).withDeleteLockOnRelease(deleteOnRelease);

        final LockItem lockItem = this.createLockItem(options.build(), item);
        return lockItem;
    }

    /**
     * Checks whether the lock table exists.
     *
     * @return true if the table exists, false otherwise.
     */
    @Override
    public boolean lockTableExists() {
        return this.mongoDB.getDatabase(databaseName).listCollectionNames().into(new ArrayList<>()).contains(collectionName);    
    }
    
    @Override
    protected LockItem buildLockItem(AcquireLockOptions options, String recordVersionNumber) {

        item = new HashMap<>();
        item.putAll(options.getAdditionalAttributes());
        item.put(this.getKeys().get(0), key);
        item.put(OWNER_NAME, this.ownerName);
        item.put(LEASE_DURATION, String.valueOf(this.leaseDurationInMilliseconds));
        item.put(RECORD_VERSION_NUMBER, String.valueOf(recordVersionNumber));

        newLockData.ifPresent(byteBuffer -> item.put(DATA, SdkBytes.fromByteBuffer(byteBuffer)));

        //if the existing lock does not exist or exists and is released
        if (!existingLock.isPresent() && !options.getAcquireOnlyIfLockAlreadyExists()) {
            return upsertAndMonitorNewLock(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData,
                item, recordVersionNumber);
        } else if (existingLock.isPresent() && existingLock.get().isReleased()) {
            return upsertAndMonitorReleasedLock(options, key, sortKey, deleteLockOnRelease, sessionMonitor, existingLock,
                newLockData, item, recordVersionNumber);
        }

        return null;
    }


    /**
     * Creates a DynamoDB table with the right schema for it to be used by this locking library. The table should be set up in advance,
     * because it takes a few minutes for DynamoDB to provision a new instance. Also, if the table already exists, this will throw an exception.
     * <p>
     * This method lets you specify a sort key to be used by the lock client. This sort key then needs to be specified in the
     * AmazonDynamoDBLockClientOptions when the lock client object is created.
     *
     */
    @Override
    public void createLockTable(Object options){
        createLockTableInMongoDB(options);
    }

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
    @Override
    protected LockItem upsertAndMonitorNewLock(AcquireLockOptions options, String key, Optional<String> sortKey,
        boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
        Optional<ByteBuffer> newLockData, Map<String, Object> item, String recordVersionNumber) {

        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();

        if (updateExistingLockRecord) {
            logger.trace("Acquiring a new lock on " + lockKeyName + "=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        } else {
        /* No one has the lock, go ahead and acquire it.
         * The person storing the lock into DynamoDB should err on the side of thinking the lock will expire
         * sooner than it actually will, so they start counting towards its expiration before the Put succeeds
         */
            logger.trace("Acquiring a new lock on " + lockKeyName + "=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        }
    }

    @Override
    protected LockItem upsertAndMonitorExpiredLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer> newLockData, Map<String, Object> item, String recordVersionNumber) {
        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();

        if (updateExistingLockRecord) {
            logger.trace("Acquiring an existing lock whose revisionVersionNumber did not change for " + lockKeyName + " partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        } else {
            logger.trace("Acquiring an existing lock whose revisionVersionNumber did not change for " + lockKeyName + " partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        }
    }

    @Override
    protected LockItem upsertAndMonitorReleasedLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean
            deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer>
            newLockData, Map<String, Object> item, String recordVersionNumber) {

        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();

        if (updateExistingLockRecord) {
            logger.trace("Acquiring an existing released whose revisionVersionNumber did not change for " + lockKeyName + " " +
                                 "partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        } else {
            logger.trace("Acquiring an existing released lock whose revisionVersionNumber did not change for " + lockKeyName + " " +
                                 "partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        }

    }

    @Override
    protected LockItem putLockItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber) {
        final long lastUpdatedTime = LockClientUtils.INSTANCE.millisecondTime();

        Document document = new Document();
        document.append("_id", key);
        document.append("ownerName", this.ownerName);
        document.append("leaseDuration", this.leaseDurationInMilliseconds);
        document.append("lastUpdatedTime", lastUpdatedTime);
        document.append("recordVersionNumber", recordVersionNumber);

        if (newLockData.isPresent())
            document.append("data", newLockData);
        
        this.mongoDB.getDatabase("lockdb").getCollection("locks").insertOne(document);

        final LockItem lockItem =
            new MongoDBLockItem(this, key, newLockData, deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds, lastUpdatedTime,
                recordVersionNumber, false, sessionMonitor, options.document);
        this.locks.put(lockItem.getUniqueIdentifier(), lockItem);
        this.tryAddSessionMonitor(lockItem.getUniqueIdentifier(), lockItem);
        return lockItem;
    }

    @Override
    protected LockItem updateItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
                                                      Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber) {
        final long lastUpdatedTime = LockClientUtils.INSTANCE.millisecondTime();

        MongoCollection<Document> collection = this.mongoDB.getDatabase(databaseName).getCollection(collectionName);
        // Update the isReleased field to true if the document matches the criteria
        Document lockDoc = collection.findOneAndUpdate(
            Filters.and(
                Filters.eq("_id", key),
                Filters.eq("ownerName", this.ownerName),
                Filters.eq("recordVersionNumber", recordVersionNumber)),
                newLockData.isPresent() ?
                    Updates.combine(
                        Updates.set("isReleased", true),
                        Updates.set("data", newLockData)) :
                    Updates.set("isReleased", true)
            );

        // Check if a document was updated
        if (lockDoc == null) {
            logger.info("Document with _id: " + key + " was unable to be updated.");
            return null;
        }

        final LockItem lockItem =
                new MongoDBLockItem(this, key, newLockData, deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds, lastUpdatedTime,
                        recordVersionNumber, !IS_RELEASED_INDICATOR, sessionMonitor, options.document);
        this.locks.put(lockItem.getUniqueIdentifier(), lockItem);
        this.tryAddSessionMonitor(lockItem.getUniqueIdentifier(), lockItem);
        return lockItem;
    }

    /**
     * Releases the given lock if the current user still has it, returning true if the lock was successfully released, and false
     * if someone else already stole the lock. Deletes the lock item if it is released and deleteLockItemOnClose is set.
     *
     * @param lockItem The lock item to release
     * @return true if the lock is released, false otherwise
     */
    @Override
    public boolean releaseLock(final ReleaseLockOptions options) {
        Objects.requireNonNull(options, "ReleaseLockOptions cannot be null");

        final LockItem lockItem = options.getLockItem();
        final boolean deleteLock = options.isDeleteLock();
        Objects.requireNonNull(lockItem, "Cannot release null lockItem");
        if (!lockItem.getOwnerName().equals(this.ownerName)) {
            return false;
        }

        synchronized (lockItem) {
            try {
                // Always remove the heartbeat for the lock. The
                // caller's intention is to release the lock. Stopping the
                // heartbeat alone will do that regardless of whether the Dynamo
                // write succeeds or fails.
                this.locks.remove(lockItem.getUniqueIdentifier());

                //set up expression stuff for DeleteItem or UpdateItem
                //basically any changes require:
                //1. I own the lock
                //2. I know the current version number
                //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)
                MongoCollection<Document> collection = this.mongoDB.getDatabase(databaseName).getCollection(collectionName);
                Document lockDoc = null;
                if (deleteLock) {
                    // Find and delete the document atomically
                    lockDoc = collection.findOneAndDelete(Filters.and(
                        Filters.eq("_id", lockItem.getPartitionKey()),
                        Filters.eq("ownerName", lockItem.getOwnerName()),
                        Filters.eq("recordVersionNumber", lockItem.getRecordVersionNumber())
                    ));

                    // Check if a document was deleted
                    if (lockDoc == null) {
                        throw new Exception("Document with _id: " + lockItem.getPartitionKey() + " was unable to be deleted.");
                    }
                } else {
                    // Update the isReleased field to true if the document matches the criteria
                    lockDoc = collection.findOneAndUpdate(
                        Filters.and(
                            Filters.eq("_id", lockItem.getPartitionKey()),
                            Filters.eq("ownerName", lockItem.getOwnerName()),
                            Filters.eq("recordVersionNumber", lockItem.getRecordVersionNumber())
                        ),
                        lockItem.getData() != null ?
                            Updates.combine(
                                Updates.set("isReleased", true),
                                Updates.set("data", lockItem.getData())) :
                            Updates.set("isReleased", true)
                        );

                    // Check if a document was updated
                    if (lockDoc == null) {
                        throw new Exception("Document with _id: " + lockItem.getPartitionKey() + " was unable to be updated.");
                    }
                }
            } catch (final Exception exception) {
                logger.debug(exception.getMessage());
                return false;
            }

            // Only remove the session monitor if no exception thrown above.
            // While moving the heartbeat removal before the DynamoDB call
            // should not cause existing clients problems, there
            // may be existing clients that depend on the monitor firing if they
            // get exceptions from this method.
            this.removeKillSessionMonitor(lockItem.getUniqueIdentifier());
        }
        return true;
    }

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
    @Override
    public Stream<LockItem> getLocksByPartitionKey(String key, final boolean deleteOnRelease) {
        MongoCursor<Document> cursor = this.mongoDB.getDatabase(databaseName).getCollection(collectionName).find(eq("_id", key)).cursor();
        final MongoDBLockItemIterator iterator = new MongoDBLockItemIterator(cursor, item -> {
            final String lockKey = item.get(this.lockKeyName).toString();
            return getLockItem(lockKey, deleteOnRelease, item);
        });

        final Iterable<LockItem> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false /*isParallelStream*/);
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
     * This method will also set the lease duration of the lock to the given value.
     * </p>
     * <p>
     * This will also either update or delete the data from the lock, as specified in the options
     * </p>
     *
     * @param options a set of optional arguments for how to send the heartbeat
     */
    @Override
    public void sendHeartbeat(final SendHeartbeatOptions options) {
        Objects.requireNonNull(options, "options is required");
        Objects.requireNonNull(options.getLockItem(), "Cannot send heartbeat for null lock");
        final boolean deleteData = options.getDeleteData() != null && options.getDeleteData();
        if (deleteData && options.getData().isPresent()) {
            throw new IllegalArgumentException("data must not be present if deleteData is true");
        }

        long leaseDurationToEnsureInMilliseconds = this.leaseDurationInMilliseconds;
        if (options.getLeaseDurationToEnsure() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "TimeUnit must not be null if leaseDurationToEnsure is not null");
            leaseDurationToEnsureInMilliseconds = options.getTimeUnit().toMillis(options.getLeaseDurationToEnsure());
        }

        final LockItem lockItem = options.getLockItem();
        if (lockItem.isExpired() || !lockItem.getOwnerName().equals(this.ownerName) || lockItem.isReleased()) {
            this.locks.remove(lockItem.getUniqueIdentifier());
            throw new LockNotGrantedException("Cannot send heartbeat because lock is not granted");
        }

        synchronized (lockItem) {
            //Set up condition for UpdateItem. Basically any changes require:
            //1. I own the lock
            //2. I know the current version number
            //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)
            final String recordVersionNumber = this.generateRecordVersionNumber();

            try {
                final long lastUpdateOfLock = LockClientUtils.INSTANCE.millisecondTime();

                MongoCollection<Document> collection = this.mongoDB.getDatabase(recordVersionNumber).getCollection(recordVersionNumber);
                UpdateOptions updateOptions = new UpdateOptions().upsert(true);

                Document newLock = new Document();
                newLock.put("recordVersionNumber", recordVersionNumber);
                newLock.put("ownerName", lockItem.getOwnerName());
                newLock.put("_id", lockItem.getPartitionKey());
                newLock.put("deleteLockItemOnClose", lockItem.getDeleteLockItemOnClose());
                newLock.put("isReleased", lockItem.isReleased());
                newLock.put("lookupTime", System.currentTimeMillis());
                newLock.put("leaseDuration", leaseDurationInMilliseconds);
                newLock.put("additionalAttributes", lockItem.getAdditionalAttributes());

                if (lockItem.getData() != null) {
                    newLock.put("data", lockItem.getData());
                }
                
                Document filter = new Document("_id", newLock.get("_id"))
                    .append("ownerName", newLock.get("ownerName"))
                    .append("recordVersionNumber", lockItem.getRecordVersionNumber());

                UpdateResult  result = collection.updateOne(filter, new Document("$set", newLock), updateOptions);

                if (result.getMatchedCount() == 0 && result.getUpsertedId() == null) {
                    throw new Exception("Lock on _id=" + lockItem.getPartitionKey() + " already acquired.");
                }

                if (result.getModifiedCount() == 0) {
                    throw new Exception("Unable to send heartbeat for _id=" + lockItem.getPartitionKey() + ".");
                }
                lockItem.updateRecordVersionNumber(recordVersionNumber, lastUpdateOfLock, leaseDurationToEnsureInMilliseconds);
                if (deleteData) {
                    lockItem.updateData(null);
                } else if (options.getData().isPresent()) {
                    lockItem.updateData(options.getData().get());
                }
            } catch (final Exception ex) {
                if (!holdLockOnServiceUnavailable) {
                    logger.debug("Someone else acquired the lock, so we will stop heartbeating it", ex);
                    this.locks.remove(lockItem.getUniqueIdentifier());
                } else {
                    logger.info("MongoDB Service Unavailable. Holding the lock.");
                    lockItem.updateLookUpTime(LockClientUtils.INSTANCE.millisecondTime());
                }
                    
                throw new LockNotGrantedException(ex.getMessage(), ex);
            
            } 
        }
    }

    @Override
    protected List<String> getKeys() {
        List<String> keys = new ArrayList<String>();
        keys.add(this.lockKeyName);
        if (sortKeyName.isPresent())
            keys.add(sortKeyName.get());
        
        return keys;
    }

    @Override
    protected List<String> getKeyValues() {
        List<String> keyNames = new ArrayList<String>();
        keyNames.add(this.key);
        if (sortKey.isPresent())
            keyNames.add(sortKey.get());
        
        return keyNames;
    }

    @Override
    protected Optional<LockItem> getLock(GetLockOptions lockOptions) {
        return this.getLockFromMongoDB(lockOptions);
    }

    @Override
    public Stream<LockItem> getAllLocksFromDB(boolean deleteOnRelease) {
        return this.getAllLocksFromDB(deleteOnRelease);
    }
}
