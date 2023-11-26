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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.GetLockOptions.GetLockOptionsBuilder;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

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
public class AmazonDynamoDBLockClient extends LockClient {
    private static final Log logger = LogFactory.getLog(AmazonDynamoDBLockClient.class);
    private static final Set<TableStatus> availableStatuses;
    protected static final String SK_PATH_EXPRESSION_VARIABLE = "#sk";
    protected static final String PK_PATH_EXPRESSION_VARIABLE = "#pk";
    protected static final String PK_VALUE_EXPRESSION_VARIABLE = ":pk";
    protected static final String NEW_RVN_VALUE_EXPRESSION_VARIABLE = ":newRvn";
    protected static final String LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE = "#ld";
    protected static final String LEASE_DURATION_VALUE_EXPRESSION_VARIABLE = ":ld";
    protected static final String RVN_PATH_EXPRESSION_VARIABLE = "#rvn";
    protected static final String RVN_VALUE_EXPRESSION_VARIABLE = ":rvn";
    protected static final String OWNER_NAME_PATH_EXPRESSION_VARIABLE = "#on";
    protected static final String OWNER_NAME_VALUE_EXPRESSION_VARIABLE = ":on";
    protected static final String DATA_PATH_EXPRESSION_VARIABLE = "#d";
    protected static final String DATA_VALUE_EXPRESSION_VARIABLE = ":d";
    protected static final String IS_RELEASED_PATH_EXPRESSION_VARIABLE = "#ir";
    protected static final String IS_RELEASED_VALUE_EXPRESSION_VARIABLE = ":ir";

    //attribute_not_exists(#pk)
    protected static final String ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION = String.format(
        "attribute_not_exists(%s)",
        PK_PATH_EXPRESSION_VARIABLE);

    //attribute_not_exists(#pk) AND attribute_not_exists(#sk)
    protected static final String ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION = String.format(
            "attribute_not_exists(%s) AND attribute_not_exists(%s)",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND #ir = :ir
    protected static final String PK_EXISTS_AND_IS_RELEASED_CONDITION = String.format("attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND attribute_exists(#sk) AND #ir = :ir
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION = String.format(
            "attribute_exists(%s) AND attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE, IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn AND #ir = :ir
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION = String.format(
            "attribute_exists(%s) AND attribute_exists(%s) AND %s = %s AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE,
            IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION =
        String.format("attribute_exists(%s) AND attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE);

    //(attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn) AND (attribute_not_exists(#if) OR #if = :if) AND #on = :on
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION =
        String.format("%s AND %s = %s ",
                PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION, OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME_VALUE_EXPRESSION_VARIABLE);

    //(attribute_exists(#pk) AND #rvn = :rvn AND #ir = :ir) AND (attribute_not_exists(#if) OR #if = :if)
    protected static final String PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION =
            String.format("(attribute_exists(%s) AND %s = %s AND %s = %s)",
                          PK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE,
                          IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND #rvn = :rvn AND (attribute_not_exists(#if) OR #if = :if)
    protected static final String PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION =
        String.format("attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND #rvn = :rvn AND (attribute_not_exists(#if) OR #if = :if) AND #on = :on
    protected static final String PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION =
        String.format("%s AND %s = %s",
            PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION, OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME_VALUE_EXPRESSION_VARIABLE);

    protected static final String UPDATE_IS_RELEASED = String.format("SET %s = %s", IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);
    protected static final String UPDATE_IS_RELEASED_AND_DATA =
        String.format("%s, %s = %s", UPDATE_IS_RELEASED, DATA_PATH_EXPRESSION_VARIABLE, DATA_VALUE_EXPRESSION_VARIABLE);
    protected static final String UPDATE_LEASE_DURATION_AND_RVN = String.format(
        "SET %s = %s, %s = %s",
        LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION_VALUE_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, NEW_RVN_VALUE_EXPRESSION_VARIABLE);
    protected static final String UPDATE_LEASE_DURATION_AND_RVN_AND_REMOVE_DATA = String.format("%s REMOVE %s", UPDATE_LEASE_DURATION_AND_RVN, DATA_PATH_EXPRESSION_VARIABLE);
    protected static final String UPDATE_LEASE_DURATION_AND_RVN_AND_DATA = String.format("%s, %s = %s",
        UPDATE_LEASE_DURATION_AND_RVN, DATA_PATH_EXPRESSION_VARIABLE, DATA_VALUE_EXPRESSION_VARIABLE);
    protected static final String REMOVE_IS_RELEASED_UPDATE_EXPRESSION = String.format(" REMOVE %s ", IS_RELEASED_PATH_EXPRESSION_VARIABLE);
    protected static final String QUERY_PK_EXPRESSION = String.format("%s = %s", PK_PATH_EXPRESSION_VARIABLE, PK_VALUE_EXPRESSION_VARIABLE);

    static {
        availableStatuses = new HashSet<>();
        availableStatuses.add(TableStatus.ACTIVE);
        availableStatuses.add(TableStatus.UPDATING);
    }

    protected final DynamoDbClient dynamoDB;
    protected final String tableName;
    private final String partitionKeyName;
    private final Optional<String> sortKeyName;

    protected static final AttributeValue IS_RELEASED_ATTRIBUTE_VALUE = AttributeValue.builder().s(IS_RELEASED_VALUE).build();

    /**
     * Initializes an AmazonDynamoDBLockClient using the lock client options
     * specified in the AmazonDynamoDBLockClientOptions object.
     *
     * @param amazonDynamoDBLockClientOptions The options to use when initializing the client, i.e. the
     *                                        table name, sort key value, etc.
     */
    public AmazonDynamoDBLockClient(final AmazonDynamoDBLockClientOptions amazonDynamoDBLockClientOptions) {
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getDynamoDBClient(), "DynamoDB client object cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getTableName(), "Table name cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getOwnerName(), "Owner name cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getTimeUnit(), "Time unit cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getPartitionKeyName(), "Partition Key Name cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getSortKeyName(), "Sort Key Name cannot be null (use Optional.absent())");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getNamedThreadCreator(), "Named thread creator cannot be null");
        this.dynamoDB = amazonDynamoDBLockClientOptions.getDynamoDBClient();
        this.tableName = amazonDynamoDBLockClientOptions.getTableName();
        this.locks = new ConcurrentHashMap<>();
        this.sessionMonitors = new ConcurrentHashMap<>();
        this.ownerName = amazonDynamoDBLockClientOptions.getOwnerName();
        this.leaseDurationInMilliseconds = amazonDynamoDBLockClientOptions.getTimeUnit().toMillis(amazonDynamoDBLockClientOptions.getLeaseDuration());
        this.heartbeatPeriodInMilliseconds = amazonDynamoDBLockClientOptions.getTimeUnit().toMillis(amazonDynamoDBLockClientOptions.getHeartbeatPeriod());
        this.partitionKeyName = amazonDynamoDBLockClientOptions.getPartitionKeyName();
        this.sortKeyName = amazonDynamoDBLockClientOptions.getSortKeyName();
        this.namedThreadCreator = amazonDynamoDBLockClientOptions.getNamedThreadCreator();
        this.holdLockOnServiceUnavailable = amazonDynamoDBLockClientOptions.getHoldLockOnServiceUnavailable();

        if (amazonDynamoDBLockClientOptions.getCreateHeartbeatBackgroundThread()) {
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
     * Builds an updateExpression for all fields in item map and updates the correspoding expression attribute name and
     * value maps.
     * @param item Map of Name and AttributeValue to update or create
     * @param expressionAttributeNames
     * @param expressionAttributeValues
     * @return
     */
    private String getUpdateExpressionAndUpdateNameValueMaps(Map<String, Object> item,
        Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues) {
        final String additionalUpdateExpression = "SET ";
        StringBuilder updateExpressionBuilder = new StringBuilder(additionalUpdateExpression);
        int i = 0;
        String keyExpression;
        String valueExpression;
        Iterator<Entry<String, Object>> iterator = item.entrySet().iterator();
        String expressionSeparator = ",";
        while (iterator.hasNext()) {
            Entry<String, Object> entry = iterator.next();
            keyExpression = "#k" + i;
            valueExpression = ":v" + i;

            expressionAttributeNames.put(keyExpression, entry.getKey());
            expressionAttributeValues.put(valueExpression,(AttributeValue)entry.getValue());
            if (!iterator.hasNext()) {
                expressionSeparator = "";
            }
            updateExpressionBuilder.append("#k").append(i).append("=").append(":v").append(i).append(expressionSeparator);
            i++;
        }
        return updateExpressionBuilder.toString();
    }

    /**
     * Creates a DynamoDB table with the right schema for it to be used by this locking library. The table should be set up in advance,
     * because it takes a few minutes for DynamoDB to provision a new instance. Also, if the table already exists, this will throw an exception.
     * <p>
     * This method lets you specify a sort key to be used by the lock client. This sort key then needs to be specified in the
     * AmazonDynamoDBLockClientOptions when the lock client object is created.
     *
     * @param createDynamoDBTableOptions The options for the lock client
     */
    public static void createLockTableInDynamoDB(final CreateDynamoDBTableOptions createDynamoDBTableOptions) {
        Objects.requireNonNull(createDynamoDBTableOptions.getDynamoDBClient(), "DynamoDB client object cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getTableName(), "Table name cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getProvisionedThroughput(), "Provisioned throughput cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getPartitionKeyName(), "Hash Key Name cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getSortKeyName(), "Sort Key Name cannot be null");
        final KeySchemaElement partitionKeyElement = KeySchemaElement.builder()
                .attributeName(createDynamoDBTableOptions.getPartitionKeyName()).keyType(KeyType.HASH)
                .build();

        final List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(partitionKeyElement);

        final Collection<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(AttributeDefinition.builder()
                .attributeName(createDynamoDBTableOptions.getPartitionKeyName())
                .attributeType(ScalarAttributeType.S)
                .build());

        if (createDynamoDBTableOptions.getSortKeyName().isPresent()) {
            final KeySchemaElement sortKeyElement = KeySchemaElement.builder()
                    .attributeName(createDynamoDBTableOptions.getSortKeyName().get())
                    .keyType(KeyType.RANGE)
                    .build();
            keySchema.add(sortKeyElement);
            attributeDefinitions.add(AttributeDefinition.builder()
                    .attributeName(createDynamoDBTableOptions.getSortKeyName().get())
                    .attributeType(ScalarAttributeType.S)
                    .build());
        }

        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(createDynamoDBTableOptions.getTableName())
                .keySchema(keySchema)
                .provisionedThroughput(createDynamoDBTableOptions.getProvisionedThroughput())
                .attributeDefinitions(attributeDefinitions)
                .build();

        createDynamoDBTableOptions.getDynamoDBClient().createTable(createTableRequest);
    }

    private Map<String, AttributeValue> castMap(Map<String, Object> mapToCast) {
        Map<String, AttributeValue> castedMap = new HashMap<String, AttributeValue>();
        
        for (Entry<String, Object> entry : mapToCast.entrySet()) {
            castedMap.put(entry.getKey(), (AttributeValue)entry.getValue());
        }


        return castedMap;
    }

    private Map<String, AttributeValue> getItemKeys(LockItem lockItem) {
        return getKeys(lockItem.getPartitionKey(), lockItem.getSortKey());
    }

    private Map<String, AttributeValue> getKeys(String partitionKey, Optional<String> sortKey) {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put(this.partitionKeyName, AttributeValue.builder().s(partitionKey).build());
        if (sortKey.isPresent()) {
            key.put(this.sortKeyName.get(), AttributeValue.builder().s(sortKey.get()).build());
        }
        return key;
    }

    /**
     * Retrieves the lock item from DynamoDB. Note that this will return a
     * LockItem even if it was released -- do NOT use this method if your goal
     * is to acquire a lock for doing work.
     *
     * @param options The options such as the key, etc.
     * @return The LockItem, or absent if it is not present. Note that the item
     * can exist in the table even if it is released, as noted by
     * isReleased().
     */
    public Optional<LockItem> getLockFromDynamoDB(final GetLockOptions options) {
        Objects.requireNonNull(options, "AcquireLockOptions cannot be null");
        Objects.requireNonNull(options.getPartitionKey(), "Cannot lookup null key");
        final GetItemResponse result = this.readFromDynamoDB(options.getPartitionKey(), options.getSortKey());
        final Map<String, AttributeValue> item = result.item();

        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(this.createLockItem(options, item));
    }

    private LockItem createLockItem(final GetLockOptions options, final Map<String, AttributeValue> immutableItem) {
        Map<String, Object> item = new HashMap<>(immutableItem);
        final Optional<ByteBuffer> data = Optional.ofNullable(item.get(DATA)).map(dataAttributionValue -> {
            item.remove(DATA);
            return ((AttributeValue)dataAttributionValue).b().asByteBuffer();
        });

        final AttributeValue ownerName = (AttributeValue)item.remove(OWNER_NAME);
        final AttributeValue leaseDuration = (AttributeValue)item.remove(LEASE_DURATION);
        final AttributeValue recordVersionNumber = (AttributeValue)item.remove(RECORD_VERSION_NUMBER);

        final boolean isReleased = item.containsKey(IS_RELEASED);
        item.remove(IS_RELEASED);
        item.remove(this.partitionKeyName);

        /*
         * The person retrieving the lock in DynamoDB should err on the side of
         * not expiring the lock, so they don't start counting until after the
         * call to DynamoDB succeeds
         */
        final long lookupTime = LockClientUtils.INSTANCE.millisecondTime();
        final LockItem lockItem =
            new AmazonDynamoDBLockItem(this,
                options.getPartitionKey(),
                options.getSortKey(), data,
                options.isDeleteLockOnRelease(),
                ownerName.s(),
                Long.parseLong(leaseDuration.s()), lookupTime,
                recordVersionNumber.s(), isReleased, Optional.empty(), item);
        return lockItem;
    }

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
    public Stream<LockItem> getAllLocksFromDynamoDB(final boolean deleteOnRelease) {
        final ScanRequest scanRequest = ScanRequest.builder().tableName(this.tableName).build();
        this.item = null;
        final AmazonDynamoDBLockItemScanIterator iterator = new AmazonDynamoDBLockItemScanIterator(this.dynamoDB, scanRequest, item -> {
            Map<String, AttributeValue> newMap = item;
            final String key = newMap.get(this.partitionKeyName).s();
            return getLockItem(key, deleteOnRelease, newMap);
        });

        final Iterable<LockItem> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false /*isParallelStream*/);
    }

    /* Helper method to read a key from DynamoDB */
    private GetItemResponse readFromDynamoDB(final String key, final Optional<String> sortKey) {
        final Map<String, AttributeValue> dynamoDBKey = new HashMap<>();
        dynamoDBKey.put(this.partitionKeyName, AttributeValue.builder().s(key).build());
        if (this.sortKeyName.isPresent()) {
            dynamoDBKey.put(this.sortKeyName.get(), AttributeValue.builder().s(sortKey.get()).build());
        }
        final GetItemRequest getItemRequest = GetItemRequest.builder().tableName(tableName).key(dynamoDBKey)
                .consistentRead(true)
                .build();

        return this.dynamoDB.getItem(getItemRequest);
    }

    private LockItem getLockItem(String key, boolean deleteOnRelease,
        Map<String, AttributeValue> item) {
        GetLockOptionsBuilder options = GetLockOptions.builder(key).withDeleteLockOnRelease(deleteOnRelease);

        options = this.sortKeyName.map(item::get).map(AttributeValue::s).map(options::withSortKey).orElse(options);

        final LockItem lockItem = this.createLockItem(options.build(), item);
        return lockItem;
    }

    /**
     * Checks whether the lock table exists in DynamoDB.
     *
     * @return true if the table exists, false otherwise.
     */
    @Override
    public boolean lockTableExists() {
        try {
            final DescribeTableResponse result
                    = this.dynamoDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
            return availableStatuses.contains(result.table().tableStatus());
        } catch (final ResourceNotFoundException e) {
            // This exception indicates the table doesn't exist.
            return false;
        }
    }
    
    @Override
    protected LockItem buildLockItem(AcquireLockOptions options, String recordVersionNumber) {
        item = new HashMap<>();
        item.putAll(options.getAdditionalAttributes());
        item.put(this.getKeys().get(0), AttributeValue.builder().s(key).build());
        item.put(OWNER_NAME, AttributeValue.builder().s(this.ownerName).build());
        item.put(LEASE_DURATION, AttributeValue.builder().s(String.valueOf(this.leaseDurationInMilliseconds)).build());
        item.put(RECORD_VERSION_NUMBER, AttributeValue.builder().s(String.valueOf(recordVersionNumber)).build());
        if (this.getKeys().size() > 1)
            item.put(this.getKeys().get(1), AttributeValue.builder().s(sortKey.get()).build());

        newLockData.ifPresent(byteBuffer -> item.put(DATA, AttributeValue.builder().b(SdkBytes.fromByteBuffer(byteBuffer)).build()));

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
        createLockTableInDynamoDB((CreateDynamoDBTableOptions)options);
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

        final Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, this.partitionKeyName);

        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();

        final String conditionalExpression;
        if (this.sortKeyName.isPresent()) {
            conditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION;
            expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            conditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION;
        }

        if (updateExistingLockRecord) {
            // Remove keys from item to create updateExpression
            item.remove(partitionKeyName);
            if (sortKeyName.isPresent()) {
                item.remove(sortKeyName.get());
            }
            final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            final String updateExpression = getUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues);
            final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(getKeys(key, sortKey))
                    .updateExpression(updateExpression).expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues).conditionExpression(conditionalExpression).build();
            logger.trace("Acquiring a new lock on " + partitionKeyName + "=" + key + ", " + this.sortKeyName + "=" + sortKey);
            options.updateRequest = updateItemRequest;
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        } else {
            final PutItemRequest putItemRequest = PutItemRequest.builder().item(castMap(item)).tableName(tableName)
                    .conditionExpression(conditionalExpression)
                    .expressionAttributeNames(expressionAttributeNames).build();
        /* No one has the lock, go ahead and acquire it.
         * The person storing the lock into DynamoDB should err on the side of thinking the lock will expire
         * sooner than it actually will, so they start counting towards its expiration before the Put succeeds
         */
            logger.trace("Acquiring a new lock on " + partitionKeyName + "=" + key + ", " + this.sortKeyName + "=" + sortKey);
            options.putRequest = putItemRequest;
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        }
    }

    @Override
    protected LockItem upsertAndMonitorExpiredLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer> newLockData, Map<String, Object> item, String recordVersionNumber) {
        final String conditionalExpression;
        final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();
        expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(existingLock.get().getRecordVersionNumber()).build());
        final Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
        expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
        if (this.sortKeyName.isPresent()) {
            //We do not check the owner here because the lock is expired and it is OK to overwrite the owner
            conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
            expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            conditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
        }

        if (updateExistingLockRecord) {
            item.remove(partitionKeyName);
            if (sortKeyName.isPresent()) {
                item.remove(sortKeyName.get());
            }
            final String updateExpression = getUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues);

            final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(getItemKeys(existingLock.get()))
                    .updateExpression(updateExpression).expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues).conditionExpression(conditionalExpression).build();
            logger.trace("Acquiring an existing lock whose revisionVersionNumber did not change for " + partitionKeyName + " partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            options.updateRequest = updateItemRequest;
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        } else {
            final PutItemRequest putItemRequest = PutItemRequest.builder().item(castMap(item)).tableName(tableName).conditionExpression(conditionalExpression)
                    .expressionAttributeNames(expressionAttributeNames).expressionAttributeValues(expressionAttributeValues).build();

            logger.trace("Acquiring an existing lock whose revisionVersionNumber did not change for " + partitionKeyName + " partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            options.putRequest = putItemRequest;
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        }
    }

    @Override
    protected LockItem upsertAndMonitorReleasedLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean
            deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer>
            newLockData, Map<String, Object> item, String recordVersionNumber) {

        final String conditionalExpression;
        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();
        final boolean consistentLockData = options.getAcquireReleasedLocksConsistently();

        final Map<String, String> expressionAttributeNames = new HashMap<>();
        final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();

        if (consistentLockData) {
            expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(existingLock.get().getRecordVersionNumber()).build());
            expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
        }
        expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
        expressionAttributeNames.put(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);

        if (this.sortKeyName.isPresent()) {
            //We do not check the owner here because the lock is expired and it is OK to overwrite the owner
            if (consistentLockData) {
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
            } else {
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION;
            }
            expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            if (consistentLockData) {
                conditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
            } else {
                conditionalExpression = PK_EXISTS_AND_IS_RELEASED_CONDITION;
            }
        }
        expressionAttributeValues.put(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);

        if (updateExistingLockRecord) {
            item.remove(partitionKeyName);
            if (sortKeyName.isPresent()) {
                item.remove(sortKeyName.get());
            }
            final String updateExpression = getUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues)
                    + REMOVE_IS_RELEASED_UPDATE_EXPRESSION;

            final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(getItemKeys(existingLock.get()))
                    .updateExpression(updateExpression).expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues).conditionExpression(conditionalExpression).build();
            logger.trace("Acquiring an existing released whose revisionVersionNumber did not change for " + partitionKeyName + " " +
                                 "partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            options.updateRequest = updateItemRequest;
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        } else {
            final PutItemRequest putItemRequest = PutItemRequest.builder().item(castMap(item)).tableName(tableName).conditionExpression(conditionalExpression)
                    .expressionAttributeNames(expressionAttributeNames).expressionAttributeValues(expressionAttributeValues).build();

            logger.trace("Acquiring an existing released lock whose revisionVersionNumber did not change for " + partitionKeyName + " " +
                                 "partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            options.putRequest = putItemRequest;
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber);
        }

    }

    @Override
    protected LockItem putLockItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber) {
        final long lastUpdatedTime = LockClientUtils.INSTANCE.millisecondTime();
        this.dynamoDB.putItem(options.putRequest);

        final LockItem lockItem =
            new AmazonDynamoDBLockItem(this, key, sortKey, newLockData, deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds, lastUpdatedTime,
                recordVersionNumber, false, sessionMonitor, options.getAdditionalAttributes());
        this.locks.put(lockItem.getUniqueIdentifier(), lockItem);
        this.tryAddSessionMonitor(lockItem.getUniqueIdentifier(), lockItem);
        return lockItem;
    }

    @Override
    protected LockItem updateItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
                                                      Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber) {
        final long lastUpdatedTime = LockClientUtils.INSTANCE.millisecondTime();
        this.dynamoDB.updateItem(options.updateRequest);
        final LockItem lockItem =
                new AmazonDynamoDBLockItem(this, key, sortKey, newLockData, deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds, lastUpdatedTime,
                        recordVersionNumber, !IS_RELEASED_INDICATOR, sessionMonitor, options.getAdditionalAttributes());
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
        final boolean bestEffort = options.isBestEffort();
        final Optional<ByteBuffer> data = options.getData();
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
                final String conditionalExpression;
                final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(lockItem.getRecordVersionNumber()).build());
                expressionAttributeValues.put(OWNER_NAME_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(lockItem.getOwnerName()).build());
                final Map<String, String> expressionAttributeNames = new HashMap<>();
                expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
                expressionAttributeNames.put(OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME);
                expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
                if (this.sortKeyName.isPresent()) {
                    conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                    expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
                } else {
                    conditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                }

                final Map<String, AttributeValue> key = getItemKeys(lockItem);
                if (deleteLock) {
                    final DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                            .tableName(tableName)
                            .key(key)
                            .conditionExpression(conditionalExpression)
                            .expressionAttributeNames(expressionAttributeNames)
                            .expressionAttributeValues(expressionAttributeValues)
                            .build();

                    this.dynamoDB.deleteItem(deleteItemRequest);
                } else {
                    final String updateExpression;
                    expressionAttributeNames.put(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);
                    expressionAttributeValues.put(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);
                    if (data.isPresent()) {
                        updateExpression = UPDATE_IS_RELEASED_AND_DATA;
                        expressionAttributeNames.put(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                        expressionAttributeValues.put(DATA_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().b(SdkBytes.fromByteBuffer(data.get())).build());
                    } else {
                        updateExpression = UPDATE_IS_RELEASED;
                    }
                    final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                            .tableName(this.tableName)
                            .key(key)
                            .updateExpression(updateExpression)
                            .conditionExpression(conditionalExpression)
                            .expressionAttributeNames(expressionAttributeNames)
                            .expressionAttributeValues(expressionAttributeValues).build();

                    this.dynamoDB.updateItem(updateItemRequest);
                }
            } catch (final ConditionalCheckFailedException conditionalCheckFailedException) {
                logger.debug("Someone else acquired the lock before you asked to release it", conditionalCheckFailedException);
                return false;
            } catch (final SdkClientException sdkClientException) {
                if (bestEffort) {
                    logger.warn("Ignore SdkClientException and continue to clean up", sdkClientException);
                } else {
                    throw sdkClientException;
                }
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
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, this.partitionKeyName);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(PK_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(key).build());
        final QueryRequest queryRequest = QueryRequest.builder()
            .tableName(this.tableName)
            .keyConditionExpression(QUERY_PK_EXPRESSION)
            .expressionAttributeNames(expressionAttributeNames)
            .expressionAttributeValues(expressionAttributeValues)
            .build();
        final AmazonDynamoDBLockItemQueryIterator
            iterator = new AmazonDynamoDBLockItemQueryIterator(
            this.dynamoDB, queryRequest, item -> getLockItem(key, deleteOnRelease, item));

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
            final String conditionalExpression;
            final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(lockItem.getRecordVersionNumber()).build());
            expressionAttributeValues.put(OWNER_NAME_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(lockItem.getOwnerName()).build());
            final Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
            expressionAttributeNames.put(LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION);
            expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
            expressionAttributeNames.put(OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME);
            if (this.sortKeyName.isPresent()) {
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
            } else {
                conditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
            }

            final String recordVersionNumber = this.generateRecordVersionNumber();

            //Set up update expression for UpdateItem.
            final String updateExpression;
            expressionAttributeValues.put(NEW_RVN_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(recordVersionNumber).build());
            expressionAttributeValues.put(LEASE_DURATION_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(String.valueOf(leaseDurationToEnsureInMilliseconds)).build());
            if (deleteData) {
                expressionAttributeNames.put(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                updateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_REMOVE_DATA;
            } else if (options.getData().isPresent()) {
                expressionAttributeNames.put(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                expressionAttributeValues.put(DATA_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().b(SdkBytes.fromByteBuffer(options.getData().get())).build());
                updateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_DATA;
            } else {
                updateExpression = UPDATE_LEASE_DURATION_AND_RVN;
            }

            final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(getItemKeys(lockItem))
                    .conditionExpression(conditionalExpression)
                    .updateExpression(updateExpression)
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues).build();

            try {
                final long lastUpdateOfLock = LockClientUtils.INSTANCE.millisecondTime();
                this.dynamoDB.updateItem(updateItemRequest);
                lockItem.updateRecordVersionNumber(recordVersionNumber, lastUpdateOfLock, leaseDurationToEnsureInMilliseconds);
                if (deleteData) {
                    lockItem.updateData(null);
                } else if (options.getData().isPresent()) {
                    lockItem.updateData(options.getData().get());
                }
            } catch (final ConditionalCheckFailedException conditionalCheckFailedException) {
                logger.debug("Someone else acquired the lock, so we will stop heartbeating it", conditionalCheckFailedException);
                this.locks.remove(lockItem.getUniqueIdentifier());
                throw new LockNotGrantedException("Someone else acquired the lock, so we will stop heartbeating it", conditionalCheckFailedException);
            } catch (AwsServiceException awsServiceException) {
                if (holdLockOnServiceUnavailable
                        && awsServiceException.awsErrorDetails().sdkHttpResponse().statusCode() == HttpStatusCode.SERVICE_UNAVAILABLE) {
                    // When DynamoDB service is unavailable, other threads may get the same exception and no thread may have the lock.
                    // For systems which should always hold a lock on an item and it is okay for multiple threads to hold the lock,
                    // the lookUpTime of local state can be updated to make it believe that it still has the lock.
                    logger.info("DynamoDB Service Unavailable. Holding the lock.");
                    lockItem.updateLookUpTime(LockClientUtils.INSTANCE.millisecondTime());
                } else {
                    throw awsServiceException;
                }
            }
        }
    }

    @Override
    protected List<String> getKeys() {
        List<String> keys = new ArrayList<String>();
        keys.add(this.partitionKeyName);
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
        return this.getLockFromDynamoDB(lockOptions);
    }

    @Override
    public Stream<LockItem> getAllLocksFromDB(boolean deleteOnRelease) {
        return this.getAllLocksFromDynamoDB(deleteOnRelease);
    }
}
