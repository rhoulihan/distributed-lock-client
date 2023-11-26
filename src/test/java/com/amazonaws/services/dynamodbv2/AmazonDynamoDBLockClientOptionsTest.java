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

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

/**
 * Unit tests for AmazonDynamoDBLockClientOptions.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AmazonDynamoDBLockClientTest.class, AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder.class})
public class AmazonDynamoDBLockClientOptionsTest {
    DynamoDbClient dynamodb = PowerMockito.mock(DynamoDbClient.class);

    @Test
    public void testBuilder_whenGetLocalHostThrowsUnknownHostException_uuidCreateRandomIsCalled() throws UnknownHostException, InterruptedException {
        final UUID uuid = AmazonDynamoDBLockClientTest.setOwnerNameToUuid();
        AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder builder = AmazonDynamoDBLockClientOptions.builder(dynamodb, "table")
            .withLeaseDuration(2L)
            .withHeartbeatPeriod(1L)
            .withTimeUnit(TimeUnit.SECONDS)
            .withPartitionKeyName("customer");
        System.out.println(builder.toString());
        //verifyStatic();

        AmazonDynamoDBLockClientOptions options = builder.build();
        AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(options);
        Map<String, AttributeValue> previousLockItem = new HashMap<>(3);
        previousLockItem.put("ownerName", AttributeValue.builder().s("foobar").build());
        previousLockItem.put("recordVersionNumber", AttributeValue.builder().s("oolala").build());
        previousLockItem.put("leaseDuration", AttributeValue.builder().s("1").build());
        when(dynamodb.getItem(Matchers.<GetItemRequest>any())).thenReturn(GetItemResponse.builder().item(previousLockItem).build());
        LockItem lock = client.acquireLock(AcquireLockOptions.builder("asdf").build());
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            assertEquals(uuid.toString(), lock.getOwnerName());
        }
    }
}
