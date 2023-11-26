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

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Unit tests for LockItemPaginatedScanIterator.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class LockItemPaginatedScanIteratorTest {
    @Mock
    DynamoDbClient dynamodb;
    @Mock
    AmazonDynamoDBLockItemFactory factory;

    @Test(expected = UnsupportedOperationException.class)
    public void remove_throwsUnsupportedOperationException() {
        AmazonDynamoDBLockItemScanIterator sut = new AmazonDynamoDBLockItemScanIterator(dynamodb, ScanRequest.builder().build(), factory);
        sut.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void next_whenDoesNotHaveNext_throwsNoSuchElementException() {
        ScanRequest request = ScanRequest.builder().build();
        AmazonDynamoDBLockItemScanIterator sut = new AmazonDynamoDBLockItemScanIterator(dynamodb, request, factory);
        List<Map<String, AttributeValue>> list1 = new ArrayList<>();
        list1.add(new HashMap<>());
        when(dynamodb.scan(ArgumentMatchers.<ScanRequest>any()))
            .thenReturn(ScanResponse.builder().items(list1).count(1).lastEvaluatedKey(new HashMap<>()).build())
            .thenReturn(ScanResponse.builder().items(Collections.emptyList()).count(0).build());
        sut.next();
        sut.next();
    }
}
