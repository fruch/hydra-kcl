/**
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/

package com.scylladb.alternator;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class StreamsAdapterDemoHelper {

    /**
     * @return StreamArn
     */
    public static String createTable(AmazonDynamoDB client, String tableName, boolean enableStream) {
        java.util.List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("p").withAttributeType("S"));

        java.util.List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("p").withKeyType(KeyType.HASH)); // Partition
                                                                                                // key

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(2L)
                .withWriteCapacityUnits(2L);

        StreamSpecification streamSpecification = new StreamSpecification();
        if (enableStream) {
            streamSpecification.setStreamEnabled(true);
            streamSpecification.setStreamViewType(StreamViewType.NEW_IMAGE);
        }
        else {
            streamSpecification.setStreamEnabled(false);
        }
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions).withKeySchema(keySchema)
                .withProvisionedThroughput(provisionedThroughput).withStreamSpecification(streamSpecification);

        try {
            System.out.println("Creating table " + tableName);
            CreateTableResult result = client.createTable(createTableRequest);
            return result.getTableDescription().getLatestStreamArn();
        } catch (ResourceInUseException e) {
            System.out.println("Table already exists.");
            return describeTable(client, tableName).getTable().getLatestStreamArn();
        }
    }

    public static DescribeTableResult describeTable(AmazonDynamoDB client, String tableName) {
        return client.describeTable(new DescribeTableRequest().withTableName(tableName));
    }

    public static ScanResult scanTable(AmazonDynamoDB dynamoDBClient, String tableName) {
        ScanResult r = dynamoDBClient.scan(new ScanRequest().withTableName(tableName));
        while (r.getLastEvaluatedKey() != null && !r.getLastEvaluatedKey().isEmpty()) {
            ScanResult next = dynamoDBClient
                    .scan(new ScanRequest().withTableName(tableName).withExclusiveStartKey(r.getLastEvaluatedKey()));
            if (next.getCount() == 0) {
                break;
            }
            r = r.withCount(r.getCount() + next.getCount())
                    .withItems(concat(r.getItems().stream(), next.getItems().stream()).collect(toList()))
                    .withConsumedCapacity(next.getConsumedCapacity()).withLastEvaluatedKey(next.getLastEvaluatedKey());
        }
        return r;
    }

    public static void putItem(AmazonDynamoDB dynamoDBClient, String tableName, String id, String val) {
        dynamoDBClient.putItem(putItem(tableName, items(id, val)));
    }

    public static void putItem(AmazonDynamoDB client, String tableName, int recordNo) {
        client.putItem(putItem(tableName, items(recordNo)));
    }

    public static Map<String, AttributeValue> items(String id, String val) {
        java.util.Map<String, AttributeValue> items = new HashMap<String, AttributeValue>();
        items.put("p", new AttributeValue().withS(id));
        items.put("attribute-1", new AttributeValue().withS(val));
        return items;
    }

    public static Map<String, AttributeValue> items(int recordNo) {
        return items(String.valueOf(recordNo), "le gris " + recordNo);
    }

    public static void putItems(AmazonDynamoDB client, String tableName, int from, int to) {
        while (from < to) {
            List<WriteRequest> items = new ArrayList<>(100);

            for (int e = Math.min(to, from + 100); from < e; ++from) {
                putItem(items, items(from));
            }
            batchWrite(client, tableName, items);
        }
    }

    public static void batchWrite(AmazonDynamoDB client, String tableName, List<WriteRequest> items) {
        BatchWriteItemRequest r = new BatchWriteItemRequest()
                .withRequestItems(Collections.singletonMap(tableName, items));
        client.batchWriteItem(r);
    }

    public static void putItem(List<WriteRequest> dst, Map<String, AttributeValue> items) {
        dst.add(new WriteRequest(new PutRequest(items)));
    }

    public static void deleteItem(List<WriteRequest> dst, Map<String, AttributeValue> keys) {
        dst.add(new WriteRequest(new DeleteRequest(keys)));
    }

    public static void putItem(AmazonDynamoDB dynamoDBClient, String tableName, Map<String, AttributeValue> items) {
        dynamoDBClient.putItem(putItem(tableName, items));
    }

    public static PutItemRequest putItem(String tableName, Map<String, AttributeValue> items) {
        return new PutItemRequest().withTableName(tableName).withItem(items);
    }

    public static void updateItem(AmazonDynamoDB dynamoDBClient, String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("p", new AttributeValue().withN(id));

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate().withAction(AttributeAction.PUT)
                .withValue(new AttributeValue().withS(val));
        attributeUpdates.put("attribute-2", update);

        UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName).withKey(key)
                .withAttributeUpdates(attributeUpdates);
        dynamoDBClient.updateItem(updateItemRequest);
    }

    public static void deleteItem(AmazonDynamoDB dynamoDBClient, String tableName, Map<String, AttributeValue> atts) {
        deleteItem(dynamoDBClient, tableName, atts.get("p"));
    }

    public static void deleteItem(AmazonDynamoDB dynamoDBClient, String tableName, AttributeValue keyValue) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("p", keyValue);

        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(tableName).withKey(key);
        dynamoDBClient.deleteItem(deleteItemRequest);
    }

    public static void deleteItem(AmazonDynamoDB dynamoDBClient, String tableName, String id) {
        deleteItem(dynamoDBClient, tableName, new AttributeValue().withS(id));
    }

}
