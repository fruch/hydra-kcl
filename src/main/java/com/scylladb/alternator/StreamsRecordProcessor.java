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

import static com.scylladb.alternator.StreamsAdapterDemoHelper.deleteItem;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.putItem;

import java.nio.charset.Charset;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class StreamsRecordProcessor implements IRecordProcessor {
    private final static Logger LOGGER = LoggerFactory.getLogger("StreamsAdapterDemo");

    private Integer checkpointCounter;

    private final AmazonDynamoDB dynamoDBClient;
    private final String tableName;

    public StreamsRecordProcessor(AmazonDynamoDB dynamoDBClient, String tableName) {
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
    }

    @Override
    public void initialize(String shardId) {
        checkpointCounter = 0;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOGGER.debug("got " + records.size() + " records to process");
        for (Record record : records) {
            if (record instanceof RecordAdapter) {
                com.amazonaws.services.dynamodbv2.model.Record streamRecord = ((RecordAdapter) record)
                        .getInternalObject();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{}: {} - {}", streamRecord.getEventName(), streamRecord.getDynamodb().getKeys(),
                            new String(record.getData().array(), Charset.forName("UTF-8")));
                } else {
                    LOGGER.debug("{}: {}", streamRecord.getEventName(), streamRecord.getDynamodb().getKeys());
                }
                switch (streamRecord.getEventName()) {
                case "INSERT":
                case "MODIFY":
                    putItem(dynamoDBClient, tableName, streamRecord.getDynamodb().getNewImage());
                    break;
                case "REMOVE":
                    // deleteItem(dynamoDBClient, tableName, streamRecord.getDynamodb().getKeys().get("p").getS());
                }
            }
            ++checkpointCounter;
            if (checkpointCounter % 10 == 0) {
                try {
                    checkpointer.checkpoint();
                } catch (Exception e) {
                    LOGGER.warn("checkpoint", e);
                }
            }
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        if (reason == ShutdownReason.TERMINATE) {
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                LOGGER.warn("shutdown", e);
            }
        }
    }

}