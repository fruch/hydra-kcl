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


package com.scylladb.java;

import java.nio.charset.Charset;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			String data = new String(record.getData().array(), Charset.forName("UTF-8"));
			if (record instanceof RecordAdapter) {
				com.amazonaws.services.dynamodbv2.model.Record streamRecord = ((RecordAdapter) record)
						.getInternalObject();
                LOGGER.debug(streamRecord.getEventName() + ": " + streamRecord.getDynamodb().getKeys());
				switch (streamRecord.getEventName()) {
				case "INSERT":
				case "MODIFY":
					StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName,
							streamRecord.getDynamodb().getNewImage());
					break;
				case "REMOVE":
					StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName,
							streamRecord.getDynamodb().getKeys().get("p").getS());
				}
			}
			checkpointCounter += 1;
			if (checkpointCounter % 10 == 0) {
				try {
					checkpointer.checkpoint();
				} catch (Exception e) {
					e.printStackTrace();
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
				e.printStackTrace();
			}
		}
	}

}