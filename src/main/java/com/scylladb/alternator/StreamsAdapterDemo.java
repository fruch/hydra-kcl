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

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream.TRIM_HORIZON;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.createTable;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.describeTable;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.putItems;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.scanTable;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import java.net.URI;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class StreamsAdapterDemo {
    private final static Logger LOGGER = LoggerFactory.getLogger("StreamsAdapterDemo");

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newFor("StreamAdapterDemo").build().defaultHelp(true)
                .description("Replicate a simple table using DynamoDB/Alternator Streams.");

        parser.addArgument("--aws").action(storeTrue()).help("Run against AWS");
        parser.addArgument("-c", "--cloudwatch").setDefault(false).help("Enable Cloudwatch");
        parser.addArgument("-e", "--endpoint").setDefault(new URL("http://localhost:8000"))
                .help("DynamoDB/Alternator endpoint");
        parser.addArgument("-se", "--streams-endpoint")
                .help("DynamoDB/Alternator streams endpoint");

        parser.addArgument("-u", "--user").setDefault("none").help("Credentials username");
        parser.addArgument("-p", "--password").setDefault("none").help("Credentials password");
        parser.addArgument("-r", "--region").setDefault("us-east-1").help("AWS region");
        parser.addArgument("-t", "--table-prefix").setDefault("KCL-Demo").help("Demo table name prefix");

        parser.addArgument("-k", "--key-number").type(Integer.class).setDefault(0)
                .help("number of key in the src table");

        parser.addArgument("--timeout").type(Integer.class).setDefault(0)
                .help("number of key in the src table");

        parser.addArgument("--create").action(storeTrue()).help("Create source data set if not available");
        parser.addArgument("--threads").type(Integer.class).setDefault(Runtime.getRuntime().availableProcessors() * 2)
                .help("Max worker threads");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        String tablePrefix = ns.getString("table_prefix");
        int keyNumber = ns.getInt("key_number");
        int timeoutInSeconds =  ns.getInt("timeout");
        int threads = ns.getInt("threads");
        boolean create_data = ns.getBoolean("create");
        AmazonDynamoDBClientBuilder b = AmazonDynamoDBClientBuilder.standard().withRegion(ns.getString("region"));
        AmazonDynamoDBStreamsClientBuilder sb = AmazonDynamoDBStreamsClientBuilder.standard().withRegion(ns.getString("region"));
        AmazonCloudWatch cloudWatchClient = null;

        if (!ns.getBoolean("aws")) {
            if (ns.getString("endpoint") != null) {
                AlternatorRequestHandler handler = new AlternatorRequestHandler(URI.create(ns.getString("endpoint")));
                b.withRequestHandlers(handler);
                sb.withRequestHandlers(handler);
            }
            if (ns.getString("streams_endpoint") != null) {
                AlternatorRequestHandler handler = new AlternatorRequestHandler(URI.create(ns.getString("streams_endpoint")));
                sb.withRequestHandlers(handler);
            }
            if (ns.getString("user") != null) {
                b.withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(ns.getString("user"), ns.getString("password"))));
                sb.withCredentials(b.getCredentials());
            }
        }

        LOGGER.info("Starting demo...");

        String srcTable = tablePrefix;
        String destTable = tablePrefix + "-dest";

        IRecordProcessorFactory recordProcessorFactory = new StreamsRecordProcessorFactory(b, destTable);

        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(sb.build());
        AmazonDynamoDB dynamoDBClient = b.build();

        if (ns.getBoolean("cloudwatch")) {
            cloudWatchClient = AmazonCloudWatchClientBuilder.standard().withCredentials(b.getCredentials())
                    .withClientConfiguration(b.getClientConfiguration()).build();
        }

        ExecutorService xs = Executors.newWorkStealingPool(threads);

        try {
            String streamArn = setUpTables(dynamoDBClient, tablePrefix);

            KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration("streams-adapter-demo",
                    streamArn, b.getCredentials(), "streams-demo-worker").withParentShardPollIntervalMillis(1000)
                            .withCleanupLeasesUponShardCompletion(true).withFailoverTimeMillis(240000)
                            .withRetryGetRecordsInSeconds(10).withInitialPositionInStream(TRIM_HORIZON)
                            .withIdleTimeBetweenReadsInMillis(1).withIdleMillisBetweenCalls(1)
                            .withShardSyncIntervalMillis(20000);

            LOGGER.info("Creating worker for stream: " + streamArn);
            Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(workerConfig)
                    .kinesisClient(adapterClient).dynamoDBClient(dynamoDBClient).cloudWatchClient(cloudWatchClient)
                    .execService(xs).build();

            LOGGER.info("Starting worker...");
            Thread t = new Thread(worker);
            t.start();

            if (keyNumber != 0 || create_data) {
                ScanResult sr = scanTable(dynamoDBClient, srcTable);
                ScanResult dr = null;

                if (sr.getCount() < keyNumber && create_data) {
                    LOGGER.info("Adding {} records to source table...", sr.getCount() - keyNumber);
                    putItems(dynamoDBClient, tablePrefix, sr.getCount(), keyNumber);
                }

                for (;;) {
                    Thread.sleep(10000);

                    sr = scanTable(dynamoDBClient, srcTable);
                    LOGGER.info("Checking for source data...({}/{}): ", sr.getCount(), keyNumber);

                    dr = scanTable(dynamoDBClient, destTable);
                    LOGGER.info("keys synced: {}/{}", dr.getCount(), keyNumber);
                    if (!dr.getCount().equals(keyNumber)) {
                        continue;
                    }
                    if (dr.getCount() >= keyNumber) {
                        break;
                    }
                }

                if (create_data) {
                    if (dr != null && sr.getItems().equals(dr.getItems())) {
                        LOGGER.info("Scan result is equal.");
                    } else {
                        LOGGER.error("Tables are different!");
                    }
                }
            }

            if (timeoutInSeconds != 0) {
                LOGGER.info("Sleeping for " + timeoutInSeconds + "sec");
                Thread.sleep(timeoutInSeconds * 1000);

            }
            LOGGER.info("Shutting down Worker");
            worker.shutdown();
            t.join();

            LOGGER.info("Done.");
        } finally {
            cleanup(dynamoDBClient, tablePrefix);
        }
    }

    private static String setUpTables(AmazonDynamoDB dynamoDBClient, String tablePrefix) throws TimeoutException {
        String srcTable = tablePrefix;
        String destTable = tablePrefix + "-dest";
        String streamArn = createTable(dynamoDBClient, srcTable, true);
        createTable(dynamoDBClient, destTable, false);

        awaitTableCreation(dynamoDBClient, srcTable);
        awaitTableCreation(dynamoDBClient, destTable);

        return streamArn;
    }

    private static void awaitTableCreation(AmazonDynamoDB dynamoDBClient, String tableName) throws TimeoutException {
        Integer retries = 0;
        Boolean created = false;
        while (!created && retries < 100) {
            DescribeTableResult result = describeTable(dynamoDBClient, tableName);
            created = result.getTable().getTableStatus().equals("ACTIVE");
            if (created) {
                LOGGER.info("Table is active.");
                return;
            } else {
                retries++;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
        throw new TimeoutException("Timeout after table creation.");
    }

    private static void cleanup(AmazonDynamoDB dynamoDBClient, String tablePrefix) {
        /*
         * String srcTable = tablePrefix + "-src"; String destTable =
         * tablePrefix + "-dest"; dynamoDBClient.deleteTable(new
         * DeleteTableRequest().withTableName(srcTable));
         * dynamoDBClient.deleteTable(new
         * DeleteTableRequest().withTableName(destTable));
         */
    }
}
