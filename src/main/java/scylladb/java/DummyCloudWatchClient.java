package com.scylladb.java;

import com.amazonaws.services.cloudwatch.AbstractAmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;

public class DummyCloudWatchClient extends AbstractAmazonCloudWatch {

    public PutMetricDataResult putMetricData(PutMetricDataRequest request) {
        // System.out.println("request: " + request.toString());

        return new PutMetricDataResult();
    }
}