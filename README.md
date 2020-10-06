# hydra-kcl
code base on this example:
https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/Streams.KCLAdapter.Walkthrough.html
the can duplicate at run time a table being populated by ycsb


### example of how to run it
```
docker run -d -p 8080:8080 -p 9042:9042 scylladb/scylla-nightly:666.development-0.20201004.5b5b8b3264 --alternator-port 8080 --alternator-write-isolation always --experimental 1
# wait for scylla to be ready

# run the KCL example
./gradlew run

# after ~30sec when tables created run ycsb
python2.7 ./bin/ycsb load dynamodb  -P workloads/workloada -P dynamodb.properties
```

### TODOs

* [ ] - dockerize it for usage in SCT
* [ ] - add paramter that can be passed via gradle commandline, like table names and key configurtion, timeout and such. 

