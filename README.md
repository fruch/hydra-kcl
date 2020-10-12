# hydra-kcl
code base on this example:
https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/Streams.KCLAdapter.Walkthrough.html
the can duplicate at run time a table being populated by ycsb


### example of how to run it
```
docker run -d -p 8080:8080 -p 9042:9042 scylladb/scylla-nightly:666.development-0.20201004.5b5b8b3264 --alternator-port 8080 --alternator-write-isolation always --experimental 1
# wait for scylla to be ready

# run the KCL example
./gradlew run --args='-e http://localhost:8080 -t usertable -k 10000

# after ~30sec when tables created run ycsb
python2.7 ./bin/ycsb load dynamodb  -P workloads/workloada -P dynamodb.properties -p recordcount=10000 -p insertorder=uniform -p insertcount=10000 -p fieldcount=2 -p fieldlength=50 -s

# when you want to clear it out and try again
cqlsh> DROP KEYSPACE "alternator_streams-adapter-demo"; DROP KEYSPACE "alternator_usertable"; DROP KEYSPACE "alternator_usertable-dest";
```

### TODOs

* [ ] - dockerize it for usage in SCT
