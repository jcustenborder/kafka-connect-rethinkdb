# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-rethinkdb) | [Download from the Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-rethinkdb)

This project provides a plugin for integration with RethinkDB.

# Installation

## Confluent Hub

The following command can be used to install the plugin directly from the Confluent Hub using the
[Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).

```bash
confluent-hub install jcustenborder/kafka-connect-rethinkdb:latest
```

## Manually

The zip file that is deployed to the [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-rethinkdb) is available under
`target/components/packages/`. You can manually extract this zip file which includes all dependencies. All the dependencies
that are required to deploy the plugin are under `target/kafka-connect-target` as well. Make sure that you include all the dependencies that are required
to run the plugin.

1. Create a directory under the `plugin.path` on your Connect worker.
2. Copy all of the dependencies under the newly created subdirectory.
3. Restart the Connect worker.




# Sink Connectors
## [RethinkDB Sink Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-rethinkdb/sinks/RethinkDBSinkConnector.html)

```
com.github.jcustenborder.kafka.connect.rethinkdb.RethinkDBSinkConnector
```

This connector is used to persist data from Kafka to RethinkDB.



# Development

## Building the source

```bash
mvn clean package
```

## Contributions

Contributions are always welcomed! Before you start any development please create an issue and
start a discussion. Create a pull request against your newly created issue and we're happy to see
if we can merge your pull request. First and foremost any time you're adding code to the code base
you need to include test coverage. Make sure that you run `mvn clean package` before submitting your
pull to ensure that all of the tests, checkstyle rules, and the package can be successfully built.