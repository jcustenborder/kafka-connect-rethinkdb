package com.github.jcustenborder.kafka.connect.rethinkdb;

import org.apache.kafka.connect.sink.SinkConnector;

public abstract class AbstractRethinkDBSinkConnectorIT<C extends SinkConnector> {
  protected abstract C connector();


}
