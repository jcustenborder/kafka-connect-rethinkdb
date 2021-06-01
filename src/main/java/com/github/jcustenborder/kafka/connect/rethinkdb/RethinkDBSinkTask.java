/**
 * Copyright © 2020 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.rethinkdb;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.ast.GetAll;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.ast.TableCreate;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RethinkDBSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(RethinkDBSinkTask.class);
  RethinkDBSinkConnectorConfig config;

  @Override
  public String version() {
    return null;
  }

  static String topicPartitionKey(TopicPartition topicPartition) {
    return String.format("%s:%s", topicPartition.topic(), topicPartition.partition());
  }

  Connection connection;
  Table offsetsTable;
  Db database;

  @Override
  public void start(Map<String, String> settings) {
    this.config = new RethinkDBSinkConnectorConfig(settings);
    this.connection = this.config.connection();
    this.database = RethinkDB.r.db(this.config.database);
    this.offsetsTable = this.database.table(this.config.offsetsTable);
    Set<TopicPartition> assignment = this.context.assignment();
    Object[] offsetKeys = assignment.stream()
        .map(RethinkDBSinkTask::topicPartitionKey)
        .toArray(Object[]::new);
    log.trace("start() - Querying '{}' for offsets '{}'", this.config.offsetsTable, offsetKeys);
    GetAll getAllRequest = offsetsTable.getAll(offsetKeys);
    Result<MapObject> offsetResults = getAllRequest.run(connection, MapObject.class);
    Map<TopicPartition, Long> previousOffsets = offsetResults
        .stream()
        .collect(Collectors.toMap(
            o -> {
              String topic = (String) o.get("topic");
              Long partition = (Long) o.get("partition");
              return new TopicPartition(topic, partition.intValue());
            },
            o -> (Long) o.get("offset")
        ));
    log.info("start() - Requesting previous offsets {}", previousOffsets);
    this.context.offset(previousOffsets);
  }

  Map<String, TableState> tableCache = new HashMap<>();

  MapObjectConverter converter = new MapObjectConverter();

  @Override
  public void put(Collection<SinkRecord> records) {
    if (null == records || records.isEmpty()) {
      log.debug("put() - record(s) are empty.");
      return;
    }
    log.debug("put() - Processing {} record(s). Connection isOpen = {}", records.size(), this.connection.isOpen());
    if (!this.connection.isOpen()) {
      this.connection = this.connection.reconnect();
    }


    SinkOperations operations = new SinkOperations();

    for (SinkRecord record : records) {
      MapObject value;

      if (null == record.value()) {
        value = null;
      } else {
        value = this.converter.convert(record.value());
      }

      final TableState tableState = tableCache.computeIfAbsent(record.topic(), tableName -> {
        log.trace("put() - Setting up table '{}'", tableName);
        Result<List> tableResult = database.tableList().run(connection, List.class);
        List<String> tables = tableResult.first();
        if (this.config.tableCreationEnabled) {
          if (!tables.contains(tableName)) {
            TableCreate tableCreate = database.tableCreate(tableName);
            log.trace("put() - Creating table '{}':\n{}", tableName, tableCreate);
            Result<MapObject> tableCreateResult = tableCreate.run(connection, MapObject.class);
            MapObject result = tableCreateResult.first();
            log.info("put() - Created table '{}':\n{}", tableName, result);
          }
        }
        Table result = database.table(tableName);
        Result<Map> info = result.info().run(connection, Map.class);
        Map<String, Object> r = info.first();
        return TableState.of(result, r);
      });
      SinkOperations.SinkOperation operation;
      if (value == null) {
        operation = operations.delete(record.topic(), tableState);
        operation.delete(record.key());
      } else {
        operation = operations.insert(record.topic(), tableState);
        operation.insert(value);
      }
    }

    operations.execute(this.connection);
  }

  static void flush(Connection connection, Table offsetsTable, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    List<MapObject<Object, Object>> offsets =
        currentOffsets.entrySet()
            .stream()
            .map(e -> new MapObject<>()
                .with("id", topicPartitionKey(e.getKey()))
                .with("topic", e.getKey().topic())
                .with("partition", e.getKey().partition())
                .with("offset", e.getValue().offset())
            )
            .collect(Collectors.toList());
    Insert insert = offsetsTable.insert(offsets)
        .optArg("conflict", "replace");
    log.trace("flush() - Insert: \n{}", insert);
    insert.run(connection);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    flush(this.connection, this.offsetsTable, currentOffsets);
  }

  @Override
  public void stop() {
    if (null != this.connection) {
      this.connection.close();
    }
  }
}
