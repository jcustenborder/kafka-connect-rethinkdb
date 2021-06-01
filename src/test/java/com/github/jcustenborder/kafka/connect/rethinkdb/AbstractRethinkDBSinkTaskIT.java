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

import com.github.jcustenborder.docker.junit5.Port;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.ast.DbCreate;
import com.rethinkdb.gen.ast.GetAll;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.ast.TableCreate;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.github.jcustenborder.kafka.connect.utils.SinkRecordHelper.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public abstract class AbstractRethinkDBSinkTaskIT {
  private static final Logger log = LoggerFactory.getLogger(AbstractRethinkDBSinkTaskIT.class);

  protected final String username;
  protected final String password;

  protected RethinkDBSinkTask task;
  protected SinkTaskContext context = mock(SinkTaskContext.class);
  protected String databaseName;
  protected Connection connection;
  protected Db database;
  protected Table offsetsTable;
  protected Map<String, String> settings;

  protected AbstractRethinkDBSinkTaskIT() {
    this(null, null);
  }

  protected AbstractRethinkDBSinkTaskIT(String username, String password) {
    this.username = username;
    this.password = password;
  }


  @AfterEach
  public void after() {
    this.connection.close();
    this.task.stop();
  }

  protected Map<String, String> settings(Map<String, String> settings) {
    return settings;
  }

  @BeforeEach
  public void before(@Port(container = "rethink", internalPort = 28015) InetSocketAddress socketAddress) {
    this.task = new RethinkDBSinkTask();
    this.task.initialize(context);
    this.databaseName = this.getClass().getSimpleName();

    RethinkDB rethinkDB = new RethinkDB();
    this.connection = rethinkDB.connection()
        .hostname(socketAddress.getHostString())
        .port(socketAddress.getPort())
        .connect();
    Result<Object> dblist = rethinkDB.dbList().run(connection);
    List<String> databases = (List<String>) dblist.first();
    this.database = rethinkDB.db(this.databaseName);
    if (!databases.contains(this.databaseName)) {
      DbCreate create = rethinkDB.dbCreate(this.databaseName);
      create.run(connection);
    }
    if (null != this.username) {
      this.database.grant(this.username,
          rethinkDB.hashMap()
              .with("read", true)
              .with("write", true)
              .with("config", true)
      ).run(this.connection);
    }

    Result<List> tableList = this.database.tableList().run(this.connection, List.class);
    List<String> tables = tableList.first();
    if (!tables.contains(RethinkDBSinkConnectorConfig.OFFSETS_TABLE_DEFAULT)) {
      TableCreate tableCreate = database.tableCreate(RethinkDBSinkConnectorConfig.OFFSETS_TABLE_DEFAULT);
      tableCreate.run(connection);
    }
    this.offsetsTable = this.database.table(RethinkDBSinkConnectorConfig.OFFSETS_TABLE_DEFAULT);

    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(RethinkDBConnectorConfig.CONNECTION_HOSTNAME_CONF, socketAddress.getHostString());
    settings.put(RethinkDBConnectorConfig.CONNECTION_PORT_CONF, Integer.toString(socketAddress.getPort()));
    settings.put(RethinkDBConnectorConfig.DATABASE_CONF, this.databaseName);
    if (null != this.username) {
      settings.put(RethinkDBConnectorConfig.USERNAME_CONF, this.username);
      settings.put(RethinkDBConnectorConfig.PASSWORD_CONF, this.password);
    }
    this.settings = settings(settings);
  }

  @Test
  public void existingOffsets(@Port(container = "rethink", internalPort = 28015) InetSocketAddress socketAddress) {
    final String topic = "existingOffsets";
    log.info("address = {}", socketAddress);
    Map<TopicPartition, Long> expectedOffsets = ImmutableMap.of(
        new TopicPartition(topic, 1), 123452L,
        new TopicPartition(topic, 2), 5421234L
    );
    when(context.assignment()).thenReturn(expectedOffsets.keySet());

    MapObject<Object, Object>[] offsets = expectedOffsets.entrySet()
        .stream()
        .map(p -> new MapObject<>()
            .with("id", RethinkDBSinkTask.topicPartitionKey(p.getKey()))
            .with("topic", p.getKey().topic())
            .with("partition", p.getKey().partition())
            .with("offset", p.getValue())
        )
        .toArray(MapObject[]::new);

    this.offsetsTable.insert(offsets).run(this.connection);
    log.info("offsets = {}", offsets);

    this.task.start(this.settings);

    verify(this.context, times(1)).offset(expectedOffsets);
  }

  @Test
  public void doWrite(@Port(container = "rethink", internalPort = 28015) InetSocketAddress socketAddress) {
    this.task.start(this.settings);
    String topic = "doWrite";
    Schema valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .build();
    final int count = 100;
    List<SinkRecord> records = new ArrayList<>(count);
    Multimap<String, Object> expected = LinkedListMultimap.create();

    for (int i = 0; i < count; i++) {
      SinkRecord record = write(
          topic,
          Schema.INT32_SCHEMA,
          i,
          valueSchema,
          new Struct(valueSchema)
              .put("id", i)
              .put("firstName", "example " + i)
              .put("lastName", "user " + i)
      );
      records.add(record);
      expected.put(record.topic(), record.key());
    }
    this.task.put(records);

    expected.asMap().forEach((tableName, keys) -> {
      TableState state = this.task.tableCache.get(tableName);
      assertNotNull(state, "state should not be null.");
      Object[] keyArray = keys.toArray();
      GetAll getAllQuery = state.table.getAll(keyArray);
      log.trace("doWrite() - Query:\n{}", getAllQuery);
      Result<MapObject> results = getAllQuery.run(this.connection, Result.FetchMode.AGGRESSIVE, MapObject.class);
      log.trace("doWrite() - bufferedCount = {}", results.bufferedCount());
      for (MapObject<String, Object> result : results) {
        Long id = (Long) result.get("id");
        Optional<SinkRecord> record = records
            .stream()
            .filter(r -> tableName.equals(r.topic()) && r.key().equals(id.intValue()))
            .findFirst();
        log.trace("doWrite() - Checking for id '{}'", id);
        assertTrue(
            record.isPresent(),
            String.format(
                "Record with id %s was not found",
                id
            )
        );
      }
    });


    this.task.flush(
        ImmutableMap.of(
            new TopicPartition(topic, 1), new OffsetAndMetadata(12341L),
            new TopicPartition(topic, 2), new OffsetAndMetadata(12341L),
            new TopicPartition(topic, 3), new OffsetAndMetadata(12341L)
        )
    );
  }

  @Test
  public void writeAndDelete(@Port(container = "rethink", internalPort = 28015) InetSocketAddress socketAddress) {
    this.task.start(this.settings);
    String topic = "writeAndDelete";
    Schema valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .build();
    final int count = 100;
    List<SinkRecord> records = new ArrayList<>(count);
    Multimap<String, Object> expected = LinkedListMultimap.create();

    for (long i = 0; i < count; i++) {
      SinkRecord record = write(
          topic,
          Schema.INT64_SCHEMA,
          i,
          valueSchema,
          new Struct(valueSchema)
              .put("id", i)
              .put("firstName", "example " + i)
              .put("lastName", "user " + i)
      );
      records.add(record);
      expected.put(record.topic(), record.key());
    }
    this.task.put(records);

    expected.asMap().forEach((tableName, keys) -> {
      TableState state = this.task.tableCache.get(tableName);
      assertNotNull(state, "state should not be null.");
      Object[] keyArray = keys.toArray();
      GetAll getAllQuery = state.table.getAll(keyArray);
      log.trace("writeAndDelete() - Query:\n{}", getAllQuery);
      Result<MapObject> results = getAllQuery.run(this.connection, Result.FetchMode.AGGRESSIVE, MapObject.class);
      log.trace("writeAndDelete() - bufferedCount = {}", results.bufferedCount());
      for (MapObject<String, Object> result : results) {
        Long id = (Long) result.get("id");
        Optional<SinkRecord> record = records
            .stream()
            .filter(r -> tableName.equals(r.topic()) && r.key().equals(id))
            .findFirst();
        log.trace("doWrite() - Checking for id '{}'", id);
        assertTrue(
            record.isPresent(),
            String.format(
                "Record with id %s was not found",
                id
            )
        );
      }
    });

    Set<Object> deletes = new LinkedHashSet<>();
    Random random = new SecureRandom();
    while (deletes.size() < 5) {
      int index = random.nextInt(records.size());
      SinkRecord record = records.get(index);
      deletes.add(record.key());
    }

    AtomicLong offset = new AtomicLong(13241);
    List<SinkRecord> deleteRecords =
        deletes.stream()
            .map(i -> new SinkRecord(
                topic,
                0,
                Schema.INT64_SCHEMA,
                i,
                null,
                null,
                offset.incrementAndGet()
            ))
            .collect(Collectors.toList());

    this.task.put(deleteRecords);

    Object[] missingIDs = deletes.toArray();
    TableState tableState = this.task.tableCache.get(topic);
    assertNotNull(tableState, "tableState should not be null.");
    GetAll missingGetAll = tableState.table.getAll(missingIDs);
    log.trace("writeAndDelete() - Query:\n{}", missingGetAll);
    Result<Object> getAllResults = missingGetAll.run(this.connection);
    assertTrue(getAllResults.bufferedCount() == 0);

    flushTask(topic, 1, 12341L, topic, 2, 12341L, topic, 3, 12341L);

    this.task.flush(
        ImmutableMap.of(
            new TopicPartition(topic, 1), new OffsetAndMetadata(12341L),
            new TopicPartition(topic, 2), new OffsetAndMetadata(12341L),
            new TopicPartition(topic, 3), new OffsetAndMetadata(12341L)
        )
    );
  }

  private void flushTask(
      String topic1, int partition1, long offset1,
      String topic2, int partition2, long offset2,
      String topic3, int partition3, long offset3) {
    Map<TopicPartition, OffsetAndMetadata> flushedOffsets = new LinkedHashMap<>();
    flushedOffsets.put(
        new TopicPartition(topic1, partition1), new OffsetAndMetadata(offset1)
    );
    flushedOffsets.put(
        new TopicPartition(topic2, partition2), new OffsetAndMetadata(offset2)
    );
    flushedOffsets.put(
        new TopicPartition(topic3, partition3), new OffsetAndMetadata(offset3)
    );
    this.task.flush(flushedOffsets);

    flushedOffsets.forEach((expectedKey, expectedValue) -> {
      String topicPartitionKey = RethinkDBSinkTask.topicPartitionKey(expectedKey);
      Result<MapObject> result = this.task.offsetsTable.get(topicPartitionKey).run(this.task.connection, MapObject.class);
      MapObject actual = result.first();
      assertNotNull(actual, String.format("Should have found a result for %s", topicPartitionKey));
      assertEquals(expectedKey.topic(), actual.get("topic"), String.format("topic does not match. key:%s", topicPartitionKey));
      assertEquals((long) expectedKey.partition(), (long) actual.get("partition"), String.format("partition does not match. key:%s", topicPartitionKey));
      assertEquals((long) expectedValue.offset(), (long) actual.get("offset"), String.format("offset does not match. key:%s", topicPartitionKey));
    });
  }
}
