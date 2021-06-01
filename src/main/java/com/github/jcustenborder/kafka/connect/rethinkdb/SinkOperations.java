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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Delete;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class SinkOperations {
  private static final Logger log = LoggerFactory.getLogger(SinkOperations.class);

  Multimap<String, SinkOperation> operations = LinkedListMultimap.create();
  Map<String, SinkOperation> lastOperation = new LinkedHashMap<>();


  public SinkOperation insert(String tableName, TableState table) {
    final BiFunction<String, SinkOperation, SinkOperation> compute = (t, existing) -> {
      InsertOperation ins = new InsertOperation(tableName, table);
      operations.put(tableName, ins);
      return ins;
    };

    SinkOperation operation = this.lastOperation.get(tableName);
    if (null == operation || Type.Insert != operation.type) {
      operation = this.lastOperation.compute(tableName, compute);
    }
    return operation;
  }

  public SinkOperation delete(String tableName, TableState table) {
    final BiFunction<String, SinkOperation, SinkOperation> compute = (t, existing) -> {
      DeleteOperation ins = new DeleteOperation(tableName, table);
      operations.put(tableName, ins);
      return ins;
    };

    SinkOperation operation = this.lastOperation.get(tableName);
    if (null == operation || Type.Delete != operation.type) {
      operation = this.lastOperation.compute(tableName, compute);
    }
    return operation;
  }

  public void execute(Connection connection) {
    for (String tableName : this.operations.keySet()) {
      Collection<SinkOperation> sinkOperations = this.operations.get(tableName);
      for (SinkOperation sinkOperation : sinkOperations) {
        sinkOperation.execute(connection);
      }
    }
  }

  enum Type {
    Insert,
    Delete
  }

  static abstract class SinkOperation {
    final Type type;
    final String tableName;
    final TableState tableState;


    SinkOperation(Type type, String tableName, TableState tableState) {
      this.type = type;
      this.tableName = tableName;
      this.tableState = tableState;
    }

    public abstract void insert(MapObject<String, Object> value);

    public abstract void delete(Object value);

    public abstract void execute(Connection connection);
  }

  static class InsertOperation extends SinkOperation {
    final List<MapObject<String, Object>> values = new ArrayList<>(10000);

    InsertOperation(String tableName, TableState table) {
      super(Type.Insert, tableName, table);
    }

    @Override
    public void insert(MapObject<String, Object> value) {
      this.values.add(value);
    }

    @Override
    public void delete(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Connection connection) {
      Insert insert = this.tableState.table.insert(this.values)
          .optArg("conflict", "replace");
      log.trace("execute() - Processing insert:\n{}", insert);
      insert.runNoReply(connection);
    }
  }

  static class DeleteOperation extends SinkOperation {
    final List<Object> values = new ArrayList<>(10000);

    DeleteOperation(String tableName, TableState table) {
      super(Type.Delete, tableName, table);
    }

    @Override
    public void insert(MapObject<String, Object> value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Object value) {
      this.values.add(value);
    }

    @Override
    public void execute(Connection connection) {
      Object[] keys = this.values.toArray();
      Delete delete = this.tableState.table.getAll(keys)
          .delete();
      log.trace("execute() delete:\n{}", delete);
      Result<Object> result = delete.run(connection);
      log.trace("execute() - result = {}", result);
    }
  }
}
