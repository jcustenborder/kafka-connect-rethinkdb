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

import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;

import java.util.ArrayList;
import java.util.List;

public class DatabaseOperations {
  enum OperationType {
    None,
    Write,
    Delete
  }

  static abstract class Operation {
    public final OperationType operationType;
    protected final List<MapObject<String, Object>> operations;

    Operation(OperationType operationType) {
      this.operationType = operationType;
      this.operations = new ArrayList<>();
    }

    public abstract void execute(Table table);
  }

  static class WriteOperation extends Operation {
    WriteOperation() {
      super(OperationType.Write);
    }

    @Override
    public void execute(Table table) {
      //TODO: Comeback and setup an upsert
      table.insert(this.operations);
    }
  }

  static class DeleteOperation extends Operation {
    DeleteOperation() {
      super(OperationType.Delete);
    }

    @Override
    public void execute(Table table) {

    }
  }

  static class NoneOperation extends Operation {
    public static final Operation INSTANCE = new NoneOperation();

    private NoneOperation() {
      super(OperationType.None);
    }

    @Override
    public void execute(Table table) {

    }
  }
}
