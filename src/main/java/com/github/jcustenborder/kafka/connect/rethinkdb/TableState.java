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

import java.util.Map;

class TableState {
  public final String primaryKey;
  public final Table table;

  private TableState(String primaryKey, Table table) {
    this.primaryKey = primaryKey;
    this.table = table;
  }

  public static TableState of(Table table, Map<String, Object> map) {
    String primaryKey = (String) map.get("primary_key");
    return of(table, primaryKey);
  }

  public static TableState of(Table table, String primaryKey) {
    return new TableState(primaryKey, table);
  }
}
