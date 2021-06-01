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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

class RethinkDBSinkConnectorConfig extends RethinkDBConnectorConfig {


  public static final String OFFSETS_TABLE_CONF = "offsets.table";
  static final String OFFSETS_TABLE_DOC = "The table within the database that will be used to store the offsets for each batch of data written.";
  public static final String OFFSETS_TABLE_DEFAULT = "_connect_offsets";

  public static final String TABLE_CREATE_ENABLED_CONF = "table.creation.enabled";
  static final String TABLE_CREATE_ENABLED_DOC = "Flag to determine if the table should be created if it does not exist.";

  public static final String DATABASE_CREATION_ENABLED_CONF = "database.creation.enabled";
  static final String DATABASE_CREATION_ENABLED_DOC = "Flag to determine if the database should be created if it does not exist.";

  public final String offsetsTable;
  public final boolean tableCreationEnabled;
  public final boolean databaseCreationEnabled;


  public RethinkDBSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.offsetsTable = getString(OFFSETS_TABLE_CONF);
    this.tableCreationEnabled = getBoolean(TABLE_CREATE_ENABLED_CONF);
    this.databaseCreationEnabled = getBoolean(DATABASE_CREATION_ENABLED_CONF);
  }

  public static ConfigDef config() {
    return RethinkDBConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(OFFSETS_TABLE_CONF, Type.STRING)
                .documentation(OFFSETS_TABLE_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue(OFFSETS_TABLE_DEFAULT)
                .build()
        ).define(
            ConfigKeyBuilder.of(TABLE_CREATE_ENABLED_CONF, Type.BOOLEAN)
                .documentation(TABLE_CREATE_ENABLED_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(true)
                .build()
        ).define(
            ConfigKeyBuilder.of(DATABASE_CREATION_ENABLED_CONF, Type.BOOLEAN)
                .documentation(DATABASE_CREATION_ENABLED_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(false)
                .build()
        );
  }
}
