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
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

class RethinkDBConnectorConfig extends AbstractConfig {
  public static final String GROUP_CONNECTION = "Connection";
  public static final String GROUP_DATABASE = "Database";
  public static final String CONNECTION_HOSTNAME_CONF = "rethinkdb.connection.hostname";
  public static final String CONNECTION_PORT_CONF = "rethinkdb.connection.port";
  public static final String DATABASE_CONF = "rethinkdb.database";
  private static final String CONNECTION_HOSTNAME_DOC = "rethinkdb.connection.hostname";
  private static final String CONNECTION_PORT_DOC = "rethinkdb.connection.port";
  private static final String DATABASE_DOC = "rethinkdb.database";
  public final String hostname;
  public final int port;
  public final String database;
  public RethinkDBConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
    this.hostname = getString(CONNECTION_HOSTNAME_CONF);
    this.port = getInt(CONNECTION_PORT_CONF);
    this.database = getString(DATABASE_CONF);
  }

  public static ConfigDef config() {

    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(GROUP_CONNECTION, CONNECTION_HOSTNAME_CONF, Type.STRING)
                .documentation(CONNECTION_HOSTNAME_DOC)
                .importance(Importance.HIGH)
                .build()
        ).define(
            ConfigKeyBuilder.of(GROUP_CONNECTION, CONNECTION_PORT_CONF, Type.INT)
                .documentation(CONNECTION_PORT_DOC)
                .importance(Importance.HIGH)
                .defaultValue(28015)
                .build()
        ).define(
            ConfigKeyBuilder.of(GROUP_DATABASE, DATABASE_CONF, Type.STRING)
                .documentation(DATABASE_DOC)
                .importance(Importance.HIGH)
                .build()
        );
  }

  public Connection connection() {
    return RethinkDB.r.connection()
        .hostname(this.hostname)
        .port(this.port)
        .connect();
  }
}
