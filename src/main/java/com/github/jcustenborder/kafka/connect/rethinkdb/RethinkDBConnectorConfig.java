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
  public static final String USERNAME_CONF = "rethinkdb.username";
  public static final String PASSWORD_CONF = "rethinkdb.password";


  private static final String CONNECTION_HOSTNAME_DOC = "The hostname to connect to.";
  private static final String CONNECTION_PORT_DOC = "The port on the RethinkDB host to connect to.";
  private static final String DATABASE_DOC = "The database to use.";
  private static final String USERNAME_DOC = "The username to connect to the RethinkDB host with.";
  private static final String PASSWORD_DOC = "The password to connect to the RethinkDB host with.";


  public final String hostname;
  public final int port;
  public final String database;
  public final String username;
  public final String password;

  public RethinkDBConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
    this.hostname = getString(CONNECTION_HOSTNAME_CONF);
    this.port = getInt(CONNECTION_PORT_CONF);
    this.database = getString(DATABASE_CONF);
    String username = getString(USERNAME_CONF);
    this.username = (null != username && !username.isEmpty()) ? username : null;
    if (null != this.username) {
      this.password = getPassword(PASSWORD_CONF).value();
    } else {
      this.password = null;
    }
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
        ).define(
            ConfigKeyBuilder.of(GROUP_CONNECTION, USERNAME_CONF, Type.STRING)
                .documentation(USERNAME_DOC)
                .importance(Importance.HIGH)
                .defaultValue("")
                .build()
        ).define(
            ConfigKeyBuilder.of(GROUP_CONNECTION, PASSWORD_CONF, Type.PASSWORD)
                .documentation(PASSWORD_DOC)
                .importance(Importance.HIGH)
                .defaultValue("")
                .build()
        );
  }

  public Connection connection() {
    Connection.Builder builder = RethinkDB.r.connection()
        .hostname(this.hostname)
        .port(this.port);

    if (null != this.username) {
      builder = builder.user(this.username, this.password);
    }


    return builder.connect();
  }
}
