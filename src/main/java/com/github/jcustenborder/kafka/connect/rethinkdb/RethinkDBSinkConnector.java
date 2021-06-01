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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.TaskConfigs;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.DbCreate;
import com.rethinkdb.gen.ast.DbList;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class RethinkDBSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(RethinkDBSinkConnector.class);
  Map<String, String> settings;

  @Override
  public void start(Map<String, String> settings) {
    RethinkDBSinkConnectorConfig config = new RethinkDBSinkConnectorConfig(settings);


    try (Connection connection = config.connection()) {
      DbList dbListQuery = RethinkDB.r.dbList();
      log.trace("start() - Querying RethinkDB for list of databases.\n{}", dbListQuery);
      try (Result<List> dbListResult = dbListQuery.run(connection, List.class)) {
        List<String> databases = dbListResult.first();
        log.info("start() - Checking that database '{}' exists.", config.database);
        if (!databases.contains(config.database)) {
          if (config.databaseCreationEnabled) {
            DbCreate create = RethinkDB.r.dbCreate(config.database);
            log.info("start() - Creating database '{}'\n{}", config.database, create);
            create.runNoReply(connection);
          } else {
            throw new ConfigException(
                RethinkDBSinkConnectorConfig.DATABASE_CONF,
                config.database,
                "Database does not exist and `" + RethinkDBSinkConnectorConfig.DATABASE_CREATION_ENABLED_CONF + "` is set to false."
            );
          }
        }
      }
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RethinkDBSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return TaskConfigs.multiple(this.settings, i);
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return RethinkDBSinkConnectorConfig.config();
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
