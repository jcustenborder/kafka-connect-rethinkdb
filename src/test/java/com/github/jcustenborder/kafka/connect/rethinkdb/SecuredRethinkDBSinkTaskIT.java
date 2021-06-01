package com.github.jcustenborder.kafka.connect.rethinkdb;

import com.github.jcustenborder.docker.junit5.Compose;
import com.github.jcustenborder.docker.junit5.Port;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import org.junit.jupiter.api.BeforeAll;

import java.net.InetSocketAddress;
import java.util.Map;

@Compose(dockerComposePath = "src/test/resources/docker/anonymous/docker-compose.yml")
public class SecuredRethinkDBSinkTaskIT extends AbstractRethinkDBSinkTaskIT {

  public SecuredRethinkDBSinkTaskIT() {
    super("connect", "asdnfadsfas");
  }

  @BeforeAll
  public static void beforeAll(@Port(container = "rethink", internalPort = 28015) InetSocketAddress socketAddress) {
    RethinkDB rethinkDB = new RethinkDB();
    Connection connection = rethinkDB.connection()
        .hostname(socketAddress.getHostString())
        .port(socketAddress.getPort())
        .connect();
    rethinkDB.db("rethinkdb").table("users").insert(
        rethinkDB.hashMap()
            .with("id", "connect")
            .with("password", "asdnfadsfas")
    ).run(connection);
  }

  @Override
  protected Map<String, String> settings(Map<String, String> settings) {
    settings.put(RethinkDBConnectorConfig.USERNAME_CONF, "connect");
    settings.put(RethinkDBConnectorConfig.PASSWORD_CONF, "asdnfadsfas");
    return super.settings(settings);
  }
}
