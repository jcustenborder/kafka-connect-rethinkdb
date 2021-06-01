package com.github.jcustenborder.kafka.connect.rethinkdb;

import com.github.jcustenborder.docker.junit5.Compose;
import org.junit.jupiter.api.BeforeEach;

@Compose(dockerComposePath = "src/test/resources/docker/anonymous/docker-compose.yml")
public class AnonymousRethinkDBSinkConnectorIT extends AbstractRethinkDBSinkConnectorIT<RethinkDBSinkConnector> {
  @Override
  protected RethinkDBSinkConnector connector() {
    return new RethinkDBSinkConnector();
  }

  @BeforeEach
  public void before() {

  }
}
