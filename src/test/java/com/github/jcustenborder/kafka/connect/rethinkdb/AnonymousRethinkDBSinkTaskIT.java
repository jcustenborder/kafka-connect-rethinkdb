package com.github.jcustenborder.kafka.connect.rethinkdb;

import com.github.jcustenborder.docker.junit5.Compose;

@Compose(dockerComposePath = "src/test/resources/docker/anonymous/docker-compose.yml")
public class AnonymousRethinkDBSinkTaskIT extends AbstractRethinkDBSinkTaskIT {
  public AnonymousRethinkDBSinkTaskIT() {
    super();
  }
}
