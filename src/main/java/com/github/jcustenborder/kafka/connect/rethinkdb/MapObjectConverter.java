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

import com.github.jcustenborder.kafka.connect.utils.data.AbstractConverter;
import com.rethinkdb.model.MapObject;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

class MapObjectConverter extends AbstractConverter<MapObject> {
  @Override
  protected MapObject newValue() {
    return new MapObject();
  }

  @Override
  protected void setStringField(MapObject mapObject, String fieldName, String value) {
    mapObject.put(fieldName, value);
  }

  @Override
  protected void setFloat32Field(MapObject mapObject, String fieldName, Float value) {
    mapObject.put(fieldName, value);
  }

  @Override
  protected void setFloat64Field(MapObject mapObject, String fieldName, Double value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setTimestampField(MapObject mapObject, String fieldName, Date value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setDateField(MapObject mapObject, String fieldName, Date value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setTimeField(MapObject mapObject, String fieldName, Date value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setInt8Field(MapObject mapObject, String fieldName, Byte value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setInt16Field(MapObject mapObject, String fieldName, Short value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setInt32Field(MapObject mapObject, String fieldName, Integer value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setInt64Field(MapObject mapObject, String fieldName, Long value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setBytesField(MapObject mapObject, String fieldName, byte[] value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setDecimalField(MapObject mapObject, String fieldName, BigDecimal value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setBooleanField(MapObject mapObject, String fieldName, Boolean value) {
    mapObject.put(fieldName, value);

  }

  @Override
  protected void setStructField(MapObject mapObject, String fieldName, Struct struct) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void setArray(MapObject mapObject, String fieldName, Schema schema, List value) {
    mapObject.put(fieldName, value);
  }

  @Override
  protected void setMap(MapObject mapObject, String fieldName, Schema schema, Map value) {

  }

  @Override
  protected void setNullField(MapObject mapObject, String s) {

  }
}
