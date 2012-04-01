/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni.avro;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;

import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ValueType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;

public class AvroDissector {

  private Schema schema;
  private GenericData data;

  private List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();

  public AvroDissector(Schema schema, GenericData data) {
    this.schema = schema;
    this.data = data;
    columnize(null, schema);
  }

  public ColumnMetaData[] getColumns() {
    return columns.toArray(new ColumnMetaData[columns.size()]);
  }

  private Map<Schema,Schema> seen = new IdentityHashMap<Schema,Schema>();

  private String p(String... strings) {
    return Joiner.on(".").skipNulls().join(strings);
  }

  private void columnize(String path, Schema s) {
    if (isSimple(s)) {
        addColumn(path, ValueType.STRING);
      return;
    }

    if (seen.containsKey(s))                      // catch recursion
      throw new RuntimeException("Cannot shred recursive schemas: " + s);
    seen.put(s, s);
    
    switch (s.getType()) {
      case MAP: 
        throw new RuntimeException("Can't shred maps yet: " + s);
      case RECORD:
        for (Field field : s.getFields())
            columnize(p(path, field.name()), field.schema());
        break;
      case ARRAY: 
        columnize(path, s.getElementType());
        break;
      case UNION:
        for (Schema branch : s.getTypes()) {
          if (branch.getType() != Schema.Type.NULL) {
            columnize(path, branch);
          }
        }
        break;
      default:
        throw new RuntimeException("Unknown schema: " + s);
    }
  }

  private ColumnMetaData addColumn(String columnName, ValueType type) {
    ColumnMetaData column = new ColumnMetaData(columnName, type);
    columns.add(column);
    return column;
  }

  private boolean isSimple(Schema s) {
    switch (s.getType()) {
    case NULL:
    case INT: case LONG:
    case FLOAT: case DOUBLE: 
    case BYTES: case STRING: 
    case ENUM: case FIXED:
      return true;
    default:
      return false;
    }
  }

  private int countColumns(Schema s) throws Exception {
    if (isSimple(s)) {
      return 1;
    }
    switch (s.getType()) {
    case RECORD:
      int columnsInRecord = 0;
      for (Field f : s.getFields())
        columnsInRecord += countColumns(f.schema());
      return columnsInRecord;
    case ARRAY: 
      return countColumns(s.getElementType());
    case UNION:
      List<Schema> branches = s.getTypes();
      for (Schema branch : branches) {
        if (branch.getType() != Schema.Type.NULL) {
          return countColumns(branch);
        }
      }
    default:
      throw new RuntimeException("Unknown schema: " + s);
    }
  }
  
  public void dissect(Object value, ColumnFileWriter writer) throws Exception {
    writer.startRow();
    int count = dissect(value, schema, 0, 0, 0, 0, writer);
    assert(count == columns.size());
    writer.endRow();
  }

  private int dissect(Object o, Schema s, int column, int r, int rDepth, int d,
                    ColumnFileWriter writer) throws Exception {
    if (isSimple(s)) {
      // 1. (S, Rq)
      writer.writeValue(Joiner.on(",").join(o, r, d), column);
      return column + 1;
    }
    switch (s.getType()) {
    case MAP: 
      throw new RuntimeException("Can't dissect maps: " + s);
    case RECORD:
      // 2. (M, Rq)
      for (Field f : s.getFields())
        column = dissect(data.getField(o, f.name(), f.pos()), f.schema(), column, r, rDepth, d, writer);
      return column;
    case ARRAY: 
      Collection elements = (Collection)o;
      // 3. (S, Rp)
      if (isSimple(s.getElementType())) {
        if (elements.size() == 0) {
            writer.writeValue(Joiner.on(",").join("NULL", r, d), column);
        } else {
          d += 1;
          rDepth += 1;
          Iterator<Object> iter = elements.iterator();
          writer.writeValue(Joiner.on(",").join(iter.next(), r, d), column);
          while (iter.hasNext()) {
            writer.writeValue(Joiner.on(",").join(iter.next(), rDepth, d), column);
          }
        }
        return column + 1;
      }
      // 4. (M, Rp)
      if (elements.size() == 0) {
        int c = countColumns(s.getElementType());
        for (int i = 1; i <= c; i++) {
          writer.writeValue(Joiner.on(",").join("NULL", r, d), column + (i - 1));
        }
        return column + c;
      } else {
        d += 1;
        rDepth += 1;
        Iterator<Object> iter = elements.iterator();
        int c = dissect(iter.next(), s.getElementType(), column, r, rDepth, d, writer);
        while (iter.hasNext()) {
          dissect(iter.next(), s.getElementType(), column, rDepth, rDepth, d, writer);
        }
        return c;
      }
    case UNION:
      int b = data.resolveUnion(s, o);
      List<Schema> branches = s.getTypes();
      Schema chosenBranch = branches.get(b);
      Schema notChosenBranch = branches.get(1 - b); // only two branches
      if (chosenBranch.getType() != Schema.Type.NULL) {
        if (isSimple(chosenBranch)) {
          // 5a. (S, O)
          writer.writeValue(Joiner.on(",").join(o, r, d + 1), column);
          return column + 1;
        } else {
          // 6a. (M, O)
          return dissect(o, chosenBranch, column, r, rDepth, d + 1, writer);
        }
      } else {
        if (isSimple(notChosenBranch)) {
          // 5b. (S, O)
          writer.writeValue(Joiner.on(",").join("NULL", r, d), column);
          return column + 1;
        } else {
          // 6b. (M, O)
          int c = countColumns(notChosenBranch);
          for (int i = 1; i <= c; i++) {
            writer.writeValue(Joiner.on(",").join("NULL", r, d), column + (i - 1));
          }
          return column + c;
        }
      }
    default:
      throw new RuntimeException("Unknown schema: " + s);
    }
  }
}    
