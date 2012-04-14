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
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.TrevniRuntimeException;
import org.apache.trevni.ValueType;

public class AvroDissectedColumnator {
  private Schema schema;
  private List<DissectedColumnMetaData> dissectedColumns = new ArrayList<DissectedColumnMetaData>();
  
  public AvroDissectedColumnator(Schema schema) {
    this.schema = schema;
    columnize(new ColumnPath(), schema);
  }

  public List<DissectedColumnMetaData> getDissectedColumns() {
    return dissectedColumns;
  }
  
  public ColumnMetaData[] getColumns() {
    ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>(dissectedColumns.size());
    for (DissectedColumnMetaData dissectedColumn : dissectedColumns) {
      columns.add(dissectedColumn.getColumn());
    }
    return columns.toArray(new ColumnMetaData[columns.size()]);
  }
  
  private void columnize(ColumnPath path, Schema s) {
    if (isSimple(s)) {
      addDissectedColumn(path, ValueType.STRING);
      return;
    }
    
    switch (s.getType()) {
    case MAP: 
      throw new TrevniRuntimeException("Maps not supported: " + s);
    case RECORD:
      for (Field field : s.getFields()) {
        if (field.schema().getType() == Schema.Type.ARRAY) { 
          path.add(new ColumnPathElement(field.name(), false, true));
        } else if (field.schema().getType() == Schema.Type.UNION) {
          path.add(new ColumnPathElement(field.name(), true, false));
        } else {
          path.add(new ColumnPathElement(field.name(), false, false));
        }
        columnize(path, field.schema());
        path.remove(path.size() - 1);
      }
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
      throw new TrevniRuntimeException("Unknown schema: " + s);
    }
  }
  
  private DissectedColumnMetaData addDissectedColumn(ColumnPath path, ValueType type) {
    ColumnMetaData column = new ColumnMetaData(path.get(path.size() - 1).toString(), type);
    DissectedColumnMetaData dissectedColumn = new DissectedColumnMetaData(column, (ColumnPath)path.clone());
    dissectedColumns.add(dissectedColumn);
    return dissectedColumn;
  }

  static boolean isSimple(Schema s) {
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
  
  class DissectedColumnMetaData {    
    private ColumnMetaData column;
    private ColumnPath path;
    
    public DissectedColumnMetaData(ColumnMetaData column, ColumnPath path) {
      this.column = column;
      this.path = path;
    }
    
    public ColumnMetaData getColumn() {
      return column;
    }
    
    public ColumnPath getPath() {
      return path;
    }
  }
  
  class ColumnPath extends ArrayList<ColumnPathElement> {
    public int getRepSize() {
      int repSize = 0;
      for (ColumnPathElement e : this) {
        if (e.isRepeated) {
          repSize++;
        }       
      }
      return repSize;
    }
    
    public ArrayList<ColumnPathElement> getRepPath() {
      ArrayList<ColumnPathElement> repPath = new ArrayList<ColumnPathElement>();
      for (ColumnPathElement e : this) {
        if (e.isRepeated) {
          repPath.add(e);
        }       
      }
      return repPath;
    }
  }
  
  class ColumnPathElement {
    private String fieldName;
    private boolean isOptional;
    private boolean isRepeated;
    
    public ColumnPathElement(String fieldName, boolean isOptional, boolean isRepeated) {
      this.fieldName = fieldName;
      this.isOptional= isOptional;
      this.isRepeated = isRepeated;
      assert this.isOptional = !this.isRepeated;
    }
    
    public String getFieldName() {
      return this.fieldName;
    }
    
    public boolean isOptional() {
      return this.isOptional;
    }    
    
    public boolean isRepeated() {
      return this.isRepeated;
    }
    
    @Override
    public String toString() {
      return this.getFieldName();
    }
  }  
}    
