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

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ValueType;
import org.apache.trevni.TrevniRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/** Utility that computes the column layout of a schema. */
public class AvroDissectedColumnator {
  private Schema schema;
  private List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
  private List<FSMTransition> transitions = new ArrayList<FSMTransition>();
  
  public AvroDissectedColumnator(Schema schema) {
    this.schema = schema;
    columnize(new RepPathList(), schema);
    fsmize();
  }

  /** Return columns for the schema. */
  public ColumnMetaData[] getColumns() {
    return columns.toArray(new ColumnMetaData[columns.size()]);
  }
  
  public List<FSMTransition> getTransitions() {
    return this.transitions;
  }

  private int getCommonRepLevel(int colIndex1, int colIndex2) {
    int commonRepLevel = 0;
    
    String repPath1 = this.columns.get(colIndex1).getString("repPath");
    String repPath2 = this.columns.get(colIndex2).getString("repPath");
    Iterator<String> colPath1 = Splitter.on(".").split(repPath1).iterator();
    Iterator<String> colPath2 = Splitter.on(".").split(repPath2).iterator();
    while (colPath1.hasNext()) {
      String nextCol = colPath1.next();
      if (!nextCol.equals("") && colPath2.hasNext() && nextCol.equals(colPath2.next())) {
        commonRepLevel++;
      } else {
        break;
      }
    }
    return commonRepLevel; 
  }
  
  public void fsmize() { 
    ListIterator<ColumnMetaData> colIterator = this.columns.listIterator();
    while (colIterator.hasNext()) {
      ColumnMetaData col = colIterator.next();
      int colIndex = this.columns.indexOf(col);
      
      // maxLevel
      int maxLevel = (int)col.getLong("repLevel");
      HashMap<Integer, FSMTransition> transitions = new HashMap<Integer, FSMTransition>();
      
      // barrier, barrierLevel
      // default values (used for last column)
      int barrierLevel = 0; // I think?
      String barrierName = "END";
      if (colIterator.nextIndex() < this.columns.size()) {
        ColumnMetaData barrier = this.columns.get(colIterator.nextIndex());
        barrierName = barrier.getName();
        barrierLevel = this.getCommonRepLevel(colIndex, colIndex + 1);
      }
      
      // transitions
      for (ColumnMetaData preCol : this.columns.subList(0, this.columns.indexOf(col) + 1)) {
        if (preCol.getLong("repLevel") > barrierLevel) {
          int preColIndex = this.columns.indexOf(preCol); 
          int backLevel = this.getCommonRepLevel(colIndex, preColIndex);
          if (backLevel > 0 && !transitions.containsKey(backLevel)) {            
            transitions.put(backLevel, new FSMTransition(col.getName(), preCol.getName(), backLevel));            
          }
        }
      }
      for (int i = barrierLevel + 1; i <= maxLevel; i++) {
        if (!transitions.containsKey(i)) {
          transitions.put(i, transitions.get(i - 1));
        }
      }      
      for (int i = 0; i <= barrierLevel; i++) {
        if (!transitions.containsKey(i)) {
          transitions.put(i, new FSMTransition(col.getName(), barrierName, i));
        }
      }
      
      // Add the transitions to the FSM
      for (FSMTransition transition : transitions.values()) {
        this.transitions.add(transition);
      }
    }
  }
  
  private void columnize(RepPathList path, Schema s) {
    if (isSimple(s)) {
      addColumn(path, ValueType.STRING);
      return;
    }
    
    switch (s.getType()) {
    case MAP: 
      throw new TrevniRuntimeException("Maps not supported: " + s);
    case RECORD:
      for (Field field : s.getFields()) {
        if (field.schema().getType() == Schema.Type.ARRAY) { 
          path.add(new RepPathElement(field.name(), true));
        } else {
          path.add(new RepPathElement(field.name(), false));
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
  
  private ColumnMetaData addColumn(RepPathList path, ValueType type) {
    ColumnMetaData column = new ColumnMetaData(Joiner.on("").join(path), type);
    column.set("repLevel", (long)path.getRepSize());
    column.set("repPath", Joiner.on(".").join(path.getRepPath()));
    columns.add(column);
    return column;
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
  
  private class RepPathElement {
    private String fieldName;
    private boolean isRepeated;
    
    public RepPathElement(String fieldName, boolean isRepeated) {
      this.fieldName = fieldName;
      this.isRepeated = isRepeated;
    }
    
    public String getFieldName() {
      return this.fieldName;
    }
    
    public boolean isRepeated() {
      return this.isRepeated;
    }
    
    @Override
    public String toString() {
      return this.getFieldName();
    }
  }
  
  private class RepPathList extends ArrayList<RepPathElement> {
    public int getRepSize() {
      int repSize = 0;
      for (RepPathElement e : this) {
        if (e.isRepeated) {
          repSize++;
        }       
      }
      return repSize;
    }
    
    public ArrayList<RepPathElement> getRepPath() {
      ArrayList<RepPathElement> repPath = new ArrayList<RepPathElement>();
      for (RepPathElement e : this) {
        if (e.isRepeated) {
          repPath.add(e);
        }       
      }
      return repPath;
    }
  }
  
  private class FSMTransition {
    private String start;
    private String end;
    private int repLevel;
    
    public FSMTransition(String start, String end, int repLevel) {
      this.start = start;
      this.end = end;
      this.repLevel = repLevel;
    }
    
    public String toString() {
      return start + "(" + repLevel + ")->" + end;
    }
  }
}    
