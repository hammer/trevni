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

import static org.apache.trevni.avro.AvroColumnator.isSimple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Stack;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.Input;
import org.apache.trevni.InputFile;
import org.apache.trevni.TrevniRuntimeException;
import org.apache.trevni.avro.AvroDissectedColumnator.ColumnPath;
import org.apache.trevni.avro.AvroDissectedColumnator.ColumnPathElement;
import org.apache.trevni.avro.AvroDissectedColumnator.DissectedColumnMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuent.tungsten.commons.patterns.fsm.Action;
import com.continuent.tungsten.commons.patterns.fsm.Entity;
import com.continuent.tungsten.commons.patterns.fsm.EntityAdapter;
import com.continuent.tungsten.commons.patterns.fsm.Event;
import com.continuent.tungsten.commons.patterns.fsm.EventTypeGuard;
import com.continuent.tungsten.commons.patterns.fsm.Guard;
import com.continuent.tungsten.commons.patterns.fsm.RegexGuard;
import com.continuent.tungsten.commons.patterns.fsm.State;
import com.continuent.tungsten.commons.patterns.fsm.StateChangeListener;
import com.continuent.tungsten.commons.patterns.fsm.StateMachine;
import com.continuent.tungsten.commons.patterns.fsm.StateTransitionMap;
import com.continuent.tungsten.commons.patterns.fsm.StateType;
import com.continuent.tungsten.commons.patterns.fsm.Transition;
import com.continuent.tungsten.commons.patterns.fsm.TransitionRollbackException;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/** Read files written with {@link AvroColumnWriter}.  A subset of the schema
 * used for writing may be specified when reading.  In this case only columns
 * of the subset schema are read. */
public class AvroDissectedColumnReader<D>
  implements Iterator<D>, Iterable<D>, Closeable, StateChangeListener {

  private ColumnFileReader reader;
  private GenericData model;
  private Schema fileSchema;
  private Schema readSchema;
  
  private ColumnValues[] values;
  private int currentColumnValuesIndex;
  private List<DissectedColumnMetaData> readColumnMetaData;
 
  // Monitoring and management
  private static Logger logger = LoggerFactory.getLogger(AvroDissectedColumnReader.class);
  
  // State machine
  private StateMachine sm = null;  

  /** Parameters for reading an Avro column file. */
  public static class Params {
    Input input;
    Schema schema;
    GenericData model = GenericData.get();

    /** Construct reading from a file. */
    public Params(File file) throws IOException {
      this(new InputFile(file));
    }

    /** Construct reading from input. */
    public Params(Input input) { this.input = input; }

    /** Set subset schema to project data down to. */
    public Params setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /** Set data representation. */
    public Params setModel(GenericData model) {
      this.model = model;
      return this;
    }
  }

  /** Construct a reader for a file. */
  public AvroDissectedColumnReader(Params params) throws Exception {
    this.reader = new ColumnFileReader(params.input);
    this.model = params.model;
    Schema.Parser p = new Schema.Parser();
    this.fileSchema = p.parse(reader.getMetaData().getString(AvroColumnWriter.SCHEMA_KEY));
    this.readSchema = params.schema == null ? fileSchema : params.schema;
    initialize();
  }

  /** Return the schema for data in this file. */
  public Schema getFileSchema() { return fileSchema; }

  void initialize() throws Exception {
    // compute a mapping from column name to number for file
    Map<String,Integer> fileColumnNumbers = new HashMap<String,Integer>();
    int i = 0;
    for (ColumnMetaData c : new AvroDissectedColumnator(fileSchema).getColumns()) {
      fileColumnNumbers.put(c.getName(), i++);
    }

    // create iterator for each column in readSchema
    AvroDissectedColumnator readColumnator = new AvroDissectedColumnator(readSchema);
    ColumnMetaData[] readColumns = readColumnator.getColumns();
    values = new ColumnValues[readColumns.length];
    int j = 0;    
    for (ColumnMetaData c : readColumns) {
      Integer n = fileColumnNumbers.get(c.getName());
      if (n == null)
        throw new TrevniRuntimeException("No column named: "+c.getName());
      values[j++] = reader.getValues(n);
    }
    
    // create state machine for record assembly
    readColumnMetaData = readColumnator.getDissectedColumns();  
    StateTransitionMap stmap = new StateTransitionMap();
    
    // State for between rows
    State rowReady = new State("ROWREADY", StateType.START);
    stmap.addState(rowReady);
    
    // State for each column    
    ArrayList<State> colStates = new ArrayList<State>();
    for (int k = 0; k < readColumnMetaData.size(); k++) {
      State colState = new State(String.valueOf(k), StateType.ACTIVE);
      colStates.add(colState);
      stmap.addState(colState);
    }
    
    // End state
    State end = new State("END", StateType.END);
    stmap.addState(end);

    // Define guards
    Guard newRowGuard = new EventTypeGuard(NewRowEvent.class);
    Guard stopGuard = new EventTypeGuard(StopEvent.class);    

    // Define actions
    Action logAction = new LogAction();
    Action nullAction = new NullAction(); 
    
    // Define transitions
    stmap.addTransition(new Transition("ROWREADY-TO-0", newRowGuard, rowReady, nullAction, colStates.get(0)));
    stmap.addTransition(new Transition("ROWREADY-TO-END", stopGuard, rowReady, nullAction, end));        
    
    ListIterator<DissectedColumnMetaData> colIterator = readColumnMetaData.listIterator();
    while (colIterator.hasNext()) {
      DissectedColumnMetaData col = colIterator.next();
      int colIndex = readColumnMetaData.indexOf(col);
      
      // maxLevel
      int maxLevel = col.getPath().getRepSize();
      HashMap<Integer, Transition> transitions = new HashMap<Integer, Transition>();
      
      // barrier, barrierLevel
      // default values (used for last column)
      int barrierLevel = 0; // I think?
      State barrier = end;
      if (colIterator.nextIndex() < readColumnMetaData.size()) {
        barrier = colStates.get(colIterator.nextIndex());
        barrierLevel = getCommonAncestorLevel(readColumnMetaData.get(colIndex).getPath(), 
            readColumnMetaData.get(colIndex + 1).getPath(), true);
      }
      
      // transitions
      for (DissectedColumnMetaData preCol : readColumnMetaData.subList(0, colIndex + 1)) {
        int preColIndex = readColumnMetaData.indexOf(preCol); 
        if (preCol.getPath().getRepSize() > barrierLevel) {
          int backLevel = getCommonAncestorLevel(readColumnMetaData.get(colIndex).getPath(), 
              readColumnMetaData.get(preColIndex).getPath(), true);
          if (backLevel > 0 && !transitions.containsKey(backLevel)) {
            Transition backTransition = new Transition(
                new RegexGuard(String.valueOf(backLevel)),
                colStates.get(colIndex),
                logAction,
                colStates.get(preColIndex));
            transitions.put(backLevel, backTransition);            
          }
        }
      }
      for (int l = barrierLevel + 1; l <= maxLevel; l++) {
        if (!transitions.containsKey(l)) {
          transitions.put(l, transitions.get(l - 1));
        }
      }      
      for (int m = 0; m <= barrierLevel; m++) {
        if (!transitions.containsKey(m)) {
          Transition barrierTransition = new Transition(
              new RegexGuard(String.valueOf(m)),
              colStates.get(colIndex),
              logAction,
              barrier);
          transitions.put(m, barrierTransition);
        }
      }
      
      // Add the transitions to the FSM
      for (Transition transition : transitions.values()) {
        stmap.addTransition(transition);
      }
    }
    
    // Create the state machine
    stmap.build();    
    sm = new StateMachine(stmap, new EntityAdapter(this));
    sm.addListener(this);
  }  

  private int getCommonAncestorLevel(ColumnPath path1, ColumnPath path2, boolean rep) {
    int commonLevel = 0;
    Iterator<ColumnPathElement> pathIterator1;
    Iterator<ColumnPathElement> pathIterator2;
    if (rep) {
      pathIterator1 = path1.getRepPath().iterator();
      pathIterator2 = path2.getRepPath().iterator();
    } else {
      pathIterator1 = path1.iterator();
      pathIterator2 = path2.iterator();
    }
    while (pathIterator1.hasNext() && pathIterator2.hasNext() && pathIterator1.next() == pathIterator2.next()) {
      commonLevel++;
    }
    return commonLevel; 
  }
  
  @Override
  public Iterator<D> iterator() { return this; }

  @Override
  public boolean hasNext() {
    return values[0].hasNext();
  }

  @Override
  public D next() {
    try {
      sm.applyEvent(new NewRowEvent());
      for (int i = 0; i < values.length; i++) {
        values[i].startRow();
        values[i].nextValue(); // first repetition level is not used
      }
      currentColumnValuesIndex = 0;
      return (D)read(readSchema);
    } catch (Exception e) {
      throw new TrevniRuntimeException(e);
    }
  }

  private Object read(Schema s) throws Exception {
    RecordInAssembly record = new RecordInAssembly(s);
    currentColumnValuesIndex = 0;
    int lastColumnValuesIndex = 0;    
    while (sm.getState().getBaseName() != "ROWREADY" && sm.getState().getBaseName() != "END") {
      // 1. Read in next value; if it's not null, synchronize record and add value; if it's NULL, synchronize record
      String value = (String)values[currentColumnValuesIndex].nextValue();
      if (!value.equals("NULL")) {
        record.moveToLevel(lastColumnValuesIndex, currentColumnValuesIndex);
        record.addValue(readColumnMetaData.get(currentColumnValuesIndex).getColumn().getName(), value);
      } else {
        record.moveToLevel(lastColumnValuesIndex, currentColumnValuesIndex);
        record.addNull();
      }
      
      // 2. Advance the FSM using the repetition level of the next value      
      String repLevel = null;
      try {
        repLevel = (String)values[currentColumnValuesIndex].nextValue();
      } catch (IOException e) {
        repLevel = "0"; // use repetition level 0 at end of column
      }
      sm.applyEvent(new Event(repLevel));
      lastColumnValuesIndex = currentColumnValuesIndex;
      if (sm.getState().getBaseName() != "ROWREADY" && sm.getState().getBaseName() != "END") {
        currentColumnValuesIndex = Integer.valueOf(sm.getState().getBaseName());
      }
      
      // 3. Synchronize record in assembly
      record.returnToLevel(lastColumnValuesIndex);      
    }
    // ReturnToLevel 0
    // End all nested records
    return record.getRootRecord();
  }
  
  class RecordInAssembly {
    private Schema s;
    private Stack<Record> records;
    
    RecordInAssembly(Schema s) {
      this.s = s;
      this.records = new Stack<Record>();
      records.push(new Record(model.newRecord(null, s)));
    }
    
    public Object getRootRecord() {
      return records.get(0).getRecord(); 
    }
    
    void addValue(String name, Object value) {
      Record record = records.peek();
      model.setField(record.getRecord(), name, record.getCurrentField(), value);
      record.incrementCurrentField();
    }
    
    void addNull() {
      Record record = records.peek();
      record.incrementCurrentField();
    }
    
    void moveToLevel(int lastReader, int newReader) {
      // 0. Find common ancestor level
      int ancestorLevel = getCommonAncestorLevel(
          readColumnMetaData.get(newReader).getPath(),
          readColumnMetaData.get(lastReader).getPath(), false);
      
      // 1. End nested records
      for (int i = records.size() - 1; i > ancestorLevel; i--) {
        Record completedRecord = records.pop();
        addValue(completedRecord.name, completedRecord);
      }
      
      // 2. Start nested records
      for (int j = ancestorLevel; j < readColumnMetaData.get(newReader).getPath().size() - 1; j++) {
        // Get name
        String name = readColumnMetaData.get(newReader).getPath().get(j).getFieldName();
        
        // Get schema
        Schema newSchema = pathToSchema(readColumnMetaData.get(newReader).getPath(), s, j);
        
        // Create the record
        records.push(new Record(model.newRecord(null, newSchema), name));
      }
    }
    
    int returnToLevel(int lastReader) {
      // End nested records
      return 0;
    }
    
    // Need name for each field; comes from ColumnPath, not Schema
    class Record {
      private Object record;
      private String name;
      private int currentField;
      
      Record(Object record) {
        this.record = record;
        this.currentField = 0;
      }
      
      Record(Object record, String name) {
        this(record);
        this.name = name;
      }
      
      public Object getRecord() {
        return record;
      }
      
      public int getCurrentField() {
        return currentField;
      }
      
      public int incrementCurrentField() {
        return ++currentField;
      }
      
      public String toString() {
        return record.toString();
      }
    }
  }
  
  private Schema pathToSchema(ColumnPath p, Schema s, int level) {
    ColumnPathElement e = null;
    Schema nextSchema = null;
    int i = 0;
    while (i <= level) {
      e = p.get(i);
      if (e.isOptional()) {
        nextSchema = s.getField(e.getFieldName()).schema();
        for (Schema branch : nextSchema.getTypes()) {
          if (branch.getType() != Schema.Type.NULL) {
            nextSchema = branch;
            i++;
            break;
          }
        }
      } else if (e.isRepeated()) {
        nextSchema = s.getField(e.getFieldName()).schema().getElementType();
      } else {
        nextSchema = s.getField(e.getFieldName()).schema();
      }
      i++;
    }
    return nextSchema;
  }
  
  @Override
  public void remove() { throw new UnsupportedOperationException(); }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  // Log state changes
  public void stateChanged(Entity entity, State oldState, State newState) {
    logger.info("State changed: " + oldState.getName() + " -> " + newState.getName());
  }

  // Do nothing
  class NullAction implements Action {
    public void doAction(Event event, Entity entity, Transition transition,
        int actionType) throws TransitionRollbackException {
    }
  }
  
  // Log event data
  class LogAction implements Action {
    public void doAction(Event event, Entity entity, Transition transition,
                         int actionType) throws TransitionRollbackException {
      logger.info("Event: " + event.getData());
    }
  }  
  
  class NewRowEvent extends Event {
    public NewRowEvent()
    {
      super(null);
    }
  }  
  
  class StopEvent extends Event {
    public StopEvent()
    {
      super(null);
    }
  }
}
