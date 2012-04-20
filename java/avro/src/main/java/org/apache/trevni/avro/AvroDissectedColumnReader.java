/**
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
      ColumnPath colPath = col.getPath();
      int colIndex = readColumnMetaData.indexOf(col);
      
      // maxLevel
      int maxLevel = colPath.getRepSize();
      HashMap<Integer, Transition> transitions = new HashMap<Integer, Transition>();
      
      // barrier, barrierLevel
      // default values (used for last column)
      int barrierLevel = 0; // I think?
      State barrier = rowReady;
      if (colIterator.nextIndex() < readColumnMetaData.size()) {
        barrier = colStates.get(colIterator.nextIndex());
        barrierLevel = getCommonAncestorLevel(colPath, readColumnMetaData.get(colIndex + 1).getPath(), true);
      }
      
      // transitions
      // 1. Backwards (and self) transitions
      for (DissectedColumnMetaData preCol : readColumnMetaData.subList(0, colIndex + 1)) {
        int preColIndex = readColumnMetaData.indexOf(preCol);
        if (preCol.getPath().getRepSize() > barrierLevel) {
          int backLevel = getCommonAncestorLevel(colPath, readColumnMetaData.get(preColIndex).getPath(), true);
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
      
      // 2. Missing backwards transitions
      for (int l = maxLevel; l > barrierLevel + 1; l--) {
        if (!transitions.containsKey(l - 1)) {
          Transition oldTransition = transitions.get(l);  
          Transition newTransition = new Transition(
              new RegexGuard(String.valueOf(l - 1)),
              oldTransition.getInput(),
              oldTransition.getAction(),
              oldTransition.getOutput());
          transitions.put(l - 1, newTransition);
        }
      }  
      
      // 3. Barrier transitions
      for (int m = 0; m <= barrierLevel; m++) {
        Transition barrierTransition = new Transition(
            new RegexGuard(String.valueOf(m)),
            colStates.get(colIndex),
            logAction,
            barrier);
        transitions.put(m, barrierTransition);
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
         
    // Throw away first repetition level (not used)
    for (int n = 0; n < values.length; n++) {
      values[n].startRow();
      values[n].nextValue();
    }
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
    String repLevel = "0";
    while (sm.getState().getBaseName() != "ROWREADY") {
      // 1. Read in next value, synchronize the record, and add value
      String value = (String)values[currentColumnValuesIndex].nextValue();
      record.moveToLevel(lastColumnValuesIndex, currentColumnValuesIndex, Integer.valueOf(repLevel));
      // TODO(hammer): handle NULL values correctly
      // TODO(hammer): use definition level of NULLs to preserve record structure for sub-schemas
      record.addValue(value);
      
      // 2. Advance the FSM using the repetition level of the next value      
      // TODO(hammer): replace try/catch with hasNext(), when I figure out why hasNext() doesn't work
      try {
        repLevel = (String)values[currentColumnValuesIndex].nextValue();
      } catch (IOException e) {
        repLevel = "0"; // use repetition level 0 at end of column
      }
      sm.applyEvent(new Event(repLevel));
      lastColumnValuesIndex = currentColumnValuesIndex;
      if (sm.getState().getBaseName() != "ROWREADY") {
        currentColumnValuesIndex = Integer.valueOf(sm.getState().getBaseName());
      }      
      // 3. Synchronize record in assembly (necessary?)
    }
    // ReturnToLevel 0
    // End all nested records
    record.returnToRoot();
    return record.getRootRecord();
  }
  
  class RecordInAssembly {
    private Schema s;
    private Stack<Record> records;
    
    public RecordInAssembly(Schema s) {
      this.s = s;
      this.records = new Stack<Record>();
      records.push(new Record(model.newRecord(null, s)));
    }
    
    public Object getRootRecord() {
      return records.get(0).getRecord(); 
    }
    
    public void addValue(Object value) {
      records.peek().addValue(value);
    }
    
    public void moveToLevel(int lastReader, int nextReader, int repLevel) {
      // 0. Get paths
      ColumnPath lastPath = readColumnMetaData.get(lastReader).getPath();
      ColumnPath nextPath = readColumnMetaData.get(nextReader).getPath();
      
      // 1. Find common ancestor level
      int ancestorLevel = getCommonAncestorLevel(lastPath, nextPath, false);
      if (lastReader == nextReader && repLevel > 0) {
        int pathLevel = 0;
        int pathRepLevel = 0;
        while (pathRepLevel < repLevel) {
          if (nextPath.get(pathLevel).isRepeated()) {
            pathRepLevel++;
          }
          pathLevel++;
        }
        ancestorLevel = pathLevel;        
      }      
      System.out.println("ancestorLevel: " + ancestorLevel);
      
      // 2. End nested records
      // End the current field
      if (ancestorLevel < lastPath.size() || lastReader != nextReader) {
        records.peek().endField();
      }
      // Move up the IR
      for (int i = lastPath.size() - 1; i > ancestorLevel; i--) {
        Record completedRecord = records.pop();
        System.out.println("Going up! completedRecord: " + completedRecord);
        records.peek().addValue(completedRecord);
        records.peek().endField();
      }
      // If we're moving to the left on the IR, pop a record
      if (lastReader >= nextReader && repLevel > 0) {
        // Add the completed record to the array
        Record completedRecord = records.pop();
        System.out.println("Going left! completedRecord: " + completedRecord);
        records.peek().addValue(completedRecord);
        
        // Create a new record
        SchemaAndRepeatedStatus ancestorField = pathToSchema(nextPath, s, ancestorLevel - 1);
        records.push(new Record(model.newRecord(null, ancestorField.getSchema())));
      }
      
      // 3. Start nested records
      // Move down the IR
      for (int j = ancestorLevel; j < nextPath.size() - 1; j++) {
        SchemaAndRepeatedStatus newField = pathToSchema(nextPath, s, j);
        String fieldName = nextPath.get(j).getFieldName();
        Schema fieldSchema = newField.getSchema();
        if (newField.isRepeated()) {
          records.peek().startField(fieldName, fieldSchema);
        } else {
          records.peek().startField(fieldName);
        }
        records.push(new Record(model.newRecord(null, newField.getSchema())));        
      }
      // Initialize the field that will be written to next
      // TODO(hammer): handle the case where initial field is an array
      if (ancestorLevel < nextPath.size() || lastReader != nextReader) {
        SchemaAndRepeatedStatus newField = pathToSchema(nextPath, s, nextPath.size() - 1);
        String fieldName = nextPath.get(nextPath.size() - 1).getFieldName();
        Schema fieldSchema = newField.getSchema();
        if (newField.isRepeated()) {
          records.peek().startField(fieldName, fieldSchema);
        } else {
          records.peek().startField(fieldName);
        }      
      }
    }
    
    public void returnToRoot() {
      // End the current field
      records.peek().endField();

      // Move up the IR
      for (int i = records.size() - 1; i > 0; i--) {
        Record completedRecord = records.pop();
        records.peek().addValue(completedRecord); 
        records.peek().endField();
      }
    }
    
    class Record {
      private Object record;
      private int currentFieldIndex;
      private boolean currentFieldIsRepeated;
      private ArrayList<Object> currentFieldArray;
      private Schema currentFieldArraySchema;
      private String fieldName;
      
      Record(Object record) {
        this.record = record;
        this.currentFieldIndex = 0;
      }
      
      public Object getRecord() {
        return record;
      }
      
      public void startField(String fieldName) {
        System.out.println("Starting field: " + fieldName);
        this.fieldName = fieldName;
        currentFieldIsRepeated = false;
      }
      
      public void startField(String fieldName, Schema currentFieldArraySchema) {
        System.out.println("Starting field: " + fieldName + ", schema: " + currentFieldArraySchema);
        this.fieldName = fieldName;
        currentFieldIsRepeated = true;
        this.currentFieldArraySchema = currentFieldArraySchema;
        currentFieldArray = new ArrayList<Object>();
      }
      
      public void addValue(Object value) {
        System.out.println("Adding value: " + value + " to field " + fieldName);
        
        if (currentFieldIsRepeated) {
          currentFieldArray.add(value);
        } else {          
          model.setField(record, fieldName, currentFieldIndex, value);
        }        
      }
      
      public void endField() {
        System.out.println("Ending field " + fieldName);
        if (currentFieldIsRepeated) {
          model.setField(record, fieldName, currentFieldIndex, 
              new GenericData.Array<Object>(Schema.createArray(currentFieldArraySchema), currentFieldArray));
        }
        currentFieldIndex++;
        System.out.println("Current field index for " + fieldName + ": " + currentFieldIndex);
      }
      
      public String toString() {
        return record.toString();
      }
    }
  }
  
  class SchemaAndRepeatedStatus {
    private Schema s;
    private boolean isRepeated;
    
    public SchemaAndRepeatedStatus(Schema s, boolean isRepeated) {
      this.s = s;
      this.isRepeated = isRepeated;
    }
    
    public Schema getSchema() {
      return s;
    }
    
    public boolean isRepeated() {
      return isRepeated;
    }
  }
  
  private SchemaAndRepeatedStatus pathToSchema(ColumnPath p, Schema s, int level) {
    ColumnPathElement e = null;
    Schema nextSchema = s;
    int i = 0;
    while (i <= level) {
      e = p.get(i);
      if (e.isOptional()) {
        nextSchema = nextSchema.getField(e.getFieldName()).schema();
        for (Schema branch : nextSchema.getTypes()) {
          if (branch.getType() != Schema.Type.NULL) {
            nextSchema = branch;
          }
        }
      } else if (e.isRepeated()) {
        nextSchema = nextSchema.getField(e.getFieldName()).schema().getElementType();
      } else {
        nextSchema = nextSchema.getField(e.getFieldName()).schema();
      }
      i++;
    }
        
    return new SchemaAndRepeatedStatus(nextSchema, e.isRepeated());
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
