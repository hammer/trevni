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

import java.io.IOException;
import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.Input;
import org.apache.trevni.InputFile;
import org.apache.trevni.TrevniRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuent.tungsten.commons.patterns.fsm.Action;
import com.continuent.tungsten.commons.patterns.fsm.Entity;
import com.continuent.tungsten.commons.patterns.fsm.EntityAdapter;
import com.continuent.tungsten.commons.patterns.fsm.Event;
import com.continuent.tungsten.commons.patterns.fsm.EventTypeGuard;
import com.continuent.tungsten.commons.patterns.fsm.Guard;
import com.continuent.tungsten.commons.patterns.fsm.State;
import com.continuent.tungsten.commons.patterns.fsm.StateChangeListener;
import com.continuent.tungsten.commons.patterns.fsm.StateMachine;
import com.continuent.tungsten.commons.patterns.fsm.StateTransitionMap;
import com.continuent.tungsten.commons.patterns.fsm.StateType;
import com.continuent.tungsten.commons.patterns.fsm.StringEvent;
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
  private HashMap<String, Integer> readerColumnNumbers;
  private int column;                          // current index in values
 
  // Monitoring and management
  private static Logger logger = LoggerFactory.getLogger(AvroDissectedColumnReader.class);
  
  // State machine
  private StateTransitionMap stmap = null;
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
    // set fileSchema to readSchema for now
    this.readSchema = params.schema == null ? fileSchema : params.schema;
    //this.fileSchema = p.parse(reader.getMetaData().getString(AvroColumnWriter.SCHEMA_KEY));
    this.fileSchema = this.readSchema;
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
    this.values = new ColumnValues[readColumns.length];
    this.readerColumnNumbers = new HashMap<String, Integer>();
    int j = 0;    
    for (ColumnMetaData c : readColumns) {
      Integer n = fileColumnNumbers.get(c.getName());
      if (n == null)
        throw new TrevniRuntimeException("No column named: "+c.getName());
      this.readerColumnNumbers.put(c.getName(), j);
      values[j++] = reader.getValues(n);
    }
    
    // create state machine for record assembly
    this.stmap = new StateTransitionMap();

    // State for between rows
    State rowReady = new State("ROWREADY", StateType.START);
    this.stmap.addState(rowReady);
    
    // State for each column    
    ArrayList<State> colStates = new ArrayList<State>();
    for (ColumnMetaData col : readColumns) {
      String colName = col.getName();
      State colState = new State(colName, StateType.ACTIVE);
      colStates.add(colState);
      this.stmap.addState(colState);
    }
    
    // End state
    State end = new State("END", StateType.END);
    this.stmap.addState(end);   
    
    // Define guards
    Guard newRowGuard = new EventTypeGuard(NewRowEvent.class);
    Guard stopGuard = new EventTypeGuard(StopEvent.class);
    Guard stringGuard = new EventTypeGuard(StringEvent.class);
    
    // Define actions
    Action logAction = new LogAction();
    Action nullAction = new NullAction();

    // Define transitions
    stmap.addTransition(new Transition("ROWREADY-TO-" + colStates.get(0).getBaseName(),
        newRowGuard, rowReady, logAction, colStates.get(0)));    
    while (colStatesIterator.hasNext()) {
      State colState = colStatesIterator.next();

      // send each column to the next, except send the last column to ROWREADY
      if (colStatesIterator.nextIndex() == colStates.size()) {
        this.stmap.addTransition(new Transition(colState.getBaseName() + "-TO-ROWREADY",
            stringGuard, colState, nullAction, rowReady));
      } else {
        State nextColState = colStates.get(colStatesIterator.nextIndex());
        this.stmap.addTransition(new Transition(colState.getBaseName() + "-TO-" + nextColState.getBaseName(),
            stringGuard, colState, logAction, nextColState));      
      }
    }
    this.stmap.addTransition(new Transition("ROWREADY-TO-END", stopGuard, rowReady, nullAction, end));
    
    // Create the state machine
    stmap.build();
    sm = new StateMachine(stmap, new EntityAdapter(this));
    sm.addListener(this);
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
      this.sm.applyEvent(new NewRowEvent());
      for (int i = 0; i < values.length; i++)
        values[i].startRow();
      this.column = 0;
      return (D)read(this.readSchema);
    } catch (Exception e) {
      throw new TrevniRuntimeException(e);
    }
  }

  private Object read(Schema s) throws Exception {
    Object record = this.model.newRecord(null, s);    
    while (sm.getState().getBaseName() != "ROWREADY") {
      String name = this.sm.getState().getBaseName();
      int position = this.column;
      Object value = this.values[this.column].nextValue();

      model.setField(record, name, position, value);
      this.sm.applyEvent(new StringEvent((String)value));
    }
    return record;
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

  class NewRowEvent extends Event
  {
    public NewRowEvent()
    {
      super(null);
    }
  }  
  
  class StopEvent extends Event
  {
    public StopEvent()
    {
      super(null);
    }
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
      column = readerColumnNumbers.get(transition.getOutput().getBaseName());
    }
  }  

}
