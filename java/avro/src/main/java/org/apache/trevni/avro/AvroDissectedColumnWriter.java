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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.util.Utf8;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileWriter;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/** Write Avro records to a Trevni column file.  Each primitive type is written
 * to a separate column. */
public class AvroDissectedColumnWriter<D> {
  private Schema schema;
  private GenericData model;
  private ColumnFileWriter writer;

  public static final String SCHEMA_KEY = "avro.schema";

  public AvroDissectedColumnWriter(Schema s, ColumnFileMetaData meta)
    throws IOException {
    this(s, meta, GenericData.get());
  }

  public AvroDissectedColumnWriter(Schema s, ColumnFileMetaData meta, GenericData model)
    throws IOException {
    this.schema = s;
    AvroDissectedColumnator columnator = new AvroDissectedColumnator(s);
    meta.set(SCHEMA_KEY, s.toString());           // save schema in file
    this.writer = new ColumnFileWriter(meta, columnator.getColumns());
    this.model = model;
  }

  /** Write all rows added to the named output stream. */
  public void writeTo(OutputStream out) throws IOException {
    writer.writeTo(out);
  }

  /** Write all rows added to the named file. */
  public void writeTo(File file) throws IOException {
    writer.writeTo(file);
  }

  /** Add a row to the file. */
  public void write(D value) throws IOException {
    writer.startRow();
    int count = write(value, schema, 0, 0, 0, 0);
    assert(count == writer.getColumnCount());
    writer.endRow();
  }
  
  private int write(Object o, Schema s, int column, int r, int rDepth, int d) throws IOException {
    if (isSimple(s)) {
      // 1. (S, Rq)
      if (o != null) {
        writeValue(Joiner.on(",").join(o, r, d), s, column);
      } else {
        writeValue(Joiner.on(",").join("NULL", r, d), s, column);
      }
      return column + 1;
    }
    switch (s.getType()) {
    case MAP: 
      throw new RuntimeException("Can't dissect maps: " + s);
    case RECORD:
      // 2. (M, Rq)
      if (o != null) {
        for (Field f : s.getFields())
          column = write(model.getField(o, f.name(), f.pos()), f.schema(), column, r, rDepth, d);
      } else {
        for (Field f : s.getFields())
          column = write(null, f.schema(), column, r, rDepth, d);      
      }
      return column;
    case ARRAY:
      if (o != null) {
        Collection elements = (Collection)o;
        // 3. (S, Rp)
        if (isSimple(s.getElementType())) {
          if (elements.size() == 0) {
            writeValue(Joiner.on(",").join("NULL", r, d), s.getElementType(), column);
          } else {
            d += 1;
            rDepth += 1;
            Iterator<Object> iter = elements.iterator();
            writeValue(Joiner.on(",").join(iter.next(), r, d), s.getElementType(), column);
            while (iter.hasNext()) {
              writeValue(Joiner.on(",").join(iter.next(), rDepth, d), s.getElementType(), column);
            }
          }
          return column + 1;
        }
        // 4. (M, Rp)
        if (elements.size() == 0) {
          column = write(null, s.getElementType(), column, r, rDepth, d);
          return column;
        } else {
          d += 1;
          rDepth += 1;
          Iterator<Object> iter = elements.iterator();
          int c = write(iter.next(), s.getElementType(), column, r, rDepth, d);
          while (iter.hasNext()) {
            write(iter.next(), s.getElementType(), column, rDepth, rDepth, d);
          }
          return c;
        }
      } else {
        column = write(null, s.getElementType(), column, r, rDepth, d);
      }
    case UNION:
      if (o != null) {
        int b = model.resolveUnion(s, o);
        List<Schema> branches = s.getTypes();
        Schema chosenBranch = branches.get(b);
        Schema notChosenBranch = branches.get(1 - b); // only two branches
        if (chosenBranch.getType() != Schema.Type.NULL) {
          if (isSimple(chosenBranch)) {
            // 5a. (S, O)
            writeValue(Joiner.on(",").join(o, r, d + 1), chosenBranch, column);
            return column + 1;
          } else {
            // 6a. (M, O)
            return write(o, chosenBranch, column, r, rDepth, d + 1);
          }
        } else {
          if (isSimple(notChosenBranch)) {
            // 5b. (S, O)
            writeValue(Joiner.on(",").join("NULL", r, d), notChosenBranch, column);
            return column + 1;
          } else {
            // 6b. (M, O)
            column = write(null, notChosenBranch, column, r, rDepth, d);
            return column;
          }
        }
      } else {
        List<Schema> branches = s.getTypes();
        for (Schema branch : branches) {
          if (branch.getType() != Schema.Type.NULL) {
            column = write(null, branch, column, r, rDepth, d);
            return column;
          }
        }
      }
    default:
      throw new RuntimeException("Unknown schema: " + s);
    }
  }

  private void writeValue(Object value, Schema s, int column)
    throws IOException {
    
    switch (s.getType()) {
    case STRING:
      if (value instanceof Utf8)                    // convert Utf8 to String
        value = value.toString();
      break;
    case ENUM:
      if (value instanceof Enum)
        value = ((Enum)value).ordinal();
      else 
        value = s.getEnumOrdinal(value.toString());
      break;
    case FIXED:
      value = ((GenericFixed)value).bytes();
      break;
    }
    int i = 0;
    String r = "";
    String v = "";
    for (String item : Splitter.on(",").split((String)value)) {
      if (i == 0) {       
        v = item;  
      } else if (i == 1) {
        r = item;
        break;
      }
      i++;
    }
    // Write repetition level, then the value
    writer.writeValue(r, column);
    writer.writeValue(v, column);
  }

}
