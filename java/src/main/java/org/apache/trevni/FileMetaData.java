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
package org.apache.trevni;

import java.io.IOException;
import java.io.Closeable;
import java.util.Map;
import java.util.Map;
import java.util.LinkedHashMap;

/** */
public class FileMetaData extends MetaData {

  static final String CODEC_KEY = RESERVED_KEY_PREFIX + "codec";
  static final String CHECKSUM_KEY = RESERVED_KEY_PREFIX + "checksum";

  public String getCodec(String codec) { return getString(CODEC_KEY); }
  public FileMetaData setCodec(String codec) {
    put(CODEC_KEY, codec);
    return this;
  }
   
  public String getChecksum(String checksum) { return getString(CHECKSUM_KEY); }
  public FileMetaData setChecksum(String checksum) {
    put(CHECKSUM_KEY, checksum);
    return this;
  }

}