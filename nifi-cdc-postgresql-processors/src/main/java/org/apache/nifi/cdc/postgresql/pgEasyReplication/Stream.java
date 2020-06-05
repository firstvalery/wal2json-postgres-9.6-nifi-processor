/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import org.apache.nifi.cdc.postgresql.pgEasyReplication.ConnectionManager;


/* Streams are data changes captured (BEGIN, COMMIT, INSERT, UPDATE, DELETE, etc.) via Slot Replication API and decoded. */
public class Stream {

    private PGReplicationStream repStream;
    private Long lastReceiveLSN;
    private ConnectionManager connectionManager;

    public Stream(String tbls, String sch, String slt, ConnectionManager connectionManager) throws SQLException {
        this(tbls, sch, slt, null, connectionManager);
    }

    public Stream(String tbls, String sch, String slt, Long lsn, ConnectionManager connectionManager) throws SQLException {
        this.connectionManager = connectionManager;
        PGConnection pgcon = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);

        if (lsn == null) {
            // More details about pgoutput options in PostgreSQL project: https://github.com/postgres, source file: postgres/src/backend/replication/pgoutput/pgoutput.c
            this.repStream = pgcon.getReplicationAPI().replicationStream().logical().withSlotName(slt)
                    .withSlotOption("format-version","2")
                    .withSlotOption("write-in-chunks","1")
                    .withSlotOption("include-lsn","1")
                    .withSlotOption("add-tables",tbls)
                    .withStatusInterval(1, TimeUnit.SECONDS).start();

        } else {
            // Reading from LSN start position
            LogSequenceNumber startLSN = LogSequenceNumber.valueOf(lsn);
            // More details about pgoutput options in PostgreSQL project: https://github.com/postgres, source file: postgres/src/backend/replication/pgoutput/pgoutput.c
            this.repStream = pgcon.getReplicationAPI().replicationStream().logical().withSlotName(slt)
                    .withSlotOption("format-version","2")
                    .withSlotOption("write-in-chunks","1")
                    .withSlotOption("include-lsn","1")
                    .withStatusInterval(1, TimeUnit.SECONDS)
                    .withStartPosition(startLSN).start();
        }
    }

    public Event readStream(boolean isSimpleEvent, boolean withBeginCommit)
            throws SQLException {

        LinkedList<String> messages = new LinkedList<String>();

        while (true) {
            ByteBuffer buffer = this.repStream.readPending();
            if (buffer == null) {
                break;
            }
            byte[] buffArr = buffer.array();

            messages.addLast(new String(Arrays.copyOfRange(buffArr,25, buffArr.length), StandardCharsets.UTF_8));

            // Replication feedback
            this.repStream.setAppliedLSN(this.repStream.getLastReceiveLSN());
            this.repStream.setFlushedLSN(this.repStream.getLastReceiveLSN());
        }

        this.lastReceiveLSN = this.repStream.getLastReceiveLSN().asLong();

        return new Event(messages, this.lastReceiveLSN, isSimpleEvent, withBeginCommit, false);
    }

    public Long getLastReceiveLSN() {
        return this.lastReceiveLSN;
    }
}