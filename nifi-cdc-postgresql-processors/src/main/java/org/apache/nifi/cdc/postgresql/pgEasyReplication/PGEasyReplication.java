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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import org.postgresql.PGConnection;

import com.fasterxml.jackson.core.JsonProcessingException;

public class PGEasyReplication {

    private String schema;
    private String slot;
    private Stream stream;
    private String tables;
    private ConnectionManager connectionManager;

    public static final boolean SLOT_DROP_IF_EXISTS_DEFAULT = false;
    public static final String MIME_TYPE_OUTPUT_DEFAULT = "application/json";

    public PGEasyReplication(String tbls, String sch, String slt, ConnectionManager connectionManager) {
        this.schema = sch;
        this.slot = slt;
        this.tables = tbls;
        this.connectionManager = connectionManager;
    }

    public void initializeLogicalReplication() {
        this.initializeLogicalReplication(SLOT_DROP_IF_EXISTS_DEFAULT);
    }

    public void initializeLogicalReplication(boolean slotDropIfExists) {
        try {
            PreparedStatement stmt = this.connectionManager.getSQLConnection().prepareStatement("select 1 from pg_catalog.pg_replication_slots WHERE slot_name = ?");
            stmt.setString(1, this.slot.toLowerCase());
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                // If slot exists
                if (slotDropIfExists) {
                    this.dropReplicationSlot();
                    this.createReplicationSlot();
                }
            } else {
                this.createReplicationSlot();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createReplicationSlot() {
        try {
            PGConnection pgcon = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);
            // More details about pgoutput options in PostgreSQL project: https://github.com/postgres, source file: postgres/src/backend/replication/pgoutput/pgoutput.c
            pgcon.getReplicationAPI().createReplicationSlot().logical().withSlotName(this.slot).withOutputPlugin("wal2json").make();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropReplicationSlot() {
        try {
            PGConnection pgcon = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);
            pgcon.getReplicationAPI().dropReplicationSlot(this.slot);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Event getSnapshot() {
        return this.getSnapshot(MIME_TYPE_OUTPUT_DEFAULT);
    }

    public Event getSnapshot(String outputFormat) {
        Event event = null;

        try {
            Snapshot snapshot = new Snapshot(this.schema, this.connectionManager);
            event = snapshot.getInitialSnapshot(outputFormat);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return event;
    }

    public Event readEvent(boolean isSimpleEvent, boolean withBeginCommit) {
        return this.readEvent(isSimpleEvent, withBeginCommit, null);
    }

    public Event readEvent(boolean isSimpleEvent, boolean withBeginCommit, Long startLSN) {
        Event event = null;
        try {
            if (this.stream == null) {
                // First read stream
                this.stream = new Stream(this.tables, this.schema, this.slot, startLSN, this.connectionManager);
            }
            event = this.stream.readStream(isSimpleEvent, withBeginCommit);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return event;
    }
}