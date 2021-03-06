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
package org.apache.nifi.cdc.postgresql.processors;

import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.apache.nifi.reporting.InitializationException

/**
 * Unit test(s) for PostgreSQL CDC
 */
public class CaptureChangePostgreSQLTest {
    Wal2JsonPostgreSQL96Capture processor
    TestRunner testRunner
    //MockBinlogClient client

    @Before
    void setUp() throws Exception {
        processor = new MockCaptureChangePostgreSQL()
        testRunner = TestRunners.newTestRunner(processor)
       // client = new MockBinlogClient('localhost', 3306, 'root', 'password')
    }

    @After
    void tearDown() throws Exception {

    }
    
    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Wal2JsonPostgreSQL96Capture.class);
    }

    @Test
    public void testProcessor() {

    }


    
    class MockCaptureChangePostgreSQL extends Wal2JsonPostgreSQL96Capture {
        
                       
                @Override
                protected void registerDriver(String locationString, String drvName) throws InitializationException {
                }
        
            }

}
