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
package org.apache.zeppelin.mahout;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Properties;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MahoutInterpreterTest {
    private static MahoutSparkInterpreter mahout;
    private static InterpreterContext context;

    @BeforeClass
    public static void setUp() {
        Properties p = new Properties();
        mahout = new MahoutSparkInterpreter(p);
        mahout.open();
        context = new InterpreterContext(null, null, null, null, null, null, null, null, null, null, null);
    }

    @AfterClass
    public static void tearDown() {
        mahout.close();
        mahout.destroy();
    }

    @Test
    public void testNextLineInvocation() {
        assertEquals(InterpreterResult.Code.SUCCESS, mahout.interpret("\"123\"\n.toInt", context).code());
    }

}





