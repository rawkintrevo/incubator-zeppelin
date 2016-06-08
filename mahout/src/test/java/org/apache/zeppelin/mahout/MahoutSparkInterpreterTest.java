/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.mahout;

import static org.junit.Assert.*;

import java.io.File;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.display.AngularObjectRegistry;

import org.apache.zeppelin.resource.LocalResourcePool;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MahoutSparkInterpreterTest {
    public static MahoutSparkInterpreter repl;
    public static InterpreterGroup intpGroup;
    private InterpreterContext context;
    public static Logger LOGGER = LoggerFactory.getLogger(MahoutSparkInterpreterTest.class);
    private File tmpDir;
    /**
     * Get spark version number as a numerical value.
     * eg. 1.1.x => 11, 1.2.x => 12, 1.3.x => 13 ...
     */
    public static int getSparkVersionNumber() {
        if (repl == null) {
            return 0;
        }

        String[] split = repl.getSparkContext().version().split("\\.");
        int version = Integer.parseInt(split[0]) * 10 + Integer.parseInt(split[1]);
        return version;
    }

    public static Properties getMahoutSparkTestProperties() {
        Properties p = new Properties();
        p.setProperty("zeppelin.spark.useHiveContext", "false");
        p.setProperty("spark.app.name", "Zeppelin Test");
        p.setProperty("zeppelin.spark.maxResult", "1000");
        p.setProperty("master", "local[*]");
        p.setProperty("spark.kryo.registrator", "org.apache.mahout.sparkbindings.io.MahoutKryoRegistrator");
        p.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        p.setProperty("spark.kryo.referenceTracking", "false");
        p.setProperty("spark.kryoserializer.buffer", "300m");
        p.setProperty("mahout.home", "local");
        p.setProperty("mahout.version", "0.12.2");
        p.setProperty("zeppelin.dep.localrepo", "mahout-test-local-repo");
        return p;
    }

    public Boolean isSupportedScalaVersion() {
      String[] version = scala.util.Properties.versionNumberString().split("[.]");
      String scalaVersion = version[0] + "." + version[1];
      if (scalaVersion.equals("2.10")) {
        return true;
      } else {
        return false;
      }
    }

    public Boolean isSupportedEnv(){
      if ((getSparkVersionNumber() > 14) &&
              isSupportedScalaVersion()) {
        return true;
      } else { return false; }
    }

    @Before
    public void setUp() throws Exception {

        Properties p = getMahoutSparkTestProperties();

        if (repl == null) {
            intpGroup = new InterpreterGroup();
            intpGroup.put("mahout_note", new LinkedList<Interpreter>());
            repl = new MahoutSparkInterpreter(p);
            repl.setInterpreterGroup(intpGroup);
            intpGroup.get("mahout_note").add(repl);
            repl.open();
        }


        context = new InterpreterContext("m_note", "m_id", "m_title", "m_text",
                new AuthenticationInfo(),
                new HashMap<String, Object>(),
                new GUI(),
                new AngularObjectRegistry(intpGroup.getId(), null),
                new LocalResourcePool("m_id"),
                new LinkedList<InterpreterContextRunner>(),
                new InterpreterOutput(new InterpreterOutputListener() {
                    @Override
                    public void onAppend(InterpreterOutput out, byte[] line) {

                    }

                    @Override
                    public void onUpdate(InterpreterOutput out, byte[] output) {

                    }
                }));

    }


    @Test  // repeated basic functions check from SparkInterpreterterTest
    public void testBasicIntp() {
        assertEquals(InterpreterResult.Code.SUCCESS,
                repl.interpret("val a = 1\nval b = 2", context).code());

        // when interpret incomplete expression
        InterpreterResult incomplete = repl.interpret("val a = \"\"\"", context);
        assertEquals(InterpreterResult.Code.INCOMPLETE, incomplete.code());
        assertTrue(incomplete.message().length() > 0); // expecting some error
        // message
    /*
     * assertEquals(1, repl.getValue("a")); assertEquals(2, repl.getValue("b"));
     * repl.interpret("val ver = sc.version");
     * assertNotNull(repl.getValue("ver")); assertEquals("HELLO\n",
     * repl.interpret("println(\"HELLO\")").message());
     */
    }

    @Test
    public void testSdcCreated() {
        InterpreterResult result;
        result = repl.interpret("sdc", context);
        if (isSupportedEnv()){
          assertEquals(Code.SUCCESS, result.code());
        } else {
          assertEquals(Code.ERROR, result.code());
        }
    }

    @Test
    public void testCanCreateMatrices() {
        InterpreterResult result;
        result = repl.interpret("val mxRnd = Matrices.symmetricUniformView(50, 2, 1234)", context);
        if (isSupportedEnv()){
          assertEquals(Code.SUCCESS, result.code());
        } else {
          assertEquals(Code.ERROR, result.code());
        }
    }

    @Test
    public void testCanParallelize() {
        repl.interpret("val mxRnd = Matrices.symmetricUniformView(50, 2, 1234)", context);
        InterpreterResult result = repl.interpret("val drmRand = drmParallelize(mxRnd)", context);
        if (isSupportedEnv()){
          assertEquals(Code.SUCCESS, result.code());
        } else {
          assertEquals(Code.ERROR, result.code());
        }
    }

}

