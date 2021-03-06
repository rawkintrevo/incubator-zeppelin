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

package org.apache.zeppelin.livy;


import com.cloudera.livy.test.framework.Cluster;
import com.cloudera.livy.test.framework.Cluster$;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LivyInterpreterIT {

  private static Logger LOGGER = LoggerFactory.getLogger(LivyInterpreterIT.class);
  private static Cluster cluster;
  private static Properties properties;

  @BeforeClass
  public static void setUp() {
    if (!checkPreCondition()) {
      return;
    }
    cluster = Cluster$.MODULE$.get();
    LOGGER.info("Starting livy at {}", cluster.livyEndpoint());
    properties = new Properties();
    properties.setProperty("zeppelin.livy.url", cluster.livyEndpoint());
    properties.setProperty("zeppelin.livy.create.session.retries", "120");
    properties.setProperty("zeppelin.livy.spark.sql.maxResult", "100");
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.cleanUp();
    }
  }

  public static boolean checkPreCondition() {
    if (System.getenv("LIVY_HOME") == null) {
      LOGGER.warn(("livy integration is skipped because LIVY_HOME is not set"));
      return false;
    }
    if (System.getenv("SPARK_HOME") == null) {
      LOGGER.warn(("livy integration is skipped because SPARK_HOME is not set"));
      return false;
    }
    return true;
  }

  @Test
  public void testSparkInterpreterRDD() {
    if (!checkPreCondition()) {
      return;
    }
    InterpreterGroup interpreterGroup = new InterpreterGroup("group_1");
    interpreterGroup.put("session_1", new ArrayList<Interpreter>());
    LivySparkInterpreter sparkInterpreter = new LivySparkInterpreter(properties);
    sparkInterpreter.setInterpreterGroup(interpreterGroup);
    interpreterGroup.get("session_1").add(sparkInterpreter);
    AuthenticationInfo authInfo = new AuthenticationInfo("user1");
    MyInterpreterOutputListener outputListener = new MyInterpreterOutputListener();
    InterpreterOutput output = new InterpreterOutput(outputListener);
    InterpreterContext context = new InterpreterContext("noteId", "paragraphId", "livy.spark",
        "title", "text", authInfo, null, null, null, null, null, output);
    sparkInterpreter.open();

    try {
      InterpreterResult result = sparkInterpreter.interpret("sc.version", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("1.5.2"));

      // test RDD api
      outputListener.reset();
      result = sparkInterpreter.interpret("sc.parallelize(1 to 10).sum()", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("Double = 55.0"));

      // single line comment
      outputListener.reset();
      String singleLineComment = "// my comment";
      result = sparkInterpreter.interpret(singleLineComment, context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());

      // multiple line comment
      outputListener.reset();
      String multipleLineComment = "/* multiple \n" + "line \n" + "comment */";
      result = sparkInterpreter.interpret(multipleLineComment, context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());

      // multi-line string
      outputListener.reset();
      String multiLineString = "val str = \"\"\"multiple\n" +
          "line\"\"\"\n" +
          "println(str)";
      result = sparkInterpreter.interpret(multiLineString, context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("multiple\nline"));

      // case class
      outputListener.reset();
      String caseClassCode = "case class Person(id:Int, \n" +
          "name:String)\n" +
          "val p=Person(1, \"name_a\")";
      result = sparkInterpreter.interpret(caseClassCode, context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("defined class Person"));

      // object class
      outputListener.reset();
      String objectClassCode = "object Person {}";
      result = sparkInterpreter.interpret(objectClassCode, context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("defined module Person"));

      // error
      result = sparkInterpreter.interpret("println(a)", context);
      assertEquals(InterpreterResult.Code.ERROR, result.code());
      assertEquals(InterpreterResult.Type.TEXT, result.message().get(0).getType());
      assertTrue(result.message().get(0).getData().contains("error: not found: value a"));
    } finally {
      sparkInterpreter.close();
    }
  }

  @Test
  public void testSparkInterpreterDataFrame() {
    if (!checkPreCondition()) {
      return;
    }
    InterpreterGroup interpreterGroup = new InterpreterGroup("group_1");
    interpreterGroup.put("session_1", new ArrayList<Interpreter>());
    LivySparkInterpreter sparkInterpreter = new LivySparkInterpreter(properties);
    sparkInterpreter.setInterpreterGroup(interpreterGroup);
    interpreterGroup.get("session_1").add(sparkInterpreter);
    AuthenticationInfo authInfo = new AuthenticationInfo("user1");
    MyInterpreterOutputListener outputListener = new MyInterpreterOutputListener();
    InterpreterOutput output = new InterpreterOutput(outputListener);
    InterpreterContext context = new InterpreterContext("noteId", "paragraphId", "livy.spark",
        "title", "text", authInfo, null, null, null, null, null, output);
    sparkInterpreter.open();

    LivySparkSQLInterpreter sqlInterpreter = new LivySparkSQLInterpreter(properties);
    interpreterGroup.get("session_1").add(sqlInterpreter);
    sqlInterpreter.setInterpreterGroup(interpreterGroup);
    sqlInterpreter.open();

    try {
      // test DataFrame api
      outputListener.reset();
      sparkInterpreter.interpret("val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n"
          + "import sqlContext.implicits._", context);
      InterpreterResult result = sparkInterpreter.interpret("val df=sqlContext.createDataFrame(Seq((\"hello\",20)))\n"
          + "df.collect()", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended()
          .contains("Array[org.apache.spark.sql.Row] = Array([hello,20])"));
      sparkInterpreter.interpret("df.registerTempTable(\"df\")", context);

      // test LivySparkSQLInterpreter which share the same SparkContext with LivySparkInterpreter
      outputListener.reset();
      result = sqlInterpreter.interpret("select * from df", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(InterpreterResult.Type.TABLE, result.message().get(0).getType());
      // TODO(zjffdu), \t at the end of each line is not necessary, it is a bug of LivySparkSQLInterpreter
      assertEquals("_1\t_2\t\nhello\t20\t\n", result.message().get(0).getData());
    } finally {
      sparkInterpreter.close();
      sqlInterpreter.close();
    }
  }

  @Test
  public void testSparkSQLInterpreter() {
    if (!checkPreCondition()) {
      return;
    }
    InterpreterGroup interpreterGroup = new InterpreterGroup("group_1");
    interpreterGroup.put("session_1", new ArrayList<Interpreter>());
    LivySparkInterpreter sparkInterpreter = new LivySparkInterpreter(properties);
    sparkInterpreter.setInterpreterGroup(interpreterGroup);
    interpreterGroup.get("session_1").add(sparkInterpreter);
    LivySparkSQLInterpreter sqlInterpreter = new LivySparkSQLInterpreter(properties);
    interpreterGroup.get("session_1").add(sqlInterpreter);
    sqlInterpreter.setInterpreterGroup(interpreterGroup);
    sqlInterpreter.open();

    try {
      AuthenticationInfo authInfo = new AuthenticationInfo("user1");
      MyInterpreterOutputListener outputListener = new MyInterpreterOutputListener();
      InterpreterOutput output = new InterpreterOutput(outputListener);
      InterpreterContext context = new InterpreterContext("noteId", "paragraphId", "livy.sql",
          "title", "text", authInfo, null, null, null, null, null, output);
      InterpreterResult result = sqlInterpreter.interpret("show tables", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(InterpreterResult.Type.TABLE, result.message().get(0).getType());
      assertTrue(result.message().get(0).getData().contains("tableName"));
    } finally {
      sqlInterpreter.close();
    }
  }

  @Test
  public void testPySparkInterpreter() {
    if (!checkPreCondition()) {
      return;
    }

    LivyPySparkInterpreter pysparkInterpreter = new LivyPySparkInterpreter(properties);
    AuthenticationInfo authInfo = new AuthenticationInfo("user1");
    MyInterpreterOutputListener outputListener = new MyInterpreterOutputListener();
    InterpreterOutput output = new InterpreterOutput(outputListener);
    InterpreterContext context = new InterpreterContext("noteId", "paragraphId", "livy.pyspark",
        "title", "text", authInfo, null, null, null, null, null, output);
    pysparkInterpreter.open();

    try {
      InterpreterResult result = pysparkInterpreter.interpret("sc.version", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("1.5.2"));

      // test RDD api
      outputListener.reset();
      result = pysparkInterpreter.interpret("sc.range(1, 10).sum()", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("45"));

      // test DataFrame api
      outputListener.reset();
      pysparkInterpreter.interpret("from pyspark.sql import SQLContext\n"
          + "sqlContext = SQLContext(sc)", context);
      result = pysparkInterpreter.interpret("df=sqlContext.createDataFrame([(\"hello\",20)])\n"
          + "df.collect()", context);
      assertEquals(InterpreterResult.Code.SUCCESS, result.code());
      assertEquals(0, result.message().size());
      assertTrue(outputListener.getOutputAppended().contains("[Row(_1=u'hello', _2=20)]"));

      // error
      result = pysparkInterpreter.interpret("print(a)", context);
      assertEquals(InterpreterResult.Code.ERROR, result.code());
      assertEquals(InterpreterResult.Type.TEXT, result.message().get(0).getType());
      assertTrue(result.message().get(0).getData().contains("name 'a' is not defined"));
    } finally {
      pysparkInterpreter.close();
    }
  }

  @Test
  public void testSparkRInterpreter() {
    if (!checkPreCondition()) {
      return;
    }
    // TODO(zjffdu),  Livy's SparkRIntepreter has some issue, do it after livy-0.3 release.
  }

  public static class MyInterpreterOutputListener implements InterpreterOutputListener {
    private StringBuilder outputAppended = new StringBuilder();
    private StringBuilder outputUpdated = new StringBuilder();

    @Override
    public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
      LOGGER.info("onAppend:" + new String(line));
      outputAppended.append(new String(line));
    }

    @Override
    public void onUpdate(int index, InterpreterResultMessageOutput out) {
      try {
        LOGGER.info("onUpdate:" + new String(out.toByteArray()));
        outputUpdated.append(new String(out.toByteArray()));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public String getOutputAppended() {
      return outputAppended.toString();
    }

    public String getOutputUpdated() {
      return outputUpdated.toString();
    }

    public void reset() {
      outputAppended = new StringBuilder();
      outputUpdated = new StringBuilder();
    }

    @Override
    public void onUpdateAll(InterpreterOutput out) {

    }
  }
}
