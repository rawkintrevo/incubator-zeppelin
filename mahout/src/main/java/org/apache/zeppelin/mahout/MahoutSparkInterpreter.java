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

import java.net.URL;
import java.util.*;

import org.apache.spark.repl.SparkIMain;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.spark.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zeppelin.mahout.utils.*;

/**
 * Spark interpreter for Zeppelin.
 *
 */


public class MahoutSparkInterpreter extends SparkInterpreter {

  private static final Logger logger = LoggerFactory.getLogger(MahoutSparkInterpreter.class);
  private SparkIMain intp;
  private boolean firstRun = true;
  private boolean testing = false;
  
  public MahoutSparkInterpreter(Properties property) {
    super(property);
  }

  private void loadMahoutJars(){
    String mahoutHome;
    if (getProperty("mahout.home") != null) {
      mahoutHome = getProperty("mahout.home");
    } else if (System.getenv("MAHOUT_HOME") != null) {
      mahoutHome = System.getenv("MAHOUT_HOME");
    } else {
      logger.warn("MAHOUT_HOME not set in environement or settings, please do that.");
      mahoutHome = "";
    }
    URL[] mahoutJars = findMahoutContextJars(mahoutHome);
    super.setClassloaderUrls(mahoutJars);
  }



  public void setTestingMode(Boolean testing){
    this.testing = testing;
  }

  @Override
  public void open(Boolean testing) {
    if (this.testing){
      loadMahoutJars();
    }

    super.open();

    if (super.getSparkVersion().olderThan(SparkVersion.fromVersionString("1.5.0"))){
      logger.error("Spark Version 1.5.0+ only supported. Sorry Charlie.");
      new InterpreterResult(InterpreterResult.Code.ERROR, "Mahout-Spark requires version 1.5.0+");
    };
  }

  @Override
  public InterpreterResult interpret(String s, InterpreterContext context){
    if (firstRun) {
      if (this.testing){
        // use dependency loader to fetch jars from maven
        // https://zeppelin.incubator.apache.org/docs/latest/interpreter/spark.html
      }
      super.interpret("import org.apache.mahout.math._\n" +
              "import org.apache.mahout.math.scalabindings._\n" +
              "import org.apache.mahout.math.drm._\n" +
              "import org.apache.mahout.math.scalabindings.RLikeOps._\n" +
              "import org.apache.mahout.math.drm.RLikeDrmOps._\n" +
              "import org.apache.mahout.sparkbindings._\n" +
              "implicit val sdc: SparkDistributedContext = sc", context);
      firstRun = false;
    }

    return super.interpret(s, context);
  }

}
