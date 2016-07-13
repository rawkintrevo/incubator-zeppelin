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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;

import java.util.*;
import java.io.File;

import com.google.common.base.Joiner;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.HttpServer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.repl.SparkIMain;
import org.apache.zeppelin.dep.DependencyContext;
import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.spark.*;


import org.apache.zeppelin.spark.dep.SparkDependencyContext;
import org.apache.zeppelin.spark.dep.SparkDependencyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zeppelin.mahout.utils.*;

/**
 * Spart+Mahout interpreter for Zeppelin.
 *
 */

public class MahoutSparkInterpreter extends SparkInterpreter {

  private static final Logger logger = LoggerFactory.getLogger(MahoutSparkInterpreter.class);
  private boolean firstRun = true;

  private DependencyResolver dep;
  private SparkDependencyResolver sdep;
  private String[] jarPaths;

//  private SparkILoop interpreter;
//  private SparkIMain intp;
//  private static SparkContext sc;

  private List<String> artifacts = Arrays.asList(
          "org.apache.mahout:mahout-math:",
          "org.apache.mahout:mahout-math-scala_2.10:",
          "org.apache.mahout:mahout-spark_2.10:");

  private static String importStatement = "import org.apache.mahout.math._\n" +
          "import org.apache.mahout.math.scalabindings._\n" +
          "import org.apache.mahout.math.drm._\n" +
          "import org.apache.mahout.math.scalabindings.RLikeOps._\n" +
          "import org.apache.mahout.math.drm.RLikeDrmOps._\n" +
          "import org.apache.mahout.sparkbindings._\n";

  private static String setupSdcStatement = "implicit val sdc: SparkDistributedContext = sc";

  public MahoutSparkInterpreter(Properties property) {
    super(property);
  }

  private void loadMahoutJarsFromMahoutHome(){
    String mahoutHome;
    if (getProperty("mahout.home") != null) {
      mahoutHome = getProperty("mahout.home");
    } else if (System.getenv("MAHOUT_HOME") != null) {
      mahoutHome = System.getenv("MAHOUT_HOME");
    } else {
      logger.warn("MAHOUT_HOME not set in environement or settings, please do that.");
      mahoutHome = "";
    }
    //logger.info("Attempting to load Apache Mahout JARs from " + mahoutHome);
    URL[] mahoutJars = findMahoutContextJars(scanMahoutHome(mahoutHome));
    for (URL url: mahoutJars){ logger.info("URL: " + url.toString());  }
    //super.setClassloaderUrls(mahoutJars);
    setClassloaderUrls(mahoutJars);
    jarPaths = findMahoutContextJarsStr(scanMahoutHome(mahoutHome));
  }

  public void loadMahoutJarsFromMaven(String mahoutVersion) {
    String localRepo = getProperty("zeppelin.dep.localrepo");
    logger.info("Attemping to download Mahout jars to: " + localRepo);
    try {
      URL[] prevUrls = getClassloaderUrls();
      ArrayList<URL> newUrls = new ArrayList<URL>();
      ArrayList<String> newJars = new ArrayList<String>();
      dep = new DependencyResolver(localRepo);
      List <File> newFiles;

      for (String a: artifacts){
        newFiles = dep.load(a + mahoutVersion);
        newJars.add(a + mahoutVersion);
        for (File f: newFiles){
          newUrls.add(f.toURI().toURL());
        }
      }

      jarPaths = newJars.toArray(new String[newJars.size()]);

      URL[] newUrlArray = newUrls.toArray(new URL[newUrls.size()]);
      URL[] allClassLoaderUrls = (URL[]) ArrayUtils.addAll(prevUrls, newUrlArray);

      //super.setClassloaderUrls(allClassLoaderUrls);
      setClassloaderUrls(allClassLoaderUrls);
      logger.info("Class Path URLs are: " + allClassLoaderUrls.length);
      for (URL url: allClassLoaderUrls){ logger.info("URL: " + url.toString());  }
      logger.info("Successfully loaded Apache Mahout JARs (mahout.home=local)");
    } catch (java.lang.Exception e) {
      logger.error("Error Loading jars: " + e.getMessage(), e);
    }

  }

  @Override
  public void open() {

    if (!getProperty("mahout.home").equals("local")) {
      loadMahoutJarsFromMahoutHome();
    } else {
      String mahoutVersion = getProperty("mahout.version");
      loadMahoutJarsFromMaven(mahoutVersion);
    }

    super.open();

    if (!getProperty("master").equals("local[*]")) {
      sdep = getDependencyResolver();
      try {
        for (String a : jarPaths) {
          sdep.load(a, true);
        }
        sdep.load("com.google.guava:guava:14.0.1", true);
      } catch (java.lang.Exception e) { logger.error("Error Loading: " + e.getMessage(), e); }
    }

    if (getSparkVersion().olderThan(SparkVersion.fromVersionString("1.5.0"))){
      // todo move to before open and change mahout version to 0.11.1 if in 1.4.x
      new InterpreterResult(InterpreterResult.Code.ERROR, "Mahout-Spark requires version 1.5.0+");
    };

  }

  @Override
  public InterpreterResult interpret(String s, InterpreterContext context){

    if (firstRun) {
      super.interpret(importStatement + setupSdcStatement, context);
      firstRun = false;
    }
    if (getSparkVersion().olderThan(SparkVersion.fromVersionString("1.5.0"))){
      new InterpreterResult(InterpreterResult.Code.ERROR, "Mahout-Spark requires version 1.5.0+");
    }
    return super.interpret(s, context);
  }

/*
  @Override
  public void close() {
    logger.info("Close interpreter");
/*
    if (numReferenceOfSparkContext.decrementAndGet() == 0) {
      sc.stop();
      sc = null;
    }

    intp.close();
  }

  @Override
  public void destroy() {
    sc.stop();
    sc = null;
  }
*/
  /* no multiple spark context in zeppelin
  @Override //disable sync lock
  public SparkContext getSparkContext() {
    SparkContext sc = createSparkContext();
    return sc;

  }
*/
  /*
  @Override
  public SparkContext createSparkContext() {
    logger.info("------ Create new MahoutSparkContext {} -------", getProperty("master"));

    String execUri = System.getenv("SPARK_EXECUTOR_URI");
    String[] jars = SparkILoop.getAddedJars();

    String classServerUri = null;

    SparkILoop interpreter = getInterpreter(); //tg

    try { // in case of spark 1.1x, spark 1.2x
      Method classServer = interpreter.intp().getClass().getMethod("classServer");
      HttpServer httpServer = (HttpServer) classServer.invoke(interpreter.intp());
      classServerUri = httpServer.uri();
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
            | IllegalArgumentException | InvocationTargetException e) {
      // continue
    }

    if (classServerUri == null) {
      try { // for spark 1.3x
        Method classServer = interpreter.intp().getClass().getMethod("classServerUri");
        classServerUri = (String) classServer.invoke(interpreter.intp());
      } catch (NoSuchMethodException | SecurityException | IllegalAccessException
              | IllegalArgumentException | InvocationTargetException e) {
        // continue instead of: throw new InterpreterException(e);
        // Newer Spark versions (like the patched CDH5.7.0 one) don't contain this method
        logger.warn(String.format("Spark method classServerUri not available due to: [%s]",
                e.getMessage()));
      }
    }

    SparkConf conf =
            new SparkConf()
                    .setMaster(getProperty("master"))
                    .setAppName(getProperty("spark.app.name"));

    if (classServerUri != null) {
      conf.set("spark.repl.class.uri", classServerUri);
    }

    if (jars.length > 0) {
      conf.setJars(jars);
    }

    if (execUri != null) {
      conf.set("spark.executor.uri", execUri);
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"));
    }
    conf.set("spark.scheduler.mode", "FAIR");

    Properties intpProperty = getProperty();

    for (Object k : intpProperty.keySet()) {
      String key = (String) k;
      String val = intpProperty.get(key).toString();
      if (!key.startsWith("spark.") || !val.trim().isEmpty()) {
        logger.debug(String.format("SparkConf: key = [%s], value = [%s]", key, val));
        conf.set(key, val);
      }
    }

    // Distributes needed libraries to workers
    // when spark version is greater than or equal to 1.5.0
    if (getProperty("master").equals("yarn-client")) {
      conf.set("spark.yarn.isPython", "true");
    }

    setInterpreter(interpreter);

    SparkContext sparkContext = new SparkContext(conf);
    return sparkContext;
  }
*/
}
