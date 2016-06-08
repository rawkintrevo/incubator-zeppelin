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

package org.apache.zeppelin.mahout

import java.io._

import scala.collection._

object utils {
  def scanMahoutHome(mahoutHomeString: String) = {
    val fmhome: File = new File(mahoutHomeString);
    val closeables = mutable.ListBuffer.empty[Closeable]
    // Figure Mahout classpath using $MAHOUT_HOME/mahout classpath command.
    //val fmhome = new File(getMahoutHome())
    val bin = new File(fmhome, "bin")
    val exec = new File(bin, "mahout")
    if (!exec.canExecute)
      throw new IllegalArgumentException("Cannot execute %s.".format(exec.getAbsolutePath))

    val p = Runtime.getRuntime.exec(Array(exec.getAbsolutePath, "-spark", "classpath"))

    closeables += new Closeable {
      def close() {
        p.destroy()
      }
    }

    val r = new BufferedReader(new InputStreamReader(p.getInputStream))
    closeables += r

    val w = new StringWriter()
    closeables += w

    var continue = true
    val jars = new mutable.ArrayBuffer[String]()
    do {
      val cp = r.readLine()
      if (cp == null)
        throw new IllegalArgumentException("Unable to read output from \"mahout " +
          "-spark classpath\". Are SPARK_HOME and JAVA_HOME defined?")

      val j = cp.split(File.pathSeparatorChar)
      if (j.length > 10) {
        // assume this is a valid classpath line
        jars ++= j
        continue = false
      }
    } while (continue)

    jars.toArray
  }

  def findMahoutContextJars(jars: Array[String]) = {
    //    jars.foreach(j => log.info(j))
    // context specific jars
    val mcjars = jars.filter(j =>
      j.matches(".*mahout-math-\\d.*\\.jar") ||
      j.matches(".*mahout-math-scala_\\d.*\\.jar") ||
      j.matches(".*mahout-hdfs-\\d.*\\.jar") ||
      // no need for mapreduce jar in Spark
      // j.matches(".*mahout-mr-\\d.*\\.jar") ||
      j.matches(".*mahout-spark_\\d.*\\.jar")
    )
        // Tune out "bad" classifiers
        .filter(n =>
      !n.matches(".*-tests.jar") &&
          !n.matches(".*-sources.jar") &&
          !n.matches(".*-job.jar") &&
          // During maven tests, the maven classpath also creeps in for some reason
          !n.matches(".*/.m2/.*")
        )
    /* verify jar passed to context
    log.info("\n\n\n")
    mcjars.foreach(j => log.info(j))
    log.info("\n\n\n")
    */
    mcjars.map(f => new File(f).toURI.toURL).toArray
  }

  def findMahoutContextJarsStr(jars: Array[String]) = {
    //    jars.foreach(j => log.info(j))
    // context specific jars
    val mcjars = jars.filter(j =>
      j.matches(".*mahout-math-\\d.*\\.jar") ||
      j.matches(".*mahout-math-scala_\\d.*\\.jar") ||
      j.matches(".*mahout-hdfs-\\d.*\\.jar") ||
      // no need for mapreduce jar in Spark
      // j.matches(".*mahout-mr-\\d.*\\.jar") ||
      j.matches(".*mahout-spark_\\d.*\\.jar")
    )
        // Tune out "bad" classifiers
        .filter(n =>
      !n.matches(".*-tests.jar") &&
          !n.matches(".*-sources.jar") &&
          !n.matches(".*-job.jar") &&
          // During maven tests, the maven classpath also creeps in for some reason
          !n.matches(".*/.m2/.*")
        )
    /* verify jar passed to context
    log.info("\n\n\n")
    mcjars.foreach(j => log.info(j))
    log.info("\n\n\n")
    */
    mcjars.map(f => new File(f).getAbsolutePath).toArray
  }
}
