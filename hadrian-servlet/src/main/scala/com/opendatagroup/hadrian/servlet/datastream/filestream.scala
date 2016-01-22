package com.opendatagroup.hadrian.servlet.datastream

import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

import com.opendatagroup.hadrian.servlet.engine._

package object filestream {
  def jsonFileInputDataStream(file: java.io.File) = new InputDataStream {
    val avroType = None

    var scanner: java.util.Scanner = null
    def restart() {
      scanner = new java.util.Scanner(new java.io.FileInputStream(file))
    }
    restart()

    def hasNext = scanner.hasNextLine
    def next() = {
      val jsonText = scanner.nextLine();  // semicolon required
      {eng: PFAEngine[AnyRef, AnyRef] => eng.jsonInput(jsonText)}
    }
  }

  def jsonFileOutputDataStream(file: java.io.File) = new OutputDataStream {
    val printWriter = new java.io.PrintWriter(new java.io.FileOutputStream(file))
    def apply(eng: PFAEngine[AnyRef, AnyRef], output: AnyRef) {
      printWriter.println(eng.jsonOutput(output))
      printWriter.flush()
    }
  }

}
