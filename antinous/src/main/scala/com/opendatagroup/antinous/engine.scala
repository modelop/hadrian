// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// 
// Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.antinous

import java.io.ByteArrayInputStream
import java.io.InputStream

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import scala.language.postfixOps

import org.python.core.CompileMode
import org.python.core.Py
import org.python.core.PyBytecode
import org.python.core.PyDictionary
import org.python.core.PyException
import org.python.core.PyFunction
import org.python.core.PyObject
import org.python.core.PyString
import org.python.core.ThreadState
import org.python.util.PythonInterpreter

import org.apache.avro.file.DataFileStream
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.Schema

import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.ast.Method
import com.opendatagroup.hadrian.data.AvroOutputDataStream
import com.opendatagroup.hadrian.data.JsonOutputDataStream
import com.opendatagroup.hadrian.{data => hadriandata}
import com.opendatagroup.hadrian.datatype.AvroCompiled
import com.opendatagroup.hadrian.datatype.AvroConversions
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroTypeBuilder
import com.opendatagroup.hadrian.datatype.ForwardDeclarationParser
import com.opendatagroup.hadrian.errors.PFAInitializationException
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.options.EngineOptions
import com.opendatagroup.hadrian.shared.SharedState

import com.opendatagroup.antinous.data.JythonDatumReader
import com.opendatagroup.antinous.data.JythonDatumWriter
import com.opendatagroup.antinous.data.JythonEnumSymbol
import com.opendatagroup.antinous.data.JythonFixed
import com.opendatagroup.antinous.data.JythonRecord
import com.opendatagroup.antinous.data.JythonSpecificData

import com.opendatagroup.antinous.translate.AvroToJythonDataTranslator
import com.opendatagroup.antinous.translate.PFAToJythonDataTranslator
import com.opendatagroup.antinous.translate.JythonToPFADataTranslator

package engine {
  class EmitFunction extends PyFunction(new PyDictionary, Array.empty[PyObject],
    // the PyCode is ignored; dummy (but vaguely relevant) values
    new PyBytecode(1, 1, 0, 67, "<internal>", Array.empty[PyObject], Array.empty[String], Array.empty[String], "<internal>", "emit", 1, "")) {

    // these signatures are always wrong
    override def __call__(arg0: PyObject, arg1: PyObject, arg2: PyObject, arg3: PyObject): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (4 given)")
    override def __call__(arg0: PyObject, arg1: PyObject, arg2: PyObject): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (3 given)")
    override def __call__(arg0: PyObject, arg1: PyObject): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (2 given)")
    override def __call__(): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (0 given)")
    override def __call__(state: ThreadState, arg0: PyObject, arg1: PyObject, arg2: PyObject, arg3: PyObject): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (4 given)")
    override def __call__(state: ThreadState, arg0: PyObject, arg1: PyObject, arg2: PyObject): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (3 given)")
    override def __call__(state: ThreadState, arg0: PyObject, arg1: PyObject): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (2 given)")
    override def __call__(state: ThreadState): PyObject =
      throw Py.TypeError("emit() takes exactly 1 argument (0 given)")

    // these signatures may be right
    override def __call__(arg0: PyObject, args: Array[PyObject], keywords: Array[String]): PyObject =
      if (args.size + 1 != 1)
        throw Py.TypeError(s"emit() takes exactly 1 argument (${args.size + 1} given)")
      else if (!keywords.isEmpty)
        throw Py.TypeError("emit() does not take keyword arguments")
      else
        __call__(arg0)
    override def __call__(args: Array[PyObject], keywords: Array[String]): PyObject =
      if (args.size != 1)
        throw Py.TypeError(s"emit() takes exactly 1 argument (${args.size} given)")
      else if (!keywords.isEmpty)
        throw Py.TypeError("emit() does not take keyword arguments")
      else
        __call__(args(0))
    override def __call__(args: Array[PyObject]): PyObject =
      if (args.size != 1)
        throw Py.TypeError(s"emit() takes exactly 1 argument (${args.size} given)")
      else
        __call__(args(0))
    override def __call__(state: ThreadState, arg0: PyObject, args: Array[PyObject], keywords: Array[String]): PyObject =
      __call__(arg0, args, keywords)
    override def __call__(state: ThreadState, arg0: PyObject): PyObject =
      __call__(arg0)
    override def __call__(state: ThreadState, args: Array[PyObject], keywords: Array[String]): PyObject =
      __call__(args, keywords)
    override def __call__(state: ThreadState, args: Array[PyObject]): PyObject =
      __call__(args)

    private[engine] var jythonToPFADataTranslator: JythonToPFADataTranslator = null

    // only implemented one signature; the rest call this
    override def __call__(arg0: PyObject): PyObject = {
      emit(jythonToPFADataTranslator.translate(arg0))
      Py.None
    }

    var emit: Function1[AnyRef, Unit] = {x: AnyRef => println(x)}
  }

  class LogFunction extends PyFunction(new PyDictionary, Array.empty[PyObject],
    // the PyCode is ignored; dummy (but vaguely relevant) values
    new PyBytecode(2, 2, 0, 67, "<internal>", Array.empty[PyObject], Array.empty[String], Array.empty[String], "<internal>", "emit", 1, "")) {

    // these signatures are always wrong
    override def __call__(arg0: PyObject, arg1: PyObject, arg2: PyObject, arg3: PyObject): PyObject =
      throw Py.TypeError("log() takes at most 2 arguments (4 given)")
    override def __call__(arg0: PyObject, arg1: PyObject, arg2: PyObject): PyObject =
      throw Py.TypeError("log() takes at most 2 arguments (3 given)")
    override def __call__(): PyObject =
      throw Py.TypeError("log() takes at least 1 argument (0 given)")
    override def __call__(state: ThreadState, arg0: PyObject, arg1: PyObject, arg2: PyObject, arg3: PyObject): PyObject =
      throw Py.TypeError("log() takes at most 2 arguments (4 given)")
    override def __call__(state: ThreadState, arg0: PyObject, arg1: PyObject, arg2: PyObject): PyObject =
      throw Py.TypeError("log() takes at most 2 arguments (3 given)")
    override def __call__(state: ThreadState): PyObject =
      throw Py.TypeError("log() takes at least 1 argument (0 given)")

    // these signatures may be right
    override def __call__(arg0: PyObject, args: Array[PyObject], keywords: Array[String]): PyObject =
      if (!keywords.isEmpty)
        throw Py.TypeError("log() does not take keyword arguments")
      else if (args.size + 1 < 1)
        throw Py.TypeError(s"log() takes at least 1 argument (${args.size + 1} given)")
      else if (args.size + 1 > 2)
        throw Py.TypeError(s"log() takes at most 2 arguments (${args.size + 1} given)")
      else if (args.size + 1 == 1)
        __call__(arg0, Py.None)
      else
        __call__(arg0, args(0))
    override def __call__(arg0: PyObject): PyObject =
      __call__(arg0, Py.None)
    override def __call__(args: Array[PyObject], keywords: Array[String]): PyObject =
      if (!keywords.isEmpty)
        throw Py.TypeError("log() does not take keyword arguments")
      else if (args.size < 1)
        throw Py.TypeError(s"log() takes at least 1 argument (${args.size} given)")
      else if (args.size > 2)
        throw Py.TypeError(s"log() takes at most 2 arguments (${args.size} given)")
      else if (args.size == 1)
        __call__(args(0), Py.None)
      else
        __call__(args(0), args(1))
    override def __call__(args: Array[PyObject]): PyObject =
      if (args.size < 1)
        throw Py.TypeError(s"log() takes at least 1 argument (${args.size} given)")
      else if (args.size > 2)
        throw Py.TypeError(s"log() takes at most 2 arguments (${args.size} given)")
      else if (args.size == 1)
        __call__(args(0), Py.None)
      else
        __call__(args(0), args(1))
    override def __call__(state: ThreadState, arg0: PyObject, arg1: PyObject): PyObject =
      __call__(arg0, arg1)
    override def __call__(state: ThreadState, arg0: PyObject, args: Array[PyObject], keywords: Array[String]): PyObject =
      __call__(arg0, args, keywords)
    override def __call__(state: ThreadState, arg0: PyObject): PyObject =
      __call__(arg0)
    override def __call__(state: ThreadState, args: Array[PyObject], keywords: Array[String]): PyObject =
      __call__(args, keywords)
    override def __call__(state: ThreadState, args: Array[PyObject]): PyObject =
      __call__(args)

    // only implemented one signature; the rest call this
    override def __call__(arg0: PyObject, arg1: PyObject): PyObject = {
      val x = arg0 match {
        case y: PyString => y.toString
        case _ => throw Py.TypeError("first argument of log() must be a string")
      }
      val ns = arg1 match {
        case y: PyString => Some(y.toString)
        case Py.None => None
        case _ => throw Py.TypeError("second argument of log() must be a string or None")
      }
      log(x, ns)
      Py.None
    }

    var log: Function2[String, Option[String], Unit] =
      (str, namespace) => namespace match {
        case Some(x) => println(x + ": " + str)
        case None => println(str)
      }
  }

  object JythonEngine {
    def factoryFromPython(in: InputStream, fileName: Option[String]): (() => JythonEngine) = {
      var instance = 0
      () => {
        val compiled = Py.compile(in, fileName.getOrElse("<string>"), CompileMode.exec)

        val pythonInterpreter = PythonInterpreter.threadLocalStateInterpreter(new PyDictionary)
        val pyEmit = new EmitFunction
        val pyLog = new LogFunction
        pythonInterpreter.set("emit", pyEmit)
        pythonInterpreter.set("log", pyLog)

        pythonInterpreter.exec(compiled)
        pythonInterpreter.exec("import antinous.prepare")
        val spec = pythonInterpreter.eval("antinous.prepare.normalizeEngine(locals())").asInstanceOf[PyDictionary]

        val avroTypeBuilder = new AvroTypeBuilder
        val inputTypePlaceholder = avroTypeBuilder.makePlaceholder(spec.get("input").toString)
        val outputTypePlaceholder = avroTypeBuilder.makePlaceholder(spec.get("output").toString)
        avroTypeBuilder.resolveTypes()

        val pyBegin = spec.get("begin").asInstanceOf[PyFunction]
        val pyAction = spec.get("action").asInstanceOf[PyFunction]
        val pyEnd = spec.get("end").asInstanceOf[PyFunction]
        val pyFcns = spec.get("fcns").asInstanceOf[java.util.Map[String, PyFunction]]

        val initialState = pythonInterpreter.eval("antinous.prepare.saveState(locals())").asInstanceOf[PyDictionary]
        def revertState(): Unit =
          for (varName: String <- pythonInterpreter.getLocals.asInstanceOf[java.util.Map[String, _]].keys)
            if (initialState.containsKey(varName))
              pythonInterpreter.set(varName, initialState.get(new PyString(varName)).__getattr__("revert").__call__())

        val jythonEngine = new JythonEngine(
          inputTypePlaceholder.avroType,
          outputTypePlaceholder.avroType,
          pyBegin,
          pyAction,
          pyEnd,
          pyFcns,
          revertState,
          instance,
          avroTypeBuilder.forwardDeclarationParser)
        instance += 1

        jythonEngine.pyEmit = pyEmit
        jythonEngine.pyEmit.jythonToPFADataTranslator = new JythonToPFADataTranslator(outputTypePlaceholder.avroType)

        jythonEngine.pyLog = pyLog
        jythonEngine
      }
    }

    def factoryFromPython(in: String, fileName: Option[String]): (() => JythonEngine) =
      factoryFromPython(new ByteArrayInputStream(in.getBytes), fileName)

    def factoryFromPython(in: java.io.File, fileName: Option[String]): (() => JythonEngine) =
      factoryFromPython(new java.io.FileInputStream(in), fileName)

    def fromPython(in: AnyRef, fileName: String = "<string>", multiplicity: Int = 1): Seq[JythonEngine] = {
      val factory = in match {
        case x: InputStream => factoryFromPython(x, Some(fileName))
        case x: String => factoryFromPython(x, Some(fileName))
        case x: java.io.File => factoryFromPython(x, Some(fileName))
      }
      Vector.fill(multiplicity)(factory())
    }
  }

  class JythonEngine(val inputType: AvroType, val outputType: AvroType, val pyBegin: PyFunction, val pyAction: PyFunction, val pyEnd: PyFunction, val pyFcns: java.util.Map[String, PyFunction], val revertState: () => Unit, val instance: Int, val typeParser: ForwardDeclarationParser) extends PFAEmitEngine[PyObject, AnyRef] {
    private var pyLog: LogFunction = null
    def log = pyLog.log
    def log_=(value: Function2[String, Option[String], Unit]) {
      pyLog.log = value
    }

    private var pyEmit: EmitFunction = null
    def emit = pyEmit.emit
    def emit_=(value: Function1[AnyRef, Unit]) {
      pyEmit.emit = value
    }

    val method = Method.EMIT

    def config: EngineConfig = throw new NotImplementedError
    def options: EngineOptions = throw new NotImplementedError
    def callGraph: Map[String, Set[String]] = throw new NotImplementedError
    def calledBy(fcnName: String, exclude: Set[String] = Set[String]()): Set[String] = throw new NotImplementedError
    def callDepth(fcnName: String, exclude: Set[String] = Set[String](), startingDepth: Double = 0): Double = throw new NotImplementedError
    def isRecursive(fcnName: String): Boolean = throw new NotImplementedError
    def hasRecursive(fcnName: String): Boolean = throw new NotImplementedError
    def hasSideEffects(fcnName: String): Boolean = throw new NotImplementedError

    val specificData = new JythonSpecificData(getClass.getClassLoader)
    def datumReader: DatumReader[PyObject] = new JythonDatumReader[PyObject](specificData)
    val inputClass: java.lang.Class[AnyRef] = classOf[PyObject].asInstanceOf[Class[AnyRef]]
    val outputClass: java.lang.Class[AnyRef] = classOf[AnyRef]
    val classLoader: java.lang.ClassLoader = getClass.getClassLoader
    
    private def exceptionString(err: PyException): String = {
      val preamble = PyException.exceptionClassName(err.`type`)
      if (preamble.startsWith("exceptions."))
        preamble.replace("exceptions.", "") + ": " + err.value.__str__.toString + "\nPython " + err.toString + "Java Traceback (most recent call first):"
      else
        err.value.__str__.toString
    }

    private val lock = new AnyRef()

    def begin(): Unit = lock.synchronized {
      try {
        pyBegin.__call__()
      }
      catch {
        case err: PyException => throw new PFARuntimeException(exceptionString(err), err)
      }
    }

    def action(input: PyObject): PyObject = lock.synchronized {
      try {
        pyAction.__call__(input)
      }
      catch {
        case err: PyException => throw new PFARuntimeException(exceptionString(err), err)
      }
    }

    def end(): Unit = lock.synchronized {
      try {
        pyEnd.__call__()
      }
      catch {
        case err: PyException => throw new PFARuntimeException(exceptionString(err), err)
      }
    }

    def snapshotCell(name: String): AnyRef = throw new NotImplementedError
    def snapshotPool(name: String): Map[String, AnyRef] = throw new NotImplementedError
    def snapshot: EngineConfig = throw new NotImplementedError
    def analyzeCell[X](name: String, analysis: Any => X): X = throw new NotImplementedError
    def analyzePool[X](name: String, analysis: Any => X): Map[String, X] = throw new NotImplementedError

    def namedTypes: Map[String, AvroCompiled] = throw new NotImplementedError

    def avroInputIterator[X](inputStream: InputStream): DataFileStream[X] = {    // DataFileStream is a java.util.Iterator
      val reader = new JythonDatumReader[X](specificData)
      reader.setSchema(inputType.schema)
      val out = new DataFileStream[X](inputStream, reader)
      
      val fileSchema = AvroConversions.schemaToAvroType(out.getSchema)
      if (!inputType.accepts(fileSchema))
        throw new PFAInitializationException("InputStream has schema %s\nbut expecting schema %s".format(fileSchema, inputType))
      out
    }

    def jsonInputIterator[X](inputStream: InputStream): java.util.Iterator[X] = {
      val reader = new JythonDatumReader[X](specificData)
      reader.setSchema(inputType.schema)
      val scanner = new java.util.Scanner(inputStream)

      new java.util.Iterator[X] {
        def hasNext(): Boolean = scanner.hasNextLine
        def next(): X = {
          val json = scanner.nextLine()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
        def remove(): Unit = throw new java.lang.UnsupportedOperationException
      }
    }

    def jsonInputIterator[X](inputIterator: java.util.Iterator[String]): java.util.Iterator[X] = {
      val reader = new JythonDatumReader[X](specificData)
      reader.setSchema(inputType.schema)

      new java.util.Iterator[X] {
        def hasNext(): Boolean = inputIterator.hasNext
        def next(): X = {
          val json = inputIterator.next()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
        def remove(): Unit = throw new java.lang.UnsupportedOperationException
      }
    }

    def jsonInputIterator[X](inputIterator: scala.collection.Iterator[String]): scala.collection.Iterator[X] = {
      val reader = new JythonDatumReader[X](specificData)
      reader.setSchema(inputType.schema)

      new scala.collection.Iterator[X] {
        override def hasNext: Boolean = inputIterator.hasNext
        override def next(): X = {
          val json = inputIterator.next()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
      }
    }

    private def fromJson(json: Array[Byte], schema: Schema): AnyRef = {
      val reader = new JythonDatumReader[AnyRef](specificData)
      reader.setSchema(schema)
      val decoder = DecoderFactory.get.jsonDecoder(schema, new ByteArrayInputStream(json))
      reader.read(null, decoder)
    }

    def jsonInput(json: Array[Byte]): PyObject = fromJson(json, inputType.schema).asInstanceOf[PyObject]
    def jsonInput(json: String): PyObject = fromJson(json.getBytes, inputType.schema).asInstanceOf[PyObject]

    def avroOutputDataStream(outputStream: java.io.OutputStream): AvroOutputDataStream =
      hadriandata.avroOutputDataStream(outputStream, outputType)
    def avroOutputDataStream(file: java.io.File): AvroOutputDataStream =
      hadriandata.avroOutputDataStream(file, outputType)
    def avroOutputDataStream(fileName: String): AvroOutputDataStream =
      hadriandata.avroOutputDataStream(fileName, outputType)
    def jsonOutputDataStream(outputStream: java.io.OutputStream, writeSchema: Boolean): JsonOutputDataStream =
      hadriandata.jsonOutputDataStream(outputStream, outputType, writeSchema)
    def jsonOutputDataStream(file: java.io.File, writeSchema: Boolean): JsonOutputDataStream =
      hadriandata.jsonOutputDataStream(file, outputType, writeSchema)
    def jsonOutputDataStream(fileName: String, writeSchema: Boolean): JsonOutputDataStream =
      hadriandata.jsonOutputDataStream(fileName, outputType, writeSchema)
    def jsonOutput(obj: AnyRef): String =
      hadriandata.toJson(obj, outputType)

    private val pfaToJythonDataTranslator = new PFAToJythonDataTranslator(inputType, specificData)
    private val avroToJythonDataTranslator = new AvroToJythonDataTranslator(inputType, specificData)
    def fromPFAData(datum: AnyRef): PyObject = pfaToJythonDataTranslator.translate(datum)
    def fromGenericAvroData(datum: AnyRef): PyObject = avroToJythonDataTranslator.translate(datum)

    def randomGenerator: Random = throw new NotImplementedError

    def revert(): Unit = lock.synchronized {
      try {
        revertState()
      }
      catch {
        case err: PyException => throw new PFARuntimeException(exceptionString(err), err)
      }
    }
    def revert(sharedState: Option[SharedState]): Unit = throw new NotImplementedError
    
    def fcn0(name: String): Function0[AnyRef] =
      {() => lock synchronized pyFcns.get(name).__call__()}
    def fcn1(name: String): Function1[AnyRef, AnyRef] =
      {arg1: AnyRef => lock synchronized pyFcns.get(name).__call__(fromPFAData(arg1))}
    def fcn2(name: String): Function2[AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef) => lock synchronized pyFcns.get(name).__call__(fromPFAData(arg1), fromPFAData(arg2))}
    def fcn3(name: String): Function3[AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef) => lock synchronized pyFcns.get(name).__call__(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3))}
    def fcn4(name: String): Function4[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef) => lock synchronized pyFcns.get(name).__call__(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4))}
    def fcn5(name: String): Function5[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5)))}
    def fcn6(name: String): Function6[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6)))}
    def fcn7(name: String): Function7[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7)))}
    def fcn8(name: String): Function8[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8)))}
    def fcn9(name: String): Function9[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9)))}
    def fcn10(name: String): Function10[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10)))}
    def fcn11(name: String): Function11[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11)))}
    def fcn12(name: String): Function12[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12)))}
    def fcn13(name: String): Function13[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13)))}
    def fcn14(name: String): Function14[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14)))}
    def fcn15(name: String): Function15[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15)))}
    def fcn16(name: String): Function16[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef, arg16: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15), fromPFAData(arg16)))}
    def fcn17(name: String): Function17[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef, arg16: AnyRef, arg17: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15), fromPFAData(arg16), fromPFAData(arg17)))}
    def fcn18(name: String): Function18[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef, arg16: AnyRef, arg17: AnyRef, arg18: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15), fromPFAData(arg16), fromPFAData(arg17), fromPFAData(arg18)))}
    def fcn19(name: String): Function19[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef, arg16: AnyRef, arg17: AnyRef, arg18: AnyRef, arg19: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15), fromPFAData(arg16), fromPFAData(arg17), fromPFAData(arg18), fromPFAData(arg19)))}
    def fcn20(name: String): Function20[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef, arg16: AnyRef, arg17: AnyRef, arg18: AnyRef, arg19: AnyRef, arg20: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15), fromPFAData(arg16), fromPFAData(arg17), fromPFAData(arg18), fromPFAData(arg19), fromPFAData(arg20)))}
    def fcn21(name: String): Function21[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef, arg16: AnyRef, arg17: AnyRef, arg18: AnyRef, arg19: AnyRef, arg20: AnyRef, arg21: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15), fromPFAData(arg16), fromPFAData(arg17), fromPFAData(arg18), fromPFAData(arg19), fromPFAData(arg20), fromPFAData(arg21)))}
    def fcn22(name: String): Function22[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] =
      {(arg1: AnyRef, arg2: AnyRef, arg3: AnyRef, arg4: AnyRef, arg5: AnyRef, arg6: AnyRef, arg7: AnyRef, arg8: AnyRef, arg9: AnyRef, arg10: AnyRef, arg11: AnyRef, arg12: AnyRef, arg13: AnyRef, arg14: AnyRef, arg15: AnyRef, arg16: AnyRef, arg17: AnyRef, arg18: AnyRef, arg19: AnyRef, arg20: AnyRef, arg21: AnyRef, arg22: AnyRef) => lock synchronized pyFcns.get(name).__call__(Array(fromPFAData(arg1), fromPFAData(arg2), fromPFAData(arg3), fromPFAData(arg4), fromPFAData(arg5), fromPFAData(arg6), fromPFAData(arg7), fromPFAData(arg8), fromPFAData(arg9), fromPFAData(arg10), fromPFAData(arg11), fromPFAData(arg12), fromPFAData(arg13), fromPFAData(arg14), fromPFAData(arg15), fromPFAData(arg16), fromPFAData(arg17), fromPFAData(arg18), fromPFAData(arg19), fromPFAData(arg20), fromPFAData(arg21), fromPFAData(arg22)))}
  }
}
