// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.hadrian

import java.io.ByteArrayInputStream

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.runtime.ScalaRunTime

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.JsonToken
import org.codehaus.jackson.node.BinaryNode
import org.codehaus.jackson.node.BooleanNode
import org.codehaus.jackson.node.DoubleNode
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.JsonNodeFactory
import org.codehaus.jackson.node.LongNode
import org.codehaus.jackson.node.NullNode
import org.codehaus.jackson.node.TextNode

import org.apache.avro.io.DecoderFactory

import com.opendatagroup.hadrian.data.PFADatumReader
import com.opendatagroup.hadrian.data.PFASpecificData
import com.opendatagroup.hadrian.data.toAvro
import com.opendatagroup.hadrian.data.toJson
import com.opendatagroup.hadrian.errors.PFASemanticException
import com.opendatagroup.hadrian.errors.PFASyntaxException
import com.opendatagroup.hadrian.errors.PFAInitializationException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.JVMCompiler
import com.opendatagroup.hadrian.jvmcompiler.JVMNameMangle
import com.opendatagroup.hadrian.jvmcompiler.W
import com.opendatagroup.hadrian.options.EngineOptions
import com.opendatagroup.hadrian.util.convertFromJson
import com.opendatagroup.hadrian.util.convertToJson

import com.opendatagroup.hadrian.signature.IncompatibleTypes
import com.opendatagroup.hadrian.signature.LabelData
import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs
import com.opendatagroup.hadrian.signature.PFAVersion

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.ExceptionType
import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroCompiled
import com.opendatagroup.hadrian.datatype.AvroNumber
import com.opendatagroup.hadrian.datatype.AvroRaw
import com.opendatagroup.hadrian.datatype.AvroIdentifier
import com.opendatagroup.hadrian.datatype.AvroContainer
import com.opendatagroup.hadrian.datatype.AvroMapping
import com.opendatagroup.hadrian.datatype.AvroNull
import com.opendatagroup.hadrian.datatype.AvroBoolean
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroBytes
import com.opendatagroup.hadrian.datatype.AvroFixed
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.datatype.AvroEnum
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion
import com.opendatagroup.hadrian.datatype.AvroPlaceholder
import com.opendatagroup.hadrian.datatype.ForwardDeclarationParser

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.reader.JsonDom
import com.opendatagroup.hadrian.reader.JsonObject
import com.opendatagroup.hadrian.reader.JsonArray
import com.opendatagroup.hadrian.reader.JsonNull
import com.opendatagroup.hadrian.reader.JsonTrue
import com.opendatagroup.hadrian.reader.JsonFalse
import com.opendatagroup.hadrian.reader.JsonString
import com.opendatagroup.hadrian.reader.JsonLong
import com.opendatagroup.hadrian.reader.JsonDouble

package object ast {
  /** Utility function to infer the type of a given expression.
    * 
    * @param expr expression to examine
    * @param symbols data types of variables used in the expression
    * @param cells data types of cells used in the expression
    * @param pools data types of pools used in the expression
    * @param fcns functions used in the expression
    * @return data type of the expression's return value
    */
  def inferType(
    expr: Expression,
    symbols: Map[String, AvroType] = Map[String, AvroType](),
    cells: Map[String, Cell] = Map[String, Cell](),
    pools: Map[String, Pool] = Map[String, Pool](),
    fcns: Map[String, UserFcn] = Map[String, UserFcn](),
    version: PFAVersion = PFAVersion(0, 0, 0)): AvroType = {

    val symbolTable = SymbolTable(None, mutable.Map[String, AvroType](symbols.toSeq: _*), cells, pools, true, false)
    val functionTable = FunctionTable(FunctionTable.blank.functions ++ fcns)

    val (context, result) = expr.walk(NoTask, symbolTable, functionTable, new EngineOptions(Map[String, JsonNode](), Map[String, JsonNode]()), version)
    context.asInstanceOf[ExpressionContext].retType
  }
}

package ast {
  //////////////////////////////////////////////////////////// symbols
  /** Determine if a symbol name is valid.
    */
  object validSymbolName extends Function1[String, Boolean] {
    /** `[A-Za-z_][A-Za-z0-9_]*`
      */
    val pattern = "[A-Za-z_][A-Za-z0-9_]*".r
    /** @param test symbol (variable) name to check
      * @return `true` if valid; `false` otherwise
      */
    def apply(test: String): Boolean =
      pattern.unapplySeq(test) != None
  }

  /** Represents the symbols (variables) and their data types in a lexical scope.
    * 
    * @param parent enclosing scope; symbol lookup defers to the parent scope if not found here
    * @param symbols internal symbol names and their types
    * @param cells initial cell names and their types
    * @param pools initial pool names and their types
    * @param sealedAbove if `true`, symbols in the parent scope cannot be modified (but can be accessed); if `false`, this scope does not restrict access (but a parent might)
    */
  case class SymbolTable(parent: Option[SymbolTable], symbols: mutable.Map[String, AvroType], cells: Map[String, Cell], pools: Map[String, Pool], sealedAbove: Boolean, sealedWithin: Boolean) {
    /** Get a symbol's type specifically from '''this''' scope.
      * 
      * @param name name of the symbol
      * @return the symbol's type if defined in '''this''' scope, `None` otherwise
      */
    def getLocal(name: String): Option[AvroType] = symbols.get(name)
    /** Get a symbol's type specifically from a '''parent's''' sopce, not this one.
      * 
      * @param name name of the symbol
      * @return the symbol's type if defined in a '''parent's''' scope, `None` otherwise
      */
    def getAbove(name: String): Option[AvroType] = parent match {
      case Some(p) => p.get(name)
      case None => None
    }
    /** Get a symbol's type from this scope or a parent's.
      * 
      * @param name name of the symbol
      * @return the symbol's type if defined, `None` otherwise
      */
    def get(name: String): Option[AvroType] = getLocal(name) orElse getAbove(name)
    /** Get a symbol's type from this scope or a parent's and raise a `java.util.NoSuchElementException` if not defined
      * 
      * @param name name of the symbol
      * @return the symbol's type if defined, raise a `java.util.NoSuchElementException` otherwise
      */
    def apply(name: String): AvroType = get(name).get

    /** Determine if a symbol can be modified in this scope.
      * 
      * @param name name of the symbol
      * @return `true` if the symbol can be modified; `false` otherwise
      */
    def writable(name: String): Boolean =
      if (sealedWithin)
        false
      else
        symbols.get(name) match {
          case Some(x) => true
          case None =>
            if (sealedAbove)
              false
            else
              parent match {
                case Some(p) => p.writable(name)
                case None => throw new IllegalArgumentException("no such symbol: " + name)
              }
        }

    /** Creaste or overwrite a symbol's type in the table.
      * 
      * @param name name of the symbol
      * @param avroType the data type to associate with this symbol
      */
    def put(name: String, avroType: AvroType): Unit = symbols.put(name, avroType)

    /** Get a cell's type from this scope or a parent's.
      * 
      * @param name name of the cell
      * @return the cell's type if defined, `None` otherwise
      */
    def cell(name: String): Option[Cell] = cells.get(name) orElse (parent match {
      case Some(p) => p.cell(name)
      case None => None
    })

    /** Get a pool's type from this scope or a parent's.
      * 
      * @param name name of the cell
      * @return the pool's type if defined, `None` otherwise
      */
    def pool(name: String): Option[Pool] = pools.get(name) orElse (parent match {
      case Some(p) => p.pool(name)
      case None => None
    })

    /** Create a new scope with this as parent.
      * 
      * @param sealedAbove if `true`, symbols in the parent scope cannot be modified (but can be accessed); if `false`, this scope does not restrict access (but a parent might)
      * @param sealedWithin if `true`, new symbols cannot be created in this scope
      * @return a new scope, linked to this one
      */
    def newScope(sealedAbove: Boolean, sealedWithin: Boolean): SymbolTable =
      SymbolTable(Some(this), mutable.Map[String, AvroType](), Map[String, Cell](), Map[String, Pool](), sealedAbove, sealedWithin)

    /** All symbols (and their types) that are defined in this scope ('''not''' in any parents).
      * 
      * @return symbols and their types
      */
    def inThisScope: Map[String, AvroType] = symbols.toMap
    /** All symbols (and their types) that are defined in this scope and all parents.
      * 
      * @return symbols and their types
      */
    def allInScope: Map[String, AvroType] = parent match {
      case Some(p) => p.allInScope ++ symbols.toMap
      case None => symbols.toMap
    }
  }
  object SymbolTable {
    /** Create a blank symbol table.
      * 
      * @return a symbol table containing nothing
      */
    def blank = new SymbolTable(None, mutable.Map[String, AvroType](), Map[String, Cell](), Map[String, Pool](), true, false)
  }

  //////////////////////////////////////////////////////////// functions

  /** Determine if a function name is valid.
    */
  object validFunctionName extends Function1[String, Boolean] {
    /** `[A-Za-z_]([A-Za-z0-9_]|\\.[A-Za-z][A-Za-z0-9_]*)*`
      */
    val pattern = "[A-Za-z_]([A-Za-z0-9_]|\\.[A-Za-z][A-Za-z0-9_]*)*".r
    /** @param test function name to check
    * @return `true` if valid; `false` otherwise
    */
    def apply(test: String): Boolean =
      pattern.unapplySeq(test) != None
  }

  /** Trait for a function in PFA: could be a library function, user-defined function, or emit.
    */
  trait Fcn {
    /** Signature of the function as used in PFA.
      */
    def sig: Signature
    /** Java code for a reference to this function (not a call of this function).
      * 
      * @param fcnType argument and return types after generics-resolution
      * @return Java code for a reference to this function
      */
    def javaRef(fcnType: FcnType): JavaCode
    /** Java code for a call of this function (not a reference to the function).
      * 
      * @param args Java code for each argument
      * @param argContext context objects for the arguments, after semantics checks
      * @param paramTypes argument types after generics-resolution
      * @param retType return type after generics-resolution
      * @param engineOptions global options for this scoring engine (may be set in PFA or overridden by host environment)
      * @return Java code for a call of this function
      */
    def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode

    /** Utility function for adding explicit casts to each argument.
      * 
      * @param args Java code for each argument
      * @param paramTypes argument types after generics-resolution
      * @param boxed if `true`, cast with boxed primitives; if `false`, use the raw primitives
      * @return Java code for the arguments (as a String)
      */
    def wrapArgs(args: Seq[JavaCode], paramTypes: Seq[Type], boxed: Boolean): String =
      args zip paramTypes map {
        case (j, t: AvroType) => W.wrapExpr(j.toString, t, boxed)
        case (j, t) => j.toString
      } mkString(", ")

    /** Utility function for adding an explicit cast to one argument.
      * 
      * @param i argument number
      * @param args Java code for each argument
      * @param paramTypes argument types after generics-resolution
      * @param boxed if `true`, cast as a boxed primitive; if `false` use a raw primitive
      * @return Java code for one argument (as a String)
      */
    def wrapArg(i: Int, args: Seq[JavaCode], paramTypes: Seq[Type], boxed: Boolean): String = (args(i), paramTypes(i)) match {
      case (j, t: AvroType) => W.wrapExpr(j.toString, t, boxed)
      case (j, t) => j.toString
    }

    /** Write a deprecation warning on standard error if a matched signature is in the deprecated interval of its lifespan, given the requested PFA version.
      * 
      * @param sig the signature (we assume that is has already been matched)
      * @param version the requested PFA version
      */
    def deprecationWarning(sig: Sig, version: PFAVersion) {}
  }

  /** Trait for a library function in PFA.
    * 
    * Each library function is a class inheriting from this trait and each library function used in a live scoring engine is an instance of that class with a different `pos`.
    */
  trait LibFcn extends Fcn {
    /** PFA name of the function, with prefix (e.g. `"model.tree.simpleTree"`).
      */
    def name: String
    /** Documentation XML for the function.
      */
    def doc: scala.xml.Elem
    /** First error code number for runtime errors in this function (if any). Used only as a baseline for counting.
      */
    def errcodeBase: Int
    def javaRef(fcnType: FcnType): JavaCode = JavaCode("(new " + this.getClass.getName + "(" + posToJava + "))")
    def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("%s.apply(%s)", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArgs(args, paramTypes, true))
    /** Keeps track of the original source code line number where this function was called.
      */
    def pos: Option[String]
    /** Utility function to write the `pos` as Java code.
      */
    def posToJava: String = pos match {
      case None => "scala.Option.apply((String)null)"
      case Some(x) => "scala.Option.apply(\"" + StringEscapeUtils.escapeJava(x) + "\")"
    }
    override def deprecationWarning(sig: Sig, version: PFAVersion) {
      if (sig.lifespan.deprecated(version)) {
        val contingency = sig.lifespan.contingency match {
          case Some(x) => "; " + x
          case None => ""
        }
        System.err.println(s"WARNING: $name$sig is deprecated in PFA $version, will be removed in PFA ${sig.lifespan.death.get}$contingency")
      }
    }
  }

  /** Represents a user-defined function.
    * 
    * @param name name of the function
    * @param sig function signature (note that [[com.opendatagroup.hadrian.signature.Sigs Sigs]] is not allowed for user-defined functions)
    */
  case class UserFcn(name: String, sig: Sig) extends Fcn {
    import JVMNameMangle._
    def javaRef(fcnType: FcnType) = JavaCode("(new %s())", f(name))
    def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("""(new %s()).apply(%s)""", f(name), wrapArgs(args, paramTypes, true))
  }
  object UserFcn {
    /** Create an executable function object from an abstract syntax tree of a function definition.
      * 
      * @param n name of the new function
      * @param fcnDef the abstract syntax tree function definition
      * @return the executable function
      */
    def fromFcnDef(n: String, fcnDef: FcnDef): UserFcn =
      UserFcn(n, Sig(fcnDef.params.map({case (k, t) => (k, P.fromType(t))}), P.fromType(fcnDef.ret)))
  }

  /** The special `emit` function.
    * 
    * @param outputType output type of the PFA document, which is also known as the signature of the `emit` function
    */
  case class EmitFcn(outputType: AvroType) extends Fcn {
    val sig = Sig(List(("output", P.fromType(outputType))), P.Null)
    def javaRef(fcnType: FcnType): JavaCode = throw new PFASemanticException("cannot reference the emit function", None)
    def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("com.opendatagroup.hadrian.jvmcompiler.W.n(f_emit.apply(%s))", wrapArg(0, args, paramTypes, true).toString)
  }

  /** Represents a table of all accessible PFA function names, such as library functions, user-defined functions, and possibly emit.
    * 
    * @param functions function lookup table
    */
  case class FunctionTable(functions: Map[String, Fcn])
  object FunctionTable {
    /** Create a function table containing nothing but library functions.
      * 
      * This is where all the PFA library modules are enumerated.
      */
    def blank = new FunctionTable(
      lib.core.provides ++
      lib.math.provides ++
      lib.spec.provides ++
      lib.link.provides ++
      lib.kernel.provides ++
      lib.la.provides ++
      lib.metric.provides ++
      lib.rand.provides ++
      lib.string.provides ++
      lib.regex.provides ++
      lib.parse.provides ++
      lib.cast.provides ++
      lib.array.provides ++
      lib.map.provides ++
      lib.bytes.provides ++
      lib.fixed.provides ++
      lib.enum.provides ++
      lib.time.provides ++
      lib.impute.provides ++
      lib.interp.provides ++
      lib.prob.dist.provides ++
      lib.stat.test.provides ++
      lib.stat.sample.provides ++
      lib.stat.change.provides ++
      lib.model.reg.provides ++
      lib.model.tree.provides ++
      lib.model.cluster.provides ++
      lib.model.neighbor.provides ++
      lib.model.naive.provides ++
      lib.model.neural.provides ++
      lib.model.svm.provides
    )  // TODO: ++ lib.other.provides ++ lib.stillother.provides
  }

  //////////////////////////////////////////////////////////// type-checking and transforming ASTs

  /** Trait for [[com.opendatagroup.hadrian.ast.Ast Ast]] context classes.
    */
  trait AstContext
  /** Subtrait for context classes of [[com.opendatagroup.hadrian.ast.Argument Argument]].
    */
  trait ArgumentContext extends AstContext {
    val calls: Set[String]
  }
  /** Subtrait for context classes of [[com.opendatagroup.hadrian.ast.Expression Expression]].
    */
  trait ExpressionContext extends ArgumentContext {
    val retType: AvroType
  }
  /** Subtrait for context classes of [[com.opendatagroup.hadrian.ast.FcnDef FcnDef]].
    */
  trait FcnContext extends ArgumentContext {
    val fcnType: FcnType
  }
  /** Trait for result of a generic task, passed to [[com.opendatagroup.hadrian.ast.Ast Ast]] `walk`.
    */
  trait TaskResult

  /** Trait for a generic task, passed to [[com.opendatagroup.hadrian.ast.Ast Ast]] `walk`.
    */
  trait Task extends Function3[AstContext, EngineOptions, Option[Type], TaskResult] {
    /** Perform a task from context generated by a [[com.opendatagroup.hadrian.ast.Ast Ast]] `walk`.
      * 
      * @param astContext data about the [[com.opendatagroup.hadrian.ast.Ast Ast]] node after type-checking
      * @param engineOptions implementation options
      * @param resolvedType ?
      * @return the result of this task
      */
    def apply(astContext: AstContext, engineOptions: EngineOptions, resolvedType: Option[Type] = None): TaskResult
  }

  /** Concrete [[com.opendatagroup.hadrian.ast.Task Task]] that does nothing, used for type-checking without producing an engine.
    */
  object NoTask extends Task {
    /** Concrete [[com.opendatagroup.hadrian.ast.TaskResult TaskResult]] that contains no result.
      */
    case object EmptyResult extends TaskResult
    /** Do nothing.
      * 
      * @param astContext data about the [[com.opendatagroup.hadrian.ast.Ast Ast]] node after type-checking
      * @param engineOptions implementation options
      * @param resolvedType ?
      * @return empty result object
      */
    def apply(astContext: AstContext, engineOptions: EngineOptions, resolvedType: Option[Type] = None): TaskResult = {EmptyResult}
  }

  //////////////////////////////////////////////////////////// AST nodes

  /** Abstract base class for a PFA abstract syntax tree.
    */
  trait Ast {
    /** Position in the original source code where this AST element resides (if any).
      */
    def pos: Option[String]
    /** Check equality for all fields ''except'' `pos`.
      */
    override def equals(other: Any): Boolean
    /** Compute hash code for all fields ''except'' `pos`.
      */
    override def hashCode(): Int
    /** Walk over tree applying a partial function, returning a list of results in its domain.
      * 
      * @param pf partial function that takes any [[com.opendatagroup.hadrian.ast.Ast Ast]] as an argument, returning anything
      * @return a result for each abstract syntax tree node in the `pf` function's domain
      */
    def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      if (pf.isDefinedAt(this)) List(pf.apply(this)) else List[X]()
    /** Walk over tree applying a partial function, returning a transformed copy of the tree.
      * 
      * @param pf partial function that takes any [[com.opendatagroup.hadrian.ast.Ast Ast]] as an argument, returning a replacement [[com.opendatagroup.hadrian.ast.Ast Ast]]
      * @return tree with nodes in the `pf` function's domain transformed; everything else left as-is
      */
    def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this)) pf.apply(this) else this

    /** Walk over tree applying a [[com.opendatagroup.hadrian.ast.Task Task]] while checking for semantic errors.
      * 
      * This is how Java is generated from an abstract syntax tree: the [[com.opendatagroup.hadrian.ast.Task Task]] in that case is [[com.opendatagroup.hadrian.jvmcompiler.JVMCompiler JVMCompiler]].
      * 
      * @param task generic task to perform on this abstract syntax tree node's context
      * @param symbolTable used to look up symbols, cells, and pools
      * @param functionTable used to look up functions
      * @param engineOptions implementation options
      * @param version version of the PFA language in which to interpret this PFA
      * @return (information about this abstract syntax tree node after type-checking, result of the generic task)
      */
    def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult)
    /** Walk with a blank `symbolTable`, a blank `functionTable`, and empty `engineOptions`
      */
    def walk(task: Task, version: PFAVersion): TaskResult = walk(task, SymbolTable.blank, FunctionTable.blank, new EngineOptions(Map[String, JsonNode](), Map[String, JsonNode]()), version)._2

    /** Serialize this abstract syntax tree as a JSON string.
      * 
      * @param lineNumbers if `true`, include locator marks at the beginning of each JSON object
      * @return JSON string
      */
    def toJson(lineNumbers: Boolean = true) = convertToJson(jsonNode(lineNumbers, mutable.Set[String]()))
    /** Convert this abstract syntax tree into a Jackson node
      * 
      * @param lineNumbers if `true`, include locator marks at the beginning of each JSON object
      * @param memo used to avoid recursion; provide an empty set if unsure
      * @return Jackson representation of the JSON
      */
    def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode

    /** Calls `toJson(false)`.
      */
    override def toString(): String = toJson(false)
  }

  /** PFA execution method may be "map", "emit", or "fold".
    */
  object Method extends Enumeration {
    type Method = Value
    val MAP, EMIT, FOLD = Value
  }

  /** Abstract syntax tree for a whole PFA document.
    * 
    * @param name name of the PFA engine (may be auto-generated)
    * @param method execution method ("map", "emit", or "fold")
    * @param inputPlaceholder input type as a placeholder (so it can exist before type resolution)
    * @param outputPlaceholder output type as a placeholder (so it can exist before type resolution)
    * @param begin "begin" algorithm
    * @param action "action" algorithm
    * @param end "end" algorithm
    * @param fcns user-defined functions
    * @param zero initial value for "fold" `tally`
    * @param merge "merge" algorithm for "fold"
    * @param cells "cell" definitions
    * @param pools "pool" definitions
    * @param randseed random number seed
    * @param doc optional documentation string
    * @param version optional version number
    * @param metadata computer-readable documentation
    * @param options implementation options
    * @param pos source file location from the locator mark
    */
  case class EngineConfig(
    name: String,
    method: Method.Method,
    inputPlaceholder: AvroPlaceholder,
    outputPlaceholder: AvroPlaceholder,
    begin: Seq[Expression],
    action: Seq[Expression],
    end: Seq[Expression],
    fcns: Map[String, FcnDef],
    zero: Option[String],
    merge: Option[Seq[Expression]],
    cells: Map[String, Cell],
    pools: Map[String, Pool],
    randseed: Option[Long],
    doc: Option[String],
    version: Option[Int],
    metadata: Map[String, String],
    options: Map[String, JsonNode],
    pos: Option[String] = None) extends Ast {

    /** Input type after type resolution.
      */
    def input: AvroType = inputPlaceholder.avroType
    /** Output type after type resolution.
      */
    def output: AvroType = outputPlaceholder.avroType

    override def equals(other: Any): Boolean = other match {
      case that: EngineConfig => {
        this.name == that.name  &&  this.method == that.method  &&  this.inputPlaceholder.toString == that.inputPlaceholder.toString  &&  this.outputPlaceholder.toString == that.outputPlaceholder.toString  &&
        this.begin == that.begin  &&  this.action == that.action  &&  this.end == that.end  &&  this.fcns == that.fcns  &&
        this.zero == that.zero  &&  this.merge == that.merge  &&
        this.cells == that.cells  &&  this.pools == that.pools  &&  this.randseed == that.randseed  &&
        this.doc == that.doc  &&  this.version == that.version  &&  this.metadata == that.metadata  &&
        this.options == that.options  // but not pos
      }
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((name, method, inputPlaceholder.toString, outputPlaceholder.toString, begin, action, end, fcns, cells, pools, randseed, doc, version, metadata, options))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        begin.flatMap(_.collect(pf)) ++
        action.flatMap(_.collect(pf)) ++
        end.flatMap(_.collect(pf)) ++
        fcns.values.flatMap(_.collect(pf)) ++
        cells.values.flatMap(_.collect(pf)) ++
        pools.values.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        EngineConfig(name,
                     method,
                     inputPlaceholder,
                     outputPlaceholder,
                     begin.map(_.replace(pf).asInstanceOf[Expression]),
                     action.map(_.replace(pf).asInstanceOf[Expression]),
                     end.map(_.replace(pf).asInstanceOf[Expression]),
                     fcns map {case (k, v) => (k, v.replace(pf).asInstanceOf[FcnDef])},
                     zero,
                     merge,
                     cells map {case (k, v) => (k, v.replace(pf).asInstanceOf[Cell])},
                     pools map {case (k, v) => (k, v.replace(pf).asInstanceOf[Pool])},
                     randseed,
                     doc,
                     version,
                     metadata,
                     options,
                     pos)

    if (action.size < 1)
      throw new PFASyntaxException("\"action\" must contain least one expression", pos)

    if (method == Method.FOLD  &&  (zero == None  ||  merge == None))
      throw new PFASyntaxException("folding engines must include \"zero\" and \"merge\" top-level fields", pos)

    if (method != Method.FOLD  &&  (zero != None  ||  merge != None))
      throw new PFASyntaxException("non-folding engines must not include \"zero\" and \"merge\" top-level fields", pos)

    if (merge != None  &&  merge.get.size < 1)
      throw new PFASyntaxException("\"merge\" must contain least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, pfaVersion: PFAVersion): (AstContext, TaskResult) = {
      val topWrapper = SymbolTable(Some(symbolTable), mutable.Map[String, AvroType](), cells, pools, true, false)

      val userFunctions = mutable.Map[String, Fcn]()
      for ((fname, fcnDef) <- fcns) {
        val ufname = "u." + fname
        if (!validFunctionName(ufname))
          throw new PFASemanticException("\"%s\" is not a valid function name".format(fname), pos)
        userFunctions.put(ufname, UserFcn.fromFcnDef(ufname, fcnDef))
      }

      val emitFcn =
        if (method == Method.EMIT)
          Map("emit" -> EmitFcn(output))
        else
          Map[String, Fcn]()

      val withUserFunctions = FunctionTable(functionTable.functions ++ userFunctions.toMap ++ emitFcn)

      val userFcnContexts =
        for ((fname, fcnDef) <- fcns) yield {
          val ufname = "u." + fname
          val scope = topWrapper.newScope(true, false)
          val (fcnContext: FcnDef.Context, _) = fcnDef.walk(task, scope, withUserFunctions, engineOptions, pfaVersion)
          (ufname, fcnContext)
        }

      val beginScopeWrapper = topWrapper.newScope(true, false)
      beginScopeWrapper.put("name", AvroString())
      beginScopeWrapper.put("instance", AvroInt())
      if (version != None) beginScopeWrapper.put("version", AvroInt())
      beginScopeWrapper.put("metadata", AvroMap(AvroString()))
      val beginScope = beginScopeWrapper.newScope(true, false)

      val beginContextResults: Seq[(ExpressionContext, TaskResult)] =
        begin.map(_.walk(task, beginScope, withUserFunctions, engineOptions, pfaVersion)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      val beginResults = beginContextResults map {_._2}
      val beginCalls = beginContextResults.map(_._1.calls).flatten.toSet

      val mergeOption = merge map {unwrappedMerge =>
        val mergeScopeWrapper = topWrapper.newScope(true, false)
        mergeScopeWrapper.put("tallyOne", output)
        mergeScopeWrapper.put("tallyTwo", output)
        mergeScopeWrapper.put("name", AvroString())
        mergeScopeWrapper.put("instance", AvroInt())
        if (version != None) mergeScopeWrapper.put("version", AvroInt())
        mergeScopeWrapper.put("metadata", AvroMap(AvroString()))
        val mergeScope = mergeScopeWrapper.newScope(true, false)

        val mergeContextResults: Seq[(ExpressionContext, TaskResult)] = unwrappedMerge.map(_.walk(task, mergeScope, withUserFunctions, engineOptions, pfaVersion)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
        val mergeCalls = mergeContextResults.map(_._1.calls).flatten.toSet

        if (!output.accepts(mergeContextResults.last._1.retType))
          throw new PFASemanticException("merge's inferred output type is %s but the declared output type is %s".format(mergeContextResults.last._1.retType, output), pos)
        (mergeContextResults map {_._2},
          mergeScopeWrapper.inThisScope ++ mergeScope.inThisScope,
          mergeCalls)
      }

      // this will go into end and action, but not begin or merge
      if (method == Method.FOLD)
        topWrapper.put("tally", output)

      val endScopeWrapper = topWrapper.newScope(true, false)
      endScopeWrapper.put("name", AvroString())
      endScopeWrapper.put("instance", AvroInt())
      if (version != None) endScopeWrapper.put("version", AvroInt())
      endScopeWrapper.put("metadata", AvroMap(AvroString()))
      endScopeWrapper.put("actionsStarted", AvroLong())
      endScopeWrapper.put("actionsFinished", AvroLong())
      val endScope = endScopeWrapper.newScope(true, false)

      val endContextResults: Seq[(ExpressionContext, TaskResult)] =
        end.map(_.walk(task, endScope, withUserFunctions, engineOptions, pfaVersion)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      val endResults = endContextResults map {_._2}
      val endCalls = endContextResults.map(_._1.calls).flatten.toSet

      val actionScopeWrapper = topWrapper.newScope(true, false)
      actionScopeWrapper.put("input", input)
      actionScopeWrapper.put("name", AvroString())
      actionScopeWrapper.put("instance", AvroInt())
      if (version != None) actionScopeWrapper.put("version", AvroInt())
      actionScopeWrapper.put("metadata", AvroMap(AvroString()))
      actionScopeWrapper.put("actionsStarted", AvroLong())
      actionScopeWrapper.put("actionsFinished", AvroLong())
      val actionScope = actionScopeWrapper.newScope(true, false)

      val actionContextResults: Seq[(ExpressionContext, TaskResult)] = action.map(_.walk(task, actionScope, withUserFunctions, engineOptions, pfaVersion)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      val actionCalls = actionContextResults.map(_._1.calls).flatten.toSet

      if (method == Method.MAP  ||  method == Method.FOLD) {
        if (!output.accepts(actionContextResults.last._1.retType))
          throw new PFASemanticException("action's inferred output type is %s but the declared output type is %s".format(actionContextResults.last._1.retType, output), pos)
      }

      val context = EngineConfig.Context(
        name,
        method,
        input,
        output,
        inputPlaceholder.parser.compiledTypes,
        (beginResults,
          beginScopeWrapper.inThisScope ++ beginScope.inThisScope,
          beginCalls),
        (actionContextResults map {_._2},
          actionScopeWrapper.inThisScope ++ actionScope.inThisScope,
          actionCalls),
        (endResults,
          endScopeWrapper.inThisScope ++ endScope.inThisScope,
          endCalls),
        userFcnContexts,
        zero,
        mergeOption,
        cells,
        pools,
        randseed,
        doc,
        version,
        metadata,
        options,
        inputPlaceholder.parser)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      out.put("name", name)

      out.put("method", method match {
        case Method.MAP => "map"
        case Method.EMIT => "emit"
        case Method.FOLD => "fold"
      })

      out.put("input", inputPlaceholder.jsonNode(memo))
      out.put("output", outputPlaceholder.jsonNode(memo))

      if (!begin.isEmpty) {
        val jsonBegin = factory.arrayNode
        for (expr <- begin)
          jsonBegin.add(expr.jsonNode(lineNumbers, memo))
        out.put("begin", jsonBegin)
      }

      val jsonAction = factory.arrayNode
      for (expr <- action)
        jsonAction.add(expr.jsonNode(lineNumbers, memo))
      out.put("action", jsonAction)

      if (!end.isEmpty) {
        val jsonEnd = factory.arrayNode
        for (expr <- end)
          jsonEnd.add(expr.jsonNode(lineNumbers, memo))
        out.put("end", jsonEnd)
      }

      if (!fcns.isEmpty) {
        val jsonFcns = factory.objectNode
        for ((n, fcnDef) <- fcns)
          jsonFcns.put(n, fcnDef.jsonNode(lineNumbers, memo))
        out.put("fcns", jsonFcns)
      }

      zero foreach {x => out.put("zero", convertFromJson(x))}

      merge foreach {x =>
        val jsonMerge = factory.arrayNode
        for (expr <- x)
          jsonMerge.add(expr.jsonNode(lineNumbers, memo))
        out.put("merge", jsonMerge)
      }

      if (!cells.isEmpty) {
        val jsonCells = factory.objectNode
        for ((name, cell) <- cells)
          jsonCells.put(name, cell.jsonNode(lineNumbers, memo))
        out.put("cells", jsonCells)
      }

      if (!pools.isEmpty) {
        val jsonPools = factory.objectNode
        for ((name, pool) <- pools)
          jsonPools.put(name, pool.jsonNode(lineNumbers, memo))
        out.put("pools", jsonPools)
      }

      randseed foreach {x => out.put("randseed", x)}
      doc foreach {x => out.put("doc", x)}
      version foreach {x => out.put("version", x)}

      if (!metadata.isEmpty) {
        val jsonMetadata = factory.objectNode
        for ((name, value) <- metadata)
          jsonMetadata.put(name, value)
        out.put("metadata", jsonMetadata)
      }

      if (!options.isEmpty) {
        val jsonOptions = factory.objectNode
        for ((name, value) <- options)
          jsonOptions.put(name, value)
        out.put("options", jsonOptions)
      }

      out
    }
  }
  object EngineConfig {
    case class Context(
      name: String,
      method: Method.Method,
      input: AvroType,
      output: AvroType,
      compiledTypes: Set[AvroCompiled],
      begin: (Seq[TaskResult], Map[String, AvroType], Set[String]),
      action: (Seq[TaskResult], Map[String, AvroType], Set[String]),
      end: (Seq[TaskResult], Map[String, AvroType], Set[String]),
      fcns: Map[String, FcnDef.Context],
      zero: Option[String],
      merge: Option[(Seq[TaskResult], Map[String, AvroType], Set[String])],
      cells: Map[String, Cell],
      pools: Map[String, Pool],
      randseed: Option[Long],
      doc: Option[String],
      version: Option[Int],
      metadata: Map[String, String],
      options: Map[String, JsonNode],
      parser: ForwardDeclarationParser
    ) extends AstContext
  }

  /** Source methods for cells and pools.
    */
  object CellPoolSource extends Enumeration {
    type CellPoolSource = Value
    val EMBEDDED, JSON, AVRO = Value
  }

  trait CellSource {
    /** Express the cell data as a Jackson node.
      */
    def jsonNode: JsonNode
    /** Create a live object from the cell data.
      * 
      * @param specificData used to make the live objects
      */
    def value(specificData: PFASpecificData): AnyRef
  }

  trait PoolSource {
    /** Express the pool data as a Jackson node.
      */
    def jsonNode: JsonNode
    /** Start the process of creating live objects from the pool data (step 1).
      * 
      * @param specificData used to make the live objects
      */
    def initialize(specificData: PFASpecificData): Unit
    /** Get all the keys in the pool (step 2).
      */
    def keys: Set[String]
    /** Get the value of each key from the pool (step 3).
      */
    def value(key: String): AnyRef
  }

  /** Source for cell data embedded in the original PFA document.
    * 
    * @param jsonDom already-loaded JSON data
    * @param avroPlaceholder cell type as a placeholder (so it can exist before type resolution)
    */
  case class EmbeddedJsonDomCellSource(jsonDom: JsonDom, avroPlaceholder: AvroPlaceholder) extends CellSource {
    /** Cell type after type resolution
      */
    def avroType = avroPlaceholder.avroType
    def jsonNode = jsonDom.jsonNode
    def value(specificData: PFASpecificData): AnyRef = interpret(avroType, jsonDom, specificData)
    private def interpret(t: AvroType, x: JsonDom, specificData: PFASpecificData): AnyRef = (t, x) match {
      case (_: AvroNull, JsonNull) => null
      case (_: AvroBoolean, JsonTrue) => java.lang.Boolean.TRUE
      case (_: AvroBoolean, JsonFalse) => java.lang.Boolean.FALSE
      case (_: AvroInt, y @ JsonLong(value)) if (value >= java.lang.Integer.MIN_VALUE  &&  value <= java.lang.Integer.MAX_VALUE) => y.asJavaInteger
      case (_: AvroLong, JsonLong(value)) => java.lang.Long.valueOf(value)
      case (_: AvroFloat, JsonDouble(value)) => java.lang.Float.valueOf(value.toFloat)
      case (_: AvroFloat, JsonLong(value)) => java.lang.Float.valueOf(value.toFloat)
      case (_: AvroDouble, JsonDouble(value)) => java.lang.Double.valueOf(value)
      case (_: AvroDouble, JsonLong(value)) => java.lang.Double.valueOf(value)
      case (_: AvroBytes, JsonString(value)) => value.getBytes
      case (tt: AvroFixed, JsonString(value)) if (value.getBytes.size == tt.size) =>
        val constructor = specificData.getClassLoader.loadClass(tt.fullName).getConstructor(classOf[Array[Byte]])
        constructor.newInstance(value.getBytes).asInstanceOf[AnyRef]
      case (_: AvroString, JsonString(value)) => value
      case (tt: AvroEnum, JsonString(value)) if (tt.symbols.contains(value)) =>
        val constructor = specificData.getClassLoader.loadClass(tt.fullName).getConstructor(classOf[Schema], classOf[String])
        constructor.newInstance(tt.schema, value).asInstanceOf[AnyRef]
      case (AvroArray(items), JsonArray(entries)) =>
        val out = PFAArray.empty[AnyRef](entries.size)
        entries foreach {y => out.add(interpret(items, y, specificData))}
        out
      case (AvroMap(values), JsonObject(entries)) =>
        val out = PFAMap.empty[AnyRef](entries.size)
        entries foreach {case (k, v) => out.put(k.value, interpret(values, v, specificData))}
        out
      case (tt: AvroRecord, JsonObject(entries)) =>
        val constructor = specificData.getClassLoader.loadClass(tt.fullName).getConstructor(classOf[Array[AnyRef]])
        val fields = tt.fields
        val convertedFields = Array.fill[AnyRef](fields.size)(null)
        var index = 0
        while (index < fields.size) {
          val fieldName = fields(index).name
          val fieldType = fields(index).avroType
          entries.get(JsonString(fieldName)) match {
            case Some(y) => convertedFields(index) = interpret(fieldType, y, specificData)
            case None => throw new PFAInitializationException("record field \"%s\" is missing from JSON datum %s".format(fieldName, x.json))
          }
          index += 1
        }
        constructor.newInstance(convertedFields).asInstanceOf[AnyRef]
      case (AvroUnion(types), JsonNull) if (types.contains(AvroNull())) => null
      case (AvroUnion(types), JsonObject(entries)) if (entries.size == 1) =>
        val tag = entries.keys.head.value
        val value = entries.values.head
        types.find(_.fullName == tag) match {
          case Some(tt) => interpret(tt, value, specificData)
          case None => throw new PFAInitializationException("JSON datum %s does not match any type in union %s".format(x.json, t))
        }
      case _ => throw new PFAInitializationException("JSON datum %s does not match type %s".format(x.json, t))
    }
  }

  /** Source for pool data embedded in the original PFA document.
    * 
    * @param jsonDom already-loaded JSON data
    * @param avroPlaceholder pool type as a placeholder (so it can exist before type resolution)
    */
  case class EmbeddedJsonDomPoolSource(jsonDoms: Map[String, JsonDom], avroPlaceholder: AvroPlaceholder) extends PoolSource {
    /** Pool type after type resolution
      */
    def avroType = avroPlaceholder.avroType
    def jsonNode = {
      val out = JsonNodeFactory.instance.objectNode
      jsonDoms foreach {case (k, v) => out.put(k, v.jsonNode)}
      out
    }
    private var specificData: PFASpecificData = null
    def initialize(specificData: PFASpecificData) {
      this.specificData = specificData
    }
    def keys = jsonDoms.keySet
    def value(key: String): AnyRef =
      EmbeddedJsonDomCellSource(jsonDoms(key), avroPlaceholder).value(specificData)
  }

  /** Source for cell data in an Avro file outside of the original PFA document.
    * 
    * @param url location of the data
    * @param avroPlaceholder cell type as a placeholder (so it can exist before type resolution)
    */
  case class ExternalAvroCellSource(url: java.net.URL, avroPlaceholder: AvroPlaceholder) extends CellSource {
    /** Cell type after type resolution
      */
    def avroType = avroPlaceholder.avroType
    def jsonNode = JsonNodeFactory.instance.textNode(url.toString)
    def value(specificData: PFASpecificData): AnyRef = {
      val reader = new PFADatumReader[AnyRef](specificData)
      reader.setSchema(avroType.schema)
      val fileStream = new DataFileStream[AnyRef](url.openStream(), reader)

      val fileSchema = schemaToAvroType(fileStream.getSchema)
      if (!avroType.accepts(fileSchema))
        throw new PFAInitializationException("external Avro file %s has schema %s\nbut expecting schema %s".format(url.toString, fileSchema, avroType))

      val out = fileStream.next()
      fileStream.close()
      out
    }
  }

  /** Source for pool data in an Avro file outside of the original PFA document.
    * 
    * @param url location of the data
    * @param avroPlaceholder pool type as a placeholder (so it can exist before type resolution)
    */
  case class ExternalAvroPoolSource(url: java.net.URL, avroPlaceholder: AvroPlaceholder) extends PoolSource {
    /** Pool type after type resolution
      */
    def avroType = avroPlaceholder.avroType
    def jsonNode = JsonNodeFactory.instance.textNode(url.toString)
    private val data = new java.util.HashMap[String, AnyRef]()
    def initialize(specificData: PFASpecificData) {
      val poolAvroType = AvroMap(avroType)

      val reader = new PFADatumReader[AnyRef](specificData)
      reader.setSchema(poolAvroType.schema)
      val fileStream = new DataFileStream[AnyRef](url.openStream(), reader)

      val fileSchema = schemaToAvroType(fileStream.getSchema)
      if (!poolAvroType.accepts(fileSchema))
        throw new PFAInitializationException("external Avro file %s has schema %s\nbut expecting schema %s".format(url.toString, fileSchema, poolAvroType))

      val out = fileStream.next()
      fileStream.close()
      out.asInstanceOf[PFAMap[AnyRef]].toMap foreach {case (k, v) => data.put(k, v)}
    }
    def keys = asScalaSet(data.keySet).toSet
    def value(key: String): AnyRef = {
      val out = data.get(key)
      data.remove(key)
      out
    }
  }

  /** Interpreter for cell or pool data in a JSON file outside of the original PFA document.
    * 
    * Streams the external JSON data directly into live objects, skipping the [[com.opendatagroup.hadrian.reader.JsonDom JsonDom]] intermediary.
    */
  trait DirectJsonToData {
    /** Interpret the external JSON data as a live object (recursive function).
      * 
      * @param t type of the object
      * @param parser streaming JSON parser
      * @param token current token in the streaming JSON document
      * @param specificData used to make the live objects
      * @param strings cache of (manually) interned strings, to avoid memory overhead of many identical strings
      * @param dot breadcrumbs for position in the file, for error reporting
      */
    def interpret(t: AvroType, parser: JsonParser, token: JsonToken, specificData: PFASpecificData, strings: mutable.Map[String, String], dot: String): AnyRef = (t, token) match {
      case (_: AvroNull, JsonToken.VALUE_NULL) => null
      case (_: AvroBoolean, JsonToken.VALUE_TRUE) => java.lang.Boolean.TRUE
      case (_: AvroBoolean, JsonToken.VALUE_FALSE) => java.lang.Boolean.FALSE
      case (_: AvroInt, JsonToken.VALUE_NUMBER_INT) => java.lang.Integer.valueOf(parser.getIntValue)
      case (_: AvroLong, JsonToken.VALUE_NUMBER_INT) => java.lang.Long.valueOf(parser.getLongValue)
      case (_: AvroFloat, JsonToken.VALUE_NUMBER_INT) => java.lang.Float.valueOf(parser.getFloatValue)
      case (_: AvroFloat, JsonToken.VALUE_NUMBER_FLOAT) => java.lang.Float.valueOf(parser.getFloatValue)
      case (_: AvroDouble, JsonToken.VALUE_NUMBER_INT) => java.lang.Double.valueOf(parser.getDoubleValue)
      case (_: AvroDouble, JsonToken.VALUE_NUMBER_FLOAT) => java.lang.Double.valueOf(parser.getDoubleValue)
      case (_: AvroBytes, JsonToken.VALUE_STRING) => parser.getText.getBytes
      case (tt: AvroFixed, JsonToken.VALUE_STRING) if (parser.getText.getBytes.size == tt.size) =>
        val constructor = specificData.getClassLoader.loadClass(tt.fullName).getConstructor(classOf[Array[Byte]])
        constructor.newInstance(parser.getText.getBytes).asInstanceOf[AnyRef]
      case (_: AvroString, JsonToken.VALUE_STRING) =>
        val x = parser.getText
        strings.get(x) match {
          case Some(y) => y
          case None =>
            strings(x) = x
            x
        }
      case (tt: AvroEnum, JsonToken.VALUE_STRING) if (tt.symbols.contains(parser.getText)) =>
        val constructor = specificData.getClassLoader.loadClass(tt.fullName).getConstructor(classOf[Schema], classOf[String])
        constructor.newInstance(tt.schema, parser.getText).asInstanceOf[AnyRef]
      case (AvroArray(items), JsonToken.START_ARRAY) =>
        val out = PFAArray.empty[AnyRef]
        var subtoken = parser.nextToken()
        var index = 0
        while (subtoken != JsonToken.END_ARRAY) {
          out.add(interpret(items, parser, subtoken, specificData, strings, dot + "." + index.toString))
          subtoken = parser.nextToken()
          index += 1
        }
        out
      case (AvroMap(values), JsonToken.START_OBJECT) =>
        val out = PFAMap.empty[AnyRef]
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          if (parser.getCurrentName == "@")
            reader.jsonToAst.ingestJsonAndIgnore(parser, parser.nextToken(), "")
          else {
            val x = parser.getCurrentName
            val key = strings.get(x) match {
              case Some(y) => y
              case None =>
                strings(x) = x
                x
            }
            out.put(key, interpret(values, parser, parser.nextToken(), specificData, strings, dot + "." + key))
          }
          subtoken = parser.nextToken()
        }
        out
      case (tt: AvroRecord, JsonToken.START_OBJECT) =>
        val constructor = specificData.getClassLoader.loadClass(tt.fullName).getConstructor(classOf[Array[AnyRef]])
        val fields = tt.fields
        val nameToIndex = fields.zipWithIndex map {case (x, i) => (x.name, i)} toMap
        val convertedFields = Array.fill[AnyRef](fields.size)(null)
        var numFieldsFilled = 0
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          if (nameToIndex.contains(parser.getCurrentName)) {
            val index = nameToIndex(parser.getCurrentName)
            convertedFields(index) = interpret(fields(index).avroType, parser, parser.nextToken(), specificData, strings, dot + "." + parser.getCurrentName)
            numFieldsFilled += 1
          }
          else
            reader.jsonToAst.ingestJsonAndIgnore(parser, parser.nextToken(), "")
          subtoken = parser.nextToken()
        }
        if (numFieldsFilled != fields.size)
          throw new PFAInitializationException("record field is missing from JSON datum representing %s at %s".format(tt, dot))
        constructor.newInstance(convertedFields).asInstanceOf[AnyRef]
      case (AvroUnion(types), JsonToken.VALUE_NULL) if (types.contains(AvroNull())) => null
      case (AvroUnion(types), JsonToken.START_OBJECT) =>
        var out: AnyRef = null
        var subtoken = parser.nextToken()
        var numPairs = 0
        while (subtoken != JsonToken.END_OBJECT) {
          if (parser.getCurrentName == "@")
            reader.jsonToAst.ingestJsonAndIgnore(parser, parser.nextToken(), "")
          else {
            val tag = parser.getCurrentName
            types.find(_.fullName == tag) match {
              case Some(tt) =>
                out = interpret(tt, parser, parser.nextToken(), specificData, strings, dot)
              case None => throw new PFAInitializationException("JSON datum does not match type %s at %s".format(tag, dot))
            }
            numPairs += 1
          }
          subtoken = parser.nextToken()
        }
        if (numPairs == 1)
          out
        else
          throw new PFAInitializationException("JSON datum representing union must have only one tag-value pair at %s".format(dot))
      case (_, x) => throw new PFAInitializationException("JSON token %s does not match type %s at %s".format(token, t, dot))
    }
  }

  /** Source for cell data in a JSON file outside of the original PFA document.
    * 
    * @param url location of the data
    * @param avroPlaceholder cell type as a placeholder (so it can exist before type resolution)
    */
  case class ExternalJsonCellSource(url: java.net.URL, avroPlaceholder: AvroPlaceholder) extends CellSource with DirectJsonToData {
    /** Cell type after type resolution
      */
    def avroType = avroPlaceholder.avroType
    def jsonNode = reader.jsonToAst.objectMapper.readTree(url.openStream())
    def value(specificData: PFASpecificData): AnyRef = {
      val parser = reader.jsonToAst.jsonFactory.createJsonParser(url.openStream())
      try {
        interpret(avroType, parser, parser.nextToken(), specificData, mutable.Map[String, String](), url.toString + ": ")
      }
      finally {
        parser.close()
      }
    }
  }

  /** Source for pool data in a JSON file outside of the original PFA document.
    * 
    * @param url location of the data
    * @param avroPlaceholder cell type as a placeholder (so it can exist before type resolution)
    */
  case class ExternalJsonPoolSource(url: java.net.URL, avroPlaceholder: AvroPlaceholder) extends PoolSource with DirectJsonToData {
    /** Pool type after type resolution
      */
    def avroType = avroPlaceholder.avroType
    def jsonNode = reader.jsonToAst.objectMapper.readTree(url.openStream())
    private val data = new java.util.HashMap[String, AnyRef]()
    def initialize(specificData: PFASpecificData): Unit = {
      val t = avroType
      val parser = reader.jsonToAst.jsonFactory.createJsonParser(url.openStream())
      val strings = mutable.Map[String, String]()
      try {
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          if (parser.getCurrentName == "@")
            reader.jsonToAst.ingestJsonAndIgnore(parser, parser.nextToken(), "")
          else
            data.put(parser.getCurrentName, interpret(t, parser, parser.nextToken(), specificData, strings, url.toString + ": " + parser.getCurrentName))
          subtoken = parser.nextToken()
        }
      }
      finally {
        parser.close()
      }
    }
    def keys = asScalaSet(data.keySet).toSet
    def value(key: String): AnyRef = {
      val out = data.get(key)
      data.remove(key)
      out
    }
  }

  /** Abstract syntax tree for a `cell` definition.
    * 
    * @param avroPlaceholder cell type as a placeholeder (so it can exist before type resolution)
    * @param init initial cell state (embedded or external)
    * @param shared if `true`, this cell shares data with all others in the same [[com.opendatagroup.hadrian.shared.SharedState SharedState]]
    * @param rollback if `true`, this cell rolls back its value when it encounters an exception
    * @param source value of cell's `source` field
    * @param pos source file location from the locator mark
    */
  case class Cell(avroPlaceholder: AvroPlaceholder, init: CellSource, shared: Boolean, rollback: Boolean, source: CellPoolSource.CellPoolSource, pos: Option[String] = None) extends Ast {
    /** Cell type after type resolution.
      */
    def avroType: AvroType = avroPlaceholder.avroType

    if (shared && rollback)
      throw new PFASyntaxException("shared and rollback are mutually incompatible flags for a Cell", pos)

    override def equals(other: Any): Boolean = other match {
      case that: Cell =>
        this.avroPlaceholder.toString == that.avroPlaceholder.toString  &&  this.init.jsonNode == that.init.jsonNode  &&  this.shared == that.shared  &&  this.rollback == that.rollback  &&  this.source == that.source  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((avroPlaceholder.toString, this.init.jsonNode, shared, rollback, source))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = Cell.Context()
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("type", avroPlaceholder.jsonNode(memo))
      out.put("init", init.jsonNode)
      out.put("shared", shared)
      out.put("rollback", rollback)
      source match {
        case CellPoolSource.EMBEDDED =>
        case CellPoolSource.JSON => out.put("source", "json")
        case CellPoolSource.AVRO => out.put("source", "avro")
      }
      out
    }
  }
  object Cell {
    case class Context() extends AstContext
  }

  /** Abstract syntax tree for a `pool` definition.
    * 
    * @param avroPlaceholder pool type as a placeholeder (so it can exist before type resolution)
    * @param init initial pool state (embedded or external)
    * @param shared if `true`, this pool shares data with all others in the same [[com.opendatagroup.hadrian.shared.SharedState SharedState]]
    * @param rollback if `true`, this pool rolls back its value when it encounters an exception
    * @param source value of pool's `source` field
    * @param pos source file location from the locator mark
    */
  case class Pool(avroPlaceholder: AvroPlaceholder, init: PoolSource, shared: Boolean, rollback: Boolean, source: CellPoolSource.CellPoolSource, pos: Option[String] = None) extends Ast {
    /** Pool type after type resolution
      */
    def avroType: AvroType = avroPlaceholder.avroType

    if (shared && rollback)
      throw new PFASyntaxException("shared and rollback are mutually incompatible flags for a Pool", pos)

    override def equals(other: Any): Boolean = other match {
      case that: Pool =>
        this.avroPlaceholder.toString == that.avroPlaceholder.toString  &&
        this.init.jsonNode == that.init.jsonNode  &&
        this.shared == that.shared  &&
        this.rollback == that.rollback  &&
        this.source == that.source  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((avroPlaceholder.toString, this.init.jsonNode, shared, rollback, source))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = Pool.Context()
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("type", avroPlaceholder.jsonNode(memo))
      out.put("init", init.jsonNode)
      out.put("shared", shared)
      out.put("rollback", rollback)
      source match {
        case CellPoolSource.EMBEDDED =>
        case CellPoolSource.JSON => out.put("source", "json")
        case CellPoolSource.AVRO => out.put("source", "avro")
      }
      out
    }
  }
  object Pool {
    case class Context() extends AstContext
  }

  /** Trait for all function arguments, which can be expressions or function references.
   */
  trait Argument extends Ast
  /** Trait for all PFA expressions, which resolve to Avro-typed values.
    */
  trait Expression extends Argument
  /** Trait for all PFA literal values, which are known constants at compile-time.
    */
  trait LiteralValue extends Expression
  /** Trait for path index elements, which can be used in `attr`, `cell`, and `pool` `path` arrays.
    */
  trait PathIndex
  /** Array indexes, which are concrete [[com.opendatagroup.hadrian.ast.PathIndex PathIndex]] elements that dereference arrays (expressions of `int` type).
    */
  case class ArrayIndex(i: TaskResult, t: AvroType) extends PathIndex
  /** Map indexes, which are concrete [[com.opendatagroup.hadrian.ast.PathIndex PathIndex]] elements that dereference maps (expressions of `string` type).
    */
  case class MapIndex(k: TaskResult, t: AvroType) extends PathIndex
  /** Record indexes, which are concrete [[com.opendatagroup.hadrian.ast.PathIndex PathIndex]] elements that dereference records (literal `string` expressions).
    */
  case class RecordIndex(f: String, t: AvroType) extends PathIndex

  /** Mixin for [[com.opendatagroup.hadrian.ast.Ast Ast]] classes taht have paths (`attr`, `cell`, `pool`).
    */
  trait HasPath {
    def path: Seq[Expression]
    def pos: Option[String]

    /** Dereference a `path`, checking all types along the way.
      * 
      * @param avroType data type of the base expression or cell/pool
      * @param task generic task to perform on each expression in the path
      * @param symbolTable used to look up symbols, cells, and pools
      * @param functionTable used to look up functions
      * @param engineOptions implementation options
      * @param version version of the PFA language in which to interpret this PFA
      * @return (type of dereferenced object, functions called, path indexes)
      */
    def walkPath(avroType: AvroType, task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AvroType, Set[String], Seq[PathIndex]) = {
      val calls = mutable.Set[String]()
      val scope = symbolTable.newScope(true, true)
      var walkingType = avroType

      val pathIndexes =
        for ((expr, indexIndex) <- path.zipWithIndex) yield {
          val (exprContext: ExpressionContext, exprResult) = expr.walk(task, scope, functionTable, engineOptions, version)
          calls ++= exprContext.calls

          (walkingType, exprContext.retType) match {
            case (x: AvroArray, y: AvroLong) => walkingType = x.items;  ArrayIndex(exprResult, walkingType)
            case (x: AvroArray, y: AvroInt) => walkingType = x.items;  ArrayIndex(exprResult, walkingType)
            case (x: AvroArray, y) => throw new PFASemanticException("path index for an array must resolve to a long or int; item %d is a %s".format(indexIndex, y.toString), pos)

            case (x: AvroMap, y: AvroString) => walkingType = x.values;  MapIndex(exprResult, walkingType)
            case (x: AvroMap, y) => throw new PFASemanticException("path index for a map must resolve to a string; item %d is a %s".format(indexIndex, y.toString), pos)

            case (x: AvroRecord, y: AvroString) => {
              val name: String = exprContext match {
                case LiteralString.Context(_, _, z) => z
                case Literal.Context(t: AvroString, _, z) => convertFromJson(z).asInstanceOf[String]
                case z => throw new PFASemanticException("path index for record %s must be a literal string; item %d is an object of type %s".format(x.toString, indexIndex, z.retType.toString), pos)
              }
                (x.fields find {_.name == name}) match {
                case Some(field) => walkingType = field.avroType;  RecordIndex(name, walkingType)
                case None => throw new PFASemanticException("record %s has no field named \"%s\" (path index %d)".format(x.toString, name, indexIndex), pos)
              }
            }
            case (x: AvroRecord, y) => throw new PFASemanticException("path index for a record must be a string; item %d is a %s".format(indexIndex, y.toString), pos)

            case (x, _) => throw new PFASemanticException("path item %d is a %s, which cannot be indexed".format(indexIndex, x.toString), pos)
          }
        }

      (walkingType, calls.toSet, pathIndexes)
    }
  }

  /** Abstract syntax tree for a fucntion definition.
    * 
    * @param paramsPlaceholder function parameter types as placeholders (so they can exist before type resolution)
    * @param retPlaceholder return type as a placeholder (so it can exist before type resolution)
    * @param body expressions to evaluate
    * @param pos source file location from the locator mark
    */
  case class FcnDef(paramsPlaceholder: Seq[(String, AvroPlaceholder)], retPlaceholder: AvroPlaceholder, body: Seq[Expression], pos: Option[String] = None) extends Argument {
    /** Names fo the parameters (list of strings).
      */
    def paramNames: Seq[String] = paramsPlaceholder.map({case (k, v) => k})
    /** Resolved parameter types (list of string -> [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] singletons).
      */
    def params: Seq[(String, AvroType)] = paramsPlaceholder.map({case (k, v) => (k, v.avroType)})
    /** Resolved return type.
      */
    def ret: AvroType = retPlaceholder.avroType

    override def equals(other: Any): Boolean = other match {
      case that: FcnDef =>
        this.paramsPlaceholder.corresponds(that.paramsPlaceholder)({case ((k1, v1), (k2, v2)) => k1 == k2  &&  v1.toString == v2.toString})  &&  this.ret == that.ret  &&  this.body == that.body  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((paramsPlaceholder.map({case (k1, v1) => (k1, v1.toString)})), ret, body)

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        body.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        FcnDef(paramsPlaceholder,
               retPlaceholder,
               body.map(_.replace(pf).asInstanceOf[Expression]),
               pos)

    if (body.size < 1)
      throw new PFASyntaxException("function's \"do\" list must contain least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      if (paramsPlaceholder.size > 22)
        throw new PFASemanticException("function can have at most 22 parameters", pos)

      val scope = symbolTable.newScope(true, false)
      for ((name, avroType) <- params) {
        if (!validSymbolName(name))
          throw new PFASemanticException("\"%s\" is not a valid parameter name".format(name), pos)
        scope.put(name, avroType)
      }

      val results: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, scope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}

      val inferredRetType = results.last._1.retType
      if (!inferredRetType.isInstanceOf[ExceptionType]  &&  !ret.accepts(inferredRetType))
        throw new PFASemanticException("function's inferred return type is %s but its declared return type is %s".format(results.last._1.retType, ret), pos)

      val context = FcnDef.Context(FcnType(params map {_._2}, ret), results.map(_._1.calls).flatten.toSet, paramNames, params, ret, scope.inThisScope, results map {_._2})
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonParams = factory.arrayNode
      for ((n, p) <- paramsPlaceholder) {
        val pair = factory.objectNode
        pair.put(n, p.jsonNode(memo))
        jsonParams.add(pair)
      }
      out.put("params", jsonParams)

      out.put("ret", ret.jsonNode(memo))

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)

      out
    }
  }
  object FcnDef {
    case class Context(fcnType: FcnType, calls: Set[String], paramNames: Seq[String], params: Seq[(String, AvroType)], ret: AvroType, symbols: Map[String, AvroType], exprs: Seq[TaskResult]) extends FcnContext
  }

  /** Abstract syntax tree for a function reference.
    * 
    * @param name name of the function to reference
    * @param pos source file location from the locator mark
    */
  case class FcnRef(name: String, pos: Option[String] = None) extends Argument {
    override def equals(other: Any): Boolean = other match {
      case that: FcnRef => this.name == that.name  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[String](name))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val fcn = functionTable.functions.get(name) match {
        case Some(x) => x
        case None => throw new PFASemanticException("unknown function \"%s\" (be sure to include \"u.\" to reference user functions)".format(name), pos)
      }

      val fcnType = {
        val fcnsig = fcn.sig match {
          case x: Sig if (x.lifespan.current(version)  ||  x.lifespan.deprecated(version)) => x
          case x: Sigs =>
            x.cases filter {y: Sig => y.lifespan.current(version)  ||  y.lifespan.deprecated(version)} match {
              case Seq(y) => y
              case _ => throw new PFASemanticException("only one-signature functions without generics can be referenced (wrap \"%s\" in a function definition with the desired signature)".format(name), pos)
            }
          case _ => throw new PFASemanticException("only one-signature functions without generics can be referenced (wrap \"%s\" in a function definition with the desired signature)".format(name), pos)
        }
        fcn.deprecationWarning(fcnsig, version)
        val params = fcnsig.params
        val ret = fcnsig.ret

        try {
          FcnType(params map {case (k, p) => P.toType(p)}, P.mustBeAvro(P.toType(ret)))
        }
        catch {
          case err: IncompatibleTypes => throw new PFASemanticException("only one-signature functions without generics can be referenced (wrap \"%s\" in a function definition with the desired signature)".format(name), pos)
        }
      }

      val context = FcnRef.Context(fcnType, Set(name), fcn)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("fcn", name)
      out
    }
  }
  object FcnRef {
    case class Context(fcnType: FcnType, calls: Set[String], fcn: Fcn) extends FcnContext
  }

  /** Abstract syntax tree for a function reference with partial application.
    * 
    * @param name name of function to reference
    * @param fill parameters to partially apply
    * @param pos source file location from the locator mark
    */
  case class FcnRefFill(name: String, fill: Map[String, Argument], pos: Option[String] = None) extends Argument {
    override def equals(other: Any): Boolean = other match {
      case that: FcnRefFill => this.name == that.name  &&  this.fill == that.fill  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple2[String, Map[String, Argument]](name, fill))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        fill.values.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        FcnRefFill(name,
                   fill map {case (k, v) => (k, v.replace(pf).asInstanceOf[Argument])},
                   pos)

    if (fill.size < 1)
      throw new PFASyntaxException("\"fill\" must contain at least one parameter name-argument mapping", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set(name)

      val fcn = functionTable.functions.get(name) match {
        case Some(x) => x
        case None => throw new PFASemanticException("unknown function \"%s\" (be sure to include \"u.\" to reference user functions)".format(name), pos)
      }

      val fillScope = symbolTable.newScope(true, true)
      val argTypeResult: Map[String, (Type, TaskResult)] = fill map {case (name, arg) =>
        val (argCtx: ArgumentContext, argRes: TaskResult) = arg.walk(task, fillScope, functionTable, engineOptions, version)

        calls ++= argCtx.calls

        argCtx match {
          case x: FcnContext => (name, (x.fcnType, argRes))
          case x: ExpressionContext => (name, (x.retType, argRes))
        }
      }

      val (fcnType, originalParamNames) = {
        val fcnsig = fcn.sig match {
          case x: Sig if (x.lifespan.current(version)  ||  x.lifespan.deprecated(version)) => x
          case x: Sigs =>
            x.cases filter {y: Sig => y.lifespan.current(version)  ||  y.lifespan.deprecated(version)} match {
              case Seq(y) => y
              case _ => throw new PFASemanticException("only one-signature functions without generics can be referenced (wrap \"%s\" in a function definition with the desired signature)".format(name), pos)
            }
          case _ => throw new PFASemanticException("only one-signature functions without generics can be referenced (wrap \"%s\" in a function definition with the desired signature)".format(name), pos)
        }
        fcn.deprecationWarning(fcnsig, version)
        val params = fcnsig.params
        val ret = fcnsig.ret

        try {
          val originalParamNames = params map {_._1}
          val fillNames = argTypeResult.keySet

          if (!fillNames.subsetOf(originalParamNames.toSet))
            throw new PFASemanticException("fill argument names (\"%s\") are not a subset of function \"%s\" parameter names (\"%s\")".format(fillNames.toSeq.sorted.mkString("\", \""), name, originalParamNames.mkString("\", \"")), pos)

          val fcnType = FcnType(params filter {case (k, p) => !fillNames.contains(k)} map {case (k, p) => P.mustBeAvro(P.toType(p))}, P.mustBeAvro(P.toType(ret)))

          (fcnType, originalParamNames)
        }
        catch {
          case err: IncompatibleTypes => throw new PFASemanticException("only one-signature functions without generics can be referenced (wrap \"%s\" in a function definition with the desired signature)".format(name), pos)
        }
      }

      val context = FcnRefFill.Context(fcnType, calls.toSet, fcn, originalParamNames, argTypeResult)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("fcn", name)
      val jsonFill = factory.objectNode
      for ((name, arg) <- fill)
        jsonFill.put(name, arg.jsonNode(lineNumbers, memo))
      out.put("fill", jsonFill)
      out
    }
  }
  object FcnRefFill {
    case class Context(fcnType: FcnType, calls: Set[String], fcn: Fcn, originalParamNames: Seq[String], argTypeResult: Map[String, (Type, TaskResult)]) extends FcnContext
  }

  /** Abstract syntax tree for calling a user-defined function; choice of function determined at runtime.
    * 
    * @param name resolves to an enum type with each enum specifying a user-defined function
    * @param args arguments to pass to the chosen function
    * @param pos source file location from the locator mark
    */
  case class CallUserFcn(name: Expression, args: Seq[Expression], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: CallUserFcn => this.name == that.name  &&  this.args == that.args  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((name, args))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        name.collect(pf) ++
        args.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        CallUserFcn(name.replace(pf).asInstanceOf[Expression],
                    args.map(_.replace(pf).asInstanceOf[Expression]),
                    pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val nameScope = symbolTable.newScope(true, true)
      val (nameContext: ExpressionContext, nameResult) = name.walk(task, nameScope, functionTable, engineOptions, version)
      val fcnNames = nameContext.retType match {
        case x: AvroEnum => x.symbols
        case _ => throw new PFASemanticException("\"call\" name should be an enum, but is " + nameContext.retType, pos)
      }
      val nameToNum = fcnNames map {x => (x, nameContext.retType.schema.getEnumOrdinal(x))} toMap

      val scope = symbolTable.newScope(true, true)
      val argResults: Seq[(ExpressionContext, TaskResult)] = args.map(_.walk(task, scope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}

      val calls = mutable.Set[String]((fcnNames map {"u." + _}): _*)
      val argTypes: Seq[AvroType] =
        for ((exprCtx, res) <- argResults) yield {
          calls ++ exprCtx.calls
          exprCtx.retType
        }

      val nameToFcn = mutable.Map[String, Fcn]()
      val nameToParamTypes = mutable.Map[String, Seq[Type]]()
      val nameToRetTypes = mutable.Map[String, AvroType]()
      var retTypes: List[AvroType] = Nil
      for (n <- fcnNames) {
        val fcn = functionTable.functions.get("u." + n) match {
          case Some(x: UserFcn) => x
          case Some(x) => throw new PFASemanticException("function \"%s\" is not a user function".format(n), pos)
          case None => throw new PFASemanticException("unknown function \"%s\" in enumeration type".format(n), pos)
        }
        fcn.sig.accepts(argTypes, version) match {
          case Some((sig, paramTypes, retType)) => {
            fcn.deprecationWarning(sig, version)
            nameToFcn(n) = fcn
            nameToParamTypes(n) = paramTypes
            nameToRetTypes(n) = retType
            retTypes = retType :: retTypes
          }
          case None =>
            throw new PFASemanticException("parameters of function \"%s\" (in enumeration type) do not accept [%s]".format(n, argTypes.mkString(",")), pos)
        }
      }
      val retType =
        try {
          P.mustBeAvro(LabelData.broadestType(retTypes))
        }
        catch {
          case err: IncompatibleTypes => throw new PFASemanticException(err.getMessage, pos)
        }

      val context = CallUserFcn.Context(retType, calls.toSet, nameResult, nameToNum, nameToFcn.toMap, argResults map { _._2 }, argResults map { _._1 }, nameToParamTypes.toMap, nameToRetTypes.toMap, engineOptions)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("call", name.jsonNode(lineNumbers, memo))

      val jsonArgs = factory.arrayNode
      for (expr <- args)
        jsonArgs.add(expr.jsonNode(lineNumbers, memo))
      out.put("args", jsonArgs)

      out
    }
  }
  object CallUserFcn {
    case class Context(retType: AvroType, calls: Set[String], name: TaskResult, nameToNum: Map[String, Int], nameToFcn: Map[String, Fcn], args: Seq[TaskResult], argContext: Seq[AstContext], nameToParamTypes: Map[String, Seq[Type]], nameToRetTypes: Map[String, AvroType], engineOptions: EngineOptions) extends ExpressionContext
  }

  /** Abstract syntax tree for a function call.
    * 
    * @param name name of the function to call
    * @param args arguemtns to pass to the function
    * @param pos source file location from the locator mark
    */
  case class Call(name: String, args: Seq[Argument], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Call => this.name == that.name  &&  this.args == that.args  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((name, args))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        args.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Call(name,
             args.map(_.replace(pf).asInstanceOf[Argument]),
             pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val fcn = functionTable.functions.get(name) match {
        case Some(x) => x
        case None => throw new PFASemanticException("unknown function \"%s\" (be sure to include \"u.\" to reference user functions)".format(name), pos)
      }

      val scope = symbolTable.newScope(true, true)
      val argResults: Seq[(AstContext, TaskResult)] = args.map(_.walk(task, scope, functionTable, engineOptions, version))

      val calls = mutable.Set[String](name)
      val argTypes: Seq[Type] =
        for ((ctx, res) <- argResults) yield {
          ctx match {
            case exprCtx: ExpressionContext => calls ++= exprCtx.calls;  exprCtx.retType
            case FcnDef.Context(fcnType, fcnCalls, _, _, _, _, _) => calls ++= fcnCalls;  fcnType
            case FcnRef.Context(fcnType, fcnCalls, _) => calls ++= fcnCalls;  fcnType
            case FcnRefFill.Context(fcnType, fcnCalls, _, _, _) => calls ++= fcnCalls;  fcnType
          }
        }

      val context =
        fcn.sig.accepts(argTypes, version) match {
          case Some((sig, paramTypes, retType)) => {
            fcn.deprecationWarning(sig, version)

            val argContexts = argResults map { _._1 }
            val argTaskResults = argResults map { _._2 } toArray

            for ((a, i) <- args.zipWithIndex) a match {
              case _: FcnRef => argTaskResults(i) = task(argContexts(i), engineOptions, Some(paramTypes(i)))
              case _: FcnRefFill => argTaskResults(i) = task(argContexts(i), engineOptions, Some(paramTypes(i)))
              case _ =>
            }

            Call.Context(retType, calls.toSet, fcn, argTaskResults.toList, argContexts, paramTypes, engineOptions)
          }
          case None => throw new PFASemanticException("parameters of function \"%s\" do not accept [%s]".format(name, argTypes.mkString(",")), pos)
        }
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonArgs = factory.arrayNode
      for (expr <- args)
        jsonArgs.add(expr.jsonNode(lineNumbers, memo))
      out.put(name, jsonArgs)

      out
    }
  }
  object Call {
    case class Context(retType: AvroType, calls: Set[String], fcn: Fcn, args: Seq[TaskResult], argContext: Seq[AstContext], paramTypes: Seq[Type], engineOptions: EngineOptions) extends ExpressionContext
  }

  /** Abstract syntax tree for a variable (symbol) reference.
    * 
    * @param variable (symbol) to reference
    * @param pos source file location from the locator mark
    */
  case class Ref(name: String, pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Ref => this.name == that.name  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[String](name))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      if (symbolTable.get(name) == None)
        throw new PFASemanticException("unknown symbol \"%s\"".format(name), pos)
      val context = Ref.Context(symbolTable(name), Set[String](), name)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = new TextNode(name)
  }
  object Ref {
    case class Context(retType: AvroType, calls: Set[String], name: String) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal `null`.
    * 
    * @param pos source file location from the locator mark.
    */
  case class LiteralNull(pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralNull => true  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Null](null))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralNull.Context(AvroNull(), Set[String](LiteralNull.desc))
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = NullNode.getInstance
  }
  object LiteralNull {
    /** `"(null)"`
      */
    val desc = "(null)"
    case class Context(retType: AvroType, calls: Set[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal `true` or `false`.
    * 
    * @param value `true` or `false`
    * @param pos source file location from the locator mark.
    */
  case class LiteralBoolean(value: Boolean, pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralBoolean => this.value == that.value  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Boolean](value))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralBoolean.Context(AvroBoolean(), Set[String](LiteralBoolean.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = if (value) BooleanNode.getTrue else BooleanNode.getFalse
  }
  object LiteralBoolean {
    /** `"(boolean)"`
      */
    val desc = "(boolean)"
    case class Context(retType: AvroType, calls: Set[String], value: Boolean) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal integer.
    * 
    * @param value value
    * @param pos source file location from the locator mark.
    */
  case class LiteralInt(value: Int, pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralInt => this.value == that.value  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Int](value))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralInt.Context(AvroInt(), Set[String](LiteralInt.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = new IntNode(value)
  }
  object LiteralInt {
    /** `"(int)"`
      */
    val desc = "(int)"
    case class Context(retType: AvroType, calls: Set[String], value: Int) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal long.
    * 
    * @param value value
    * @param pos source file location from the locator mark.
    */
  case class LiteralLong(value: Long, pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralLong => this.value == that.value  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Long](value))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralLong.Context(AvroLong(), Set[String](LiteralLong.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode =
      if (java.lang.Integer.MIN_VALUE <= value  &&  value <= java.lang.Integer.MAX_VALUE) {
        val factory = JsonNodeFactory.instance
        val out = factory.objectNode
        out.put("long", new LongNode(value))
        out
      }
    else
      new LongNode(value)
  }
  object LiteralLong {
    /** `"(long)"`
      */
    val desc = "(long)"
    case class Context(retType: AvroType, calls: Set[String], value: Long) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal float.
    * 
    * @param value value
    * @param pos source file location from the locator mark.
    */
  case class LiteralFloat(value: Float, pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralFloat => this.value == that.value  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Float](value))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralFloat.Context(AvroFloat(), Set[String](LiteralFloat.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      out.put("float", new DoubleNode(value))
      out
    }
  }
  object LiteralFloat {
    /** `"(float)"`
      */
    val desc = "(float)"
    case class Context(retType: AvroType, calls: Set[String], value: Float) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal double.
    * 
    * @param value value
    * @param pos source file location from the locator mark.
    */
  case class LiteralDouble(value: Double, pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralDouble => this.value == that.value  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Double](value))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralDouble.Context(AvroDouble(), Set[String](LiteralDouble.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = new DoubleNode(value)
  }
  object LiteralDouble {
    /** `"(double)"`
      */
    val desc = "(double)"
    case class Context(retType: AvroType, calls: Set[String], value: Double) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal string.
    * 
    * @param value value
    * @param pos source file location from the locator mark.
    */
  case class LiteralString(value: String, pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralString => this.value == that.value  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[String](value))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralString.Context(AvroString(), Set[String](LiteralString.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      out.put("string", new TextNode(value))
      out
    }
  }
  object LiteralString {
    /** `"(string)"`
      */
    val desc = "(string)"
    case class Context(retType: AvroType, calls: Set[String], value: String) extends ExpressionContext
  }

  /** Abstract syntax tree for a literal base-64 encoded binary.
    * 
    * @param value already base-64 decoded value
    * @param pos source file location from the locator mark.
    */
  case class LiteralBase64(value: Array[Byte], pos: Option[String] = None) extends LiteralValue {
    override def equals(other: Any): Boolean = other match {
      case that: LiteralBase64 => this.value.sameElements(that.value)  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Array[Byte]](value))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = LiteralBase64.Context(AvroBytes(), Set[String](LiteralBase64.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("base64", new BinaryNode(value))
      out
    }
  }
  object LiteralBase64 {
    /** `"(bytes)"`
      */
    val desc = "(bytes)"
    case class Context(retType: AvroType, calls: Set[String], value: Array[Byte]) extends ExpressionContext
  }

  /** Abstract syntax tree for an arbitrary literal value.
    * 
    * @param avroPlaceholder data type as a placeholder (so it can exist before type resolution)
    * @param literal value encoded as a JSON string
    * @param pos source file location from the locator mark
    */
  case class Literal(avroPlaceholder: AvroPlaceholder, value: String, pos: Option[String] = None) extends LiteralValue {
    /** Resolved data type.
      */
    def avroType: AvroType = avroPlaceholder.avroType

    override def equals(other: Any): Boolean = other match {
      case that: Literal => this.avroPlaceholder.toString == that.avroPlaceholder.toString  &&  convertFromJson(this.value) == convertFromJson(that.value)  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((avroPlaceholder.toString, convertFromJson(value)))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = Literal.Context(avroType, Set[String](Literal.desc), value)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("type", avroType.jsonNode(memo))
      out.put("value", convertFromJson(value))
      out
    }
  }
  object Literal {
    /** `"(literal)"`
      */
    val desc = "(literal)"
    case class Context(retType: AvroType, calls: Set[String], value: String) extends ExpressionContext
  }

  /** Abstract syntax tree for a new map or record expression.
    * 
    * @param fields values to fill
    * @param avroPlaceholder mapr or record type as a placeholder (so it can exist before type resolution)
    * @param pos source file location from the locator mark
    */
  case class NewObject(fields: Map[String, Expression], avroPlaceholder: AvroPlaceholder, pos: Option[String] = None) extends Expression {
    /** Resolved map or record type.
      */
    def avroType: AvroType = avroPlaceholder.avroType

    override def equals(other: Any): Boolean = other match {
      case that: NewObject => this.fields == that.fields  &&  this.avroType == that.avroType  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((fields, avroType))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        fields.values.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        NewObject(fields map {case (k, v) => (k, v.replace(pf).asInstanceOf[Expression])},
                  avroPlaceholder,
                  pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val scope = symbolTable.newScope(true, true)
      val fieldNameTypeExpr: Seq[(String, AvroType, TaskResult)] =
        for ((name, expr) <- fields.toList) yield {
          val (exprContext: ExpressionContext, exprResult) = expr.walk(task, scope, functionTable, engineOptions, version)
          calls ++= exprContext.calls
          (name, exprContext.retType, exprResult)
        }

      avroType match {
        case AvroRecord(flds, name, namespace, _, _) =>
          val fldsMap = flds.map(x => (x.name, x.avroType)).toMap
          for ((n, t, _) <- fieldNameTypeExpr)
            fldsMap.get(n) match {
              case None => throw new PFASemanticException("record constructed with \"new\" has unexpected field named \"%s\"".format(n), pos)
              case Some(fieldType) =>
                if (!fieldType.accepts(t))
                  throw new PFASemanticException("record constructed with \"new\" is has wrong field type for \"%s\": %s rather than %s".format(n, t.toString, fieldType.toString), pos)
            }
          if (fldsMap.keySet != fieldNameTypeExpr.map(_._1).toSet)
            throw new PFASemanticException("record constructed with \"new\" is missing fields: [%s] rather than [%s]".format(fieldNameTypeExpr.map(_._1).sorted.mkString(", "), fldsMap.keys.toSeq.sorted.mkString(", ")), pos)

        case AvroMap(values) =>
          for ((n, t, _) <- fieldNameTypeExpr)
            if (!values.accepts(t))
              throw new PFASemanticException("map constructed with \"new\" has wrong type for value associated with key \"%s\": %s rather than %s".format(n, t.toString, values.toString), pos)

        case x => throw new PFASemanticException("object constructed with \"new\" must have record or map type, not %s".format(x), pos)
      }

      val context = NewObject.Context(avroType, calls.toSet + NewObject.desc, fieldNameTypeExpr map {x => (x._1, x._3)} toMap)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonFields = factory.objectNode
      for ((name, expr) <- fields)
        jsonFields.put(name, expr.jsonNode(lineNumbers, memo))
      out.put("new", jsonFields)

      out.put("type", avroPlaceholder.jsonNode(memo))
      out
    }
  }
  object NewObject {
    /** `"new (object)"`
      */
    val desc = "new (object)"
    case class Context(retType: AvroType, calls: Set[String], fields: Map[String, TaskResult]) extends ExpressionContext
  }

  /** Abstract syntax tree for a new array expression.
    * 
    * @param items items to fill
    * @param avroPlaceholder array type as a placeholder (so it can exist before type resolution)
    * @param pos source file location from the locator mark
    */
  case class NewArray(items: Seq[Expression], avroPlaceholder: AvroPlaceholder, pos: Option[String] = None) extends Expression {
    /** Resolved array type.
      */
    def avroType: AvroType = avroPlaceholder.avroType

    override def equals(other: Any): Boolean = other match {
      case that: NewArray => this.items == that.items  &&  this.avroType == that.avroType  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((items, avroType))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        items.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        NewArray(items.map(_.replace(pf).asInstanceOf[Expression]),
                 avroPlaceholder,
                 pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val scope = symbolTable.newScope(true, true)
      val itemTypeExpr: Seq[(AvroType, TaskResult)] =
        for (expr <- items) yield {
          val (exprContext: ExpressionContext, exprResult) = expr.walk(task, scope, functionTable, engineOptions, version)
          calls ++= exprContext.calls
          (exprContext.retType, exprResult)
        }

      avroType match {
        case AvroArray(itms) =>
          for (((t, _), i) <- itemTypeExpr.zipWithIndex)
            if (!itms.accepts(t))
              throw new PFASemanticException("array constructed with \"new\" has wrong type for item %d: %s rather than %s".format(i, t.toString, itms.toString), pos)
        case x => throw new PFASemanticException("array constructed with \"new\" must have array type, not %s".format(x), pos)
      }

      val context = NewArray.Context(avroType, calls.toSet + NewArray.desc, itemTypeExpr map {_._2})
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonItems = factory.arrayNode
      for (expr <- items)
        jsonItems.add(expr.jsonNode(lineNumbers, memo))
      out.put("new", jsonItems)

      out.put("type", avroPlaceholder.jsonNode(memo))
      out
    }
  }
  object NewArray {
    /** `"new (array)"`
      */
    val desc = "new (array)"
    case class Context(retType: AvroType, calls: Set[String], items: Seq[TaskResult]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `do` block.
    * 
    * @param body expressions to evaluate.
    * @param pos source file location from the locator mark
    */
  case class Do(body: Seq[Expression], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Do => this.body == that.body  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Seq[Expression]](body))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        body.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Do(body.map(_.replace(pf).asInstanceOf[Expression]),
           pos)

    if (body.size < 1)
      throw new PFASyntaxException("\"do\" block must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val scope = symbolTable.newScope(false, false)
      val results: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, scope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}

      val inferredType =
        if (results.last._1.retType.isInstanceOf[ExceptionType])
          AvroNull()
        else
          results.last._1.retType

      val context = Do.Context(inferredType, results.map(_._1.calls).flatten.toSet + Do.desc, scope.inThisScope, results map {_._2})
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)
      out
    }
  }
  object Do {
    /** `"do"`
      */
    val desc = "do"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], exprs: Seq[TaskResult]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `let` variable declaration.
    * 
    * @param values new variables and their initial values
    * @param pos source file location from the locator mark
    */
  case class Let(values: Map[String, Expression], pos: Option[String] = None) extends Expression {
    values.find {case (key, _) => !validSymbolName(key)}
          .foreach {case (badKey, _) => throw new PFASyntaxException(s"${badKey} is not a valid symbol name.", None)}

    override def equals(other: Any): Boolean = other match {
      case that: Let => this.values == that.values  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Map[String, Expression]](values))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        values.values.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Let(values map {case (k, v) => (k, v.replace(pf).asInstanceOf[Expression])},
            pos)

    if (values.size < 1)
      throw new PFASyntaxException("\"let\" must contain at least one declaration", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      if (symbolTable.sealedWithin)
        throw new PFASemanticException("new variable bindings are forbidden in this scope, but you can wrap your expression with \"do\" to make temporary variables", pos)

      val calls = mutable.Set[String]()

      val newSymbols = mutable.Map[String, AvroType]()

      val nameTypeExpr: Seq[(String, AvroType, TaskResult)] =
        for ((name, expr) <- values.toList) yield {
          if (symbolTable.get(name) != None)
            throw new PFASemanticException("symbol \"%s\" may not be redeclared or shadowed".format(name), pos)

          if (!validSymbolName(name))
            throw new PFASemanticException("\"%s\" is not a valid symbol name".format(name), pos)

          val scope = symbolTable.newScope(true, true)
          val (exprContext: ExpressionContext, exprResult) = expr.walk(task, scope, functionTable, engineOptions, version)
          calls ++= exprContext.calls

          if (exprContext.retType.isInstanceOf[ExceptionType])
            throw new PFASemanticException("cannot declare a variable with exception type", pos)

          newSymbols(name) = exprContext.retType

          (name, exprContext.retType, exprResult)
        }

      for ((name, avroType) <- newSymbols)
        symbolTable.put(name, avroType)

      val context = Let.Context(AvroNull(), calls.toSet + Let.desc, nameTypeExpr)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonValues = factory.objectNode
      for ((name, expr) <- values)
        jsonValues.put(name, expr.jsonNode(lineNumbers, memo))
      out.put("let", jsonValues)

      out
    }
  }
  object Let {
    /** `"let"`
      */
    val desc = "let"
    case class Context(retType: AvroType, calls: Set[String], nameTypeExpr: Seq[(String, AvroType, TaskResult)]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `set` variable update.
    * 
    * @param values variables and their updated values
    * @param pos source file location from the locator mark
    */
  case class SetVar(values: Map[String, Expression], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: SetVar => this.values == that.values  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[Map[String, Expression]](values))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        values.values.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        SetVar(values map {case (k, v) => (k, v.replace(pf).asInstanceOf[Expression])},
               pos)

    if (values.size < 1)
      throw new PFASyntaxException("\"set\" must contain at least one assignment", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val nameTypeExpr: Seq[(String, AvroType, TaskResult)] =
        for ((name, expr) <- values.toList) yield {
          if (symbolTable.get(name) == None)
            throw new PFASemanticException("unknown symbol \"%s\" cannot be assigned with \"set\" (use \"let\" to declare a new symbol)".format(name), pos)
          else if (!symbolTable.writable(name))
            throw new PFASemanticException("symbol \"%s\" belongs to a sealed enclosing scope; it cannot be modified within this block)".format(name), pos)

          val scope = symbolTable.newScope(true, true)
          val (exprContext: ExpressionContext, exprResult) = expr.walk(task, scope, functionTable, engineOptions, version)
          calls ++= exprContext.calls

          if (!symbolTable(name).accepts(exprContext.retType))
            throw new PFASemanticException("symbol \"%s\" was declared as %s; it cannot be re-assigned as %s".format(name, symbolTable(name), exprContext.retType), pos)

          (name, symbolTable(name), exprResult)
        }

      val context = SetVar.Context(AvroNull(), calls.toSet + SetVar.desc, nameTypeExpr)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonValues = factory.objectNode
      for ((name, expr) <- values)
        jsonValues.put(name, expr.jsonNode(lineNumbers, memo))
      out.put("set", jsonValues)

      out
    }
  }
  object SetVar {
    /** `"set"`
      */
    val desc = "set"
    case class Context(retType: AvroType, calls: Set[String], nameTypeExpr: Seq[(String, AvroType, TaskResult)]) extends ExpressionContext
  }

  /** Abstract syntax tree for an `attr` array, map, record extraction.
    * 
    * @param expr base object to extract from
    * @param path array, map, and record indexes to extract from the base object
    * @param pos source file location from the locator mark
    */
  case class AttrGet(expr: Expression, path: Seq[Expression], pos: Option[String] = None) extends Expression with HasPath {
    override def equals(other: Any): Boolean = other match {
      case that: AttrGet => this.expr == that.expr  &&  this.path == that.path  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((expr, path))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        expr.collect(pf) ++
        path.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        AttrGet(expr.replace(pf).asInstanceOf[Expression],
                path.map(_.replace(pf).asInstanceOf[Expression]),
                pos)

    if (path.size < 1)
      throw new PFASyntaxException("attr path must have at least one key", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val exprScope = symbolTable.newScope(true, true)

      val (exprContext: ExpressionContext, exprResult) = expr.walk(task, exprScope, functionTable, engineOptions, version)

      exprContext.retType match {
        case _: AvroArray | _: AvroMap | _: AvroRecord =>
        case _ => throw new PFASemanticException("expression is not an array, map, or record", pos)
      }

      val (retType, calls, pathResult) = walkPath(exprContext.retType, task, symbolTable, functionTable, engineOptions, version)
      val context = AttrGet.Context(retType, calls + AttrGet.desc, exprResult, exprContext.retType, pathResult, pos)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("attr", expr.jsonNode(lineNumbers, memo))
      val jsonPath = factory.arrayNode
      for (p <- path)
        jsonPath.add(p.jsonNode(lineNumbers, memo))
      out.put("path", jsonPath)
      out
    }
  }
  object AttrGet {
    /** `"attr"`
      */
    val desc = "attr"
    case class Context(retType: AvroType, calls: Set[String], expr: TaskResult, exprType: AvroType, path: Seq[PathIndex], pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for an `attr-to` update.
    * 
    * @param expr base object to extract from
    * @param path array, map, and record indexes to extract from the base object
    * @param to expression for a replacement object or function reference to use as an updator
    * @param pos source file location from the locator mark
    */
  case class AttrTo(expr: Expression, path: Seq[Expression], to: Argument, pos: Option[String] = None) extends Expression with HasPath {
    override def equals(other: Any): Boolean = other match {
      case that: AttrTo => this.expr == that.expr  &&  this.path == that.path  &&  this.to == that.to  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((expr, path, to))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        expr.collect(pf) ++
        path.flatMap(_.collect(pf)) ++
        to.collect(pf)

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        AttrTo(expr.replace(pf).asInstanceOf[Expression],
               path.map(_.replace(pf).asInstanceOf[Expression]),
               to.replace(pf).asInstanceOf[Argument],
               pos)

    if (path.size < 1)
      throw new PFASyntaxException("attr path must have at least one key", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val exprScope = symbolTable.newScope(true, true)

      val (exprContext: ExpressionContext, exprResult) = expr.walk(task, exprScope, functionTable, engineOptions, version)

      exprContext.retType match {
        case _: AvroArray | _: AvroMap | _: AvroRecord =>
        case _ => throw new PFASemanticException("expression is not an array, map, or record", pos)
      }

      val (setType, calls, pathResult) = walkPath(exprContext.retType, task, symbolTable, functionTable, engineOptions, version)

      val (toContext, toResult) = to.walk(task, symbolTable, functionTable, engineOptions, version)

      val context =
        toContext match {
          case toCtx: ExpressionContext => {
            if (!setType.accepts(toCtx.retType))
              throw new PFASemanticException("attr-and-path has type %s but attempting to assign with type %s".format(setType.toString, toCtx.retType.toString), pos)
            AttrTo.Context(exprContext.retType, calls ++ toCtx.calls + AttrTo.desc, exprResult, exprContext.retType, setType, pathResult, toResult, toCtx.retType, pos)
          }

          case FcnDef.Context(fcnType, fcnCalls, _, _, _, _, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("attr-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            AttrTo.Context(exprContext.retType, calls ++ fcnCalls + AttrTo.desc, exprResult, exprContext.retType, setType, pathResult, toResult, fcnType, pos)
          }

          case FcnRef.Context(fcnType, fcnCalls, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("attr-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            AttrTo.Context(exprContext.retType, calls ++ fcnCalls + AttrTo.desc, exprResult, exprContext.retType, setType, pathResult, task(toContext, engineOptions, Some(fcnType)), fcnType, pos)
          }

          case FcnRefFill.Context(fcnType, fcnCalls, _, _, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("attr-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            AttrTo.Context(exprContext.retType, calls ++ fcnCalls + AttrTo.desc, exprResult, exprContext.retType, setType, pathResult, task(toContext, engineOptions, Some(fcnType)), fcnType, pos)
          }
        }
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("attr", expr.jsonNode(lineNumbers, memo))
      val jsonPath = factory.arrayNode
      for (expr <- path)
        jsonPath.add(expr.jsonNode(lineNumbers, memo))
      out.put("path", jsonPath)
      out.put("to", to.jsonNode(lineNumbers, memo))
      out
    }
  }
  object AttrTo {
    /** `"attr-to"`
      */
    val desc = "attr-to"
    case class Context(retType: AvroType, calls: Set[String], expr: TaskResult, exprType: AvroType, setType: AvroType, path: Seq[PathIndex], to: TaskResult, toType: Type, pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `cell` reference or extraction.
    * 
    * @param cell cell name
    * @param path array, map, and record indexes to extract from the cell
    * @param pos source file location from the locator mark
    */
  case class CellGet(cell: String, path: Seq[Expression], pos: Option[String] = None) extends Expression with HasPath {
    override def equals(other: Any): Boolean = other match {
      case that: CellGet => this.cell == that.cell  &&  this.path == that.path  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((cell, path))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        path.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        CellGet(cell,
                path.map(_.replace(pf).asInstanceOf[Expression]),
                pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val (cellType, shared) = symbolTable.cell(cell) match {
        case Some(x) => (x.avroType, x.shared)
        case None => throw new PFASemanticException("no cell named \"%s\"".format(cell), pos)
      }

      val (retType, calls, pathResult) = walkPath(cellType, task, symbolTable, functionTable, engineOptions, version)
      val context = CellGet.Context(retType, calls + CellGet.desc, cell, cellType, pathResult, shared, pos)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("cell", cell)
      if (!path.isEmpty) {
        val jsonPath = factory.arrayNode
        for (expr <- path)
          jsonPath.add(expr.jsonNode(lineNumbers, memo))
        out.put("path", jsonPath)
      }
      out
    }
  }
  object CellGet {
    /** `"cell"`
      */
    val desc = "cell"
    case class Context(retType: AvroType, calls: Set[String], cell: String, cellType: AvroType, path: Seq[PathIndex], shared: Boolean, pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `cell-to` update.
    * 
    * @param cell cell name
    * @param path array, map, and record indexes to extract from the cell
    * @param to expression for a replacement object or function reference to use as an updator
    * @param pos souce file location from the locator mark
    */
  case class CellTo(cell: String, path: Seq[Expression], to: Argument, pos: Option[String] = None) extends Expression with HasPath {
    override def equals(other: Any): Boolean = other match {
      case that: CellTo => this.cell == that.cell  &&  this.path == that.path  &&  this.to == that.to  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((cell, path, to))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        path.flatMap(_.collect(pf)) ++
        to.collect(pf)

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        CellTo(cell,
               path.map(_.replace(pf).asInstanceOf[Expression]),
               to.replace(pf).asInstanceOf[Argument],
               pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val (cellType, shared) = symbolTable.cell(cell) match {
        case Some(x) => (x.avroType, x.shared)
        case None => throw new PFASemanticException("no cell named \"%s\"".format(cell), pos)
      }

      val (setType, calls, pathResult) = walkPath(cellType, task, symbolTable, functionTable, engineOptions, version)

      val (toContext, toResult) = to.walk(task, symbolTable, functionTable, engineOptions, version)

      val context =
        toContext match {
          case toCtx: ExpressionContext => {
            if (!setType.accepts(toCtx.retType))
              throw new PFASemanticException("cell-and-path has type %s but attempting to assign with type %s".format(setType.toString, toCtx.retType.toString), pos)
            CellTo.Context(cellType, calls ++ toCtx.calls + CellTo.desc, cell, cellType, setType, pathResult, toResult, toCtx.retType, shared, pos)
          }

          case FcnDef.Context(fcnType, fcnCalls, _, _, _, _, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("cell-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            CellTo.Context(cellType, calls ++ fcnCalls + CellTo.desc, cell, cellType, setType, pathResult, toResult, fcnType, shared, pos)
          }

          case FcnRef.Context(fcnType, fcnCalls, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("cell-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            CellTo.Context(cellType, calls ++ fcnCalls + CellTo.desc, cell, cellType, setType, pathResult, task(toContext, engineOptions, Some(fcnType)), fcnType, shared, pos)
          }

          case FcnRefFill.Context(fcnType, fcnCalls, _, _, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("cell-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            CellTo.Context(cellType, calls ++ fcnCalls + CellTo.desc, cell, cellType, setType, pathResult, task(toContext, engineOptions, Some(fcnType)), fcnType, shared, pos)
          }
        }
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("cell", cell)
      if (!path.isEmpty) {
        val jsonPath = factory.arrayNode
        for (expr <- path)
          jsonPath.add(expr.jsonNode(lineNumbers, memo))
        out.put("path", jsonPath)
      }
      out.put("to", to.jsonNode(lineNumbers, memo))
      out
    }
  }
  object CellTo {
    /** `"cell-to"`
      */
    val desc = "cell-to"
    case class Context(retType: AvroType, calls: Set[String], cell: String, cellType: AvroType, setType: AvroType, path: Seq[PathIndex], to: TaskResult, toType: Type, shared: Boolean, pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `pool` reference or extraction.
    * 
    * @param pool pool name
    * @param path array, map, and record indexes to extract from the pool
    * @param pos souce file location from the locator mark
    */
  case class PoolGet(pool: String, path: Seq[Expression], pos: Option[String] = None) extends Expression with HasPath {
    override def equals(other: Any): Boolean = other match {
      case that: PoolGet => this.pool == that.pool  &&  this.path == that.path  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((pool, path))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        path.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        PoolGet(pool,
                path.map(_.replace(pf).asInstanceOf[Expression]),
                pos)

    if (path.size < 1)
      throw new PFASyntaxException("pool path must have at least one key", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val (poolType, shared) = symbolTable.pool(pool) match {
        case Some(x) => (x.avroType, x.shared)
        case None => throw new PFASemanticException("no pool named \"%s\"".format(pool), pos)
      }

      val (retType, calls, pathResult) = walkPath(AvroMap(poolType), task, symbolTable, functionTable, engineOptions, version)
      val context = PoolGet.Context(retType, calls + PoolGet.desc, pool, pathResult, shared, pos)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("pool", pool)
      val jsonPath = factory.arrayNode
      for (expr <- path)
        jsonPath.add(expr.jsonNode(lineNumbers, memo))
      out.put("path", jsonPath)
      out
    }
  }
  object PoolGet {
    /** `"pool"`
      */
    val desc = "pool"
    case class Context(retType: AvroType, calls: Set[String], pool: String, path: Seq[PathIndex], shared: Boolean, pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `pool-to` update.
    * 
    * @param pool pool name
    * @param path array, map, and record indexes to extract from the pol
    * @param to expression for a replacement object or function reference to use as an updator
    * @param init initial value provided in case the desired pool item is empty
    * @param pos souce file location from the locator mark
    */
  case class PoolTo(pool: String, path: Seq[Expression], to: Argument, init: Expression, pos: Option[String] = None) extends Expression with HasPath {
    override def equals(other: Any): Boolean = other match {
      case that: PoolTo => this.pool == that.pool  &&  this.path == that.path  &&  this.to == that.to  &&  this.init == that.init  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((pool, path, to, init))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        path.flatMap(_.collect(pf)) ++
        to.collect(pf) ++
        init.collect(pf)

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        PoolTo(pool,
               path.map(_.replace(pf).asInstanceOf[Expression]),
               to.replace(pf).asInstanceOf[Argument],
               init.replace(pf).asInstanceOf[Expression],
               pos)

    if (path.size < 1)
      throw new PFASyntaxException("pool path must have at least one key", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val (poolType, shared) = symbolTable.pool(pool) match {
        case Some(x) => (x.avroType, x.shared)
        case None => throw new PFASemanticException("no pool named \"%s\"".format(pool), pos)
      }

      val (setType, calls, pathResult) = walkPath(AvroMap(poolType), task, symbolTable, functionTable, engineOptions, version)

      val (toContext, toResult) = to.walk(task, symbolTable, functionTable, engineOptions, version)

      val (initContext: ExpressionContext, initResult) = init.walk(task, symbolTable, functionTable, engineOptions, version)
      if (!poolType.accepts(initContext.retType))
        throw new PFASemanticException("pool has type %s but attempting to init with type %s".format(poolType.toString, initContext.retType.toString), pos)

      val context =
        toContext match {
          case toCtx: ExpressionContext => {
            if (!setType.accepts(toCtx.retType))
              throw new PFASemanticException("pool-and-path has type %s but attempting to assign with type %s".format(setType.toString, toCtx.retType.toString), pos)
            PoolTo.Context(poolType, calls ++ toCtx.calls + PoolTo.desc, pool, poolType, setType, pathResult, toResult, toCtx.retType, initResult, shared, pos)
          }

          case FcnDef.Context(fcnType, fcnCalls, _, _, _, _, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("pool-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            PoolTo.Context(poolType, calls ++ fcnCalls + PoolTo.desc, pool, poolType, setType, pathResult, toResult, fcnType, initResult, shared, pos)
          }

          case FcnRef.Context(fcnType, fcnCalls, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("pool-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            PoolTo.Context(poolType, calls ++ fcnCalls + PoolTo.desc, pool, poolType, setType, pathResult, task(toContext, engineOptions, Some(fcnType)), fcnType, initResult, shared, pos)
          }

          case FcnRefFill.Context(fcnType, fcnCalls, _, _, _) => {
            if (!FcnType(List(setType), setType).accepts(fcnType))
              throw new PFASemanticException("pool-and-path has type %s but attempting to assign with a function of type %s".format(setType.toString, fcnType.toString), pos)
            PoolTo.Context(poolType, calls ++ fcnCalls + PoolTo.desc, pool, poolType, setType, pathResult, task(toContext, engineOptions, Some(fcnType)), fcnType, initResult, shared, pos)
          }
        }
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("pool", pool)
      val jsonPath = factory.arrayNode
      for (expr <- path)
        jsonPath.add(expr.jsonNode(lineNumbers, memo))
      out.put("path", jsonPath)
      out.put("init", init.jsonNode(lineNumbers, memo))
      out.put("to", to.jsonNode(lineNumbers, memo))
      out
    }
  }
  object PoolTo {
    /** `"pool-to"`
      */
    val desc = "pool-to"
    case class Context(retType: AvroType, calls: Set[String], pool: String, poolType: AvroType, setType: AvroType, path: Seq[PathIndex], to: TaskResult, toType: Type, init: TaskResult, shared: Boolean, pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `pool-del` removal.
    * 
    * @param pool pool name
    * @param del item to delete
    * @param pos source file location from the locator mark
    */
  case class PoolDel(pool: String, del: Expression, pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: PoolDel => this.pool == that.pool  &&  this.del == that.del  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((pool, del))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        del.collect(pf)

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        PoolDel(pool, del.replace(pf).asInstanceOf[Expression], pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val shared = symbolTable.pool(pool) match {
        case Some(x) => x.shared
        case None => throw new PFASemanticException("no pool named \"%s\"".format(pool), pos)
      }

      val (delContext: ExpressionContext, delResult) = del.walk(task, symbolTable, functionTable, engineOptions, version)
      if (!AvroString().accepts(delContext.retType))
        throw new PFASemanticException("\"pool-del\" expression should evaluate to a string, but is " + delContext.retType, pos)

      val calls = delContext.calls.toSet + PoolDel.desc

      val context = PoolDel.Context(AvroNull(), calls, pool, delResult, shared)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))
      out.put("pool", pool)
      out.put("del", del.jsonNode(lineNumbers, memo))
      out
    }
  }
  object PoolDel {
    /** `"pool-del"`
      */
    val desc = "pool-del"
    case class Context(retType: AvroType, calls: Set[String], pool: String, del: TaskResult, shared: Boolean) extends ExpressionContext
  }

  /** Abstract syntax tree for an `if` branch.
    * 
    * @param predicate test expression taht evaluates to `boolean`
    * @param thenClause expressions to evaluate if the `predicate` evaluates to `true`
    * @param elseClause expressions to evaluate if present and the `predicate` evaluates to `false`
    * @param pos source file location from the locator mark
    */
  case class If(predicate: Expression, thenClause: Seq[Expression], elseClause: Option[Seq[Expression]], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: If =>
        this.predicate == that.predicate  &&  this.thenClause == that.thenClause  &&  this.elseClause == that.elseClause  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((predicate, thenClause, elseClause))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        predicate.collect(pf) ++
        thenClause.flatMap(_.collect(pf)) ++
        (if (elseClause == None) List[X]() else elseClause.get.flatMap(_.collect(pf)))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        If(predicate.replace(pf).asInstanceOf[Expression],
           thenClause.map(_.replace(pf).asInstanceOf[Expression]),
           elseClause.map(x => x.map(_.replace(pf).asInstanceOf[Expression])),
           pos)

    if (thenClause.size < 1)
      throw new PFASyntaxException("\"then\" clause must contain at least one expression", pos)

    if (elseClause != None  &&  elseClause.get.size < 1)
      throw new PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val predScope = symbolTable.newScope(true, true)
      val (predContext: ExpressionContext, predResult) = predicate.walk(task, predScope, functionTable, engineOptions, version)
      if (!AvroBoolean().accepts(predContext.retType))
        throw new PFASemanticException("\"if\" predicate should be boolean, but is " + predContext.retType, pos)
      calls ++= predContext.calls

      val thenScope = symbolTable.newScope(false, false)
      val thenResults: Seq[(ExpressionContext, TaskResult)] = thenClause.map(_.walk(task, thenScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- thenResults)
        calls ++= exprCtx.calls

      val (retType, elseTaskResults, elseSymbols) =
        elseClause match {
          case Some(clause) => {
            val elseScope = symbolTable.newScope(false, false)

            val elseResults = clause.map(_.walk(task, elseScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
            for ((exprCtx, _) <- elseResults)
              calls ++= exprCtx.calls

            val thenType = thenResults.last._1.retType
            val elseType = elseResults.last._1.retType
            val retType =
              if (thenType.isInstanceOf[ExceptionType]  &&  elseType.isInstanceOf[ExceptionType])
                AvroNull()
              else
                try {
                  P.mustBeAvro(LabelData.broadestType(List(thenType, elseType)))
                }
                catch {
                  case err: IncompatibleTypes => throw new PFASemanticException(err.getMessage, pos)
                }

            (retType, Some(elseResults map {_._2}), Some(elseScope.inThisScope))
          }
          case None => (AvroNull(), None, None)
        }

      val context = If.Context(retType, calls.toSet + If.desc, thenScope.inThisScope, predResult, thenResults map {_._2}, elseSymbols, elseTaskResults)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("if", predicate.jsonNode(lineNumbers, memo))

      val jsonThenClause = factory.arrayNode
      for (expr <- thenClause)
        jsonThenClause.add(expr.jsonNode(lineNumbers, memo))
      out.put("then", jsonThenClause)

      elseClause match {
        case Some(clause) => {
          val jsonElseClause = factory.arrayNode
          for (expr <- clause)
            jsonElseClause.add(expr.jsonNode(lineNumbers, memo))
          out.put("else", jsonElseClause)
        }
        case None =>
      }

      out
    }
  }
  object If {
    /** `"if"`
      */
    val desc = "if"
    case class Context(retType: AvroType, calls: Set[String], thenSymbols: Map[String, AvroType], predicate: TaskResult, thenClause: Seq[TaskResult], elseSymbols: Option[Map[String, AvroType]], elseClause: Option[Seq[TaskResult]]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `cond` branch.
    * 
    * @param ifthens if-then pairs without else clauses
    * @param elseClause expressions to evaluate if present and all if-then predicates evaluate to `false`
    * @param pos source file location from the locator mark
    */
  case class Cond(ifthens: Seq[If], elseClause: Option[Seq[Expression]], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Cond =>
        this.ifthens == that.ifthens  &&  this.elseClause == that.elseClause  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((ifthens, elseClause))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        ifthens.flatMap(_.collect(pf)) ++
        (if (elseClause == None) List[X]() else elseClause.get.flatMap(_.collect(pf)))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Cond(ifthens.map(_.replace(pf).asInstanceOf[If]),
             elseClause.map(x => x.map(_.replace(pf).asInstanceOf[Expression])),
             pos)

    if (ifthens.size < 1)
      throw new PFASyntaxException("\"cond\" must contain at least one predicate-block pair", pos)

    for (If(_, thenClause, _, ifpos) <- ifthens)
      if (thenClause.size < 1)
        throw new PFASyntaxException("\"then\" clause must contain at least one expression", ifpos)

    if (elseClause != None  &&  elseClause.get.size < 1)
      throw new PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val withoutElse: Seq[Cond.WalkBlock] =
        for (If(predicate, thenClause, _, ifpos) <- ifthens) yield {
          val predScope = symbolTable.newScope(true, true)
          val (predContext: ExpressionContext, predResult) = predicate.walk(task, predScope, functionTable, engineOptions, version)
          if (!AvroBoolean().accepts(predContext.retType))
            throw new PFASemanticException("\"if\" predicate should be boolean, but is " + predContext.retType, ifpos)
          calls ++= predContext.calls

          val thenScope = symbolTable.newScope(false, false)
          val thenResults: Seq[(ExpressionContext, TaskResult)] = thenClause.map(_.walk(task, thenScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
          for ((exprCtx, _) <- thenResults)
            calls ++= exprCtx.calls

          Cond.WalkBlock(thenResults.last._1.retType, thenScope.inThisScope, Some(predResult), thenResults map {_._2})
        }

      val walkBlocks = elseClause match {
        case Some(clause) => {
          val elseScope = symbolTable.newScope(false, false)

          val elseResults = clause.map(_.walk(task, elseScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
          for ((exprCtx, _) <- elseResults)
            calls ++= exprCtx.calls

          withoutElse :+ Cond.WalkBlock(elseResults.last._1.retType, elseScope.inThisScope, None, elseResults map {_._2})
        }
        case None => withoutElse
      }

      val retType =
        if (elseClause == None)
          AvroNull()
        else {
          val walkTypes = walkBlocks.collect({case Cond.WalkBlock(t, _, _, _) => t.asInstanceOf[AvroType]}).toList
          if (walkTypes forall {_.isInstanceOf[ExceptionType]})
            AvroNull()
          else
            try {
              LabelData.broadestType(walkTypes)
            }
            catch {
              case err: IncompatibleTypes => throw new PFASemanticException(err.getMessage, pos)
            }
        }

      val context = Cond.Context(retType.asInstanceOf[AvroType], calls.toSet + Cond.desc, (elseClause != None), walkBlocks)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonIfthens = factory.arrayNode
      for (ifthen <- ifthens)
        jsonIfthens.add(ifthen.jsonNode(lineNumbers, memo))
      out.put("cond", jsonIfthens)

      elseClause match {
        case Some(clause) => {
          val jsonElseClause = factory.arrayNode
          for (expr <- clause)
            jsonElseClause.add(expr.jsonNode(lineNumbers, memo))
          out.put("else", jsonElseClause)
        }
        case None =>
      }

      out
    }
  }
  object Cond {
    /** `"cond"`
      */
    val desc = "cond"
    case class WalkBlock(retType: AvroType, symbols: Map[String, AvroType], pred: Option[TaskResult], exprs: Seq[TaskResult])
    case class Context(retType: AvroType, calls: Set[String], complete: Boolean, walkBlocks: Seq[WalkBlock]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `while` loop.
    * 
    * @param predicate test expression that evaluates to `boolean`
    * @param body expressions to evaluate while `predicate` evaluates to `true`
    * @param pos source file location from the locator mark
    */
  case class While(predicate: Expression, body: Seq[Expression], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: While =>
        this.predicate == that.predicate  &&  this.body == that.body  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((predicate, body))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        predicate.collect(pf) ++
        body.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        While(predicate.replace(pf).asInstanceOf[Expression],
              body.map(_.replace(pf).asInstanceOf[Expression]),
              pos)

    if (body.size < 1)
      throw new PFASyntaxException("\"do\" block must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()
      val loopScope = symbolTable.newScope(false, false)
      val predScope = loopScope.newScope(true, true)

      val (predContext: ExpressionContext, predResult) = predicate.walk(task, predScope, functionTable, engineOptions, version)
      if (!AvroBoolean().accepts(predContext.retType))
        throw new PFASemanticException("\"while\" predicate should be boolean, but is " + predContext.retType, pos)
      calls ++= predContext.calls

      val loopResults: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, loopScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- loopResults)
        calls ++= exprCtx.calls

      val context = While.Context(AvroNull(), calls.toSet + While.desc, loopScope.inThisScope, predResult, loopResults map {_._2})
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("while", predicate.jsonNode(lineNumbers, memo))

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)
      out
    }
  }
  object While {
    /** `"while"`
      */
    val desc = "while"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], predicate: TaskResult, loopBody: Seq[TaskResult]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `do-until` post-test loop.
    * 
    * @param body expressions to evaluate until `predicate` evaluates to `true`
    * @param predicate test expression that evaluates to `boolean`
    * @param pos source file location from the locator mark
    */
  case class DoUntil(body: Seq[Expression], predicate: Expression, pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: DoUntil =>
        this.body == that.body  &&  this.predicate == that.predicate  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((body, predicate))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        body.flatMap(_.collect(pf)) ++
        predicate.collect(pf)

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        DoUntil(body.map(_.replace(pf).asInstanceOf[Expression]),
                predicate.replace(pf).asInstanceOf[Expression],
                pos)

    if (body.size < 1)
      throw new PFASyntaxException("\"do\" block must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()
      val loopScope = symbolTable.newScope(false, false)
      val predScope = loopScope.newScope(true, true)

      val loopResults: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, loopScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- loopResults)
        calls ++= exprCtx.calls

      val (predContext: ExpressionContext, predResult) = predicate.walk(task, predScope, functionTable, engineOptions, version)
      if (!AvroBoolean().accepts(predContext.retType))
        throw new PFASemanticException("\"until\" predicate should be boolean, but is " + predContext.retType, pos)
      calls ++= predContext.calls

      val context = DoUntil.Context(AvroNull(), calls.toSet + DoUntil.desc, loopScope.inThisScope, loopResults map {_._2}, predResult)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)

      out.put("until", predicate.jsonNode(lineNumbers, memo))
      out
    }
  }
  object DoUntil {
    /** `"do-until"`
      */
    val desc = "do-until"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], loopBody: Seq[TaskResult], predicate: TaskResult) extends ExpressionContext
  }

  /** Abstract syntax tree for a `for` loop.
    * 
    * @param init initial values for the dummy variables
    * @param predicate test expression that evaluates to `boolean`
    * @param step update expressions for the dummy variables
    * @param body expressions to evaluate while `predicate` evaluates to `true`
    * @param pos source file locadtion from the locator mark
    */
  case class For(init: Map[String, Expression], predicate: Expression, step: Map[String, Expression], body: Seq[Expression], pos: Option[String] = None) extends Expression {
    (init.keys ++ step.keys).find {key => !validSymbolName(key)}
                            .foreach {badKey => throw new PFASyntaxException(s"${badKey} is not a valid symbol name!", None)}

    override def equals(other: Any): Boolean = other match {
      case that: For =>
        this.init == that.init  &&  this.predicate == that.predicate  &&  this.step == that.step  &&  this.body == that.body  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((init, predicate, step, body))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        init.values.flatMap(_.collect(pf)) ++
        predicate.collect(pf) ++
        step.values.flatMap(_.collect(pf)) ++
        body.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        For(init map {case (k, v) => (k, v.replace(pf).asInstanceOf[Expression])},
            predicate.replace(pf).asInstanceOf[Expression],
            step map {case (k, v) => (k, v.replace(pf).asInstanceOf[Expression])},
            body.map(_.replace(pf).asInstanceOf[Expression]),
            pos)

    if (init.size < 1)
      throw new PFASyntaxException("\"for\" must contain at least one declaration", pos)

    if (step.size < 1)
      throw new PFASyntaxException("\"step\" must contain at least one assignment", pos)

    if (body.size < 1)
      throw new PFASyntaxException("\"do\" must contain at least one statement", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()
      val loopScope = symbolTable.newScope(false, false)

      val newSymbols = mutable.Map[String, AvroType]()

      val initNameTypeExpr: Seq[(String, AvroType, TaskResult)] =
        for ((name, expr) <- init.toList) yield {
          if (loopScope.get(name) != None)
            throw new PFASemanticException("symbol \"%s\" may not be redeclared or shadowed".format(name), pos)

          if (!validSymbolName(name))
            throw new PFASemanticException("\"%s\" is not a valid symbol name".format(name), pos)

          val initScope = loopScope.newScope(true, true)
          val (exprContext: ExpressionContext, exprResult) = expr.walk(task, initScope, functionTable, engineOptions, version)
          calls ++= exprContext.calls

          newSymbols(name) = exprContext.retType

          (name, exprContext.retType, exprResult)
        }

      for ((name, avroType) <- newSymbols)
          loopScope.put(name, avroType)

      val predicateScope = loopScope.newScope(true, true)
      val (predicateContext: ExpressionContext, predicateResult) = predicate.walk(task, predicateScope, functionTable, engineOptions, version)
      if (!AvroBoolean().accepts(predicateContext.retType))
        throw new PFASemanticException("predicate should be boolean, but is " + predicateContext.retType, pos)
      calls ++= predicateContext.calls

      val stepNameTypeExpr: Seq[(String, AvroType, TaskResult)] =
        for ((name, expr) <- step.toList) yield {
          if (loopScope.get(name) == None)
            throw new PFASemanticException("unknown symbol \"%s\" cannot be assigned with \"step\"".format(name), pos)
          else if (!loopScope.writable(name))
            throw new PFASemanticException("symbol \"%s\" belongs to a sealed enclosing scope; it cannot be modified within this block".format(name), pos)

          val stepScope = loopScope.newScope(true, true)
          val (exprContext: ExpressionContext, exprResult) = expr.walk(task, stepScope, functionTable, engineOptions, version)
          calls ++= exprContext.calls

          if (!loopScope(name).accepts(exprContext.retType))
            throw new PFASemanticException("symbol \"%s\" was declared as %s; it cannot be re-assigned as %s".format(name, loopScope(name), exprContext.retType), pos)

          (name, loopScope(name), exprResult)
        }

      val bodyScope = loopScope.newScope(false, false)
      val bodyResults: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, bodyScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- bodyResults)
        calls ++= exprCtx.calls

      val context = For.Context(AvroNull(), calls.toSet + For.desc, bodyScope.inThisScope ++ loopScope.inThisScope, initNameTypeExpr, predicateResult, bodyResults map {_._2}, stepNameTypeExpr)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonValues = factory.objectNode
      for ((name, expr) <- init)
        jsonValues.put(name, expr.jsonNode(lineNumbers, memo))
      out.put("for", jsonValues)

      out.put("while", predicate.jsonNode(lineNumbers, memo))

      val jsonStep = factory.objectNode
      for ((name, expr) <- step)
        jsonStep.put(name, expr.jsonNode(lineNumbers, memo))
      out.put("step", jsonStep)

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)

      out
    }
  }
  object For {
    /** `"for"`
      */
    val desc = "for"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], initNameTypeExpr: Seq[(String, AvroType, TaskResult)], predicate: TaskResult, loopBody: Seq[TaskResult], stepNameTypeExpr: Seq[(String, AvroType, TaskResult)]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `foreach` loop.
    * 
    * @param name new variable name for array items
    * @param array expression that evaluates to an array
    * @param body expressions to evaluate for each item of the array
    * @param seq if `false`, seal the scope from above and allow implementations to parallelize (Hadrian doesn't)
    * @param pos source file location from the locator mark
    */
  case class Foreach(name: String, array: Expression, body: Seq[Expression], seq: Boolean, pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Foreach =>
        this.name == that.name  &&  this.array == that.array  &&  this.body == that.body  &&  this.seq == that.seq  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((name, array, body, seq))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        array.collect(pf) ++
        body.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Foreach(name,
                array.replace(pf).asInstanceOf[Expression],
                body.map(_.replace(pf).asInstanceOf[Expression]),
                seq,
                pos)

    if (body.size < 1)
      throw new PFASyntaxException("\"do\" must contain at least one statement", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()
      val loopScope = symbolTable.newScope(!seq, false)

      if (symbolTable.get(name) != None)
        throw new PFASemanticException("symbol \"%s\" may not be redeclared or shadowed".format(name), pos)

      if (!validSymbolName(name))
        throw new PFASemanticException("\"%s\" is not a valid symbol name".format(name), pos)

      val objScope = loopScope.newScope(true, true)
      val (objContext: ExpressionContext, objResult) = array.walk(task, objScope, functionTable, engineOptions, version)
      calls ++= objContext.calls

      val elementType = objContext.retType match {
        case x: AvroArray => x.items
        case _ => throw new PFASemanticException("expression referred to by \"in\" should be an array, but is " + objContext.retType, pos)
      }

      loopScope.put(name, elementType)

      val bodyScope = loopScope.newScope(false, false)
      val bodyResults: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, bodyScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- bodyResults)
        calls ++= exprCtx.calls

      val context = Foreach.Context(AvroNull(), calls.toSet + Foreach.desc, bodyScope.inThisScope ++ loopScope.inThisScope, objContext.retType, objResult, elementType, name, bodyResults map {_._2})
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("foreach", name)
      out.put("in", array.jsonNode(lineNumbers, memo))

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)

      out.put("seq", if (seq) BooleanNode.getTrue else BooleanNode.getFalse)
      out
    }
  }
  object Foreach {
    /** `"foreach"`
      */
    val desc = "foreach"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], objType: AvroType, objExpr: TaskResult, itemType: AvroType, name: String, loopBody: Seq[TaskResult]) extends ExpressionContext
  }

  /** Abstract syntax tree for `forkey-forval` loops.
    * 
    * @param forkey new variable name for map keys
    * @param forval new variable name for map values
    * @param map expression that evaluates to a map
    * @param body expressions that evaluate for each item of the map
    * @param pos source file location from the locator mark
    */
  case class Forkeyval(forkey: String, forval: String, map: Expression, body: Seq[Expression], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Forkeyval =>
        this.forkey == that.forkey  &&  this.forval == that.forval  &&  this.map == that.map  &&  this.body == that.body  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((forkey, forval, map, body))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        map.collect(pf) ++
        body.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Forkeyval(forkey,
                  forval,
                  map.replace(pf).asInstanceOf[Expression],
                  body.map(_.replace(pf).asInstanceOf[Expression]),
                  pos)

    if (body.size < 1)
      throw new PFASyntaxException("\"do\" must contain at least one statement", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()
      val loopScope = symbolTable.newScope(false, false)

      if (symbolTable.get(forkey) != None)
        throw new PFASemanticException("symbol \"%s\" may not be redeclared or shadowed".format(forkey), pos)
      if (symbolTable.get(forval) != None)
        throw new PFASemanticException("symbol \"%s\" may not be redeclared or shadowed".format(forval), pos)

      if (!validSymbolName(forkey))
        throw new PFASemanticException("\"%s\" is not a valid symbol name".format(forkey), pos)
      if (!validSymbolName(forval))
        throw new PFASemanticException("\"%s\" is not a valid symbol name".format(forval), pos)

      val objScope = loopScope.newScope(true, true)
      val (objContext: ExpressionContext, objResult) = map.walk(task, objScope, functionTable, engineOptions, version)
      calls ++= objContext.calls

      val elementType = objContext.retType match {
        case x: AvroMap => x.values
        case _ => throw new PFASemanticException("expression referred to by \"in\" should be a map, but is " + objContext.retType, pos)
      }

      loopScope.put(forkey, AvroString())
      loopScope.put(forval, elementType)

      val bodyScope = loopScope.newScope(false, false)
      val bodyResults: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, bodyScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- bodyResults)
        calls ++= exprCtx.calls

      val context = Forkeyval.Context(AvroNull(), calls.toSet + Forkeyval.desc, bodyScope.inThisScope ++ loopScope.inThisScope, objContext.retType, objResult, elementType, forkey, forval, bodyResults map {_._2})
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("forkey", forkey)
      out.put("forval", forval)
      out.put("in", map.jsonNode(lineNumbers, memo))

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)
      out
    }
  }
  object Forkeyval {
    /** `"forkey-forval"`
      */
    val desc = "forkey-forval"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], objType: AvroType, objExpr: TaskResult, valueType: AvroType, forkey: String, forval: String, loopBody: Seq[TaskResult]) extends ExpressionContext
  }

  /** Abstract syntax tree for one `case` of a `cast-case` block.
    * 
    * @param avroPlaceholder subtype cast as a placeholder (so it can exist before type resolution)
    * @param named new variable name to hold the casted value
    * @param body expressions to evaluate if casting to this subtype is possible
    * @param pos source file location from the locator mark
    */
  case class CastCase(avroPlaceholder: AvroPlaceholder, named: String, body: Seq[Expression], pos: Option[String] = None) extends Ast {
    def avroType: AvroType = avroPlaceholder.avroType

    override def equals(other: Any): Boolean = other match {
      case that: CastCase =>
        this.avroPlaceholder.toString == that.avroPlaceholder.toString  &&  this.named == that.named  &&  this.body == that.body  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((avroPlaceholder.toString, named, body))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        body.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        CastCase(avroPlaceholder,
                 named,
                 body.map(_.replace(pf).asInstanceOf[Expression]),
                 pos)

    if (body.size < 1)
      throw new PFASyntaxException("\"do\" block must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      if (!validSymbolName(named))
        throw new PFASemanticException("\"%s\" is not a valid symbol name".format(named), pos)

      val scope = symbolTable.newScope(false, false)
      scope.put(named, avroType)

      val results: Seq[(ExpressionContext, TaskResult)] = body.map(_.walk(task, scope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      val context = CastCase.Context(results.last._1.retType, named, avroType, results.map(_._1.calls).flatten.toSet, scope.inThisScope, results map {_._2})
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("as", avroPlaceholder.jsonNode(memo))
      out.put("named", named)

      val jsonBody = factory.arrayNode
      for (expr <- body)
        jsonBody.add(expr.jsonNode(lineNumbers, memo))
      out.put("do", jsonBody)

      out
    }
  }
  object CastCase {
    case class Context(retType: AvroType, name: String, toType: AvroType, calls: Set[String], symbols: Map[String, AvroType], clause: Seq[TaskResult]) extends AstContext
  }

  /** Abstract syntax tree for a `cast-case` block.
    * 
    * @param expr expression to cast
    * @param castCases subtype cases to attempt to cast
    * @param partial if `true`, allow subtype cases to not fully cover the expression type
    * @param pos source file location from the locator mark
    */
  case class CastBlock(expr: Expression, castCases: Seq[CastCase], partial: Boolean, pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: CastBlock =>
        this.expr == that.expr  &&  this.castCases == that.castCases  &&  this.partial == that.partial  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((expr, castCases, partial))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        expr.collect(pf) ++
        castCases.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        CastBlock(expr.replace(pf).asInstanceOf[Expression],
                  castCases.map(_.replace(pf).asInstanceOf[CastCase]),
                  partial,
                  pos)

    if (partial) {
      if (castCases.size < 1)
        throw new PFASyntaxException("\"cases\" must contain at least one case", pos)
    }
    else {
      if (castCases.size < 2)
        throw new PFASyntaxException("\"cases\" must contain at least two cases", pos)
    }

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val exprScope = symbolTable.newScope(true, true)
      val (exprContext: ExpressionContext, exprResult) = expr.walk(task, exprScope, functionTable, engineOptions, version)
      calls ++= exprContext.calls

      val exprType = exprContext.retType
      val types = castCases map {_.avroType}

      // do you have anything extraneous?
      types.find(!exprType.accepts(_)) match {
        case Some(t) => throw new PFASemanticException("\"cast\" of expression with type %s can never satisfy case %s".format(exprType, t), pos)
        case None =>
      }

      val cases = castCases.map(_.walk(task, symbolTable, functionTable, engineOptions, version))
      for ((castCtx: CastCase.Context, _) <- cases)
        calls ++= castCtx.calls

      val retType =
        if (partial)
          AvroNull()

        else {
          // are you missing anything necessary?
          val mustFindThese = exprType match {
            case x: AvroUnion => x.types
            case x => List(x)
          }

          for (mustFind <- mustFindThese)
            if (!types.exists(t => t.accepts(mustFind)  &&  mustFind.accepts(t)))
              throw new PFASemanticException("\"cast\" of expression with type %s does not contain a case for %s".format(exprType, mustFind), pos)

          try {
            P.mustBeAvro(LabelData.broadestType(cases map {case (castCtx: CastCase.Context, _) => castCtx.retType} toList))
          }
          catch {
            case err: IncompatibleTypes => throw new PFASemanticException(err.getMessage, pos)
          }
        }

      val context = CastBlock.Context(retType, calls.toSet + CastBlock.desc, exprType, exprResult, cases map {case (castCtx: CastCase.Context, castRes: TaskResult) => (castCtx, castRes)}, partial)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("cast", expr.jsonNode(lineNumbers, memo))

      val jsonCastCase = factory.arrayNode
      for (castCase <- castCases)
        jsonCastCase.add(castCase.jsonNode(lineNumbers, memo))
      out.put("cases", jsonCastCase)

      out.put("partial", if (partial) BooleanNode.getTrue else BooleanNode.getFalse)
      out
    }
  }
  object CastBlock {
    /** `"cast-cases"`
      */
    val desc = "cast-cases"
    case class Context(retType: AvroType, calls: Set[String], exprType: AvroType, expr: TaskResult, cases: Seq[(CastCase.Context, TaskResult)], partial: Boolean) extends ExpressionContext
  }

  /** Abstract syntax tree for an `upcast`.
    * 
    * @param expr expression to up-cast
    * @param avroPlaceholder supertype as a placeholder (so it can exist before type resolution)
    * @param pos source file location from the locator mark
    */
  case class Upcast(expr: Expression, avroPlaceholder: AvroPlaceholder, pos: Option[String] = None) extends Expression {
    /** Resolved supertype.
      */
    def avroType: AvroType = avroPlaceholder.avroType

    override def equals(other: Any): Boolean = other match {
      case that: Upcast =>
        this.expr == that.expr  &&  this.avroPlaceholder.toString == that.avroPlaceholder.toString  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((expr, avroPlaceholder))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        expr.collect(pf)

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Upcast(expr.replace(pf).asInstanceOf[Expression],
               avroPlaceholder,
               pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val scope = symbolTable.newScope(true, true)
      val (exprContext: ExpressionContext, exprResult) = expr.walk(task, scope, functionTable, engineOptions, version)

      if (!avroType.accepts(exprContext.retType))
        throw new PFASemanticException("expression results in %s; cannot expand (\"upcast\") to %s".format(exprContext.retType, avroType), pos)

      val context = Upcast.Context(avroType, exprContext.calls + Upcast.desc, exprResult)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("upcast", expr.jsonNode(lineNumbers, memo))
      out.put("as", avroPlaceholder.jsonNode(memo))
      out
    }
  }
  object Upcast {
    /** `"upcast"`
      */
    val desc = "upcast"
    case class Context(retType: AvroType, calls: Set[String], expr: TaskResult) extends ExpressionContext
  }

  /** Abstract syntax tree for an `ifnotnull` block.
    * 
    * @param exprs variables to check for `null`
    * @param thenClause expressions to evaluate if all variables are not `null`
    * @param elseClause expressions to evaluate if present and any variable is `null`
    * @param pos source file location from the locator mark
    */
  case class IfNotNull(exprs: Map[String, Expression], thenClause: Seq[Expression], elseClause: Option[Seq[Expression]], pos: Option[String] = None) extends Expression {
    exprs.keys.find {key => !validSymbolName(key)}
              .foreach {badKey => throw new PFASyntaxException(s"${badKey} is not a valid symbol name!", None)}

    override def equals(other: Any): Boolean = other match {
      case that: IfNotNull => this.exprs == that.exprs  &&  this.thenClause == that.thenClause  &&  this.elseClause == that.elseClause  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((exprs, thenClause, elseClause))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        exprs.values.flatMap(_.collect(pf)) ++
        thenClause.flatMap(_.collect(pf)) ++
        (if (elseClause == None) List[X]() else elseClause.get.flatMap(_.collect(pf)))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        IfNotNull(exprs map {case (k, v) => (k, v.replace(pf).asInstanceOf[Expression])},
                  thenClause.map(_.replace(pf).asInstanceOf[Expression]),
                  elseClause.map(x => x.map(_.replace(pf).asInstanceOf[Expression])),
                  pos)

    if (exprs.size < 1)
      throw new PFASyntaxException("\"ifnotnull\" must contain at least one symbol-expression mapping", pos)

    if (thenClause.size < 1)
        throw new PFASyntaxException("\"then\" clause must contain at least one expression", pos)

    if (elseClause != None  &&  elseClause.size < 1)
      throw new PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val exprArgsScope = symbolTable.newScope(true, true)
      val assignmentScope = symbolTable.newScope(false, false)

      val symbolTypeResult: Seq[(String, AvroType, TaskResult)] = exprs map {case (name, expr) =>
        if (!validSymbolName(name))
          throw new PFASemanticException("\"%s\" is not a valid symbol name".format(name), pos)

        val (exprCtx: ExpressionContext, exprRes: TaskResult) = expr.walk(task, exprArgsScope, functionTable, engineOptions, version)

        val avroType =
          exprCtx.retType match {
            case AvroUnion(types) if (types.size > 2  &&  types.contains(AvroNull())) => AvroUnion(types filter {_ != AvroNull()})
            case AvroUnion(types) if (types.size > 1  &&  types.contains(AvroNull())) => types filter {_ != AvroNull()} head
            case x => throw new PFASemanticException("\"ifnotnull\" expressions must all be unions of something and null; case \"%s\" has type %s".format(name, x.toString), pos)
          }
        assignmentScope.put(name, avroType)

        calls ++= exprCtx.calls

        (name, avroType, exprRes)
      } toList

      val thenScope = assignmentScope.newScope(false, false)
      val thenResults: Seq[(ExpressionContext, TaskResult)] = thenClause.map(_.walk(task, thenScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- thenResults)
        calls ++= exprCtx.calls

      val (retType, elseTaskResults, elseSymbols) =
        elseClause match {
          case Some(clause) => {
            val elseScope = symbolTable.newScope(false, false)

            val elseResults = clause.map(_.walk(task, elseScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
            for ((exprCtx, _) <- elseResults)
              calls ++= exprCtx.calls

            val thenType = thenResults.last._1.retType
            val elseType = elseResults.last._1.retType
            val retType =
              try {
                P.mustBeAvro(LabelData.broadestType(List(thenType, elseType)))
              }
              catch {
                case err: IncompatibleTypes => throw new PFASemanticException(err.getMessage, pos)
              }

            (retType, Some(elseResults map {_._2}), Some(elseScope.inThisScope))
          }
          case None => (AvroNull(), None, None)
        }

      val context = IfNotNull.Context(retType, calls.toSet + IfNotNull.desc, symbolTypeResult, thenScope.inThisScope, thenResults map {_._2}, elseSymbols, elseTaskResults)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonExprs = factory.objectNode
      for ((name, expr) <- exprs)
        jsonExprs.put(name, expr.jsonNode(lineNumbers, memo))
      out.put("ifnotnull", jsonExprs)

      val jsonThenClause = factory.arrayNode
      for (expr <- thenClause)
        jsonThenClause.add(expr.jsonNode(lineNumbers, memo))
      out.put("then", jsonThenClause)

      elseClause match {
        case Some(clause) => {
          val jsonElseClause = factory.arrayNode
          for (expr <- clause)
            jsonElseClause.add(expr.jsonNode(lineNumbers, memo))
          out.put("else", jsonElseClause)
        }
        case None =>
      }

      out
    }
  }
  object IfNotNull {
    /** `"ifnotnull"`
      */
    val desc = "ifnotnull"
    case class Context(retType: AvroType, calls: Set[String], symbolTypeResult: Seq[(String, AvroType, TaskResult)], thenSymbols: Map[String, AvroType], thenClause: Seq[TaskResult], elseSymbols: Option[Map[String, AvroType]], elseClause: Option[Seq[TaskResult]]) extends ExpressionContext
  }

  /** Helper object for `pack` and `unpack` that checks format strings.
    */
  object BinaryFormatter {
    val FormatPad = """\s*(pad)\s*""".r
    val FormatBoolean = """\s*(boolean)\s*""".r
    val FormatByte = """\s*(byte|int8)\s*""".r
    val FormatUnsignedByte = """\s*unsigned\s*(byte|int8)\s*""".r
    val FormatShort = """\s*(<|>|!|little|big|network)?\s*(short|int16)\s*""".r
    val FormatUnsignedShort = """\s*(<|>|!|little|big|network)?\s*(unsigned\s*short|unsigned\s*int16)\s*""".r
    val FormatInt = """\s*(<|>|!|little|big|network)?\s*(int|int32)\s*""".r
    val FormatUnsignedInt = """\s*(<|>|!|little|big|network)?\s*(unsigned\s*int|unsigned\s*int32)\s*""".r
    val FormatLong = """\s*(<|>|!|little|big|network)?\s*(long|long\s+long|int64)\s*""".r
    val FormatUnsignedLong = """\s*(<|>|!|little|big|network)?\s*(unsigned\s*long|unsigned\s*long\s+long|unsigned\s*int64)\s*""".r
    val FormatFloat = """\s*(<|>|!|little|big|network)?\s*(float|float32)\s*""".r
    val FormatDouble = """\s*(<|>|!|little|big|network)?\s*(double|float64)\s*""".r
    val FormatRaw = """\s*raw\s*""".r
    val FormatRawSize = """\s*raw\s*([0-9]+)\s*""".r
    val FormatToNull = """\s*(null\s*)?terminated\s*""".r
    val FormatPrefixed = """\s*(length\s*)?prefixed\s*""".r

    def isLittleEndian(endianness: String) = (endianness == "<"  ||  endianness == "little")

    /** Trait for binary format declaration.
      */
    trait Declare[T] {
      val value: T
      val avroType: AvroType
    }

    /** Binary format declaration for a padded byte.
      */
    case class DeclarePad[T](value: T) extends Declare[T] {
      val avroType = AvroNull()
    }
    /** Binary format declaration for a boolean type.
      */
    case class DeclareBoolean[T](value: T) extends Declare[T] {
      val avroType = AvroBoolean()
    }
    /** Binary format declaration for an integer byte.
      */
    case class DeclareByte[T](value: T, unsigned: Boolean) extends Declare[T] {
      val avroType = AvroInt()
    }
    /** Binary format declaration for a short (16-bit) integer.
      */
    case class DeclareShort[T](value: T, littleEndian: Boolean, unsigned: Boolean) extends Declare[T] {
      val avroType = AvroInt()
    }
    /** Binary format declaration for a regular (32-bit) integer.
      */
    case class DeclareInt[T](value: T, littleEndian: Boolean, unsigned: Boolean) extends Declare[T] {
      val avroType = if (unsigned) AvroLong() else AvroInt()
    }
    /** Binary format declaration for a long (64-bit) integer.
      */
    case class DeclareLong[T](value: T, littleEndian: Boolean, unsigned: Boolean) extends Declare[T] {
      val avroType = if (unsigned) AvroDouble() else AvroLong()
    }
    /** Binary format declaration for a single-precision (32-bit) floating point number.
      */
    case class DeclareFloat[T](value: T, littleEndian: Boolean) extends Declare[T] {
      val avroType = AvroFloat()
    }
    /** Binary format declaration for a double-precision (64-bit) floating point number.
      */
    case class DeclareDouble[T](value: T, littleEndian: Boolean) extends Declare[T] {
      val avroType = AvroDouble()
    }
    /** Binary format declaration for arbitrary-width raw data.
      */
    case class DeclareRaw[T](value: T) extends Declare[T] {
      val avroType = AvroBytes()
    }
    /** Binary format declaration for fixed-width raw data.
      */
    case class DeclareRawSize[T](value: T, size: Int) extends Declare[T] {
      val avroType = AvroBytes()
    }
    /** Binary format declaration for null-terminated raw data.
      */
    case class DeclareToNull[T](value: T) extends Declare[T] {
      val avroType = AvroBytes()
    }
    /** Binary format declaration for length-prefixed raw data.
      */
    case class DeclarePrefixed[T](value: T) extends Declare[T] {
      val avroType = AvroBytes()
    }

    /** Convert a format string to a [[com.opendatagroup.hadrian.ast.BinaryFormatter.Declare Declare]] object.
      * 
      * @param value task result, usually Java code, passed to the format declarer
      * @param f format string
      * @param pos source file location from the locator mark
      * @param output `true` for `pack`; `false` for `unpack`
      * @return format declarer
      */
    def formatToDeclare[T](value: T, f: String, pos: Option[String], output: Boolean): Declare[T] = f match {
      case FormatPad(_) =>                       DeclarePad(value)
      case FormatBoolean(_) =>                   DeclareBoolean(value)
      case FormatByte(_) =>                      DeclareByte(value, false)
      case FormatUnsignedByte(_) =>              DeclareByte(value, true)
      case FormatShort(endianness, _) =>         DeclareShort(value, isLittleEndian(endianness), false)
      case FormatUnsignedShort(endianness, _) => DeclareShort(value, isLittleEndian(endianness), true)
      case FormatInt(endianness, _) =>           DeclareInt(value, isLittleEndian(endianness), false)
      case FormatUnsignedInt(endianness, _) =>   DeclareInt(value, isLittleEndian(endianness), true)
      case FormatLong(endianness, _) =>          DeclareLong(value, isLittleEndian(endianness), false)
      case FormatUnsignedLong(endianness, _) =>  DeclareLong(value, isLittleEndian(endianness), true)
      case FormatFloat(endianness, _) =>         DeclareFloat(value, isLittleEndian(endianness))
      case FormatDouble(endianness, _) =>        DeclareDouble(value, isLittleEndian(endianness))
      case FormatRawSize(size) =>                DeclareRawSize(value, size.toInt)
      case FormatToNull(_) =>                    DeclareToNull(value)
      case FormatPrefixed(_) =>                  DeclarePrefixed(value)
      case FormatRaw() =>
        if (output)
          DeclareRaw(value)
        else
          throw new PFASemanticException("cannot read from unsized \"raw\" in binary formatter", pos)
      case x => throw new PFASemanticException("unrecognized \"%s\" found in binary formatter".format(x), pos)
    }
  }

  /** Abstract syntax tree for a `pack` construct.
    * 
    * @param exprs (format, expression) pairs
    * @param pos source file location from the locator mark
    */
  case class Pack(exprs: Seq[(String, Expression)], pos: Option[String]) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Pack => this.exprs == that.exprs  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1(exprs))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        exprs.flatMap(_._2.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Pack(exprs map {case (k, v) => (k, v.replace(pf).asInstanceOf[Expression])}, pos)

    if (exprs.size < 1)
      throw new PFASyntaxException("\"pack\" must contain at least one format-expression mapping", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      var exprsDeclareRes = List[BinaryFormatter.Declare[TaskResult]]()
      for ((f, expr) <- exprs) {
        val (exprCtx: ExpressionContext, exprRes: TaskResult) = expr.walk(task, symbolTable.newScope(true, true), functionTable, engineOptions, version)

        val declare = BinaryFormatter.formatToDeclare(exprRes, f, pos, true)

        if (!declare.avroType.accepts(exprCtx.retType))
          throw new PFASemanticException("\"pack\" expression with type %s cannot be cast to %s".format(exprCtx.retType, f), pos)
        calls ++= exprCtx.calls

        exprsDeclareRes = declare :: exprsDeclareRes
      }
      exprsDeclareRes = exprsDeclareRes.reverse

      val context = Pack.Context(AvroBytes(), calls.toSet + Pack.desc, exprsDeclareRes, pos)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonPack = factory.arrayNode
      for ((f, expr) <- exprs) {
        val pair = factory.objectNode
        pair.put(f, expr.jsonNode(lineNumbers, memo))
        jsonPack.add(pair)
      }
      out.put("pack", jsonPack)

      out
    }
  }
  object Pack {
    /** `"pack"`
      */
    val desc = "pack"
    case class Context(retType: AvroType, calls: Set[String], exprsDeclareRes: Seq[BinaryFormatter.Declare[TaskResult]], pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for the `unpack` construct.
    * 
    * @param bytes expression that evaluates to a `bytes` object
    * @param format (new bariable name, formatter) pairs
    * @param thenClause expressions to evaluate if the `bytes` matches the format
    * @param elseClause expressions to evalaute if present and the `bytes` does not match the format
    * @param pos source file location from the locator mark
    */
  case class Unpack(bytes: Expression, format: Seq[(String, String)], thenClause: Seq[Expression], elseClause: Option[Seq[Expression]], pos: Option[String]) extends Expression {
    format.find {case (key, _) => !validSymbolName(key)}
          .foreach {case (badKey, _) => throw new PFASyntaxException(s"${badKey} is not a valid symbol name!", None)}

    override def equals(other: Any): Boolean = other match {
      case that: Unpack => this.bytes == that.bytes  &&  this.format == that.format  &&  this.thenClause == that.thenClause  &&  this.elseClause == that.elseClause  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((bytes, format, thenClause, elseClause))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        bytes.collect(pf) ++
        thenClause.flatMap(_.collect(pf)) ++
        (if (elseClause == None) List[X]() else elseClause.get.flatMap(_.collect(pf)))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Unpack(bytes.replace(pf).asInstanceOf[Expression],
          format,
          thenClause.map(_.replace(pf).asInstanceOf[Expression]),
          elseClause.map(x => x.map(_.replace(pf).asInstanceOf[Expression])),
          pos)

    if (format.size < 1)
      throw new PFASyntaxException("unpack's \"format\" must contain at least one symbol-format mapping", pos)

    if (thenClause.size < 1)
        throw new PFASyntaxException("\"then\" clause must contain at least one expression", pos)

    if (elseClause != None  &&  elseClause.size < 1)
      throw new PFASyntaxException("\"else\" clause must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val calls = mutable.Set[String]()

      val bytesScope = symbolTable.newScope(true, true)
      val assignmentScope = symbolTable.newScope(false, false)

      val (bytesCtx: ExpressionContext, bytesRes: TaskResult) = bytes.walk(task, bytesScope, functionTable, engineOptions, version)
      if (!bytesCtx.retType.isInstanceOf[AvroBytes])
        throw new PFASemanticException("\"unpack\" expression must be a bytes object", pos)
      calls ++= bytesCtx.calls

      var formatter = List[BinaryFormatter.Declare[String]]()
      for ((s, f) <- format) {
        if (assignmentScope.get(s) != None)
          throw new PFASemanticException("symbol \"%s\" may not be redeclared or shadowed".format(s), pos)

        if (!validSymbolName(s))
          throw new PFASemanticException("\"%s\" is not a valid symbol name".format(s), pos)

        val declare = BinaryFormatter.formatToDeclare(s, f, pos, false)
        formatter = declare :: formatter
        assignmentScope.put(s, declare.avroType)
      }
      formatter = formatter.reverse

      val thenScope = assignmentScope.newScope(false, false)
      val thenResults: Seq[(ExpressionContext, TaskResult)] = thenClause.map(_.walk(task, thenScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      for ((exprCtx, _) <- thenResults)
        calls ++= exprCtx.calls

      val (retType, elseTaskResults, elseSymbols) =
        elseClause match {
          case Some(clause) => {
            val elseScope = symbolTable.newScope(false, false)

            val elseResults = clause.map(_.walk(task, elseScope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
            for ((exprCtx, _) <- elseResults)
              calls ++= exprCtx.calls

            val thenType = thenResults.last._1.retType
            val elseType = elseResults.last._1.retType
            val retType =
              try {
                P.mustBeAvro(LabelData.broadestType(List(thenType, elseType)))
              }
              catch {
                case err: IncompatibleTypes => throw new PFASemanticException(err.getMessage, pos)
              }

            (retType, Some(elseResults map {_._2}), Some(elseScope.inThisScope))
          }
          case None => (AvroNull(), None, None)
        }

      val context = Unpack.Context(retType, calls.toSet + Unpack.desc, bytesRes, formatter, thenScope.inThisScope, thenResults map {_._2}, elseSymbols, elseTaskResults, pos)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("unpack", bytes.jsonNode(lineNumbers, memo))

      val jsonFormat = factory.arrayNode
      for ((s, f) <- format) {
        val pair = factory.objectNode
        pair.put(s, f)
        jsonFormat.add(pair)
      }
      out.put("format", jsonFormat)

      val jsonThenClause = factory.arrayNode
      for (expr <- thenClause)
        jsonThenClause.add(expr.jsonNode(lineNumbers, memo))
      out.put("then", jsonThenClause)

      elseClause match {
        case Some(clause) => {
          val jsonElseClause = factory.arrayNode
          for (expr <- clause)
            jsonElseClause.add(expr.jsonNode(lineNumbers, memo))
          out.put("else", jsonElseClause)
        }
        case None =>
      }

      out
    }
  }
  object Unpack {
    /** `"unpack"`
      */
    val desc = "unpack"
    case class Context(retType: AvroType, calls: Set[String], bytes: TaskResult, formatter: Seq[BinaryFormatter.Declare[String]], thenSymbols: Map[String, AvroType], thenClause: Seq[TaskResult], elseSymbols: Option[Map[String, AvroType]], elseClause: Option[Seq[TaskResult]], pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for inline documentation.
    * 
    * @param comment inline documentation
    * @param pos source file location from the locator mark
    */
  case class Doc(comment: String, pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Doc =>
        this.comment == that.comment  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode(Tuple1[String](comment))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = Doc.Context(AvroNull(), Set[String](Doc.desc))
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("doc", comment)
      out
    }
  }
  object Doc {
    /** `"doc"`
      */
    val desc = "doc"
    case class Context(retType: AvroType, calls: Set[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a user-defined error.
    * 
    * @param message error message
    * @param code optional error code
    * @param pos source file location from the locator mark
    */
  case class Error(message: String, code: Option[Int], pos: Option[String] = None) extends Expression {
    if (code.exists(_ >= 0))
      throw new PFASyntaxException("\"code\" for user-defined errors must be negative", pos)

    override def equals(other: Any): Boolean = other match {
      case that: Error =>
        this.message == that.message  &&  this.code == that.code  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((message, code))

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val context = Error.Context(ExceptionType(), Set[String](Error.desc), message, code, pos)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      out.put("error", message)

      code match {
        case Some(number) => out.put("code", number)
        case None =>
      }
      out
    }
  }
  object Error {
    /** `"error"`
      */
    val desc = "error"
    case class Context(retType: AvroType, calls: Set[String], message: String, code: Option[Int], pos: Option[String]) extends ExpressionContext
  }

  /** Abstract syntax tree for a `try` form.
    * 
    * @param exprs exprssions to evaluate taht might raise an exception
    * @param filter optional filter for error messages or error codes
    * @param pos source file locationn from the locator mark
    */
  case class Try(exprs: Seq[Expression], filter: Option[Seq[Either[String, Int]]], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Try =>
        this.exprs == that.exprs  &&  this.filter == that.filter  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((exprs, filter))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        exprs.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Try(exprs.map(_.replace(pf).asInstanceOf[Expression]),
            filter,
            pos)

    if (exprs.size < 1)
      throw new PFASyntaxException("\"try\" block must contain at least one expression", pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val scope = symbolTable.newScope(true, true)
      val results: Seq[(ExpressionContext, TaskResult)] = exprs.map(_.walk(task, scope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}

      val (exprType, inferredType) =
        results.last._1.retType match {
          case _: ExceptionType | _: AvroNull => (AvroNull(), AvroNull())
          case AvroUnion(types) if (types.contains(AvroNull())) => (AvroUnion(types), AvroUnion(types))
          case AvroUnion(types) => (AvroUnion(types), AvroUnion(types :+ AvroNull()))
          case x => (x, AvroUnion(List(x, AvroNull())))
        }

      val context = Try.Context(inferredType, results.map(_._1.calls).flatten.toSet + Try.desc, scope.inThisScope, results map {_._2}, exprType, filter)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonExprs = factory.arrayNode
      for (expr <- exprs)
        jsonExprs.add(expr.jsonNode(lineNumbers, memo))
      out.put("try", jsonExprs)

      filter match {
        case Some(x) => {
          val fout = factory.arrayNode
          for (obj <- x) obj match {
            case Left(str: String) => fout.add(str)
            case Right(int: Int) => fout.add(java.lang.Integer.valueOf(int))
          }
          out.put("filter", fout)
        }
        case None =>
      }

      out
    }
  }
  object Try {
    /** `"try"`
      */
    val desc = "try"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], exprs: Seq[TaskResult], exprType: AvroType, filter: Option[Seq[Either[String, Int]]]) extends ExpressionContext
  }

  /** Abstract syntax tree for log messages.
    * 
    * @param exprs expressions to print to the log
    * @param namespace optional namespace string
    * @param pos source file location from the locator mark
    */
  case class Log(exprs: Seq[Expression], namespace: Option[String], pos: Option[String] = None) extends Expression {
    override def equals(other: Any): Boolean = other match {
      case that: Log =>
        this.exprs == that.exprs  &&  this.namespace == that.namespace  // but not pos
      case _ => false
    }
    override def hashCode(): Int = ScalaRunTime._hashCode((exprs, namespace))

    override def collect[X](pf: PartialFunction[Ast, X]): Seq[X] =
      super.collect(pf) ++
        exprs.flatMap(_.collect(pf))

    override def replace(pf: PartialFunction[Ast, Ast]): Ast =
      if (pf.isDefinedAt(this))
        pf.apply(this)
      else
        Log(exprs.map(_.replace(pf).asInstanceOf[Expression]),
            namespace,
            pos)

    override def walk(task: Task, symbolTable: SymbolTable, functionTable: FunctionTable, engineOptions: EngineOptions, version: PFAVersion): (AstContext, TaskResult) = {
      val scope = symbolTable.newScope(true, true)
      val results: Seq[(ExpressionContext, TaskResult)] = exprs.map(_.walk(task, scope, functionTable, engineOptions, version)) collect {case (x: ExpressionContext, y: TaskResult) => (x, y)}
      val context = Log.Context(AvroNull(), results.map(_._1.calls).flatten.toSet + Log.desc, scope.inThisScope, results map { case (exprContext, taskResult) => (taskResult, exprContext.retType) }, namespace)
      (context, task(context, engineOptions))
    }

    override def jsonNode(lineNumbers: Boolean, memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      if (lineNumbers) pos.foreach(out.put("@", _))

      val jsonExprs = factory.arrayNode
      for (expr <- exprs)
        jsonExprs.add(expr.jsonNode(lineNumbers, memo))
      out.put("log", jsonExprs)

      namespace match {
        case Some(name) => out.put("namespace", name)
        case None =>
      }
      out
    }
  }
  object Log {
    /** `"log"`
      */
    val desc = "log"
    case class Context(retType: AvroType, calls: Set[String], symbols: Map[String, AvroType], exprTypes: Seq[(TaskResult, AvroType)], namespace: Option[String]) extends ExpressionContext
  }

}
