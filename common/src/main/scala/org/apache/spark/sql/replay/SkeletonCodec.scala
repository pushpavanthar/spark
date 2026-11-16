package org.apache.spark.sql.replay

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Command, GlobalLimit, LeafNode, LocalLimit,
  LogicalPlan, Offset, Statistics, Tail}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{LogicalRelation, WriteFiles}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/** Stands in for an expression the codec cannot represent (a UDF, a set of literal values). */
case class OpaqueExpr(name: String, dataType: DataType, nullable: Boolean,
    isDeterministic: Boolean, children: Seq[Expression]) extends Expression with Unevaluable {
  override lazy val deterministic: Boolean = isDeterministic
  override protected def withNewChildrenInternal(c: IndexedSeq[Expression]): Expression =
    copy(children = c)
}

class ReplayUnsupportedException(msg: String) extends RuntimeException(msg)

/**
 * Writes an optimized logical plan as JSON and reads it back as real Catalyst operators.
 *
 * Generic over node classes: a node is its class name plus its constructor arguments (the case
 * class fields followed by `otherCopyArgs`), the same pair `TreeNode.makeCopy` uses. Three things
 * are special:
 *  - leaves become a stub with output attributes and statistics, and are read back as
 *    [[LogicalRDD]] over an empty RDD;
 *  - literal values are not written. Each distinct value in a plan gets a small number instead, so
 *    two subplans that differed only in a literal still differ after the round trip;
 *  - an expression with an argument that cannot be written becomes an [[OpaqueExpr]].
 */
object SkeletonCodec {
  private val f = JsonNodeFactory.instance

  // Expression ids read back from a skeleton must not collide with ids the planner creates in the
  // engine's JVM, so they get their own namespace.
  val replayJvmId: UUID = UUID.fromString("5eb1a700-0000-4000-8000-000000000000")

  // Classes are loaded by name from event-log content: restrict what a log can make us construct.
  private val allowed = Seq("org.apache.spark.sql.", "scala.Tuple", "scala.Some")
  private def load(name: String): Class[_] = {
    if (!allowed.exists(name.startsWith)) throw new ReplayUnsupportedException(s"class $name")
    Class.forName(name)
  }

  /**
   * A write's own arguments (table, options, file format) are not replayed; its query is. Any
   * distribution or ordering the write requires is already a node of that query.
   */
  def unwrapCommand(plan: LogicalPlan): LogicalPlan = plan match {
    case c: Command if c.children.size == 1 => unwrapCommand(c.children.head)
    case w: WriteFiles => unwrapCommand(w.child)
    case p => p
  }

  // ------------------------------------------------------------------ encode

  final class Encoder {
    var opaqueNodes = 0
    private val literalIds = mutable.Map[(DataType, Any), Int]()
    // Set by an argument that cannot be written; consumed by the nearest enclosing node.
    private var rawOpaque = false

    def plan(p: LogicalPlan): JsonNode = p match {
      case l: LeafNode => leaf(l)
      case other => node(other)
    }

    private def leaf(l: LeafNode): JsonNode = {
      val name = l match {
        case r: LogicalRelation =>
          r.catalogTable.map(_.identifier.unquotedString).getOrElse(r.relation.getClass.getSimpleName)
        case r: DataSourceV2ScanRelation => r.relation.name
        case o => o.nodeName
      }
      val o = f.objectNode()
      val stats = l.stats
      val body = o.putObject("leaf")
      body.put("relation", name)
      body.set[JsonNode]("output", seq(l.output))
      body.put("sizeInBytes", stats.sizeInBytes.toString)
      stats.rowCount.foreach(r => body.put("rowCount", r.toString))
      o
    }

    private def node(t: TreeNode[_]): JsonNode = t match {
      case a: AttributeReference =>
        val o = f.objectNode()
        val b = o.putObject("attr")
        b.put("name", a.name).put("dataType", a.dataType.json).put("nullable", a.nullable)
          .put("exprId", a.exprId.id)
        b.set[JsonNode]("qualifier", seq(a.qualifier))
        o
      case l: Literal => literal(l)
      case _ =>
        val outer = rawOpaque
        rawOpaque = false
        val p = t.asInstanceOf[Product]
        val args = f.arrayNode()
        // A limit is not data: the planner sizes the subplan from it, so its value is kept.
        val isLimit = t.isInstanceOf[GlobalLimit] || t.isInstanceOf[LocalLimit] ||
          t.isInstanceOf[Offset] || t.isInstanceOf[Tail]
        (p.productIterator ++ otherCopyArgs(t).iterator).foreach {
          case l @ Literal(_: Int, IntegerType) if isLimit => args.add(literal(l, keepInt = true))
          case a => args.add(value(a))
        }
        val mine = rawOpaque
        rawOpaque = outer
        if (!mine) {
          val o = f.objectNode()
          o.put("cls", t.getClass.getName)
          o.set[JsonNode]("args", args)
          o
        } else {
          opaqueNodes += 1
          t match {
            case e: Expression =>
              val o = f.objectNode()
              val b = o.putObject("opaqueExpr")
              b.put("name", e.prettyName).put("dataType", e.dataType.json)
                .put("nullable", e.nullable).put("deterministic", e.deterministic)
              b.set[JsonNode]("children", seq(e.children))
              o
            case other =>
              f.objectNode().put("opaquePlan", other.getClass.getName)
          }
        }
    }

    private def literal(l: Literal, keepInt: Boolean = false): JsonNode = {
      val o = f.objectNode()
      val b = o.putObject("lit")
      b.put("dataType", l.dataType.json)
      (l.value, l.dataType) match {
        case (null, _) => b.putNull("value")
        case (v: Boolean, _) => b.put("value", v)
        case (v: Int, _) if keepInt => b.put("int", v)
        case (v, dt) => b.put("n", literalIds.getOrElseUpdate((dt, v), literalIds.size + 1))
      }
      o
    }

    private def seq(s: Seq[_]): ArrayNode = {
      val a = f.arrayNode()
      s.foreach(x => a.add(value(x)))
      a
    }

    private def tagged(tag: String)(put: ObjectNode => Unit): JsonNode = {
      val o = f.objectNode()
      put(o)
      o.put("t", tag)
      o
    }

    private def value(v: Any): JsonNode = v match {
      case null => f.nullNode()
      // Before the tree cases: some expressions are singletons (UnboundedPreceding, CurrentRow).
      case o: TreeNode[_] if isModule(o) => tagged("obj")(_.put("cls", o.getClass.getName))
      case p: LogicalPlan => plan(p)
      case t: TreeNode[_] => node(t)
      case d: DataType => tagged("dt")(_.put("v", d.json))
      case m: Metadata => tagged("meta")(_.put("v", m.json))
      case e: ExprId => tagged("exprId")(_.put("v", e.id))
      case None => tagged("none")(_ => ())
      case Some(x) => tagged("some")(_.set[JsonNode]("v", value(x)))
      case s: Seq[_] => tagged("seq")(_.set[JsonNode]("v", seq(s)))
      // Operator options (to_json, from_json, csv): configuration, not data.
      case m: Map[_, _] => tagged("map")(_.set[JsonNode]("v", seq(m.toSeq.flatMap(e => Seq(e._1, e._2)))))
      // StaticInvoke names its target class: Iceberg's partition transforms in a write's distribution.
      case c: Class[_] => tagged("class")(_.put("v", c.getName))
      case s: String => tagged("s")(_.put("v", s))
      case b: Boolean => tagged("b")(_.put("v", b))
      case i: Int => tagged("i")(_.put("v", i))
      case l: Long => tagged("l")(_.put("v", l))
      case d: Double => tagged("d")(_.put("v", d))
      case c: Char => tagged("c")(_.put("v", c.toInt))
      case e: Enumeration#Value =>
        tagged("enum")(_.put("cls", enumOf(e).getClass.getName).put("v", e.toString))
      case o: AnyRef if isModule(o) => tagged("obj")(_.put("cls", o.getClass.getName))
      case p: Product if !p.isInstanceOf[Iterable[_]] =>
        tagged("prod") { o =>
          o.put("cls", p.getClass.getName)
          o.set[JsonNode]("v", seq(p.productIterator.toSeq))
        }
      case _ =>
        // A function, an encoder, a set of values: nothing safe or useful to write.
        rawOpaque = true
        tagged("opaque")(_ => ())
    }
  }

  private def otherCopyArgs(t: TreeNode[_]): Seq[AnyRef] = {
    val m = classOf[TreeNode[_]].getDeclaredMethod("otherCopyArgs")
    m.setAccessible(true)
    m.invoke(t).asInstanceOf[Seq[AnyRef]]
  }

  private def isModule(o: AnyRef): Boolean =
    o.getClass.getName.endsWith("$") && o.getClass.getFields.exists(_.getName == "MODULE$")

  private def enumOf(v: Enumeration#Value): Enumeration = {
    val m = classOf[Enumeration#Value].getMethod("scala$Enumeration$$outerEnum")
    m.setAccessible(true)
    m.invoke(v).asInstanceOf[Enumeration]
  }

  // ------------------------------------------------------------------ decode

  /** Rebuilds the plan. Leaves are empty [[LogicalRDD]]s that carry the recorded statistics. */
  def decode(root: JsonNode, session: SparkSession): LogicalPlan =
    new Decoder(session).any(root).asInstanceOf[LogicalPlan]

  private final class Decoder(session: SparkSession) {
    def any(j: JsonNode): Any = {
      if (j == null || j.isNull) null
      else if (j.has("leaf")) leaf(j.get("leaf"))
      else if (j.has("attr")) attr(j.get("attr"))
      else if (j.has("lit")) literal(j.get("lit"))
      else if (j.has("cls") && !j.has("t")) construct(j.get("cls").asText, items(j.get("args")))
      else if (j.has("opaqueExpr")) {
        val b = j.get("opaqueExpr")
        OpaqueExpr(b.get("name").asText, DataType.fromJson(b.get("dataType").asText),
          b.get("nullable").asBoolean, b.get("deterministic").asBoolean,
          items(b.get("children")).map(_.asInstanceOf[Expression]))
      } else if (j.has("opaquePlan")) {
        throw new ReplayUnsupportedException(s"operator ${j.get("opaquePlan").asText}")
      } else tagged(j)
    }

    private def items(a: JsonNode): Seq[Any] = a.elements().asScala.map(any).toList

    private def tagged(j: JsonNode): Any = j.get("t").asText match {
      case "dt" => DataType.fromJson(j.get("v").asText)
      case "meta" => Metadata.fromJson(j.get("v").asText)
      case "exprId" => ExprId(j.get("v").asLong, replayJvmId)
      case "none" => None
      case "some" => Some(any(j.get("v")))
      case "seq" => items(j.get("v"))
      case "map" => items(j.get("v")).grouped(2).map(e => e.head -> e(1)).toMap
      // Loaded without initialization, so no code of the named class runs; it is never invoked.
      case "class" =>
        val name = j.get("v").asText
        try Utils.classForName[AnyRef](name, initialize = false) catch {
          case _: ClassNotFoundException =>
            throw new ReplayUnsupportedException(s"class $name is not on the engine classpath")
        }
      case "s" => j.get("v").asText
      case "b" => j.get("v").asBoolean
      case "i" => j.get("v").asInt
      case "l" => j.get("v").asLong
      case "d" => j.get("v").asDouble
      case "c" => j.get("v").asInt.toChar
      case "enum" =>
        val e = load(j.get("cls").asText).getField("MODULE$").get(null).asInstanceOf[Enumeration]
        e.withName(j.get("v").asText)
      case "obj" => load(j.get("cls").asText).getField("MODULE$").get(null)
      case "prod" => construct(j.get("cls").asText, items(j.get("v")))
      case other => throw new ReplayUnsupportedException(s"value of kind $other")
    }

    private def construct(cls: String, args: Seq[Any]): Any = {
      val boxed = args.map(_.asInstanceOf[AnyRef])
      val ctors = load(cls).getConstructors.filter(_.getParameterCount == boxed.size)
      ctors.iterator.map { c =>
        try Some(c.newInstance(boxed: _*)) catch { case _: IllegalArgumentException => None }
      }.collectFirst { case Some(x) => x }.getOrElse(
        throw new ReplayUnsupportedException(s"no constructor of $cls takes ${boxed.size} args"))
    }

    private def attr(b: JsonNode): AttributeReference =
      AttributeReference(b.get("name").asText, DataType.fromJson(b.get("dataType").asText),
        b.get("nullable").asBoolean)(ExprId(b.get("exprId").asLong, replayJvmId),
        b.get("qualifier").elements().asScala.map(x => any(x).asInstanceOf[String]).toList)

    private def leaf(b: JsonNode): LogicalPlan = {
      val output = b.get("output").elements().asScala.map(x => any(x).asInstanceOf[Attribute]).toList
      val stats = Statistics(BigInt(b.get("sizeInBytes").asText),
        Option(b.get("rowCount")).map(r => BigInt(r.asText)))
      LogicalRDD(output, session.sparkContext.emptyRDD[InternalRow])(session, Some(stats), None)
    }

    // The value is gone; what is kept is the type and which literals were equal to each other.
    private def literal(b: JsonNode): Literal = {
      val dt = DataType.fromJson(b.get("dataType").asText)
      if (b.has("value")) {
        Literal(if (b.get("value").isNull) null else b.get("value").asBoolean, dt)
      } else if (b.has("int")) {
        Literal(b.get("int").asInt, dt)
      } else {
        val n = b.get("n").asInt
        val v: Any = dt match {
          case ByteType => n.toByte
          case ShortType => n.toShort
          case IntegerType | DateType => n
          case LongType | TimestampType | TimestampNTZType => n.toLong
          case FloatType => n.toFloat
          case DoubleType => n.toDouble
          // As the unscaled value, so that it fits any precision and scale.
          case d: DecimalType =>
            Decimal(BigDecimal(BigInt(n) % BigInt(10).pow(d.precision), d.scale), d.precision, d.scale)
          case StringType => UTF8String.fromString(s"lit_$n")
          case BinaryType => s"lit_$n".getBytes("UTF-8")
          case other => try Literal.default(other).value catch { case NonFatal(_) => null }
        }
        Literal(v, dt)
      }
    }
  }
}
