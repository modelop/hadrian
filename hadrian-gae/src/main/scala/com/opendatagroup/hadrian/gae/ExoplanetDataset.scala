package com.opendatagroup.hadrian.gae

import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import javax.servlet.ServletContext

import scala.collection.mutable
import scala.collection.JavaConversions.mutableSeqAsJavaList
import scala.language.postfixOps
import scala.xml.XML

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

import com.opendatagroup.hadrian.datatype.AvroConversions.schemaToAvroType
import com.opendatagroup.hadrian.datatype.AvroType

trait Dataset {
  def avroType: AvroType
  def inputStream: InputStream
}

class ExoplanetDataset(servletContext: ServletContext) extends Dataset {
  private def JDouble(x: String): java.lang.Double =
    if (x == "")
      null
    else
      new java.lang.Double(x)

  private val parser = new Schema.Parser()

  val planetSchema = parser.parse("""
{"type": "record", "name": "Planet", "fields": [
    {"name": "name",          "type": "string",            "doc": "Name of the planet"},
    {"name": "detection",     "type": {"type": "enum", "name": "DetectionType",
        "symbols": ["astrometry", "imaging", "microlensing", "pulsar", "radial_velocity", "transit", "ttv", "OTHER"]},
                                                           "doc": "Technique used to make discovery"},
    {"name": "discovered",    "type": "string",            "doc": "Year of discovery"},
    {"name": "updated",       "type": "string",            "doc": "Date of last update"},
    {"name": "mass",          "type": ["double", "null"],  "doc": "Mass of the planet (multiples of Jupiter's mass)"},
    {"name": "radius",        "type": ["double", "null"],  "doc": "Radius of the planet (multiples of Jupiter's radius)"},
    {"name": "period",        "type": ["double", "null"],  "doc": "Duration of planet's year (Earth days)"},
    {"name": "max_distance",  "type": ["double", "null"],  "doc": "Maximum distance of planet from star (semi-major axis in AU)"},
    {"name": "eccentricity",  "type": ["double", "null"],  "doc": "Ellipticalness of orbit (0 == circle, 1 == escapes star)"},
    {"name": "temperature",   "type": ["double", "null"],  "doc": "Measured or calculated temperature of the planet (degrees Kelvin)"},
    {"name": "temp_measured", "type": ["boolean", "null"], "doc": "True iff the temperature was actually measured"},
    {"name": "molecules",     "type": {"type": "array", "items": "string"}, "doc": "Molecules identified in the planet's atmosphere"}
]}
""")

  val starSchema = parser.parse("""
{"type": "record", "name": "Star", "fields": [
    {"name": "name",   "type": "string",           "doc": "Name of the host star"},
    {"name": "ra",     "type": ["double", "null"], "doc": "Right ascension of the star in our sky (degrees, J2000)"},
    {"name": "dec",    "type": ["double", "null"], "doc": "Declination of the star in our sky (degrees, J2000)"},
    {"name": "mag",    "type": ["double", "null"], "doc": "Magnitude (dimness) of the star in our sky (unitless)"},
    {"name": "dist",   "type": ["double", "null"], "doc": "Distance of the star from Earth (parsecs)"},
    {"name": "mass",   "type": ["double", "null"], "doc": "Mass of the star (multiples of our Sun's mass)"},
    {"name": "radius", "type": ["double", "null"], "doc": "Radius of the star (multiples of our Sun's radius)"},
    {"name": "age",    "type": ["double", "null"], "doc": "Age of the star (billions of years)"},
    {"name": "temp",   "type": ["double", "null"], "doc": "Effective temperature of the star (degrees Kelvin)"},
    {"name": "type",   "type": ["string", "null"], "doc": "Spectral type of the star"},
    {"name": "planets", "type": {"type": "array", "items": "Planet"}}
]}
""")

  val detectionSchema = planetSchema.getField("detection").schema

  val avroType = schemaToAvroType(starSchema)

  def votToAvroBytes(vot: scala.xml.Elem): Array[Byte] = {
    val fieldsXml = vot \\ "TABLE" \\ "FIELD"
    val field = (fieldsXml \\ "@name").zipWithIndex.map({case (x, i) => (x.toString, i)}).toMap

    val rowsXml = vot \\ "DATA" \\ "TABLEDATA" \\ "TR"

    val stars = mutable.Map[String, GenericData.Record]()

    for (rowXml <- rowsXml) {
      val row = rowXml \\ "TD" map {_.text}

      val star_name = row(field("star_name"))
      if (!stars.contains(star_name)) {
        val star = new GenericData.Record(starSchema)

        star.put("name", star_name)
        star.put("ra", JDouble(row(field("ra"))))
        star.put("dec", JDouble(row(field("dec"))))
        star.put("mag", JDouble(row(field("mag_v"))))
        star.put("dist", JDouble(row(field("star_distance"))))
        star.put("mass", JDouble(row(field("star_mass"))))
        star.put("radius", JDouble(row(field("star_radius"))))
        star.put("age", JDouble(row(field("star_age"))))
        star.put("temp", JDouble(row(field("star_teff"))))
        star.put("type", row(field("star_sp_type")))
        star.put("planets", new java.util.ArrayList[GenericData.Record]())

        stars(star_name) = star
      }

      val planet = new GenericData.Record(planetSchema)
      planet.put("name", row(field("name")))

      planet.put("detection", new GenericData.EnumSymbol(detectionSchema, row(field("detection_type")) match {
        case "detected by astrometry"      => "astrometry"
        case "detected by imaging"         => "imaging"
        case "detected by microlensing"    => "microlensing"
        case "pulsar"                      => "pulsar"
        case "detected by radial velocity" => "radial_velocity"
        case "detected by transit"         => "transit"
        case "TTV"                         => "ttv"
        case _                             => "OTHER"
      }))

      planet.put("discovered", row(field("discovered")))
      planet.put("updated", row(field("updated")))
      planet.put("mass", JDouble(row(field("mass"))))
      planet.put("radius", JDouble(row(field("radius"))))
      planet.put("period", JDouble(row(field("orbital_period"))))
      planet.put("max_distance", JDouble(row(field("semi_major_axis"))))
      planet.put("eccentricity", JDouble(row(field("eccentricity"))))

      planet.put("temperature",
        if (row(field("temp_measured")) != null)
          JDouble(row(field("temp_measured")))
        else
          JDouble(row(field("temp_calculated"))))

      planet.put("temp_measured",
        if (row(field("temp_measured")) != null)
          java.lang.Boolean.TRUE
        else if (row(field("temp_calculated")) != null)
          java.lang.Boolean.FALSE
        else
          null)

      planet.put("molecules", mutableSeqAsJavaList(row(field("molecules")) split(",") map {_.trim} filter {!_.isEmpty} distinct))

      stars(star_name).get("planets").asInstanceOf[java.util.List[GenericData.Record]].add(planet)
    }

    val baos = new ByteArrayOutputStream
    val datumWriter = new GenericDatumWriter[GenericRecord](starSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(starSchema, baos)
    stars foreach {case (n, v) => dataFileWriter.append(v)}
    dataFileWriter.close()

    baos.toByteArray
  }

  val bytes = votToAvroBytes(XML.load(servletContext.getResourceAsStream("/WEB-INF/exoplanet_catalog.vot")))
  def inputStream = new ByteArrayInputStream(bytes)
}

object ExoplanetDataset {
  // we use this sample engine as a startup test and as default contents for the index.jsp page
  val sampleEngine = """
# Since this scoring engine is used in a data pipeline, its input is fixed.  Changing it would cause an error.
input:
  type: record
  name: Star
  fields:
    # The host star has the following fields.  Note that [double, "null"] means double or null.
    - {name: name,   type: string,           doc: "Name of the host star"}
    - {name: ra,     type: [double, "null"], doc: "Right ascension of the star in our sky (degrees, J2000)"}
    - {name: dec,    type: [double, "null"], doc: "Declination of the star in our sky (degrees, J2000)"}
    - {name: mag,    type: [double, "null"], doc: "Magnitude (dimness) of the star in our sky (unitless)"}
    - {name: dist,   type: [double, "null"], doc: "Distance of the star from Earth (parsecs)"}
    - {name: mass,   type: [double, "null"], doc: "Mass of the star (multiples of our Sun's mass)"}
    - {name: radius, type: [double, "null"], doc: "Radius of the star (multiples of our Sun's radius)"}
    - {name: age,    type: [double, "null"], doc: "Age of the star (billions of years)"}
    - {name: temp,   type: [double, "null"], doc: "Effective temperature of the star (degrees Kelvin)"}
    - {name: type,   type: [string, "null"], doc: "Spectral type of the star"}
    - name: planets
      type:
        # planets is an array of Planet records.  This taxonomy cannot be represented in a flat n-tuple.
        type: array
        items:
          type: record
          name: Planet
          fields:
            # A planet has the following fields.  Note the use of an enumeration type and an array of strings.
            - {name: name,          type: string,            doc: "Name of the planet"}
            - name: detection
              type:
                type: enum
                name: DetectionType
                symbols: [astrometry, imaging, microlensing, pulsar, radial_velocity, transit, ttv, OTHER]
                doc: "Technique used to make discovery"
            - {name: discovered,    type: string,            doc: "Year of discovery"}
            - {name: updated,       type: string,            doc: "Date of last update"}
            - {name: mass,          type: [double, "null"],  doc: "Mass of the planet (multiples of Jupiter's mass)"}
            - {name: radius,        type: [double, "null"],  doc: "Radius of the planet (multiples of Jupiter's radius)"}
            - {name: period,        type: [double, "null"],  doc: "Duration of planet's year (Earth days)"}
            - {name: max_distance,  type: [double, "null"],  doc: "Maximum distance of planet from star (semi-major axis in AU)"}
            - {name: eccentricity,  type: [double, "null"],  doc: "Ellipticalness of orbit (0 == circle, 1 == escapes star)"}
            - {name: temperature,   type: [double, "null"],  doc: "Measured or calculated temperature of the planet (degrees Kelvin)"}
            - {name: temp_measured, type: [boolean, "null"], doc: "True iff the temperature was actually measured"}
            - name: molecules
              type: {type: array, items: string}
              doc: Molecules identified in the planet's atmosphere

# Since this scoring engine is used in a data pipeline, its output is fixed.  Changing it would break the scatter plot.
output:
  type: record
  name: Output
  doc: "Interpreted by as positions, radii and colors of dots in the scatter plot."
  fields:
    - {name: x,       type: double,  doc: "Horizontal coordinate"}
    - {name: y,       type: double,  doc: "Vertical coordinate"}
    - {name: radius,  type: double,  doc: "Size of dot in screen pixels"}
    - {name: color,   type: double,  doc: "Rainbow colors from 0 to 1"}
    - {name: opacity, type: double,  doc: "Opacity of color from 0 to 1"}

# This "emit" method provides a global "emit" function that the action can call to return results.  The default method, "map", would require exactly one output per input (one per star), but "emit" allows more (one per planet) or fewer (filtered data).
method: emit

# This section describes what the scoring engine should do with the input.
action:
  - foreach: planet
    in: input.planets
    do:
      - log: [input.name, planet.name, planet.detection, planet.molecules]
      - ifnotnull: {mass: planet.mass, radius: planet.radius}
        then:
          emit:
            new: {x: mass, y: radius, radius: 5.0, color: 0.0, opacity: 1.0}
            type: Output

# This section has additional information about the scoring engine that is outside the scope of the PFA language, but is informative for a particular PFA host.  In this case, we tell the GAE server how to set up the data pipeline.
metadata:
  dataset: exoplanets
"""
}
