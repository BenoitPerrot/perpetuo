package com.criteo.perpetuo.dao

import com.twitter.inject.Test
import slick.jdbc.SQLServerProfile

import scala.io.Source


class SchemaSpec extends Test {

  val schema = new Schema(new DbContext(SQLServerProfile, null))

  def getResourceAsString(resourceFileName: String): String =
    Source.fromURL(getClass.getResource(resourceFileName)).mkString

  def toWords(string: String): List[String] = {
    """"[^"]*"|\w+|[^"\w]+""".r.findAllIn(string.trim).toList.map {
      case quotedName if quotedName.startsWith("\"") => quotedName
      case sqlWord if sqlWord.head.isLetterOrDigit => sqlWord.toUpperCase
      case sep => ((s: String) => if (s.isEmpty) " " else s) (sep.replaceAll("\\s+", ""))
    }
  }

  def asSql(text: String): String = toWords(text).mkString

  def asSql(statements: Iterator[String]): String = asSql(statements.mkString(" "))

  test("Self-test splits in upper case words and remove extra white spaces") {
    toWords(" Abc de_f   \nghi  \t ") shouldEqual List("ABC", " ", "DE_F", " ", "GHI")
  }
  test("Self-test leaves quoted blocks untouched") {
    // we don't support quote escaping because we clearly don't need it
    toWords(""" abc "de_f "  ghi   """) shouldEqual List("ABC", " ", """"de_f """", " ", "GHI")
    toWords(""""  abc "de_f   ghi   """) shouldEqual List(""""  abc """", "DE_F", " ", "GHI")
    toWords(""" abc de_f   gh"i"""") shouldEqual List("ABC", " ", "DE_F", " ", "GH", """"i"""")
  }
  test("Self-test keeps the punctuation but not extra spaces") {
    // we don't support quote escaping because we clearly don't need it
    toWords(""" abc, "de_f "  ghi   """) shouldEqual List("ABC", ",", """"de_f """", " ", "GHI")
    toWords(""""abc "de_f ;-)ghi   """) shouldEqual List(""""abc """", "DE_F", ";-)", "GHI")
    toWords(""" abc de_f("gh", i )""") shouldEqual List("ABC", " ", "DE_F", "(", """"gh"""", ",", "I", ")")
  }
  test("Self-test normalizes SQL and drop spaces") {
    asSql(
      Seq(
        """create table   "Foo_Bar" (""",
        """  "id"    BIGINT    NOT NULL,""",
        """  "others" [...]""",
        """)"""
      ).iterator
    ) shouldEqual
      """CREATE TABLE "Foo_Bar"("id" BIGINT NOT NULL,"others"[...])"""
  }

  test("Schema has correct create statements") {
    asSql(schema.all.createStatements) shouldEqual asSql(getResourceAsString("create-db.sql"))
  }

  test("Schema has correct drop statements") {
    asSql(schema.all.dropStatements) shouldEqual asSql(getResourceAsString("drop-db.sql"))
  }
}
