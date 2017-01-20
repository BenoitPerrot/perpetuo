package com.criteo.perpetuo.dao

import com.criteo.perpetuo.TestUtils._
import com.criteo.perpetuo.app.DbContext
import com.twitter.inject.Test
import freeslick.MSSQLServerProfile
import slick.driver.JdbcDriver


class SchemaSpec extends Test {

  class DatabaseBinder(val driver: JdbcDriver)
    extends DeploymentRequestBinder with OperationTraceBinder with ExecutionTraceBinder
      with DbContextProvider {

    override val dbContext: DbContext = new DbContext(driver, null)

    import driver.api._

    val schema: driver.DDL = deploymentRequestQuery.schema ++ operationTraceQuery.schema ++ executionTraceQuery.schema
  }

  object dumper extends DatabaseBinder(MSSQLServerProfile)

  def toWords(string: String): List[String] = {
    """"[^"]*"|\w+|[^"\w]+""".r.findAllIn(string.trim).toList.map {
      case quotedName if quotedName.startsWith("\"") => quotedName
      case sqlWord if sqlWord.head.isLetterOrDigit => sqlWord.toUpperCase
      case sep => ((s: String) => if (s.isEmpty) " " else s) (sep.replaceAll("\\s+", ""))
    }
  }

  def asSql(text: String): String = toWords(text).mkString
  def asSql(statements: Iterator[String]): String = asSql(statements.mkString(" "))

  "Self-test" should {
    "split in upper case words and remove extra white spaces" in {
      toWords(" Abc de_f   \nghi  \t ") shouldEqual List("ABC", " ", "DE_F", " ", "GHI")
    }
    "leave quoted blocks untouched" in {
      // we don't support quote escaping because we clearly don't need it
      toWords(""" abc "de_f "  ghi   """) shouldEqual List("ABC", " ", """"de_f """", " ", "GHI")
      toWords(""""  abc "de_f   ghi   """) shouldEqual List(""""  abc """", "DE_F", " ", "GHI")
      toWords(""" abc de_f   gh"i"""") shouldEqual List("ABC", " ", "DE_F", " ", "GH", """"i"""")
    }
    "keep the punctuation but not extra spaces" in {
      // we don't support quote escaping because we clearly don't need it
      toWords(""" abc, "de_f "  ghi   """) shouldEqual List("ABC", ",", """"de_f """", " ", "GHI")
      toWords(""""abc "de_f ;-)ghi   """) shouldEqual List(""""abc """", "DE_F", ";-)", "GHI")
      toWords(""" abc de_f("gh", i )""") shouldEqual List("ABC", " ", "DE_F", "(", """"gh"""", ",", "I", ")")
    }
    "normalize SQL and drop spaces" in {
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
  }

  "Schema" should {
    "have correct create statements" in {
      asSql(new DatabaseBinder(MSSQLServerProfile).schema.createStatements) shouldEqual
        asSql(getResourceAsString("create-db.sql"))
    }

    "have correct drop statements" in {
      asSql(new DatabaseBinder(MSSQLServerProfile).schema.dropStatements) shouldEqual
        asSql(getResourceAsString("drop-db.sql"))
    }
  }
}
