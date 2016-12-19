package com.criteo.perpetuo.dao.drivers

import slick.driver.H2Driver


// There is an error in H2 SQL generator that makes tests below fail: https://github.com/slick/slick/issues/763
// Ported a fix from https://github.com/dangerousben/slick/commit/50528fffc0f64abb1a10455430ae9374a750167d
// Remove this fix and use H2Driver profile when this is officially fixed
object FixedH2Driver extends H2Driver {
  override def createColumnDDLBuilder(column: slick.ast.FieldSymbol, table: Table[_]): ColumnDDLBuilder = new ColumnDDLBuilder(column)

  class ColumnDDLBuilder(column: slick.ast.FieldSymbol) extends super.ColumnDDLBuilder(column) {
    override protected def appendOptions(sb: StringBuilder) {
      if (defaultLiteral ne null) sb append " DEFAULT " append defaultLiteral
      if (notNull) sb append " NOT NULL"
      if (primaryKey) sb append " PRIMARY KEY"
      if (autoIncrement) sb append " AUTO_INCREMENT"
    }
  }
}
