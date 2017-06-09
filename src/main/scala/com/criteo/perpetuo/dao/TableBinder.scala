package com.criteo.perpetuo.dao

import slick.ast.{ColumnOption, TypedType}
import slick.lifted.{AbstractTable, ForeignKeyQuery, Index, PrimaryKey}

import scala.collection.mutable

trait TableBinder {
  this: DbContextProvider =>

  import dbContext.driver.api._

  abstract class Table[T](tag: Tag, name: String) extends dbContext.driver.api.Table[T](tag, name) {
    val columnNames = mutable.HashMap[AnyRef, String]()

    override def column[C](n: String, options: ColumnOption[C]*)(implicit tt: TypedType[C]): Rep[C] = {
      val col = super.column(n, options: _*)
      columnNames.put(col, n)
      col
    }

    def primaryKey[C](sourceColumns: C)
                     (implicit shape: Shape[_ <: FlatShapeLevel, C, _, _]): PrimaryKey = {
      val name = s"pk_$tableName"
      primaryKey(name, sourceColumns)
    }

    def foreignKey[P <: AnyRef, PU, TT <: AbstractTable[_], U]
    (sourceColumns: P, targetTableQuery: TableQuery[TT])
    (targetColumns: TT => P)
    (implicit unpack: Shape[_ <: FlatShapeLevel, TT, U, _], unpackp: Shape[_ <: FlatShapeLevel, P, PU, _]): ForeignKeyQuery[TT, U] = {
      val columnName = columnNames(sourceColumns)
      val targetTableName = targetTableQuery.baseTableRow.tableName
      assert(columnName.startsWith(targetTableName), s"Column $columnName must start with $targetTableName")
      val name = s"fk_${tableName}_$columnName"
      foreignKey(name, sourceColumns, targetTableQuery)(targetColumns)
    }

    def index[C <: AnyRef](on: C)
                          (implicit shape: Shape[_ <: FlatShapeLevel, C, _, _]): Index = {
      index(on, unique = false)
    }

    def index[C <: AnyRef](on: C, unique: Boolean)
                          (implicit shape: Shape[_ <: FlatShapeLevel, C, _, _]): Index = {
      val name = s"ix_${tableName}_${columnNames(on)}"
      index(name, on, unique)
    }
  }
}
