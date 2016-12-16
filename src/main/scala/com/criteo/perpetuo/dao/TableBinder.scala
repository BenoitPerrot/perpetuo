package com.criteo.perpetuo.dao

import slick.ast.{ColumnOption, TypedType}
import slick.lifted.{AbstractTable, ForeignKeyQuery, Index, PrimaryKey}

import scala.collection.mutable

trait TableBinder {
  this: ProfileProvider =>

  import profile.api._

  abstract class Table[T](tag: Tag, name: String) extends profile.api.Table[T](tag, name) {
    val columnNames = mutable.HashMap[AnyRef, String]()

    override def column[C](n: String, options: ColumnOption[C]*)(implicit tt: TypedType[C]): Rep[C] = {
      val col = super.column(n, options: _*)
      columnNames.put(col, n)
      col
    }

    def primaryKey[C](sourceColumns: C)
                     (implicit shape: Shape[_ <: FlatShapeLevel, C, _, _]): PrimaryKey = {
      val name = s"PK_$tableName"
      primaryKey(name, sourceColumns)
    }

    def foreignKey[P <: AnyRef, PU, TT <: AbstractTable[_], U]
    (sourceColumns: P, targetTableQuery: TableQuery[TT])
    (targetColumns: TT => P)
    (implicit unpack: Shape[_ <: FlatShapeLevel, TT, U, _], unpackp: Shape[_ <: FlatShapeLevel, P, PU, _]): ForeignKeyQuery[TT, U] = {
      val name = s"FK_${tableName}_${targetTableQuery.baseTableRow.tableName}_${columnNames(sourceColumns)}"
      foreignKey(name, sourceColumns, targetTableQuery)(targetColumns)
    }

    def index[C <: AnyRef](on: C, unique: Boolean = false)
                          (implicit shape: Shape[_ <: FlatShapeLevel, C, _, _]): Index = {
      val name = s"IX_${tableName}_${columnNames(on)}"
      index(name, on, unique)
    }
  }
}
