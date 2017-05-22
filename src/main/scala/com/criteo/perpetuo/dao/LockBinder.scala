package com.criteo.perpetuo.dao


private[dao] case class LockRecord(name: String,
                                   operationTraceId: Long)


trait LockBinder extends TableBinder {
  this: OperationTraceBinder with DbContextProvider =>

  import dbContext.driver.api._

  class LockTable(tag: Tag) extends Table[LockRecord](tag, "lock") {
    def name = column[String]("name", O.SqlType("nvarchar(128)"))
    protected def pk = primaryKey(name)

    def operationTraceId = column[Long]("operation_trace_id")
    protected def fk = foreignKey(operationTraceId, operationTraceQuery)(_.id)

    def * = (name, operationTraceId) <> (LockRecord.tupled, LockRecord.unapply)
  }

  val lockQuery: TableQuery[LockTable] = TableQuery[LockTable]
}
