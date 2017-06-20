package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{Status, TargetAtom}


private[dao] case class TargetStatusRecord(operationTraceId: Long,
                                           executionSpecificationId: Long,
                                           targetAtom: String,
                                           code: Status.Code,
                                           detail: String)


trait TargetStatusBinder extends TableBinder {
  this: OperationTraceBinder with ExecutionSpecificationBinder with DbContextProvider =>

  import dbContext.driver.api._

  private implicit lazy val operationMapper = MappedColumnType.base[Status.Code, Short](
    op => op.id.toShort,
    short => Status(short.toInt)
  )

  class TargetStatusTable(tag: Tag) extends Table[TargetStatusRecord](tag, "target_status") {
    def operationTraceId = column[Long]("operation_trace_id")
    protected def opFk = foreignKey(operationTraceId, operationTraceQuery)(_.id)
    def executionSpecificationId = column[Long]("execution_specification_id")
    protected def exFk = foreignKey(executionSpecificationId, executionSpecificationQuery)(_.id)
    protected def pk = primaryKey((operationTraceId, executionSpecificationId))

    def targetAtom = column[TargetAtom.Type]("target", O.SqlType(s"nvarchar(${TargetAtom.maxSize})"))
    protected def targetIdx = index(targetAtom)
    def code = column[Status.Code]("code")
    def detail = column[String]("detail", O.SqlType(s"nvarchar(4000)"))

    def * = (operationTraceId, executionSpecificationId, targetAtom, code, detail) <> (TargetStatusRecord.tupled, TargetStatusRecord.unapply)
  }

  val targetStatusQuery: TableQuery[TargetStatusTable] = TableQuery[TargetStatusTable]
}
