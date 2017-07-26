package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{Status, TargetAtom, TargetAtomStatus}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class TargetStatusRecord(id: Option[Long],
                                           executionId: Long,
                                           operationTraceId: Long, //TODO: Remove after migration to targetStatus
                                           executionSpecificationId: Long, //TODO: Remove after migration to targetStatus
                                           targetAtom: String,
                                           code: Status.Code,
                                           detail: String)


trait TargetStatusBinder extends TableBinder {
  this: ExecutionBinder with DbContextProvider =>

  import dbContext.driver.api._

  private implicit lazy val statusMapper = MappedColumnType.base[Status.Code, Short](
    op => op.id.toShort,
    short => Status(short.toInt)
  )

  class TargetStatusTable(tag: Tag) extends Table[TargetStatusRecord](tag, "target_status") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def executionId = column[Long]("execution_id")
    protected def fk = foreignKey(executionId, executionQuery)(_.id)

    def operationTraceId = column[Long]("operation_trace_id")
    def executionSpecificationId = column[Long]("execution_specification_id")

    def targetAtom = column[TargetAtom.Type]("target", O.SqlType(s"nvarchar(${TargetAtom.maxSize})"))
    protected def targetIdx = index(targetAtom)
    def code = column[Status.Code]("code")
    def detail = column[String]("detail", O.SqlType(s"nvarchar(4000)"))

    def * = (id.?, executionId, operationTraceId, executionSpecificationId, targetAtom, code, detail) <> (TargetStatusRecord.tupled, TargetStatusRecord.unapply)
  }

  val targetStatusQuery: TableQuery[TargetStatusTable] = TableQuery[TargetStatusTable]

  def addToExecution(executionId: Long, statusMap: Map[String, TargetAtomStatus]): Future[Unit] = {
    val listOfTargetStatus = statusMap.map { case (targetAtom, targetStatus) =>
      TargetStatusRecord(None, executionId, -1, -1, targetAtom, targetStatus.code, targetStatus.detail) //TODO: Remove after migration to targetStatus
    }
    dbContext.db.run((targetStatusQuery ++= listOfTargetStatus).transactionally).map(_ => ())
  }
}
