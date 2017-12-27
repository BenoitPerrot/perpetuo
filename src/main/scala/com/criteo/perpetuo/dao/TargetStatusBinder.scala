package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{Status, TargetAtom, TargetAtomStatus, TargetStatus}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class TargetStatusRecord(id: Option[Long],
                                           executionId: Long,
                                           targetAtom: String,
                                           code: Status.Code,
                                           detail: String) {
  def toTargetStatus: TargetStatus =
    TargetStatus(executionId, targetAtom, code, detail)
}


trait TargetStatusBinder extends TableBinder {
  this: ExecutionBinder with DbContextProvider =>

  import dbContext.driver.api._

  protected implicit lazy val statusMapper = MappedColumnType.base[Status.Code, Short](
    op => op.id.toShort,
    short => Status(short.toInt)
  )

  class TargetStatusTable(tag: Tag) extends Table[TargetStatusRecord](tag, "target_status") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def executionId = column[Long]("execution_id")
    protected def fk = foreignKey(executionId, executionQuery)(_.id)

    def targetAtom = column[TargetAtom.Type]("target", O.SqlType(s"nvarchar(${TargetAtom.maxSize})"))
    protected def targetIdx = index(targetAtom)
    def code = column[Status.Code]("code")
    def detail = column[String]("detail", O.SqlType(s"nvarchar(4000)"))

    def * = (id.?, executionId, targetAtom, code, detail) <> (TargetStatusRecord.tupled, TargetStatusRecord.unapply)
  }

  val targetStatusQuery: TableQuery[TargetStatusTable] = TableQuery[TargetStatusTable]

  def insertTargetStatuses(executionId: Long, statusMap: Map[String, TargetAtomStatus]): Future[Unit] = {
    val listOfTargetStatus = statusMap.map { case (targetAtom, targetStatus) =>
      TargetStatusRecord(None, executionId, targetAtom, targetStatus.code, targetStatus.detail) //TODO: Remove after migration to targetStatus
    }
    dbContext.db.run((targetStatusQuery ++= listOfTargetStatus).transactionally).map(_ => ())
  }
}
