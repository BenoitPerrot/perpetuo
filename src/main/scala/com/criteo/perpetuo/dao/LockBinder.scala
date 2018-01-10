package com.criteo.perpetuo.dao


private[dao] case class LockRecord(name: String,
                                   deploymentRequestId: Long)


trait LockBinder extends TableBinder {
  this: DeploymentRequestBinder with DbContextProvider =>

  import dbContext.driver.api._

  class LockTable(tag: Tag) extends Table[LockRecord](tag, "lock") {
    def name = column[String]("name", O.SqlType("nvarchar(128)"))
    protected def pk = primaryKey(name)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def * = (name, deploymentRequestId) <> (LockRecord.tupled, LockRecord.unapply)
  }

  val lockQuery: TableQuery[LockTable] = TableQuery[LockTable]
}
