CREATE TABLE "product" (
  "id"   INTEGER       NOT NULL IDENTITY,
  "name" NVARCHAR(128) NOT NULL
)
ALTER TABLE "product"
  ADD CONSTRAINT "pk_product" PRIMARY KEY ("id")
CREATE UNIQUE INDEX "ix_product_name"
  ON "product" ("name")


CREATE TABLE "deployment_request" (
  "id"            BIGINT        NOT NULL IDENTITY,
  "product_id"    INTEGER       NOT NULL,
  "version"       NVARCHAR(64)  NOT NULL,
  "target"        NVARCHAR(MAX) NOT NULL,
  "comment"       NVARCHAR(256) NOT NULL,
  "creator"       NVARCHAR(64)  NOT NULL,
  "creation_date" DATETIME      NOT NULL
)
ALTER TABLE "deployment_request"
  ADD CONSTRAINT "pk_deployment_request" PRIMARY KEY ("id")
CREATE INDEX "ix_deployment_request_creation_date"
  ON "deployment_request" ("creation_date")


CREATE TABLE "operation_trace" (
  "id"                    BIGINT        NOT NULL IDENTITY,
  "deployment_request_id" BIGINT        NOT NULL,
  "operation"             SMALLINT      NOT NULL,
  "target_status"         NVARCHAR(MAX) NOT NULL,
  "creator"               NVARCHAR(64)  DEFAULT 'qabot' NOT NULL,
  "creation_date"         DATETIME      DEFAULT(CONVERT(DATETIME,{TS '1970-01-01 00:00:00.0'})) NOT NULL,
  "closing_date"          DATETIME      DEFAULT(CONVERT(DATETIME,{TS '1970-01-01 00:00:00.0'}))
)
ALTER TABLE "operation_trace"
  ADD CONSTRAINT "pk_operation_trace" PRIMARY KEY ("id")
CREATE INDEX "ix_operation_trace_closing_date"
  ON "operation_trace" ("closing_date")
CREATE INDEX "ix_operation_trace_creation_date"
  ON "operation_trace" ("creation_date")


CREATE TABLE "execution_trace" (
  "id"                 BIGINT   NOT NULL IDENTITY,
  "operation_trace_id" BIGINT   NOT NULL,
  "log_href"           NVARCHAR(1024),
  "state"              SMALLINT NOT NULL
)
ALTER TABLE "execution_trace"
  ADD CONSTRAINT "pk_execution_trace" PRIMARY KEY ("id")
CREATE UNIQUE INDEX "ix_execution_trace_log_href"
  ON "execution_trace" ("log_href")


CREATE TABLE "lock" (
  "name"               NVARCHAR(128) NOT NULL,
  "operation_trace_id" BIGINT        NOT NULL
)
ALTER TABLE "lock"
  ADD CONSTRAINT "pk_lock" PRIMARY KEY ("name")


ALTER TABLE "deployment_request"
  ADD CONSTRAINT "fk_deployment_request_product_id" FOREIGN KEY ("product_id") REFERENCES "product" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
ALTER TABLE "operation_trace"
  ADD CONSTRAINT "fk_operation_trace_deployment_request_id" FOREIGN KEY ("deployment_request_id") REFERENCES "deployment_request" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
ALTER TABLE "execution_trace"
  ADD CONSTRAINT "fk_execution_trace_operation_trace_id" FOREIGN KEY ("operation_trace_id") REFERENCES "operation_trace" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
ALTER TABLE "lock"
  ADD CONSTRAINT "fk_lock_operation_trace_id" FOREIGN KEY ("operation_trace_id") REFERENCES "operation_trace" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
