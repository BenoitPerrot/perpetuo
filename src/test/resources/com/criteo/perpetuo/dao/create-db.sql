CREATE TABLE "product" (
  "id"   INTEGER       NOT NULL IDENTITY,
  "name" NVARCHAR(128) NOT NULL
)
ALTER TABLE "product"
  ADD CONSTRAINT "pk_product" PRIMARY KEY ("id")
CREATE UNIQUE INDEX "ix_product_name"
  ON "product" ("name")


CREATE TABLE "deployment_request" (
  "id"            BIGINT         NOT NULL IDENTITY,
  "product_id"    INTEGER        NOT NULL,
  "version"       NVARCHAR(1024) NOT NULL,
  "target"        NVARCHAR(8000) NOT NULL,
  "comment"       NVARCHAR(4000) NOT NULL,
  "creator"       NVARCHAR(64)   NOT NULL,
  "creation_date" DATETIME       NOT NULL
)
ALTER TABLE "deployment_request"
  ADD CONSTRAINT "pk_deployment_request" PRIMARY KEY ("id")
CREATE INDEX "ix_deployment_request_creation_date"
  ON "deployment_request" ("creation_date")


CREATE TABLE "operation_trace" (
  "id"                    BIGINT        NOT NULL IDENTITY,
  "deployment_request_id" BIGINT        NOT NULL,
  "operation"             SMALLINT      NOT NULL,
  "target_status"         NVARCHAR(16000),
  "creator"               NVARCHAR(64)  NOT NULL,
  "creation_date"         DATETIME      NOT NULL,
  "closing_date"          DATETIME
)
ALTER TABLE "operation_trace"
  ADD CONSTRAINT "pk_operation_trace" PRIMARY KEY ("id")
CREATE INDEX "ix_operation_trace_closing_date"
  ON "operation_trace" ("closing_date")
CREATE INDEX "ix_operation_trace_creation_date"
  ON "operation_trace" ("creation_date")


CREATE TABLE "execution_specification" (
  "id"                  BIGINT          NOT NULL IDENTITY,
  "version"             NVARCHAR(1024)  NOT NULL,
  "specific_parameters" NVARCHAR(16000) NOT NULL
)
ALTER TABLE "execution_specification"
  ADD CONSTRAINT "pk_execution_specification" PRIMARY KEY ("id")


CREATE TABLE "execution" (
  "id"                         BIGINT NOT NULL IDENTITY,
  "operation_trace_id"         BIGINT NOT NULL,
  "execution_specification_id" BIGINT NOT NULL
)
ALTER TABLE "execution"
  ADD CONSTRAINT "pk_execution" PRIMARY KEY ("id")
CREATE UNIQUE INDEX "ix_execution"
  ON "execution" ("operation_trace_id", "execution_specification_id")


CREATE TABLE "target_status" (
  "id"                         BIGINT         NOT NULL IDENTITY,
  "execution_id"               BIGINT         NOT NULL,
  "target"                     NVARCHAR(128)  NOT NULL,
  "code"                       SMALLINT       NOT NULL,
  "detail"                     NVARCHAR(4000) NOT NULL
)
ALTER TABLE "target_status"
  ADD CONSTRAINT "pk_target_status" PRIMARY KEY ("id")
CREATE INDEX "ix_target_status_target"
  ON "target_status" ("target")


CREATE TABLE "execution_trace" (
  "id"                         BIGINT      NOT NULL IDENTITY,
  "execution_id"               BIGINT      NOT NULL,
  "log_href"                   NVARCHAR(1024),
  "state"                      SMALLINT    NOT NULL
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
ALTER TABLE "execution"
  ADD CONSTRAINT "fk_execution_execution_specification_id" FOREIGN KEY ("execution_specification_id") REFERENCES "execution_specification" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
ALTER TABLE "execution"
  ADD CONSTRAINT "fk_execution_operation_trace_id" FOREIGN KEY ("operation_trace_id") REFERENCES "operation_trace" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
ALTER TABLE "target_status"
  ADD CONSTRAINT "fk_target_status_execution_id" FOREIGN KEY ("execution_id") REFERENCES "execution" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
ALTER TABLE "execution_trace"
  ADD CONSTRAINT "fk_execution_trace_execution_id" FOREIGN KEY ("execution_id") REFERENCES "execution" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
ALTER TABLE "lock"
  ADD CONSTRAINT "fk_lock_operation_trace_id" FOREIGN KEY ("operation_trace_id") REFERENCES "operation_trace" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
