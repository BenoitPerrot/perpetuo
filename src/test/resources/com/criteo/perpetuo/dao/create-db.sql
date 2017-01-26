CREATE TABLE "product" (
  "id"   INTEGER    NOT NULL IDENTITY,
  "name" NCHAR(128) NOT NULL
)
ALTER TABLE "product"
  ADD CONSTRAINT "pk_product" PRIMARY KEY("id")
CREATE UNIQUE INDEX "ix_product_name"
  ON "product"("name")

CREATE TABLE "deployment_request" (
  "id"            BIGINT        NOT NULL IDENTITY,
  "product_id"    INTEGER       NOT NULL,
  "version"       NCHAR(64)     NOT NULL,
  "target"        NVARCHAR(MAX) NOT NULL,
  "reason"        NVARCHAR(256) NOT NULL,
  "creator"       NCHAR(64)     NOT NULL,
  "creation_date" DATETIME      NOT NULL
)
ALTER TABLE "deployment_request"
  ADD CONSTRAINT "pk_deployment_request" PRIMARY KEY ("id")

CREATE TABLE "operation_trace" (
  "id"                    BIGINT        NOT NULL IDENTITY,
  "deployment_request_id" BIGINT        NOT NULL,
  "operation"             SMALLINT      NOT NULL,
  "target_status"         NVARCHAR(MAX) NOT NULL
)
ALTER TABLE "operation_trace"
  ADD CONSTRAINT "pk_operation_trace" PRIMARY KEY ("id")


CREATE TABLE "execution_trace" (
  "id"                 BIGINT   NOT NULL IDENTITY,
  "operation_trace_id" BIGINT   NOT NULL,
  "uuid"               NCHAR(128),
  "state"              SMALLINT NOT NULL
)
ALTER TABLE "execution_trace"
  ADD CONSTRAINT "pk_execution_trace" PRIMARY KEY ("id")
CREATE INDEX "ix_execution_trace_state"
  ON "execution_trace" ("state")
CREATE UNIQUE INDEX "ix_execution_trace_uuid"
  ON "execution_trace" ("uuid")


ALTER TABLE "deployment_request"
  ADD CONSTRAINT "fk_deployment_request_product_id" FOREIGN KEY("product_id") REFERENCES "product"("id")
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
