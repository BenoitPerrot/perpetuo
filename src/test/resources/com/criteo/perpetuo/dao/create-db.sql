CREATE TABLE "deployment_request" (
  "id"            BIGINT        NOT NULL IDENTITY,
  "product_name"  NCHAR(64)     NOT NULL,
  "version"       NCHAR(64)     NOT NULL,
  "target"        NVARCHAR(MAX) NOT NULL,
  "reason"        NVARCHAR(256) NOT NULL,
  "creator"       NVARCHAR(64)  NOT NULL,
  "creation_date" DATETIME      NOT NULL
)
ALTER TABLE "deployment_request"
  ADD CONSTRAINT "pk_deployment_request" PRIMARY KEY ("id")
CREATE INDEX "ix_deployment_request_product_name"
  ON "deployment_request" ("product_name")

CREATE TABLE "deployment_trace" (
  "id"                    BIGINT        NOT NULL IDENTITY,
  "deployment_request_id" BIGINT        NOT NULL,
  "operation"             SMALLINT      NOT NULL,
  "target_status"         NVARCHAR(MAX) NOT NULL
)
ALTER TABLE "deployment_trace"
  ADD CONSTRAINT "pk_deployment_trace" PRIMARY KEY ("id")
ALTER TABLE "deployment_trace"
  ADD CONSTRAINT "fk_deployment_trace_deployment_request_id" FOREIGN KEY ("deployment_request_id") REFERENCES "deployment_request" ("id")
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
