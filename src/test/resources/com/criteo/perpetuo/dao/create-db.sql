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
