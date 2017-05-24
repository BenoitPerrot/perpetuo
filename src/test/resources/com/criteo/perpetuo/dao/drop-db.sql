ALTER TABLE "lock"
  DROP CONSTRAINT "fk_lock_operation_trace_id"
ALTER TABLE "execution_trace"
  DROP CONSTRAINT "fk_execution_trace_execution_specification_id"
ALTER TABLE "execution_trace"
  DROP CONSTRAINT "fk_execution_trace_operation_trace_id"
ALTER TABLE "execution_specification"
  DROP CONSTRAINT "fk_execution_specification_operation_trace_id"
ALTER TABLE "operation_trace"
  DROP CONSTRAINT "fk_operation_trace_deployment_request_id"
ALTER TABLE "deployment_request"
  DROP CONSTRAINT "fk_deployment_request_product_id"


ALTER TABLE "lock"
  DROP CONSTRAINT "pk_lock"
DROP TABLE "lock"


ALTER TABLE "execution_trace"
  DROP CONSTRAINT "pk_execution_trace"
DROP TABLE "execution_trace"


ALTER TABLE "execution_specification"
  DROP CONSTRAINT "pk_execution_specification"
DROP TABLE "execution_specification"


ALTER TABLE "operation_trace"
  DROP CONSTRAINT "pk_operation_trace"
DROP TABLE "operation_trace"


ALTER TABLE "deployment_request"
  DROP CONSTRAINT "pk_deployment_request"
DROP TABLE "deployment_request"


ALTER TABLE "product"
  DROP CONSTRAINT "pk_product"
DROP TABLE "product"