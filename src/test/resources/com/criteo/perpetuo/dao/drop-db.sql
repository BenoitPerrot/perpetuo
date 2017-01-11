ALTER TABLE "execution_trace"
  DROP CONSTRAINT "fk_execution_trace_operation_trace_id"
ALTER TABLE "operation_trace"
  DROP CONSTRAINT "fk_operation_trace_deployment_request_id"


ALTER TABLE "execution_trace"
  DROP CONSTRAINT "pk_execution_trace"
DROP TABLE "execution_trace"


ALTER TABLE "operation_trace"
  DROP CONSTRAINT "pk_operation_trace"
DROP TABLE "operation_trace"


ALTER TABLE "deployment_request"
  DROP CONSTRAINT "pk_deployment_request"
DROP TABLE "deployment_request"
