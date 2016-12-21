ALTER TABLE "execution_trace"
  DROP CONSTRAINT "fk_execution_trace_deployment_trace_id"
ALTER TABLE "deployment_trace"
  DROP CONSTRAINT "fk_deployment_trace_deployment_request_id"


ALTER TABLE "execution_trace"
  DROP CONSTRAINT "pk_execution_trace"
DROP TABLE "execution_trace"


ALTER TABLE "deployment_trace"
  DROP CONSTRAINT "pk_deployment_trace"
DROP TABLE "deployment_trace"


ALTER TABLE "deployment_request"
  DROP CONSTRAINT "pk_deployment_request"
DROP TABLE "deployment_request"
