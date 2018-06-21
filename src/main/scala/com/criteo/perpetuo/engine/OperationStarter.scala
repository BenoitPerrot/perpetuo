package com.criteo.perpetuo.engine

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging
import slick.dbio.DBIOAction
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class OperationStarter(val dbBinding: DbBinding) extends Logging {

  private def getDeploySpecifics(dispatcher: TargetDispatcher,
                                 deploymentRequest: DeploymentRequest,
                                 expandedTarget: Option[TargetExpr],
                                 executionSpecs: Seq[ExecutionSpecification]): OperationCreationParams = {

    val specAndInvocations = executionSpecs.map(spec =>
      (spec, dispatch(dispatcher, expandedTarget.getOrElse(deploymentRequest.parsedTarget), spec.specificParameters).toVector)
    )
    (Operation.deploy, specAndInvocations, expandedTarget.map(_.flatMap(_.select)))
  }

  def getStepSpecifics(resolver: TargetResolver,
                       dispatcher: TargetDispatcher,
                       planStep: DeploymentPlanStep): DBIOrw[OperationCreationParams] = {
    // generation of specific parameters
    val specificParameters = dispatcher.freezeParameters(planStep.deploymentRequest.product.name, planStep.deploymentRequest.version)
    // target resolution
    val expandedTarget = expandTarget(resolver, planStep.deploymentRequest.product.name, planStep.deploymentRequest.version, planStep.deploymentRequest.parsedTarget)

    // Create the execution specification outside of any transaction: it's not an issue if the request
    // fails afterward and the specification remains unbound.
    // Moreover, this will likely be rewritten eventually for the specifications to be created alongside with the
    // `deploy` operations at the time the deployment request is created.
    dbBinding.insertingExecutionSpecification(specificParameters, planStep.deploymentRequest.version).map(executionSpec =>
      getDeploySpecifics(dispatcher, planStep.deploymentRequest, expandedTarget, Seq(executionSpec))
    )
  }

  def getRetrySpecifics(resolver: TargetResolver,
                        dispatcher: TargetDispatcher,
                        planStep: DeploymentPlanStep): DBIOrw[OperationCreationParams] = {
    // todo: map the right target to the right specification
    val expandedTarget = expandTarget(resolver, planStep.deploymentRequest.product.name, planStep.deploymentRequest.version, planStep.deploymentRequest.parsedTarget)

    dbBinding.findingDeploySpecifications(planStep).map(executionSpecs =>
      getDeploySpecifics(dispatcher, planStep.deploymentRequest, expandedTarget, executionSpecs)
    )
  }

  def getRevertSpecifics(dispatcher: TargetDispatcher,
                         deploymentRequest: DeploymentRequest,
                         userName: String,
                         defaultVersion: Option[Version]): DBIOrw[(Iterable[DeploymentPlanStep], OperationCreationParams)] = {
    dbBinding
      .findingExecutionSpecificationsForRevert(deploymentRequest)
      .flatMap { case (undetermined, determined) =>
        if (undetermined.nonEmpty)
          defaultVersion.map { version =>
            val specificParameters = dispatcher.freezeParameters(deploymentRequest.product.name, version)
            // Create the execution specification outside of any transaction: it's not an issue if the request
            // fails afterward and the specification remains unbound.
            dbBinding.insertingExecutionSpecification(specificParameters, version).map(executionSpecification =>
              Stream.cons((executionSpecification, undetermined), determined.toStream)
            )
          }.getOrElse(throw MissingInfo(
            s"a default rollback version is required, as some targets have no known previous state (e.g. `${undetermined.head}`)",
            "defaultVersion"
          ))
        else
          DBIOAction.successful(determined)
      }
      .flatMap { groups =>
        val specAndInvocations = groups.map { case (spec, targets) =>
          (spec, dispatch(dispatcher, Set(TargetTerm(select = targets)), spec.specificParameters).toVector)
        }
        val atoms = groups.flatMap { case (_, targets) => targets }
        dbBinding.findingDeploymentPlan(deploymentRequest).map { plan =>
          (plan.steps, (Operation.revert, specAndInvocations, Some(atoms.toSet)))
        }
      }
  }

  def triggerExecutions(deploymentRequest: DeploymentRequest,
                        toTrigger: ExecutionsToTrigger): Future[Iterable[(Boolean, Long, Option[(ExecutionState, String, Option[String])])]] = {
    val productName = deploymentRequest.product.name
    Future.traverse(toTrigger) { case (execTraceId, version, target, executor) =>
      // log the execution
      logger.debug(s"Triggering job for execution #$execTraceId of $productName v. $version on $executor")
      // trigger the execution
      val trigger = try {
        executor.trigger(
          execTraceId,
          productName,
          version,
          target,
          deploymentRequest.creator
        )
      } catch {
        case e: Throwable => Future.failed(new Exception("Could not trigger the execution; please contact #sre-perpetual", e))
      }
      trigger
        .map(optLogHref =>
          // if that answers a log href, update the trace with it, and consider that the job
          // is running (i.e. already followable and not yet terminated, really)
          optLogHref.map(logHref =>
            (true, s"`$logHref` succeeded", Some((ExecutionState.running, "", optLogHref))) // todo: change to pending once DREDD-725 is implemented
          ).getOrElse(
            (true, "succeeded (but with an unknown log href)", None)
          )
        )
        .recover {
          // if triggering the job throws an error, mark the execution as failed at initialization
          case e: Throwable =>
            logger.error(e.getMessage, e)
            (false, s"failed (${e.getMessage})", Some((ExecutionState.initFailed, e.getMessage, None)))
        }
        .map { case (succeeded, identifier, toUpdate) =>
          logExecution(identifier, execTraceId, executor, target)
          (succeeded, execTraceId, toUpdate)
        }
    }
  }

  def expandTarget(resolver: TargetResolver, productName: String, productVersion: Version, target: TargetExpr): Option[TargetExpr] = {
    val select = target.select
    resolver.toAtoms(productName, productVersion, select).map { toAtoms =>
      checkUnchangedTarget(select, toAtoms.keySet, "resolution")
      toAtoms.foreach { case (word, atoms) =>
        if (atoms.isEmpty)
          throw UnprocessableIntent(s"`$word` is not a valid target in that context")
        atoms.foreach(atom => assert(atom.length <= TargetAtom.maxSize, s"Target `$atom` is too long"))
      }
      target.map(term => TargetTerm(term.tactics, term.select.iterator.flatMap(toAtoms).toSet))
    }
  }

  private[engine] def dispatch(dispatcher: TargetDispatcher, expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] =
    dispatchAlternatives(dispatcher, expandedTarget, frozenParameters).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.minBy(_.toJson.compactPrint.length))
    }

  private[engine] def dispatchAlternatives(dispatcher: TargetDispatcher, expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, Set[TargetExpr])] = {
    def groupOn2[A, B](it: Iterable[(A, B)]): Map[B, Set[A]] =
      it.groupBy(_._2).map { case (k, v) => (k, v.map(_._1).toSet) }

    val perSelectAtom = groupOn2(
      expandedTarget.toStream.flatMap { case TargetTerm(tactics, select) =>
        select.toStream.flatMap(selectAtom =>
          tactics.map(tactic => (tactic, selectAtom)))
      }
    )

    // infer only once for all unique targets the executors required for each target word
    dispatchToExecutors(dispatcher, perSelectAtom.keySet, frozenParameters).map { case (executor, select) =>
      val atomsAndTactics = select.toStream.map(selectAtom => (selectAtom, perSelectAtom(selectAtom)))
      val flattened = atomsAndTactics.flatMap { case (selectAtom, tactics) =>
        tactics.toStream.map(tactic => (selectAtom, tactic))
      }
      val alternatives = Seq(
        // either group first by "select" atoms then group these atoms by common "tactics"
        groupOn2(atomsAndTactics).map(TargetTerm.tupled),
        // or group first by tactic then group the tactics by common "select" atoms
        groupOn2(groupOn2(flattened)).map(_.swap).map(TargetTerm.tupled)
      )
      (executor, alternatives.map(_.toSet).toSet)
    }
  }

  private[engine] def dispatchToExecutors(targetDispatcher: TargetDispatcher, targetAtoms: Select, frozenParameters: String) = {
    val dispatched = targetDispatcher.dispatch(targetAtoms, frozenParameters)
      .toStream
      .filter { case (_, select) => select.nonEmpty }

    val flattened: Select = dispatched.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet
    checkUnchangedTarget(targetAtoms, flattened, "dispatching")

    dispatched
  }

  private[engine] def checkUnchangedTarget(before: Select, after: Select, reason: String): Unit = {
    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    assert(after.subsetOf(before),
      s"More targets after $reason than before: " + (after -- before).map(_.toString).mkString(", "))
    require(after.size == before.size,
      s"Some targets have been lost in $reason: " + (before -- after).map(_.toString).mkString(", "))
  }

  protected def logExecution(identifier: String, execId: Long, executor: ExecutionTrigger, target: TargetExpr): Unit = {
    logger.debug(s"Triggering job $identifier for execution #$execId: $executor <- ${target.toJson.compactPrint}")
  }
}
