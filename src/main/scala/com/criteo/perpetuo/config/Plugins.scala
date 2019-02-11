package com.criteo.perpetuo.config

import java.util.logging.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}
import scala.util.{Success, Try}


trait Plugin {
  /**
    * This logger is the one to use in the Groovy classes, and it should not be overridden
    */
  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
    * Timeout applicable on each plugin method, in seconds; can be overridden in Groovy classes
    */
  val timeout_s: Int
}


abstract class PluginRunner[P <: Plugin](plugin: P, base: P) {
  protected implicit def initUnit: () => Unit = () => ()

  protected implicit def initTry: () => Try[Unit] = () => Success(())

  protected def wrap[T](toCallOnPlugin: P => T, name: String = null)(implicit init: () => T): Future[T] =
    try {
      val methodName = if (name == null) Thread.currentThread.getStackTrace()(2).getMethodName else name
      val method = plugin.getClass.getMethods.filter(_.getName == methodName).head
      Future(
        try {
          if (method.getDeclaringClass != base.getClass) {
            // there is a specific implementation for this plugin method, let's start it, but time-boxed
            plugin.logger.fine(methodName)
            try {
              Await.result(Future {
                blocking {
                  toCallOnPlugin(plugin)
                }
              }, plugin.timeout_s.seconds)
            }
            catch {
              case e: ExecutionException => throw e.getCause
            }
          }
          else
            toCallOnPlugin(plugin)
        }
        catch {
          case e: Throwable =>
            // to know which method is failing, prefix the trace
            plugin.logger.severe(s"$methodName - ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
            throw e
        }
      )
    }
    catch {
      case _: Throwable => Future.successful(init())
    }
}
