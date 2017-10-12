package com.criteo.perpetuo.config

import java.io.{File, InputStreamReader}
import java.net.URL
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.typesafe.config.Config


class GroovyScriptLoader(config: Config) {
  private val engine: ScriptEngine = new ScriptEngineManager().getEngineByName("groovy") // todo? use GroovyScriptEngine
  assert(engine != null)

  def instantiate[T <: AnyRef](resource: URL): T = {
    try {
      val cls = engine.eval(new InputStreamReader(resource.openStream())).asInstanceOf[Class[T]]
      Plugins.instantiate[T](cls)
    }
    catch {
      case exc: Throwable =>
        val cause = if (exc.getCause == null) exc else exc.getCause
        val stack = cause.getStackTrace
        val basename = new File(resource.getFile).getName
        var lineNo = 0
        val newExc = new Exception(
          """Script\d+\.groovy: (\d+):""".r.replaceAllIn(cause.getMessage, { m =>
            lineNo = m.group(1).toInt
            s"$basename: line $lineNo:"
          })
        )
        stack(0) = new StackTraceElement("plugin load", " An error occurred ", basename, lineNo)
        newExc.setStackTrace(stack)
        throw newExc
    }
  }
}
