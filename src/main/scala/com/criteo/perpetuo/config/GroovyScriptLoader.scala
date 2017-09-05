package com.criteo.perpetuo.config

import java.io.{File, InputStreamReader}
import javax.script.{ScriptEngine, ScriptEngineManager}


class GroovyScriptLoader(appConfig: RootAppConfig = AppConfig) {
  private val engine: ScriptEngine = new ScriptEngineManager().getEngineByName("groovy") // todo? use GroovyScriptEngine
  assert(engine != null)

  def instantiate(scriptPath: String): AnyRef = {
    val resource = getClass.getResource(scriptPath)
    try {
      val cls = engine.eval(new InputStreamReader(resource.openStream())).asInstanceOf[Class[AnyRef]]
      Seq(// supported instantiation parameters:
        Seq(appConfig),
        Seq()
      )
        .view // lazily:
        .flatMap(instantiate(cls, _))
        .headOption
        .getOrElse {
          throw new NoSuchMethodException("Groovy plugins must have at least a constructor taking either a RootAppConfig or nothing")
        }
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

  private def instantiate(cls: Class[AnyRef], args: Seq[AnyRef]): Option[AnyRef] = {
    cls.getConstructors
      .find { c =>
        val types = c.getParameterTypes
        types.length == args.length &&
          types.zip(args).forall { case (t, o) => t.isInstance(o) }
      }
      .map(_.newInstance(args: _*).asInstanceOf[AnyRef])
  }

}
