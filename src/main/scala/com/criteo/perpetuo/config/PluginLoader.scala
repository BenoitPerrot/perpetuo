package com.criteo.perpetuo.config

import java.io.File
import java.lang.reflect.InvocationTargetException

import com.google.inject.{Inject, Singleton}
import com.typesafe.config.Config

@Singleton
class PluginLoader @Inject()(engineProxy: EngineProxy) {

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  val groovyScriptLoader = new GroovyScriptLoader()

  private def instantiate[T <: AnyRef](cls: Class[T], optPluginConfig: Option[Config]): T = {
    val instantiationParameters =
      optPluginConfig.map(pluginConfig => Seq(
        Seq(pluginConfig),
        Seq(pluginConfig, engineProxy)
      )).getOrElse(Seq()) ++ Seq(
        Seq()
      )
    instantiationParameters
      .view // lazily:
      .flatMap(instantiate(cls, _))
      .headOption
      .getOrElse {
        throw new NoSuchMethodException(s"As a plugin, ${cls.getSimpleName} must have at least a constructor taking either its Config (if one is provided), or its Config and an EngineProxy, or nothing")
      }
  }

  private def instantiate[T <: AnyRef](cls: Class[T], args: Seq[AnyRef]): Option[T] = {
    cls.getConstructors
      .find { c =>
        val types = c.getParameterTypes
        types.length == args.length &&
          types.zip(args).forall { case (t, o) => t.isInstance(o) }
      }
      .map { constructor =>
        try {
          constructor.newInstance(args: _*).asInstanceOf[T]
        } catch {
          case e: InvocationTargetException => throw e.getCause
        }
      }
  }

  def load[T <: AnyRef](config: Config, typeName: String)(f: PartialFunction[String, T] = PartialFunction.empty): T = {
    val t = config.getOrElse[String]("type", throw new Exception(s"No $typeName is configured, while one is required"))
    lazy val pluginConfig = config.tryGetConfig("config")
    lazy val stringValue = config.getString(t)

    def instantiate_(path: String) = path match {
      case "class" =>
        instantiate(Class.forName(stringValue).asInstanceOf[Class[T]], pluginConfig)
      case "groovyScriptResource" =>
        val resource = getClass.getResource(stringValue)
        assert(resource != null, s"Could not find configured resource $stringValue")
        instantiate(groovyScriptLoader.load(resource), pluginConfig)
      case "groovyScriptFile" =>
        instantiate(groovyScriptLoader.load(new File(stringValue).getAbsoluteFile.toURI.toURL), pluginConfig)
      case unknownType: String =>
        throw new Exception(s"Unknown $typeName configured: $unknownType")
    }

    f.applyOrElse(t, instantiate_)
  }
}
