package com.criteo.perpetuo.config

import java.io.File

import com.google.inject.Singleton
import com.typesafe.config.Config

@Singleton
class PluginLoader {
  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  val groovyScriptLoader = new GroovyScriptLoader()

  private def instantiate[T <: AnyRef](cls: Class[T], optPluginConfig: Option[Config]): T = {
    val instantiationParameters =
      optPluginConfig.map(pluginConfig => Seq(
        Seq(pluginConfig)
      )).getOrElse(Seq()) ++ Seq(
        Seq()
      )
    instantiationParameters
      .view // lazily:
      .flatMap(instantiate(cls, _))
      .headOption
      .getOrElse {
        throw new NoSuchMethodException("Plugins must have at least a constructor taking either a Config or nothing")
      }
  }

  private def instantiate[T <: AnyRef](cls: Class[T], args: Seq[AnyRef]): Option[T] = {
    cls.getConstructors
      .find { c =>
        val types = c.getParameterTypes
        types.length == args.length &&
          types.zip(args).forall { case (t, o) => t.isInstance(o) }
      }
      .map(_.newInstance(args: _*).asInstanceOf[T])
  }

  def load[T <: AnyRef](config: Config, typeName: String, groovySupported: Boolean = false)(f: PartialFunction[String, T] = PartialFunction.empty): T = {
    val t = config.tryGet[String]("type").getOrElse(throw new Exception(s"No $typeName is configured, while one is required"))
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

    if (groovySupported)
      f.applyOrElse(t, instantiate_)
    else
      f(t)
  }
}
