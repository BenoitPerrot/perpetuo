package com.criteo.perpetuo.config

import java.io.File
import java.lang.reflect.InvocationTargetException

import com.google.inject.{Inject, Injector, Singleton}
import com.typesafe.config.{Config, ConfigException}

@Singleton
class PluginLoader @Inject()(injector: Injector, appConfig: AppConfig) {

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  val groovyScriptLoader = new GroovyScriptLoader()

  private def instantiateAppropriate[T <: AnyRef](cls: Class[T], optPluginConfig: Option[Config]): T = {
    val instantiationParameters =
      optPluginConfig.map(pluginConfig => Seq(
        Seq(pluginConfig),
        Seq(pluginConfig, injector)
      )).getOrElse(Seq()) ++ Seq(
        Seq(),
        Seq(injector)
      )
    instantiationParameters
      .view // lazily:
      .flatMap(tryInstantiateWithArgs(cls, _))
      .headOption
      .getOrElse {
        throw new NoSuchMethodException(s"As a plugin, ${cls.getName} must have at least a constructor taking either its Config (if one is provided), or its Config and an Injector, or an Injector, or nothing")
        // note: don't use .getSimpleName on an unknown class, because of https://github.com/scala/bug/issues/2034
      }
  }

  private def tryInstantiateWithArgs[T <: AnyRef](cls: Class[T], args: Seq[AnyRef]): Option[T] = {
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

  private def defaultLoad[A <: AnyRef](typeName: String, config: Config): A = {
    lazy val stringValue = config.getString(typeName)
    instantiateAppropriate(
      typeName match {
        case "class" =>
          Class.forName(stringValue).asInstanceOf[Class[A]]
        case "groovyScriptResource" =>
          val resource = getClass.getResource(stringValue)
          if (resource == null)
            throw new RuntimeException(s"Could not find configured resource $stringValue")
          groovyScriptLoader.load(resource)
        case "groovyScriptFile" =>
          groovyScriptLoader.load(new File(stringValue).getAbsoluteFile.toURI.toURL)
        case unknownType: String =>
          throw new Exception(s"Unknown $typeName configured: $unknownType")
      },
      config.tryGetConfig("config"))
  }

  def load[A <: AnyRef](config: Config, reason: String)(f: PartialFunction[String, A] = PartialFunction.empty): A =
    try {
      f.applyOrElse(config.getString("type"), defaultLoad[A](_: String, config))
    } catch {
      case e: ConfigException => throw new RuntimeException(s"Error in the configuration of the $reason (see the cause below)", e)
    }
}
