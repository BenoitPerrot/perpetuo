package com.criteo.perpetuo.config

import java.io.File
import java.lang.reflect.InvocationTargetException

import com.google.inject.{Inject, Singleton}
import com.typesafe.config.{Config, ConfigException}

@Singleton
class PluginLoader @Inject()(engineProxy: EngineProxy) {

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  val groovyScriptLoader = new GroovyScriptLoader()

  implicit private def instantiateAppropriate[T <: AnyRef](cls: Class[T], optPluginConfig: Option[Config]): T = {
    val instantiationParameters =
      optPluginConfig.map(pluginConfig => Seq(
        Seq(pluginConfig),
        Seq(pluginConfig, engineProxy)
      )).getOrElse(Seq()) ++ Seq(
        Seq()
      )
    instantiationParameters
      .view // lazily:
      .flatMap(tryInstantiateWithArgs(cls, _))
      .headOption
      .getOrElse {
        throw new NoSuchMethodException(s"As a plugin, ${cls.getSimpleName} must have at least a constructor taking either its Config (if one is provided), or its Config and an EngineProxy, or nothing")
      }
  }

  implicit private def tryInstantiateWithArgs[T <: AnyRef](cls: Class[T], args: Seq[AnyRef]): Option[T] = {
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

  private def defaultLoad[A <: AnyRef, B, C](typeName: String, config: Config, specificArg: B)(implicit instantiate: (Class[A], B) => C): C = {
    lazy val stringValue = config.getString(typeName)
    typeName match {
      case "class" =>
        instantiate(Class.forName(stringValue).asInstanceOf[Class[A]], specificArg)
      case "groovyScriptResource" =>
        val resource = getClass.getResource(stringValue)
        assert(resource != null, s"Could not find configured resource $stringValue")
        instantiate(groovyScriptLoader.load(resource), specificArg)
      case "groovyScriptFile" =>
        instantiate(groovyScriptLoader.load(new File(stringValue).getAbsoluteFile.toURI.toURL), specificArg)
      case unknownType: String =>
        throw new Exception(s"Unknown $typeName configured: $unknownType")
    }
  }

  private def inConfig[A](config: Config, reason: String)(call: (String) => A) =
    try {
      call(config.getString("type"))
    } catch {
      case e: ConfigException => throw new RuntimeException(s"Error in the configuration of the $reason (see the cause below)", e)
    }

  def load[A <: AnyRef](config: Config, reason: String)(f: PartialFunction[String, A] = PartialFunction.empty): A =
    inConfig(config, reason) { typeName =>
      val pluginConfig = config.tryGetConfig("config")
      f.applyOrElse(typeName, defaultLoad[A, Option[Config], A](_: String, config, pluginConfig))
    }

  def load[A <: AnyRef](config: Config, reason: String, args: AnyRef*): A =
    inConfig(config, reason) { typeName =>
      defaultLoad[A, Seq[AnyRef], Option[A]](typeName, config, args.toSeq).getOrElse(
        throw new NoSuchMethodException(s"There is no constructor that takes parameter(s) of type(s): " + args.map(_.getClass.getSimpleName).mkString(", "))
      )
    }
}
