package com.criteo.perpetuo.config

import java.io.File

import com.criteo.perpetuo.auth.{AnonymousIdentityProvider, IdentityProvider}
import com.criteo.perpetuo.engine.Provider
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.util.tryInstantiateWithArgs
import com.google.inject.{Inject, Injector, Singleton}
import com.typesafe.config.{Config, ConfigException}

@Singleton
class PluginLoader @Inject()(injector: Injector) {

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

  def loadTargetResolver(config: Option[Config]): TargetResolver =
    config
      .map(desc =>
        load[Provider[TargetResolver]](desc, "target resolver")()
      )
      .getOrElse(new TargetResolver {})
      .get

  def loadTargetDispatcher(config: Option[Config]): TargetDispatcher =
    config
      .map { desc =>
        load[Provider[TargetDispatcher]](desc, "target dispatcher") {
          case t@"singleExecutor" =>
            val executorConfig = desc.getConfig(t)
            val executionTrigger = load[ExecutionTrigger](executorConfig, "executor")()
            new SingleTargetDispatcher(executionTrigger)
        }
      }
      .getOrElse(throw new Exception(s"No target dispatcher is configured, while one is required"))
      .get

  def loadIdentityProvider(config: Option[Config]): IdentityProvider =
    config
      .map(desc =>
        load[IdentityProvider](desc, "type of identity provider")()
      )
      .getOrElse(AnonymousIdentityProvider)

}
