package com.criteo.perpetuo

import java.lang.reflect.InvocationTargetException

package object util {
  def tryInstantiateWithArgs[T <: AnyRef](cls: Class[T], args: Seq[AnyRef]): Option[T] = {
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
}
