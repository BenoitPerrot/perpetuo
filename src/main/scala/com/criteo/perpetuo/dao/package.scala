package com.criteo.perpetuo

import slick.dbio.{DBIOAction, Effect, NoStream}

package object dao {
  type DBIOrw[T] = DBIOAction[T, NoStream, Effect.Read with Effect.Write]
}
