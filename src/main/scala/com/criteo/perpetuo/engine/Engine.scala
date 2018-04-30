package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.Permissions
import javax.inject.{Inject, Singleton}

@Singleton
class Engine @Inject()(val crankshaft: Crankshaft,
                       val permissions: Permissions)
