package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}
import com.twitter.inject.Test


object TestSuffixDispatcher extends {
  val fooInvoker = new DummyInvoker("Foo's invoker")
  val barInvoker = new DummyInvoker("Bar's invoker")
  val fooFooInvoker = new DummyInvoker("Foo-foo's invoker")
} with TargetDispatchingByPoset(
  new ExecutorsByPoset(
    Map(
      "foo-baz" -> fooInvoker,
      "bar-baz" -> barInvoker,
      "foo-foo-baz" -> fooFooInvoker
    ),
    // the only parent of "wxyz" is "xyz", and "" has no parent:
    targetWord => Seq(targetWord.substring(1)).filter(_.nonEmpty)
  )
)


class TargetDispatchingSpec extends Test {

  import TestSuffixDispatcher._

  implicit class PosetTest(private val select: String) {
    def sentTo(that: ExecutorInvoker*): Unit =
      TestSuffixDispatcher.assign(select) should contain theSameElementsAs that
  }


  "Assigning to a single executor" should {

    "always call the only executor" in {
      SingleTargetDispatcher(fooInvoker).assign("foo-baz") shouldEqual Set(fooInvoker)
      SingleTargetDispatcher(fooInvoker).assign("bar-tender") shouldEqual Set(fooInvoker)
    }

  }


  "Assigning a target by POSet" should {

    "call the right executor when available for the exact target" in {
      "foo-baz" sentTo fooInvoker
    }

    "call the root set of pretty much all executors for unknown targets" in {
      "abc" sentTo(fooInvoker, barInvoker)
    }

    "call the child executor" in {
      "oo-baz" sentTo fooInvoker
    }

    "call a descendant executor" in {
      "o-baz" sentTo fooInvoker
    }

    "call all representative descendant executors" in {
      "-baz" sentTo(fooInvoker, barInvoker)
      "az" sentTo(fooInvoker, barInvoker)
    }

    "call the closest ascendant executor" in {
      "little-foo-foo-baz" sentTo fooFooInvoker
      "?foo-baz" sentTo fooInvoker
    }

    "call the closest related executor" in {
      "-foo-baz" sentTo fooFooInvoker
      "toto-foo-baz" sentTo fooFooInvoker
    }

  }


  "Loading a dispatcher by name" should {
    "be possible" in {
      TargetDispatching.fromName("com.criteo.perpetuo.dispatchers.DummyTargetDispatcher")
    }

    "fail when the upper POSet doesn't have a strict order" in {
      val exc = intercept[AssertionError] {
        TargetDispatching.fromName("com.criteo.perpetuo.dispatchers.TestCyclicDispatcher")
      }
      exc.getMessage should (include("there is a cycle in") and include("toto") and include("tutu"))
    }

  }

}


// bad dispatcher because toto is the parent of tutu, which is the parent of toto:
object TestCyclicDispatcher extends TargetDispatchingByPoset(
  new ExecutorsByPoset(
    Map("foo" -> new DummyInvoker("Foo's invoker")),
    targetWord => Seq(
      targetWord match {
        case "toto" => "tutu"
        case _ => "toto"
      }
    )
  )
)
