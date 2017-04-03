package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}
import com.twitter.inject.Test


object TestSuffixDispatcher extends {
  val fooInvoker = new DummyInvoker("Foo's invoker")
  val barInvoker = new DummyInvoker("Bar's invoker")
  val fooFooInvoker = new DummyInvoker("Foo-foo's invoker")
} with TargetDispatcherByPoset(
  new ExecutorsByPoset(
    Map(
      "foo-baz" -> fooInvoker,
      "bar-baz" -> barInvoker,
      "foo-foo-baz" -> fooFooInvoker
    ),
    // the only parent of "wxyz" is "xyz", and "" has no parent:
    selectWord => Seq(selectWord.substring(1)).filter(_.nonEmpty)
  )
)


class TargetDispatcherSpec extends Test {

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

    "call the child executor when there's no executor above" in {
      "oo-baz" sentTo fooInvoker
    }

    "call a descendant executor when there's no executor above" in {
      "o-baz" sentTo fooInvoker
    }

    "call all representative/partitioning descendant executors" in {
      "-baz" sentTo(fooInvoker, barInvoker)
      "az" sentTo(fooInvoker, barInvoker)
    }

    "call the closest ascendant executor" in {
      "little-foo-foo-baz" sentTo fooFooInvoker
      "?foo-baz" sentTo fooInvoker
    }

    "call the closest ascendant executor" when {
      "there also is a descendant executor" in {
        // the descendant (foo-foo-baz) is considered as a specialization of the ascendant (foo-baz),
        // but there might be other unknown branches under foo-baz
        "-foo-baz" sentTo fooInvoker
        "o-foo-baz" sentTo fooInvoker
      }
      "there is a non-applicable descendant executor to that ascendant" in {
        "toto-foo-baz" sentTo fooInvoker
      }
    }

    "throw an exception" when {
      "the target is not covered at all" in {
        val thrown = the[Exception] thrownBy TestSuffixDispatcher.assign("abc")
        thrown.getMessage shouldEqual "abc is not covered by executors; can't operate."
      }
    }

  }


  "Loading a dispatcher by name" should {
    "fail when the upper POSet doesn't have a strict order" in {
      val exc = intercept[AssertionError] {
        TestCyclicDispatcher
      }
      exc.getMessage should (include("there is a cycle in") and include("toto") and include("tutu"))
    }
  }

}


// bad dispatcher because toto is the parent of tutu, which is the parent of toto:
object TestCyclicDispatcher extends TargetDispatcherByPoset(
  new ExecutorsByPoset(
    Map("foo" -> new DummyInvoker("Foo's invoker")),
    selectWord => Seq(
      selectWord match {
        case "toto" => "tutu"
        case _ => "toto"
      }
    )
  )
)
