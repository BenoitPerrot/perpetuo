package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}
import com.twitter.inject.Test


class TargetDispatchingSpec extends Test {

  val fooInvoker = new DummyInvoker("Foo's invoker")
  val barInvoker = new DummyInvoker("Bar's invoker")
  val fooFooInvoker = new DummyInvoker("Foo-foo's invoker")

  abstract class TestDispatcher(override val getParents: (String) => Iterable[String]) extends TargetDispatchingByPoset(
    Map(
      "foo-baz" -> fooInvoker,
      "bar-baz" -> barInvoker,
      "foo-foo-baz" -> fooFooInvoker
    ),
    getParents
  )

  // the only parent of "wxyz" is "xyz", and "" has no parent:
  object TestSuffixDispatcher extends TestDispatcher(targetWord => Seq(targetWord.substring(1)).filter(_.nonEmpty))

  implicit class PosetTest(private val select: Select) {
    def sentTo(that: ExecutorInvoker*): Unit =
      sentTo(that.map(x => (x, select)))

    def sentTo(that: Iterable[(ExecutorInvoker, Select)]): Unit =
      TestSuffixDispatcher.dispatch(select).toSeq should contain theSameElementsAs that
  }


  "Dispatching to a single executor" should {

    "always call the only executor in one shot" in {
      SingleTargetDispatcher(fooInvoker).dispatch(Seq("foo-baz")).toSeq shouldEqual
        Seq((fooInvoker, Seq("foo-baz")))
      SingleTargetDispatcher(fooInvoker).dispatch(Seq("foo-baz", "baz-bar", "bar-tender")).toSeq shouldEqual
        Seq((fooInvoker, Seq("foo-baz", "baz-bar", "bar-tender")))
    }

  }


  "Dispatching a single target by POSet" should {

    "call the right executor when available for the exact target" in {
      Seq("foo-baz") sentTo fooInvoker
    }

    "call the root set of pretty much all executors for unknown targets" in {
      Seq("abc") sentTo(fooInvoker, barInvoker)
    }

    "call the child executor" in {
      Seq("oo-baz") sentTo fooInvoker
    }

    "call a descendant executor" in {
      Seq("o-baz") sentTo fooInvoker
    }

    "call all representative descendant executors" in {
      Seq("-baz") sentTo(fooInvoker, barInvoker)
      Seq("az") sentTo(fooInvoker, barInvoker)
    }

    "call the closest ascendant executor" in {
      Seq("little-foo-foo-baz") sentTo fooFooInvoker
      Seq("?foo-baz") sentTo fooInvoker
    }

    "call the closest related executor" in {
      Seq("-foo-baz") sentTo fooFooInvoker
      Seq("toto-foo-baz") sentTo fooFooInvoker
    }

  }


  "Dispatching multiple target words to multiple executors" should {

    "call the right executor when available for each exact target word" in {
      Seq("foo-baz", "foo-foo-baz", "bar-baz") sentTo Seq(
        (fooInvoker, Seq("foo-baz")),
        (fooFooInvoker, Seq("foo-foo-baz")),
        (barInvoker, Seq("bar-baz"))
      )
    }

    "call the root set of pretty much all executors for all unknown targets" in {
      Seq("abc", "def") sentTo Seq(
        (fooInvoker, Seq("abc", "def")),
        (barInvoker, Seq("abc", "def"))
      )
    }

    "call the same executor in one shot when applicable" in {
      Seq("o-baz", "oo-baz") sentTo Seq(
        (fooInvoker, Seq("o-baz", "oo-baz"))
      )
    }

    "gather target words on executors when possible and still distribute unknown target words" in {
      Seq("o-baz", "-baz", "oo-baz") sentTo Seq(
        (fooInvoker, Seq("o-baz", "-baz", "oo-baz")),
        (barInvoker, Seq("-baz"))
      )
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
  Map("foo" -> new DummyInvoker("Foo's invoker")),
  targetWord => Seq(
    targetWord match {
      case "toto" => "tutu"
      case _ => "toto"
    }
  )
)
