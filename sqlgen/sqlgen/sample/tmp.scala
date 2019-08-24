import scala.util.Random

object Main {
  trait Rule {
    def gen(): String
  }
  case class Cat(rules: Rule*) extends Rule {
    def gen(): String =
      rules.map(_.gen()).mkString(" ")
  }
  case class Or(rules: OrOpt*) extends Rule {
    def gen(): String = {
      val shuffled = Random.shuffle(rules)
      val p        = Random.nextInt(shuffled.map(_.priority).sum)
      shuffled
        .foldLeft((p, shuffled.head)) {
          case ((0, r), _)                       => (0, r)
          case ((p, _), r) if p - r.priority < 0 => (0, r)
          case ((p, _), r)                       => (p - r.priority, r)
        }
        ._2
        .gen()
    }
  }

  case class OrOpt(rule: Rule, priority: Int) extends Rule {
    def gen(): String = rule.gen()
  }
  implicit class OrOptConv(rule: Rule) extends OrOpt(rule, 1)

  /**
  Recursive-definition BNF

    moreThan, lessThan: the limit of rec-definition call times.
        Times of rec-definition call will be [moreThan, lessThan].
        When moreThan == 0 and lessThan == -1, will no longer limit
        the call times.

    tmls: Sequence of normal definition
    recs: Sequence of recursive definition.
        A recursive definition is a function, accepting a rule which
        represents a placeholder for self call.
   */
  def rec(
           moreThan: Int,
           lessThan: Int,
           tmls: Seq[OrOpt],
           recs: Seq[Rule => OrOpt]
         ): Rule = {
    assert(moreThan >= 0 && (lessThan > moreThan || lessThan == -1))
    assert(tmls.nonEmpty && recs.nonEmpty)
    def r(cur: Int): Rule =
      () => {
        if (moreThan <= cur && lessThan - cur != 0)
          Or(recs.map(_(r(cur + 1))) ++ tmls: _*).gen()
        else if (cur < moreThan)
          Or(recs.map(_(r(cur + 1))): _*).gen()
        else
          Or(tmls: _*).gen()
      }
    r(1)
  }

  case class S(string: String) extends Rule {
    def gen(): String = string
  }

  def main(args: Array[String]): Unit = {
    """
     a {5, 10} =
      | 'A' [1]
      | 'B' [1]
      | 'C' [1]
      | 'A' a [9]
      | 'B' a [9]
      | 'C' a [9]
     """
    val a = rec(
      5,
      10,
      Vector(S("A"), S("B"), S("C")),
      Vector(
        r => OrOpt(Cat(S("A"), r), 9),
        r => OrOpt(Cat(S("B"), r), 9),
        r => OrOpt(Cat(S("C"), r), 9)
      )
    )

    """
     b =
      | 'A'
      | 'B'
      | 'C'
      | 'A' b
      | 'B' b
      | 'C' b
     """
    val b = rec(
      0,
      -1,
      Vector(S("A"), S("B"), S("C")),
      Vector(
        r => Cat(S("A"), r),
        r => Cat(S("B"), r),
        r => Cat(S("C"), r)
      )
    )

    for {
      _ <- 1 to 100
    } {
      println(a.gen())
      println(b.gen())
    }
  }
}
