package flagforge.eval

import flagforge.domain.{Weights, WeightedVariation}
import flagforge.eval.Fixtures.*
import org.scalacheck.{Gen, Prop}
import org.scalacheck.Prop.forAll

class BucketingSuite extends munit.ScalaCheckSuite:

  private val contextKey = Gen.oneOf(Gen.uuid.map(_.toString), Gen.alphaNumStr, Gen.numStr)
  private val percent    = Gen.choose(0, Weights.Total)

  property("bucket is always within [0, Total)"):
    forAll(Gen.alphaStr, Gen.alphaStr, contextKey) { (flag, salt, key) =>
      val b = Bucketing.bucket(flag, salt, key)
      b >= 0 && b < Weights.Total
    }

  property("bucketing is deterministic"):
    forAll(Gen.alphaStr, contextKey)((flag, key) => Bucketing.bucket(flag, "s", key) == Bucketing.bucket(flag, "s", key))

  property("pick always returns one of the rollout variations"):
    forAll(percent, Gen.choose(0, Weights.Total - 1)) { (onWeight, bucket) =>
      val variations = List(WeightedVariation(On, onWeight), WeightedVariation(Off, Weights.Total - onWeight))
      Bucketing.pick(variations, bucket).exists(Set(On, Off))
    }

  property("growing a rollout never takes the variation away from anyone"):
    forAll(percent, percent, contextKey) { (p1, p2, key) =>
      val (small, large) = (math.min(p1, p2), math.max(p1, p2))
      def served(p: Int) =
        Bucketing.pick(
          List(WeightedVariation(On, p), WeightedVariation(Off, Weights.Total - p)),
          Bucketing.bucket("flag", "salt", key)
        )
      Prop(served(small) != Some(On) || served(large) == Some(On))
    }

  test("buckets are uniformly distributed (chi-squared, 10 bins)"):
    val samples  = 50_000
    val bins     = 10
    val counts   = Array.fill(bins)(0)
    (0 until samples).foreach: i =>
      counts(Bucketing.bucket("new-checkout", "salt", s"user-$i") * bins / Weights.Total) += 1
    val expected = samples.toDouble / bins
    val chi2     = counts.map(c => math.pow(c - expected, 2) / expected).sum
    // Critical value of chi-squared with 9 degrees of freedom at p = 0.001 is 27.88.
    assert(chi2 < 27.88, s"distribution is skewed: ${counts.mkString(", ")}, chi2 = $chi2")

  test("a 20% rollout serves ~20% of contexts"):
    val variations = List(WeightedVariation(On, 20_000), WeightedVariation(Off, 80_000))
    val samples    = 50_000
    val on = (0 until samples).count(i => Bucketing.pick(variations, Bucketing.bucket("f", "s", s"u$i")).contains(On))
    val share = on.toDouble / samples
    assert(math.abs(share - 0.2) < 0.01, s"share = $share")

  test("different salts reshuffle contexts"):
    val keys = (0 until 1000).map(i => s"user-$i")
    val same = keys.count(k => Bucketing.bucket("f", "salt-a", k) == Bucketing.bucket("f", "salt-b", k))
    assert(same < 10, s"$same of 1000 kept the same bucket")

  // The same expected values are asserted on the JVM and on Scala.js: an SDK in the browser must put a user
  // into exactly the same bucket as the server does.
  test("golden buckets are identical on every platform"):
    val actual = List("alice", "bob", "user-42", "7f9c2b1e-3d4a-4f6b-9a8c-1e2d3f4a5b6c")
      .map(Bucketing.bucket("new-checkout", "s4lt", _))
    assertEquals(actual, List(8483, 98862, 60393, 45923))
