package flagforge.eval

import flagforge.domain.{VariationKey, Weights, WeightedVariation}

import scala.util.hashing.MurmurHash3

/** Deterministic assignment of contexts to rollout buckets.
  *
  * The bucket depends only on `(flagKey, salt, value)`, so a user always lands in the same bucket — on any
  * server, in any SDK, on the JVM or in the browser (MurmurHash3 from the Scala stdlib behaves identically on
  * both platforms). Changing the salt reshuffles everyone.
  */
object Bucketing:

  /** Bucket in `[0, Weights.Total)`. */
  def bucket(flagKey: String, salt: String, value: String): Int =
    val hash = MurmurHash3.stringHash(s"$flagKey.$salt.$value")
    ((hash.toLong & 0xffffffffL) % Weights.Total).toInt

  /** Walks the cumulative weights and returns the variation whose range contains `bucket`.
    *
    * Growing the first variation's weight (e.g. a 10% → 20% rollout) only moves users *into* it: everyone who
    * already had it keeps it.
    */
  def pick(variations: List[WeightedVariation], bucket: Int): Option[VariationKey] =
    @annotation.tailrec
    def loop(rest: List[WeightedVariation], upperBound: Int): Option[VariationKey] = rest match
      case Nil                                     => variations.lastOption.map(_.variation)
      case wv :: _ if bucket < upperBound + wv.weight => Some(wv.variation)
      case wv :: tail                              => loop(tail, upperBound + wv.weight)
    loop(variations, 0)
