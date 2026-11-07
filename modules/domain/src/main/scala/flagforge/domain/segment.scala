package flagforge.domain

import io.circe.derivation.ConfiguredCodec

/** All clauses must match. `InSegment` clauses are not allowed inside segments (no nesting). */
final case class SegmentRule(clauses: List[Clause]) derives ConfiguredCodec

/** Reusable group of contexts, e.g. "beta-testers" or "internal-employees". A context belongs to the segment
  * if it is explicitly included, or not excluded and matches any of the rules.
  */
final case class Segment(
    key: SegmentKey,
    name: String,
    description: Option[String] = None,
    included: Set[String] = Set.empty,
    excluded: Set[String] = Set.empty,
    rules: List[SegmentRule] = Nil,
    version: Long = 0
) derives ConfiguredCodec
