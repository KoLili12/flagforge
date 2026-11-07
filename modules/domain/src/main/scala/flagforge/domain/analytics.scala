package flagforge.domain

import io.circe.derivation.ConfiguredCodec

/** A context was served a variation. The first exposure of a context to a flag assigns it to that variation for
  * experiment analysis.
  *
  * @param timestamp
  *   epoch millis on the SDK side; metric events are attributed to exposures that happened before them
  */
final case class Exposure(
    environment: EnvironmentKey,
    flag: FlagKey,
    variation: VariationKey,
    contextKey: String,
    flagVersion: Long,
    timestamp: Long
) derives ConfiguredCodec:
  /** One exposure per context and flag matters for analysis; later duplicates can be dropped. */
  def dedupKey: String = s"$environment/$flag/$contextKey"

/** Something the business cares about happened: a purchase, a sign-up, a click.
  *
  * @param id
  *   client-generated unique id: retries of the same event are deduplicated
  * @param value
  *   e.g. the order amount; `None` for events that are only counted
  */
final case class MetricEvent(
    id: String,
    environment: EnvironmentKey,
    metric: MetricKey,
    contextKey: String,
    value: Option[Double] = None,
    timestamp: Long
) derives ConfiguredCodec

/** What an SDK sends to the ingest service in one request. */
final case class EventBatch(
    exposures: List[Exposure] = Nil,
    metrics: List[MetricEvent] = Nil
) derives ConfiguredCodec:
  def size: Int        = exposures.size + metrics.size
  def isEmpty: Boolean = size == 0
