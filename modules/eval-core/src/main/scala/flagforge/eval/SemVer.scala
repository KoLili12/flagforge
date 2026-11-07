package flagforge.eval

/** Minimal semantic version (https://semver.org) for `sem_ver_*` operators. Accepts a leading `v` and
  * missing minor/patch parts (`"2"` == `"2.0.0"`); build metadata is ignored.
  */
final case class SemVer(major: Int, minor: Int, patch: Int, preRelease: List[String])

object SemVer:

  private val Pattern =
    """^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z.-]+)?$""".r

  def parse(raw: String): Option[SemVer] = raw.trim match
    case Pattern(major, minor, patch, pre) =>
      for
        ma <- major.toIntOption
        mi <- Option(minor).fold(Some(0))(_.toIntOption)
        pa <- Option(patch).fold(Some(0))(_.toIntOption)
      yield SemVer(ma, mi, pa, Option(pre).fold(Nil)(_.split('.').toList))
    case _ => None

  given Ordering[SemVer] with
    def compare(x: SemVer, y: SemVer): Int =
      val core = Ordering[(Int, Int, Int)].compare((x.major, x.minor, x.patch), (y.major, y.minor, y.patch))
      if core != 0 then core else comparePreRelease(x.preRelease, y.preRelease)

  /** A version without a pre-release is greater than one with it: 1.0.0-rc.1 < 1.0.0. */
  private def comparePreRelease(x: List[String], y: List[String]): Int = (x, y) match
    case (Nil, Nil) => 0
    case (Nil, _)   => 1
    case (_, Nil)   => -1
    case _          => compareIdentifiers(x, y)

  @annotation.tailrec
  private def compareIdentifiers(x: List[String], y: List[String]): Int = (x, y) match
    case (Nil, Nil)         => 0
    case (Nil, _)           => -1
    case (_, Nil)           => 1
    case (a :: as, b :: bs) =>
      val c = (a.toLongOption, b.toLongOption) match
        case (Some(n), Some(m)) => n.compare(m)
        case (Some(_), None)    => -1 // numeric identifiers sort before alphanumeric ones
        case (None, Some(_))    => 1
        case (None, None)       => a.compare(b)
      if c != 0 then c else compareIdentifiers(as, bs)
