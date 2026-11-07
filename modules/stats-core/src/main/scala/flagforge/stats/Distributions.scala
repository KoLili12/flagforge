package flagforge.stats

/** Special functions and the distributions needed for hypothesis tests, implemented from scratch so the same
  * code runs on the JVM and in Scala.js. Accuracy is ~1e-10 or better (verified against scipy in tests).
  *
  * Algorithms: Lanczos approximation for log-gamma; series / Lentz continued fraction for the regularized
  * incomplete gamma and beta functions (Numerical Recipes, ch. 6); Acklam's rational approximation refined by
  * one Halley step for the normal quantile.
  */
object Distributions:

  private val Eps     = 1e-15
  private val FpMin   = 1e-300
  private val MaxIter = 10_000

  // --- gamma family ---

  private val Lanczos = Array(
    0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313, -176.61502916214059,
    12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7
  )

  def lnGamma(x: Double): Double =
    if x < 0.5 then math.log(math.Pi / math.abs(math.sin(math.Pi * x))) - lnGamma(1 - x)
    else
      val xx = x - 1
      val t  = xx + 7.5
      val a  = (1 until Lanczos.length).foldLeft(Lanczos(0))((acc, i) => acc + Lanczos(i) / (xx + i))
      0.5 * math.log(2 * math.Pi) + (xx + 0.5) * math.log(t) - t + math.log(a)

  /** Regularized lower incomplete gamma P(a, x). */
  def gammaP(a: Double, x: Double): Double =
    if x <= 0 then 0.0
    else if x < a + 1 then gammaSeries(a, x)
    else 1.0 - gammaContinuedFraction(a, x)

  /** Regularized upper incomplete gamma Q(a, x) = 1 - P(a, x), accurate in the far tail. */
  def gammaQ(a: Double, x: Double): Double =
    if x <= 0 then 1.0
    else if x < a + 1 then 1.0 - gammaSeries(a, x)
    else gammaContinuedFraction(a, x)

  private def gammaPrefactor(a: Double, x: Double): Double = math.exp(-x + a * math.log(x) - lnGamma(a))

  private def gammaSeries(a: Double, x: Double): Double =
    var ap  = a
    var del = 1.0 / a
    var sum = del
    var n   = 0
    while n < MaxIter && math.abs(del) > math.abs(sum) * Eps do
      ap += 1
      del *= x / ap
      sum += del
      n += 1
    sum * gammaPrefactor(a, x)

  private def gammaContinuedFraction(a: Double, x: Double): Double =
    var b    = x + 1 - a
    var c    = 1.0 / FpMin
    var d    = 1.0 / b
    var h    = d
    var i    = 1
    var done = false
    while !done && i < MaxIter do
      val an = -i * (i - a)
      b += 2
      d = an * d + b
      if math.abs(d) < FpMin then d = FpMin
      c = b + an / c
      if math.abs(c) < FpMin then c = FpMin
      d = 1.0 / d
      val del = d * c
      h *= del
      done = math.abs(del - 1) < Eps
      i += 1
    gammaPrefactor(a, x) * h

  // --- beta family ---

  /** Regularized incomplete beta I_x(a, b). */
  def betaI(a: Double, b: Double, x: Double): Double =
    if x <= 0 then 0.0
    else if x >= 1 then 1.0
    else
      val front =
        math.exp(lnGamma(a + b) - lnGamma(a) - lnGamma(b) + a * math.log(x) + b * math.log1p(-x))
      if x < (a + 1) / (a + b + 2) then front * betaContinuedFraction(a, b, x) / a
      else 1.0 - front * betaContinuedFraction(b, a, 1 - x) / b

  private def betaContinuedFraction(a: Double, b: Double, x: Double): Double =
    def clamp(v: Double) = if math.abs(v) < FpMin then FpMin else v
    val qab              = a + b
    val qap              = a + 1
    val qam              = a - 1
    var c                = 1.0
    var d                = 1.0 / clamp(1 - qab * x / qap)
    var h                = d
    var m                = 1
    var done             = false
    while !done && m < MaxIter do
      val m2   = 2 * m
      val even = m * (b - m) * x / ((qam + m2) * (a + m2))
      d = 1.0 / clamp(1 + even * d)
      c = clamp(1 + even / c)
      h *= d * c
      val odd = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
      d = 1.0 / clamp(1 + odd * d)
      c = clamp(1 + odd / c)
      val del = d * c
      h *= del
      done = math.abs(del - 1) < Eps
      m += 1
    h

  // --- normal ---

  def normalCdf(z: Double): Double =
    if z >= 0 then 1.0 - normalSf(z) else normalSf(-z)

  /** Survival function 1 - Φ(z), accurate far into the tail (no cancellation). */
  def normalSf(z: Double): Double =
    if z >= 0 then 0.5 * gammaQ(0.5, z * z / 2) else 1.0 - 0.5 * gammaQ(0.5, z * z / 2)

  /** Two-sided p-value of a standard normal statistic. */
  def normalTwoSided(z: Double): Double = math.min(1.0, gammaQ(0.5, z * z / 2))

  private val A = Array(-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
    1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00)
  private val B = Array(-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
    6.680131188771972e+01, -1.328068155288572e+01)
  private val C = Array(-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
    -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00)
  private val D = Array(
    7.784695709041462e-03,
    3.224671290700398e-01,
    2.445134137142996e+00,
    3.754408661907416e+00
  )

  /** Inverse of the standard normal CDF. */
  def normalQuantile(p: Double): Double =
    require(p > 0 && p < 1, s"p must be in (0, 1), got $p")
    def tail(q: Double) =
      (((((C(0) * q + C(1)) * q + C(2)) * q + C(3)) * q + C(4)) * q + C(5)) /
        ((((D(0) * q + D(1)) * q + D(2)) * q + D(3)) * q + 1)
    val low = 0.02425
    val x0  =
      if p < low then tail(math.sqrt(-2 * math.log(p)))
      else if p > 1 - low then -tail(math.sqrt(-2 * math.log1p(-p)))
      else
        val q = p - 0.5
        val r = q * q
        (((((A(0) * r + A(1)) * r + A(2)) * r + A(3)) * r + A(4)) * r + A(5)) * q /
          (((((B(0) * r + B(1)) * r + B(2)) * r + B(3)) * r + B(4)) * r + 1)
    // One step of Halley's method brings the error from ~1e-9 to machine precision.
    val e = normalCdf(x0) - p
    val u = e * math.sqrt(2 * math.Pi) * math.exp(x0 * x0 / 2)
    x0 - u / (1 + x0 * u / 2)

  // --- Student's t ---

  /** Two-sided p-value P(|T| >= |t|) for `df` degrees of freedom. */
  def studentTwoSided(t: Double, df: Double): Double = betaI(df / 2, 0.5, df / (df + t * t))

  def studentCdf(t: Double, df: Double): Double =
    val half = 0.5 * studentTwoSided(t, df)
    if t >= 0 then 1 - half else half

  /** Inverse CDF by bisection on the (monotone) two-sided p-value. */
  def studentQuantile(p: Double, df: Double): Double =
    require(p > 0 && p < 1, s"p must be in (0, 1), got $p")
    if p == 0.5 then 0.0
    else
      val target = 2 * math.min(p, 1 - p)
      var lo     = 0.0
      var hi     = 1.0
      while studentTwoSided(hi, df) > target do hi *= 2
      (0 until 200).foreach: _ =>
        val mid = (lo + hi) / 2
        if studentTwoSided(mid, df) > target then lo = mid else hi = mid
      val t = (lo + hi) / 2
      if p > 0.5 then t else -t

  // --- chi-squared ---

  def chiSquaredSf(x: Double, df: Double): Double = gammaQ(df / 2, x / 2)
