package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._

import scala.collection.immutable
import scala.math.Numeric._

/** The NumberFaker is a helper that works with Numeric types in Avro, to reuse strategies in a consistent way for INT,
  * LONG, FLOAT and DOUBLE types.
  */
object NumberFaker {

  /** Tests a numeric value and ensures that it is between the bounds (inclusive). */
  private def forceBounds[T](value: T, lower: T, upper: T)(implicit num: Numeric[T]): T = {
    import num._
    value min upper max lower
  }

  /** For the numbers that we handle, convert them to a numeric type that we wish to use during generation. Note that we
    * only use Doubles and Longs, which are set to the bounds or precision of Int and Float at the last minute, but we
    * handle Integers just in case they are explicitly set.
    */
  private def toNumOption[T](value: Any)(implicit num: Numeric[T]): Option[T] = {
    val pass1: Option[Any] = Option((num, value)).collect {
      case (_, false)                         => num.zero
      case (_, true)                          => num.one
      case (_, "false")                       => num.zero
      case (_, "true")                        => num.one
      case (_, b: Byte)                       => num.fromInt(b)
      case (_, s: Short)                      => num.fromInt(s)
      case (_, i: Int)                        => num.fromInt(i)
      case (_: DoubleIsFractional, l: Long)   => l.toDouble
      case (_: DoubleIsFractional, f: Float)  => f.toDouble
      case (_: DoubleIsFractional, d: Double) => d
      case (_: LongIsIntegral, l: Long)       => l
      case (_: LongIsIntegral, f: Float)      => f.toLong
      case (_: LongIsIntegral, d: Double)     => d.toLong
      case (_: IntIsIntegral, l: Long)        => l.toInt
      case (_: IntIsIntegral, f: Float)       => f.toInt
      case (_: IntIsIntegral, d: Double)      => d.toInt
    }

    pass1
      .orElse(
        Option((num, value)).flatMap {
          case (dnum: DoubleIsFractional, s: String) => dnum.parseString(s)
          case (lnum: LongIsIntegral, s: String) =>
            lnum.parseString(s).orElse(DoubleIsFractional.parseString(s).map(_.toLong))
          case (inum: IntIsIntegral, s: String) =>
            inum.parseString(s).orElse(DoubleIsFractional.parseString(s).map(_.toInt))
          case _ => None
        }
      )
      .asInstanceOf[Option[T]]
  }

  private def toNum[T](value: Any)(implicit num: Numeric[T]): T = {
    toNumOption(value)(num).getOrElse {
      sys.error(s"Unsupported numeric type: ${num.getClass} for ${value.getClass}")
    }
  }

  def apply[T](bounds: (T, T), args: Map[String, Any], key: String, dflt: Any = Map(ArgFaker -> StrategyRandom))(
      implicit num: Numeric[T]
  ): AvroFaker[T] = {

    // Get the key from the args or defaults
    val value: Option[Any] = args.get(key)

    // Shortcut defaults for the bounds
    if (key == ArgMin && value.isEmpty) return ConstantFaker(bounds._1)
    if (key == ArgMax && value.isEmpty) return ConstantFaker(bounds._2)

    // Get any explicitly specified strategy.
    val explicitFn: Option[AvroFaker[T]] = value collect {
      case StrategyRandom   => random(bounds, args)
      case StrategyGauss    => gauss(bounds, args)
      case StrategySequence => sequence[T](bounds, args)
      case StrategyValue    => bounded(bounds, args)
      case StrategyOneOf | StrategySumOf | StrategyProductOf | StrategyMinOf | StrategyMaxOf | StrategyMeanOf =>
        NumberFaker(bounds, args, args(key).toString, dflt)(num) match {
          case RandomOneOfFaker(fns) =>
            args(key) match {
              case StrategyOneOf     => OneOfFaker(args, fns)
              case StrategySumOf     => SumOfFaker(fns)(num)
              case StrategyProductOf => ProductOfFaker(fns)(num)
              case StrategyMinOf     => MinOfFaker(fns)(num)
              case StrategyMaxOf     => MaxOfFaker(fns)(num)
              case StrategyMeanOf    => MeanOfFaker(fns)(num)
            }
          case other => other
        }
      case StrategyWeights => weights(bounds, args)
    }
    if (explicitFn.nonEmpty) return explicitFn.get

    // Otherwise try to create a constant generator from the argument.
    val constantFn: Option[AvroFaker[T]] = value.flatMap(toNumOption(_)(num)).collect { ConstantFaker(_) }
    if (constantFn.nonEmpty) return constantFn.get

    // Otherwise try and get a strategy based on the property and explicitly specify it.
    if (key == ArgFaker && !args.contains(key)) {
      val implicitFn: Option[Map[String, String]] = NumberArgToImplicitStrategy.keys
        .find(args.contains)
        .map(k => Map(key -> NumberArgToImplicitStrategy.apply(k)))
      if (implicitFn.nonEmpty) return NumberFaker(bounds, args ++ implicitFn.get, key, dflt)(num)
    }

    // Otherwise, interpret the strategy depending on its type
    value match {
      case Some(m: Map[_, _]) =>
        // A map, or object, adds attributes to the strategy that can complete how it is interpreted.
        // TODO: This is pretty complicated, removing some attributes from the existing faker strategy in
        // order to use the attributes in the extra object, when what we *really* want is to prioritize the
        // attributes in m to implicitly choose the strategy, then try the combined attributes.
        val keysToRemove: immutable.Iterable[String] = NumberArgToImplicitStrategy
          .groupMap(_._2)(_._1)
          .getOrElse(args.getOrElse(ArgFaker, "").toString, Seq.empty)
          .toSeq :+ ArgFaker :+ key
        NumberFaker[T](bounds, args.removedAll(keysToRemove) ++ m.asInstanceOf[Map[String, Any]], ArgFaker, dflt)
      case Some(xs: Iterable[_]) =>
        // An array is, by default, a list of strategies that we randomly pick one from.
        RandomOneOfFaker(xs.map(v => NumberFaker[T](bounds, args ++ Map(key -> v), key, dflt)).toSeq)
      case None if dflt.isInstanceOf[AvroFaker[_]] => dflt.asInstanceOf[AvroFaker[T]]
      case None                                    =>
        // We fall back to the default value.
        NumberFaker[T](bounds, args ++ Map(key -> dflt), key, None)
      case Some(unknown) =>
        // This should never occur in our processing
        throw new IllegalArgumentException(s"Unknown argument content: $unknown")
    }
  }

  private def bounded[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): AvroFaker[T] =
    BoundedFaker(
      minFn = NumberFaker(bounds, args, ArgMin)(num),
      maxFn = NumberFaker(bounds, args, ArgMax)(num),
      fn = NumberFaker(bounds, args, StrategyValue)(num)
    )

  def random[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): AvroFaker[T] = {
    val defaultBounds: (T, T) = num match {
      case _: Fractional[T] => (num.zero, num.one)
      case _                => bounds
    }
    RandomFaker(
      minFn = NumberFaker(defaultBounds, args, ArgMin),
      maxFn = NumberFaker(defaultBounds, args, ArgMax)
    )
  }
  def random[T](min: T, max: T)(implicit num: Numeric[T]): AvroFaker[T] = {
    RandomFaker(minFn = ConstantFaker[T](min), maxFn = ConstantFaker[T](max))
  }

  private def gauss[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): AvroFaker[T] = {

    /** Fractional numbers have a standard deviation of 100 instead of 1 so we see a wider range of values. */
    val defaultStddev: Double = num match {
      case _: Fractional[T] => 1.0
      case _                => 100.0
    }

    DoubleGaussFaker[T](
      minFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMin, bounds._1),
      maxFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMax, bounds._2),
      meanFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMean, 0),
      stddevFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgStddev, defaultStddev)
    )
  }

  private def sequence[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): AvroFaker[T] = {
    SequenceFaker(
      minFn = NumberFaker[T](bounds, args, ArgMin),
      maxFn = NumberFaker[T](bounds, args, ArgMax),
      stepFn = NumberFaker[T](bounds, args, ArgStep, 1),
      startFn = if (args.contains(ArgStart)) Some(NumberFaker[T](bounds, args, ArgStart)(num)) else None,
      overridesMin = args.contains(ArgMin)
    )
  }

  private def weights[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): AvroFaker[T] = {
    import num._
    val maxFn: AvroFaker[Int] = NumberFaker((0, (num.fromInt(Int.MaxValue) min bounds._2).toInt), args, ArgMax)
    val weightFns: Seq[AvroFaker[Double]] =
      NumberFaker[Double]((0d, Double.MaxValue), args, StrategyWeights, 1) match {
        case RandomOneOfFaker(fns) => fns
        case other                 => Seq(other)
      }
    maxFn match {
      case ConstantFaker(max) if weightFns.forall(_.isInstanceOf[ConstantFaker[_]]) =>
        WeightsConstantFaker[T](max = max, weights = weightFns.collect[Double] { case ConstantFaker(weight) => weight })
      case _ => WeightFaker(maxFn = maxFn, weightFns = weightFns)
    }
  }

  /** Create an AvroFaker to generate long values, but don't default to a random value uniformly distributed along the
    * entire LONG range. If nothing is otherwise specified, use the dflt strategy provided.
    */
  def longOrElse(args: Map[String, Any], key: String, dflt: AvroFaker[Long]): AvroFaker[Long] = {
    NumberFaker[Long]((Long.MaxValue, Long.MinValue), args, ArgLength) match {
      // Here, we replace the undesirable MaxValue random generator with smaller constants.
      case RandomFaker(ConstantFaker(Long.MaxValue), ConstantFaker(Long.MinValue)) => dflt
      case other                                                                   => other
    }
  }

  /** A faker that returns a value bounded by a minimum or a maximum. */
  private[this] case class BoundedFaker[T](minFn: AvroFaker[T], maxFn: AvroFaker[T], fn: AvroFaker[T])(implicit
      num: Numeric[T]
  ) extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = NumberFaker.forceBounds(fn(ctx), minFn(ctx), maxFn(ctx))
  }

  /** A faker that generates random numbers uniformly from an interval.
    *
    * It can be configured with the following arguments:
    *
    *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
    *   - `max`: The upper bound (exclusive) of the sequence (No default, this must be supplied).
    */
  private[this] case class RandomFaker[T](minFn: AvroFaker[T], maxFn: AvroFaker[T])(implicit num: Numeric[T])
      extends AvroFaker[T] {
    import NumberFaker._
    import num._
    def apply(ctx: FakerContext): T = num match {
      case _: FloatIsFractional =>
        toNum(ctx.rnd.between(minFn(ctx).toFloat, maxFn(ctx).toFloat))(num)
      case _: DoubleIsFractional =>
        toNum(ctx.rnd.between(minFn(ctx).toDouble, maxFn(ctx).toDouble))(num)
      case _: LongIsIntegral =>
        toNum(ctx.rnd.between(minFn(ctx).toLong, maxFn(ctx).toLong))(num)
      case _: IntIsIntegral =>
        toNum(ctx.rnd.between(minFn(ctx).toInt, maxFn(ctx).toInt))(num)
      case _ => sys.error(s"Unsupported numeric type: ${num.getClass}")
    }
  }

  /** A faker that generates numbers along the Gauss distribution to have a bell curve nicely centered around a median.
    *
    * It can be configured with the following arguments:
    *
    *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
    *   - `max`: The upper bound (exclusive) of the sequence (No default, this must be supplied).
    *   - `mean` and `stddev`: The mean and standard deviation to position the bell curve (Default: 0.0 and 1.0 for
    *     DOUBLE)
    *
    * Values outside the `min` and `max` are discarded. Be careful, if your valid interval is rarely drawn from a
    * gaussian distribution, this generator might be very slow to generate value.
    */
  private[this] case class DoubleGaussFaker[T](
      minFn: AvroFaker[Double],
      maxFn: AvroFaker[Double],
      meanFn: AvroFaker[Double],
      stddevFn: AvroFaker[Double]
  )(implicit num: Numeric[T])
      extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = {
      val min = minFn(ctx)
      val max = maxFn(ctx)
      val mean = meanFn(ctx)
      val stddev = stddevFn(ctx)
      val next =
        LazyList.continually(ctx.rnd.nextGaussian() * stddev + mean).dropWhile(_ < min).dropWhile(_ >= max).head
      NumberFaker.toNum(next)(num)
    }
  }

  /** A faker that generates whole numbers within an interval, moving by a step every time. When the end of the sequence
    * is reached, it wraps around and repeats.
    *
    * It can be configured with the following arguments:
    *
    *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
    *   - `max`: The upper bound (exclusive) of the sequence (Default: Long.MaxValue for LONG, etc, depending on the
    *     type).
    *   - `step`: The distance to move in the interval (Default: 1)
    *   - `start`: The starting value for the sequence. (Default: depends on the step, whether it starts from the 0,
    *     upper or lower bound.)
    */
  private[this] case class SequenceFaker[T](
      minFn: AvroFaker[T],
      maxFn: AvroFaker[T],
      stepFn: AvroFaker[T],
      startFn: Option[AvroFaker[T]],
      overridesMin: Boolean
  )(implicit num: Numeric[T])
      extends AvroFaker[T] {
    import num._
    private[this] var next: Option[T] = None
    def apply(ctx: FakerContext): T = {

      // The direction of the next step
      val min = minFn(ctx)
      val max = maxFn(ctx)
      val step = stepFn(ctx)

      val generated = next match {
        case None if startFn.nonEmpty => startFn.get(ctx)
        case None if step < num.zero  => max + step
        case None if !overridesMin    => num.zero
        case None                     => min
        case Some(value)              => value
      }

      next = Some(generated + step) // TODO: test overflow

      if (step < num.zero && next.exists(_ < min)) {
        next = next.map(max - min + _)
      } else if (step > num.zero && next.exists(_ >= max))
        next = next.map(min - max + _)

      generated
    }
  }

  /** A faker that sums from its list of strategies */
  private[this] case class SumOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).sum
  }

  /** A faker that creates a product from its list of strategies */
  private[this] case class ProductOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T])
      extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).product
  }

  /** A faker that finds the minimum from its list of strategies */
  private[this] case class MinOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).min
  }

  /** A faker that finds the maximum from its list of strategies */
  private[this] case class MaxOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).max
  }

  /** A faker that finds the mean from its list of strategies */
  private[this] case class MeanOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = num match {
      case frac: Fractional[_] =>
        import frac._
        fns.map(_.apply(ctx)).sum / frac.fromInt(fns.size)
      case int: Integral[_] =>
        import int._
        fns.map(_.apply(ctx)).sum / int.fromInt(fns.size)
      case _ => throw new IllegalArgumentException("Unsupported numeric type")
    }
  }

  private[this] case class WeightsConstantFaker[T](max: Int, weights: Seq[Double])(implicit num: Numeric[T])
      extends AvroFaker[T] {

    // Count the number of elements that are weighted (n) and unweighted (m) and the sum of their weights
    private[this] val n = (weights.size - weights.reverse.takeWhile(_ == 1.0).size) min max
    private[this] val nSum = weights.take(n).sum
    private[this] val m = (max - n) max 0
    private[this] val mSum = m // 1.0 each

    def apply(ctx: FakerContext): T = {
      // We choose either the N set or the M set with the correct probability
      val index: Int = if (mSum == 0 || nSum != 0 && ctx.rnd.nextDouble() < nSum / (nSum + mSum)) {
        // If there's only one number in the weighted set, then return it without consuming a random
        if (n <= 1) 0
        else {
          // Otherwise find a double value somewhere in the weighted sum, and pick where it lands
          val countdown = LazyList.from(weights).scanLeft(ctx.rnd.nextDouble() * nSum) { _ - _ }
          countdown.tail.zipWithIndex.find(_._1 < 0).map(_._2).getOrElse(n - 1)
        }
      } else {
        // Choosing from the M set if there's more than one, just give all the elements an equal chance
        if (m <= 1) n
        else n + ctx.rnd.nextInt(m)
      }

      NumberFaker.toNum[T](index)
    }
  }

  private[this] case class WeightFaker[T](maxFn: AvroFaker[Int], weightFns: Seq[AvroFaker[Double]])(implicit
      num: Numeric[T]
  ) extends AvroFaker[T] {
    def apply(ctx: FakerContext): T = WeightsConstantFaker[T](maxFn(ctx), weightFns.map(_(ctx))).apply(ctx)
  }
}
