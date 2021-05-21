package it.unibo.big.temporal

import org.joda.time.{DateTime, DateTimeZone}


/**
 * This trait contains a timestamp converted into its hour and weekly day.
 */
trait WeeklyHourTimeStamp {

  /**
   * Define the day of the week of the stamp.
   * @return an integer between 0 and 6 for each different day of the week.
   */
  def weeklyDay:Int

  /**
   * Define the hour of the day of the stamp.
   * @return an integer between 0 and 23 for each different hour inside a day.
   */
  def hourOfDay:Int

  /**
   * Define the index as a combination of hour of the day and day of the week.
   * @return an integer computed as mentioned above.
   */
  def index:Int

  /**
   * Define subtraction between two timestamps.
   * @param otherStamp the other stamp to be subtracted.
   * @return an integer containing the difference between two timestamps.
   */
  def -(otherStamp: WeeklyHourTimeStamp):Int

  def bucketSize:Int

  /**
   * Compute distance in hour between two timestamps.
   * @param otherStamp the other stamp to be used inside the computation.
   * @return an integer containing the distance.
   */
  def computeDailyDistance(otherStamp:WeeklyHourTimeStamp):Int

  /**
   * Compute distance in days between two timestamps.
   * @param otherStamp the other stamp to be used inside the computation.
   * @return an integer containing the distance.
   */
  def computeWeeklyDistance(otherStamp:WeeklyHourTimeStamp):Int

}

object WeeklyHourTimeStamp {
  val MAXIMUM_DAY_OK_WEEK_BUCKET_INDEX = 7
  val MAXIMUM_HOUR_OF_DAY_BUCKET_INDEX = 24

  def apply(timeStampSecond: Long): WeeklyHourTimeStamp = new WeeklyHourTimeStampImpl(timeStampSecond)

  def apply(timeStampSecond: Long, timeBucketSize:Int): WeeklyHourTimeStamp = new WeeklyHourTimeStampImpl(timeStampSecond, timeBucketSize)


  /**
   * Utility to compute distance between two given indexes.
   * @param baseTimeStampIndex one of the two index for the computation.
   * @param otherTimeStampIndex the other index for the computation.
   * @param samplingBucketSize the size of a sampling bucket.
   * @return the distance between the two indexes as a integer.
   */
  def computeDistanceBetweenStamps(baseTimeStampIndex: Int,
                                   otherTimeStampIndex: Int,
                                   samplingBucketSize:Int):Int = {

    val MAX_TIMESTAMP_INDEX = computeMaxTimestampIndex(samplingBucketSize)
    computeTimeDistance(baseTimeStampIndex, otherTimeStampIndex, MAX_TIMESTAMP_INDEX)
  }

  /**
   * Utility to compute the distance between two day indexes as an integer.
   * @param baseValue the first day index.
   * @param otherValue the second day index.
   * @return an integer containing minimum distance between the two indexes.
   */
  def computeWeeklyDistance(baseValue:Int, otherValue:Int):Int = computeTimeDistance(baseValue, otherValue, MAXIMUM_DAY_OK_WEEK_BUCKET_INDEX)

  /**
   * Utility to compute the distance between two hour indexes as an integer.
   * @param baseValue  the first hour index.
   * @param otherValue the second hour index.
   * @return an integer containing minimum distance between the two indexes.
   */
  def computeDailyDistance(baseValue: Int, otherValue: Int): Int = computeTimeDistance(baseValue, otherValue, MAXIMUM_HOUR_OF_DAY_BUCKET_INDEX)

  def computeDistanceBetweenStamps(baseTimeStampIndex: Int, otherTimeStampIndex: Int): Int = computeDistanceBetweenStamps(baseTimeStampIndex, otherTimeStampIndex, 1)

  private def computeMaxTimestampIndex(samplingBucketSize:Int):Int = {
    val max_day_index = 7
    val max_hour_index = 24
    val previous_max_timestamp_index = ((max_day_index * max_hour_index) - 1) / samplingBucketSize
    val max_timestamp_index = (max_day_index * max_hour_index) / samplingBucketSize
    if (previous_max_timestamp_index == max_timestamp_index) max_timestamp_index + 1
    else max_timestamp_index
  }

  private def computeTimeDistance(timeStamp1:Int, timeStamp2:Int, maxAbsoluteStamp:Int):Int = {
    val maxStamp = if (timeStamp1 >= timeStamp2) timeStamp1 else timeStamp2
    val minStamp = if (maxStamp == timeStamp1) timeStamp2 else timeStamp1
    val directDistance = maxStamp - minStamp
    val invertedDistance = (maxAbsoluteStamp - maxStamp) + minStamp
    scala.math.min(directDistance, invertedDistance)
  }

  private class WeeklyHourTimeStampImpl(dateInSeconds: Long, override val bucketSize: Int = 1) extends WeeklyHourTimeStamp {
    val date: DateTime = new DateTime(dateInSeconds * 1000L, DateTimeZone.UTC)
    override def weeklyDay: Int = (date.dayOfWeek().get()) - 1
    override def hourOfDay: Int = date.hourOfDay().get()
    override def index: Int = (weeklyDay * 24 + hourOfDay) / bucketSize
    override def -(otherStamp: WeeklyHourTimeStamp): Int =
      if (this.bucketSize == otherStamp.bucketSize) {
        computeDistanceBetweenStamps(this.index, otherStamp.index, this.bucketSize)
      } else {
        throw new IllegalArgumentException()
      }
    override def computeWeeklyDistance(otherStamp: WeeklyHourTimeStamp): Int = WeeklyHourTimeStamp.computeWeeklyDistance(this.weeklyDay, otherStamp.weeklyDay)
    override def computeDailyDistance(otherStamp: WeeklyHourTimeStamp): Int = WeeklyHourTimeStamp.computeDailyDistance(this.hourOfDay, otherStamp.hourOfDay)
  }
}