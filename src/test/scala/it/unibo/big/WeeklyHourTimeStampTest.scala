package it.unibo.big

import it.unibo.big.temporal.WeeklyHourTimeStamp
import org.junit.jupiter.api.Test

class WeeklyHourTimeStampTest {

    val midnightMondayTimeStamp = 1578269438L
    val lateEveningSundayTimeStamp = 1578265838L
    val previousTimeStamp = 1578226238L
    val futureTimeStamp = 1578229838L

    @Test
    def testDateBuilding(): Unit = {
        val baseWeeklyHourDate = WeeklyHourTimeStamp(midnightMondayTimeStamp)
        assert(baseWeeklyHourDate.hourOfDay == 0)
        assert(baseWeeklyHourDate.weeklyDay == 0)
    }

    @Test
    def testLateEveningDate(): Unit = {
        val lateDate = WeeklyHourTimeStamp(lateEveningSundayTimeStamp)
        assert(lateDate.hourOfDay == 23)
        assert(lateDate.weeklyDay == 6)
    }

    @Test
    def testMaxMinIndexes(): Unit = {
        val baseWeeklyHourDate = WeeklyHourTimeStamp(midnightMondayTimeStamp)
        val otherWeeklyHourDate = WeeklyHourTimeStamp(lateEveningSundayTimeStamp)
        assert(baseWeeklyHourDate.index == 0)
        assert(otherWeeklyHourDate.index == (24 * 6) + 23)
    }

    @Test
    def testContiguityInIndexes(): Unit = {
        val previousDate = WeeklyHourTimeStamp(previousTimeStamp)
        val futureDate = WeeklyHourTimeStamp(futureTimeStamp)
        assert(previousDate.index == (futureDate.index - 1))
    }

    @Test
    def testContiguityBetweenDaysInIndexes(): Unit = {
        val lastHourPreviousDate = WeeklyHourTimeStamp(1578093038L)
        val firstHourSuccessiveDate = WeeklyHourTimeStamp(1578096638L)
        assert(lastHourPreviousDate.index == (firstHourSuccessiveDate.index - 1))
    }

    @Test
    def testDistanceBetweenIdenticalPoint(): Unit = {
        val basicDate = WeeklyHourTimeStamp(midnightMondayTimeStamp)
        val distance = basicDate - basicDate
        assert(distance == 0)
    }

    @Test
    def testDistanceBetweenTwoConsecutivePoints(): Unit = {
        val previousDate = WeeklyHourTimeStamp(previousTimeStamp)
        val futureDate = WeeklyHourTimeStamp(futureTimeStamp)
        val distance = futureDate - previousDate
        assert(distance == 1)
    }

    @Test
    def testCommutativeProprietyDistance(): Unit = {
        val previousDate = WeeklyHourTimeStamp(previousTimeStamp)
        val futureDate = WeeklyHourTimeStamp(futureTimeStamp)
        val distance = previousDate - futureDate
        assert(distance == 1)
    }

    @Test
    def testInvertedDistance(): Unit = {
        val previousDate = WeeklyHourTimeStamp(midnightMondayTimeStamp)
        val futureDate = WeeklyHourTimeStamp(lateEveningSundayTimeStamp)
        val distance = previousDate - futureDate
        assert(distance == 1)
    }

    @Test
    def testInvertedDistanceDifferentDays(): Unit = {
        val saturdayDate = WeeklyHourTimeStamp(1578096638L)
        val mondayDate = WeeklyHourTimeStamp(1578269438L)
        val distance = saturdayDate - mondayDate
        assert(distance == 48)
    }

    @Test
    def testOfflineFailingTCB(): Unit = {
        val monday10AMStamp = 1578910464L
        val monday11AMStamp = 1578914064L
        val monday12AMStamp = 1578917664L
        val tenAMMonday = WeeklyHourTimeStamp(monday10AMStamp)
        val elevenAMMonday = WeeklyHourTimeStamp(monday11AMStamp)
        val twelveAMMonday = WeeklyHourTimeStamp(monday12AMStamp)
        assert((tenAMMonday - elevenAMMonday) == 1)
        assert((tenAMMonday - twelveAMMonday) == 2)
        assert((elevenAMMonday - twelveAMMonday) == 1)
        assert(WeeklyHourTimeStamp.computeDistanceBetweenStamps(tenAMMonday.index, elevenAMMonday.index) == 1)
    }

    @Test
    def testInvertedDistanceDifferentDaysOnADayStamp(): Unit = {
        val saturdayDate = WeeklyHourTimeStamp(1578096638L, 24)
        val mondayDate = WeeklyHourTimeStamp(1578269438L, 24)
        val distance = WeeklyHourTimeStamp.computeDistanceBetweenStamps(mondayDate.index, saturdayDate.index, 24)
        assert(distance == 2)
    }

    @Test
    def testDirectedDistanceDifferentDaysOnADayStamp(): Unit = {
        val tuesdayDate = WeeklyHourTimeStamp(1579015338L, 24)
        val mondayDate = WeeklyHourTimeStamp(1578269438L, 24)
        val distance = WeeklyHourTimeStamp.computeDistanceBetweenStamps(mondayDate.index, tuesdayDate.index, 24)
        assert(distance == 1)
    }

    @Test
    def testDayInvertedDistance(): Unit = {
        val saturdayDate = WeeklyHourTimeStamp(1578096638L, 24)
        val mondayDate = WeeklyHourTimeStamp(1578269438L, 24)
        val distance = mondayDate.computeWeeklyDistance(saturdayDate)
        val otherDistance = saturdayDate.computeWeeklyDistance(mondayDate)
        assert(distance == 2)
        assert(otherDistance == 2)
    }

    @Test
    def testDayDirectedDistance(): Unit = {
        val tuesdayDate = WeeklyHourTimeStamp(1579015338L, 24)
        val mondayDate = WeeklyHourTimeStamp(1578269438L, 24)
        val distance = mondayDate.computeWeeklyDistance(tuesdayDate)
        assert(distance == 1)
    }

    @Test
    def testHourInvertedDistance(): Unit = {
        val tuesdayDate = WeeklyHourTimeStamp(1579015338L, 24)
        val mondayDate = WeeklyHourTimeStamp(1578269438L, 24)
        val distance = mondayDate.computeDailyDistance(tuesdayDate)
        assert(distance == 9)
    }

    @Test
    def testHourDirectedDistance(): Unit = {
        val wensdayDate = WeeklyHourTimeStamp(1579688610L)
        val mondayDate = WeeklyHourTimeStamp(1578269438L)
        val distance = mondayDate.computeDailyDistance(wensdayDate)
        assert(distance == 10)
    }
}
