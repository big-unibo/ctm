package it.unibo.big.temporal

/**
 * Trait to express the variable temporal scales that can be adopted by the algorithm.
 */
sealed trait TemporalScale {
    /** The identifier, as a string, of the time model. */
    val value: String
}

/** Temporal scale */
object TemporalScale {

    /**
     * Create scale from string.
     *
     * @param value scale value
     * @return scale
     */
    def apply(value: String): TemporalScale = value match {
        case WeeklyScale.value => WeeklyScale
        case DailyScale.value => DailyScale
        case AbsoluteScale.value => AbsoluteScale
        case NoScale.value => NoScale
        case _ => throw new IllegalArgumentException("Unknown time scale: " + value)
    }

    case object WeeklyScale extends TemporalScale {
        override val value: String = "weekly"
    }

    case object DailyScale extends TemporalScale {
        override val value: String = "daily"
    }

    case object AbsoluteScale extends TemporalScale {
        override val value: String = "absolute"
    }

    case object NoScale extends TemporalScale {
        override val value: String = "notime"
    }
}