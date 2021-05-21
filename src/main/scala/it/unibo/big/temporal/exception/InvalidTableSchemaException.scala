package it.unibo.big.temporal.exception

import it.unibo.big.Utils

/**
 * This exception is raised when an input table schema does not contain the necessary fields to be processed.
 * by the MACHO algorithm.
 */
final case class InvalidTableSchemaException(private val message: String = InvalidTableSchemaException.ERROR_MESSAGE,
                                             private val cause: Throwable = None.orNull) extends Exception(message, cause)

object InvalidTableSchemaException {
  val ERROR_MESSAGE = s"A valid schema was not found, please use a database whose schema contains the following fields:" +
    s"${Utils.USER_ID_FIELD}(String),${Utils.TRAJECTORY_ID_FIELD}(String), ${Utils.LATITUDE_FIELD_NAME}(Double), " +
    s"${Utils.LONGITUDE_FIELD_NAME}(Double), ${Utils.TIMESTAMP_FIELD_NAME}(Long)"
}
