package ca.mcit.bigdata.hdfs

case class CalendarDates(serviceId: String,
                         date: String,
                         exceptionType: Int)
object CalendarDates {
  def toCsv(calendarDates: CalendarDates): String = {
    s"${calendarDates.serviceId},${calendarDates.date},${calendarDates.exceptionType}"
  }
}