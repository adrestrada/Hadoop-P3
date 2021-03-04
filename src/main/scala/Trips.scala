package ca.mcit.bigdata.hdfs

case class Trips(routeId: String,
                 serviceId: String,
                 tripId : String,
                 tripHeadsign: String,
                 wheelchairAccessible: Boolean)

object Trips {
  def toCsv(trips: Trips): String = {
    s"${trips.routeId},${trips.serviceId},${trips.tripId},${trips.tripHeadsign},${trips.wheelchairAccessible}"
  }
}