package ca.mcit.bigdata.hdfs

import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import java.io.{BufferedReader, InputStreamReader}

object Hadoop05MainEnricher extends App with HadoopClient {

  val bufferedLocation = "/user/bdsf2001/adriest/stm/"
  //----------------------------------------------------------------------Trips
  val bufferedTrips = new BufferedReader(new InputStreamReader(fs.open(new Path(s"${bufferedLocation}trips.csv"))))
  val tripList: List[Trips] = Iterator
    .continually(bufferedTrips.readLine())
    .takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Trips(n(0), n(1), n(2), n(3), if (n(6) =="1") true else false))
  bufferedTrips.close()
  //---------------------------------------------------------------------Routes
  val bufferedRoutes = new BufferedReader(new InputStreamReader(fs.open(new Path(s"${bufferedLocation}routes.csv"))))
  val routesList: List[Routes] = Iterator
    .continually(bufferedRoutes.readLine())
    .takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Routes(n(0), n(3), n(6)))
  bufferedRoutes.close()
  //--------------------------------------------------------------------CalendarDate
  val bufferedCalendarDate = new BufferedReader(new InputStreamReader(fs.open(new Path(s"${bufferedLocation}calendar_dates.csv"))))
  val calendarDateList: List[CalendarDates] = Iterator
    .continually(bufferedCalendarDate.readLine())
    .takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => CalendarDates(n(0), n(1), n(2).toInt))
  bufferedCalendarDate.close()
  //---------------------------------------------------------------------TripRoute
  val lookupRoutes: Map [String, Routes] =
    routesList
      .map(route => route.routeId -> route).toMap
  val tripsRoute: List[TripsRoute] =
    tripList
      .map { trip => TripsRoute( trip, lookupRoutes(trip.routeId))}
  //---------------------------------------------------------------------EnrichedTrip
  val lookupCalendarDates: Map [String, CalendarDates] =
  calendarDateList
      .map(calendar => calendar.serviceId -> calendar).toMap
  val enrichedTrip: List[EnrichedTrip] =
    tripsRoute
      .map(tripsRoute => EnrichedTrip( tripsRoute, lookupCalendarDates.getOrElse(tripsRoute.trips.serviceId, null)))
  //------------------------------------------------------------------ write EnrichedTrip
  val writer: FSDataOutputStream = fs.create(new Path("/user/bdsf2001/adriest/course3/enriched_trips.csv"))
  writer.writeBytes("trip_id, service_id, route_id,trip_headsign,wheelchair_accessible," +
    "date,exception_type,route_long_name,route_color\n")
  enrichedTrip
    .map(line => EnrichedTrip.toCsv(line))
    .foreach(line => writer.writeUTF(line))
  writer.flush()
  writer.close()
  fs.close()
}
