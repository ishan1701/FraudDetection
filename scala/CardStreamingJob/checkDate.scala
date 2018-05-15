package CardStreamingJob
import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat

object checkDate extends App{
 /* val a = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  val b=a.parse("2012-01-05T00:00:00.000")
  println(b)*/
  var date_string=""
  val a=DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  val b=a.parseDateTime("2012-01-05T00:00:00.000+05:30")
  println
 println( b.getYear.toString()+"-"+(b.getMonthOfYear).toString()+"-"+(b.getDayOfMonth).toString())
 
}