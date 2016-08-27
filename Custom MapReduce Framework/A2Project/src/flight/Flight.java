package flight;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/** 
 * Flight POJO. Represents an airline flight from OTP dataset.
 * @author Joseph Sackett
 */
public class Flight {

	protected int year;
	protected int month;
	protected int dayOfWeek;
	protected String dateStr;
	protected String carrier;
	protected int flightNum;
	protected int origAirportId;
	protected int origAirportSeqId;
	protected int origCityMarketId;
	protected String orig;
	protected String origCityNm;
	protected String origStateAbr;
	protected int origStateFips;
	protected String origStateNm;
	protected int origWac;
	protected int destAirportId;
	protected int destAirportSeqId;
	protected int destCityMarketId;
	protected String dest;
	protected String destCityNm;
	protected String destStateAbr;
	protected int destStateFips;
	protected String destStateNm;
	protected int destWac;
	protected int crsDepTime;
	protected int depTime;
	protected int crsArrTime;
	protected int arrTime;
	protected int arrDelay;
	protected int arrDelayMins;
	protected int arrDelay15;
	protected boolean cancelled;
	protected int crsElapsedTime;
	protected int actualElapsedTime;
	protected int timeZone;
	protected int price;

	/** Constructor takes all fields. */
	public Flight(int year, int month, int dayOfWeek, String dateStr, String carrier, int flightNum, int origAirportId, 
			int origAirportSeqId, int origCityMarketId, String orig, String origCityNm, String origStateAbr, int origStateFips,
			String origStateNm, int origWac, int destAirportId, int destAirportSeqId, int destCityMarketId, String dest,
			String destCityNm, String destStateAbr, int destStateFips, String destStateNm, int destWac, int crsDepTime,
			int depTime, int crsArrTime, int arrTime, int arrDelay, int arrDelayMins, int arrDelay15, boolean cancelled,
			int crsElapsedTime, int actualElapsedTime, int timeZone, int price) {
		this.year = year;
		this.month = month;
		this.dayOfWeek = dayOfWeek;
		this.dateStr = dateStr;
		this.carrier = carrier;
		this.flightNum = flightNum;
		this.origAirportId = origAirportId;
		this.origAirportSeqId = origAirportSeqId;
		this.origCityMarketId = origCityMarketId;
		this.orig = orig;
		this.origCityNm = origCityNm;
		this.origStateAbr = origStateAbr;
		this.origStateFips = origStateFips;
		this.origStateNm = origStateNm;
		this.origWac = origWac;
		this.destAirportId = destAirportId;
		this.destAirportSeqId = destAirportSeqId;
		this.destCityMarketId = destCityMarketId;
		this.dest = dest;
		this.destCityNm = destCityNm;
		this.destStateAbr = destStateAbr;
		this.destStateFips = destStateFips;
		this.destStateNm = destStateNm;
		this.destWac = destWac;
		this.crsDepTime = crsDepTime;
		this.crsArrTime = crsArrTime;
		this.cancelled = cancelled;
		this.crsElapsedTime = crsElapsedTime;
		this.price = price;
		this.timeZone = timeZone;
		this.depTime = depTime;
		this.arrTime = arrTime;
		this.arrDelay = arrDelay;
		this.arrDelayMins = arrDelayMins;
		this.arrDelay15 = arrDelay15;
		this.actualElapsedTime = actualElapsedTime;
	}

	/** Default constructor. */
	public Flight () {
		// default.
	}

	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public Date getDate() {
		try {
			return dateFormat.parse(dateStr);
		} catch (Exception e) {
			return null;
		}
	}

	public int getYear() {
		return year;
	}

	public int getMonth() {
		return month;
	}

	public int getDayOfWeek() {
		return dayOfWeek;
	}

	public String getDateStr() {
		return dateStr;
	}

	public String getCarrier() {
		return carrier;
	}

	public int getFlightNum() {
		return flightNum;
	}

	public int getOrigAirportId() {
		return origAirportId;
	}

	public int getOrigAirportSeqId() {
		return origAirportSeqId;
	}

	public int getOrigCityMarketId() {
		return origCityMarketId;
	}

	public String getOrig() {
		return orig;
	}

	public String getOrigCityNm() {
		return origCityNm;
	}

	public String getOrigStateAbr() {
		return origStateAbr;
	}

	public int getOrigStateFips() {
		return origStateFips;
	}

	public String getOrigStateNm() {
		return origStateNm;
	}

	public int getOrigWac() {
		return origWac;
	}

	public int getDestAirportId() {
		return destAirportId;
	}

	public int getDestAirportSeqId() {
		return destAirportSeqId;
	}

	public int getDestCityMarketId() {
		return destCityMarketId;
	}

	public String getDest() {
		return dest;
	}

	public String getDestCityNm() {
		return destCityNm;
	}

	public String getDestStateAbr() {
		return destStateAbr;
	}

	public int getDestStateFips() {
		return destStateFips;
	}

	public String getDestStateNm() {
		return destStateNm;
	}

	public int getDestWac() {
		return destWac;
	}

	public int getCrsDepTime() {
		return crsDepTime;
	}

	public int getDepTime() {
		return depTime;
	}

	public int getCrsArrTime() {
		return crsArrTime;
	}

	public int getArrTime() {
		return arrTime;
	}

	public int getArrDelay() {
		return arrDelay;
	}

	public int getArrDelayMins() {
		return arrDelayMins;
	}

	public int getArrDelay15() {
		return arrDelay15;
	}

	public boolean isCancelled() {
		return cancelled;
	}

	public int getCrsElapsedTime() {
		return crsElapsedTime;
	}

	public int getActualElapsedTime() {
		return actualElapsedTime;
	}

	public int getPrice() {
		return price;
	}
	
	public int getTimeZone() {
		return timeZone;
	}

	@Override
	public String toString() {
		return "" + year + "," + month + "," + dayOfWeek + "," + dateStr + "," + carrier + "," + flightNum + "," + origAirportId
				+ "," + origAirportSeqId + "," + origCityMarketId + "," + orig + "," + origCityNm + "," + origStateAbr + "," + origStateFips
				+ "," + origStateNm + "," + origWac + "," + destAirportId + "," + destAirportSeqId + "," + destCityMarketId + "," + dest
				+ "," + destCityNm + "," + destStateAbr + "," + destStateFips + "," + destStateNm + "," + destWac + "," + crsDepTime + ","
				+ depTime + "," + crsArrTime + "," + arrTime + "," + arrDelay + "," + arrDelayMins + "," + arrDelay15 + "," + (cancelled ? "1" : "0")
				+ "," + crsElapsedTime + "," + actualElapsedTime + "," + timeZone + "," + price;
	}

}
