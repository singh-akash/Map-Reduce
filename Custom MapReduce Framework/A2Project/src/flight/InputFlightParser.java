package flight;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This parses a line of CSV flight data into a Flight POJO.
 * @author Joseph Sackett, Surekha Jadhwani
 *
 */
public class InputFlightParser {
	private int year;
	private int month;
	private int dayOfWeek;
	private String dateStr;
	private String carrier;
	private int flightNum;
	private int origAirportId;
	private int origAirportSeqId;
	private int origCityMarketId;
	private String orig;
	private String origCityNm;
	private String origStateAbr;
	private int origStateFips;
	private String origStateNm;
	private int origWac;
	private int destAirportId;
	private int destAirportSeqId;
	private int destCityMarketId;
	private String dest;
	private String destCityNm;
	private String destStateAbr;
	private int destStateFips;
	private String destStateNm;
	private int destWac;
	private int crsDepTime;
	private int depTime;
	private int crsArrTime;
	private int arrTime;
	private int arrDelay;
	private int arrDelayMins;
	private int arrDelay15;
	private boolean cancelled;
	private int crsElapsedTime;
	private int actualElapsedTime;
	private int timeZone;
	private int price;

	/** Validate a line of data, build and return a flight. */
	public Flight buildValidFlight(String line) {
		try {
			parseFields(line);
			
    		if (!isValid()) {
    			return null;
    		}
    		
	        return new Flight(year, month, dayOfWeek, dateStr, carrier, flightNum, origAirportId, origAirportSeqId,
	    			origCityMarketId, orig, origCityNm, origStateAbr, origStateFips,
	    			origStateNm, origWac, destAirportId, destAirportSeqId, destCityMarketId, dest,
	    			destCityNm, destStateAbr, destStateFips, destStateNm, destWac, crsDepTime,
	    			depTime, crsArrTime, arrTime, arrDelay, arrDelayMins, arrDelay15, cancelled,
	    			crsElapsedTime, actualElapsedTime, timeZone, price);
		} catch (Exception e) {
			return null;
		}
	}

	/** Parse method knows what data to expect at each field number. */
	private void parseFields(String line) {
		String depTimeS = "0", arrTimeS = "0", arrDelayS = "0", arrDelayMinsS = "0", arrDelay15S = "0", actualElapsedTimeS = "0";
		List<String> tokens = split(line);
     	int fieldNum = 0;
    	for (String token : tokens) {
			switch (fieldNum) {
			case 0: year = parseInt(token); break;
			case 2: month = parseInt(token); break;
			case 4: dayOfWeek = parseInt(token); break;
			case 5: dateStr = token; break;
			case 6: carrier = token; break;
			case 10: flightNum = parseInt(token); break;
			case 11: origAirportId = parseInt(token); break;
			case 12: origAirportSeqId = parseInt(token); break;
			case 13: origCityMarketId = parseInt(token); break;
			case 14: orig = token; break;
			case 15: origCityNm = token; break;
			case 16: origStateAbr = token; break;
			case 17: origStateFips = parseInt(token); break;
			case 18: origStateNm = token; break;
			case 19: origWac = parseInt(token); break;
			case 20: destAirportId = parseInt(token); break;
			case 21: destAirportSeqId = parseInt(token); break;
			case 22: destCityMarketId = parseInt(token); break;
			case 23: dest = token; break;
			case 24: destCityNm = token; break;
			case 25: destStateAbr = token; break;
			case 26: destStateFips = parseInt(token); break;
			case 27: destStateNm = token; break;
			case 28: destWac = parseInt(token); break;
			case 29: crsDepTime = toTotalMins(parseInt(token)); break;
			case 30: depTimeS = token; break;
			case 40: crsArrTime = toTotalMins(parseInt(token)); break;
			case 41: arrTimeS = token; break;
			case 42: arrDelayS = token; break;
			case 43: arrDelayMinsS = token; break;
			case 44: arrDelay15S = token; break;
			case 47: cancelled = intToBoolean(token); break;
			case 50: crsElapsedTime = parseInt(token); break;
			case 51: actualElapsedTimeS = token; break;
			case 109: price = parseCurrency(token); break;
			default:
			}
			fieldNum++;
		}
    	
		timeZone = this.crsArrTime - this.crsDepTime - this.crsElapsedTime;
		if (!cancelled) {
			depTime = toTotalMins(parseInt(depTimeS));
			arrTime = toTotalMins(parseInt(arrTimeS));
			arrDelay = floatToInt(arrDelayS);
			arrDelayMins = floatToInt(arrDelayMinsS);
			arrDelay15 = floatToInt(arrDelay15S);
			actualElapsedTime = parseInt(actualElapsedTimeS);
		}
		else {
			depTime = 0;
			arrTime = 0;
			arrDelay = 0;
			arrDelayMins = 0;
			arrDelay15 = 0;
			actualElapsedTime = 0;
		}
	}

	private static void printError(int fieldNum, List<String> tokens) {
		System.out.print("" + fieldNum);
		for (String token : tokens) {
			System.out.print(token + '\t');
		}
	}
	
	/** Lexer for line of CSV flight data. */
	public List<String> split(String line) {
		List<String> tokens = new ArrayList<String>();
		String token = "";
		char lookForChar = ',';
		boolean keep = true;
		for (int ix=0; ix<line.length(); ix++) {
			char currChar = line.charAt(ix);
			if (currChar == lookForChar) {
				if (keep) {
					tokens.add(token);
					token = "";
					if (lookForChar == '"') {
						lookForChar = ',';
						keep = false;
					}
				}
				else {
					keep = true;
				}
			}
			else {
				if (currChar == '"') {
					lookForChar = '"';
					keep = true;
				}
				else {
					token += currChar;
				}
			}
		}
		tokens.add(token);
		return tokens;
	}

	// Parsing utility functions below here.
	private static int parseInt(String str) throws NumberFormatException {
		return Integer.parseInt(str);
	} 
	
	private static boolean intToBoolean(String str) throws NumberFormatException {
		return parseInt(str) != 0;
	} 
	
	private static int floatToInt(String str) throws NumberFormatException {
		return (int)Float.parseFloat(str);
	} 
	
	/** Contverts time HHMM field to total minutes. */
	private static int toTotalMins(int time) {
		return (time / 100) * 60 + time % 100; 
	}

	private static int parseCurrency(String str) throws NumberFormatException {
		int dp = str.indexOf('.');
		if (dp <= 0 || dp == str.length()) {
			throw new NumberFormatException("Incorrect currency: " + str);
		}
		String dollars = str.substring(0, dp);
		String cents = str.substring(dp + 1);
		if (cents.length() == 1) {
			cents += '0';
		}
		return parseInt(dollars) * 100 + parseInt(cents);
	} 
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("y-M-d");
	
	private static Date parseDate(String str) throws ParseException {
		return sdf.parse(str);
	}
	
	/** Checks whether the data represent a valid flight. */
	private boolean isValid() {
		if (crsArrTime == 0 || crsDepTime == 0) {
			System.out.println("crsArrTime == 0 || crsDepTime == 0");
			return false;
		}
		if (timeZone % 60 != 0) {
			System.out.println("timeZone % 60 != 0");
			return false;
		}
		if (origAirportId <= 0 || origAirportSeqId <= 0 || origCityMarketId <= 0 || origStateFips <= 0 || origWac <= 0) {
			System.out.println("(origAirportId <= 0 ||");
			return false;
		}
		if (destAirportId <= 0 || destAirportSeqId <= 0 || destCityMarketId <= 0 || destStateFips <= 0 || destWac <= 0) {
			System.out.println("destAirportId <= 0 ||");
			return false;
		}
		if (orig.length() == 0 || origCityNm.length() == 0 || origStateAbr.length() == 0 || origStateNm.length() == 0) {
			System.out.println("orig.length() == 0 ||");
			return false;
		}
		if (dest.length() == 0 || destCityNm.length() == 0 || destStateAbr.length() == 0 || destStateNm.length() == 0) {
			System.out.println("dest.length() == 0 ||");
			return false;
		}
		if (!cancelled) {
			if ((arrTime - depTime - actualElapsedTime - timeZone) % 1440 != 0) {
				System.out.println("% 1440");
				return false;
			}
			if (arrDelay > 0) {
				if (arrDelay != arrDelayMins) {
					System.out.println("arrDelay > 0");
					return false;
				}
			}
			else if (arrDelay < 0) {
				if (arrDelayMins != 0) {
					System.out.println("arrDelay < 0");
					return false;
				}
			}
			if (arrDelayMins >= 15) {
				if (arrDelay15 == 0) {
					System.out.println("arrDelayMins >= 15");
					return false;
				}
			}
		}

		return true;
	}

}
