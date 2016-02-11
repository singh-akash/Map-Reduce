package neu.mr;

import java.util.Collections;
import java.util.Vector;

/**
 * @author Akash Singh
 * @maintainer Surekha Jadhwani
 * 
 *  CarrierRecord data structure to process the carrier records
 *
 */
public class CarrierRecord // implements Comparable
{
	private String carrier;
	private Vector<Vector<Double>> ticket_prices = new Vector<Vector<Double>>();
	private double[] mean_ticket_price = new double[12];
	private double[] median_ticket_price = new double[12];
	private boolean is_carrier_active = false;

	/**
	 * Constructor which inputs the carrier
	 * @param carrier
	 */
	public CarrierRecord(String carrier)
	{
		this.carrier = carrier;
		for (int i = 0; i < 12; i++) 
		{
			ticket_prices.add(i, new Vector<Double>());
			mean_ticket_price[i] = 0;
			median_ticket_price[i] = 0;
		}
	}

	/**
	 * Adds a ticket price value in the existing collection of ticket prices
	 * @param price
	 * @param month
	 */
	public void addTicketPrice(double price, int month) 
	{
		this.ticket_prices.get(month).add(price);
	}

	/**
	 * Computes the mean of ticket prices and stores it in the mean field
	 */
	public void computeMean() 
	{
		for (int j = 0; j < 12; j++) 
		{
			if (this.ticket_prices.get(j).size() > 0) 
			{
				double sum = 0;
				for (double ticket_price : this.ticket_prices.get(j)) 
				{
					sum += ticket_price;
				}
				this.mean_ticket_price[j] = sum / this.ticket_prices.get(j).size();
			}
		}
	}

	/**
	 * Getter of mean ticket value
	 * @return Mean ticket price
	 */
	public double getMeanTicketPrice(int month) 
	{
		return this.mean_ticket_price[month];
	}

	/**
	 * Getter of carrier field
	 * @return The carrier field value of the carrier record
	 */
	public String getCarrier() 
	{
		return this.carrier;
	}

	/**
	 * Compute and returns median ticket price
	 * @return The median ticket price
	 */
	public double getMedianTicketPrice(int month) 
	{
		return this.median_ticket_price[month];
	}

	/**
	 * Compute and returns median ticket price
	 * @return The median ticket price
	 */
	public void computeMedian() 
	{
		for (int i = 0; i < 12; i++) 
		{
			int length = ticket_prices.get(i).size();
			if(length > 0)
			{
				Collections.sort(this.ticket_prices.get(i));
				this.median_ticket_price[i] = (length % 2 == 0) ? 
												(ticket_prices.get(i).get(length / 2) + ticket_prices.get(i).get((length / 2) - 1)) / 2 :
												ticket_prices.get(i).get(length / 2);
			}
		}
	}
	
	/**
	 * Compute and returns median ticket price
	 * @return The median ticket price
	 */
	public void computeFastMedian() 
	{
		for (int i = 0; i < 12; i++) 
		{
			int length = ticket_prices.get(i).size();
			if(length > 0)
			{
				this.median_ticket_price[i] = FlightUtils.getFastMedian(this.ticket_prices.get(i));
			}
		}
	}

	/**
	 * Returns whether carrier is active in the specified month and year. The month and year are specified as constants in this
	 * file.
	 * @return whether or not the carrier is active
	 */
	public boolean getIsCarrierActive() 
	{
		return is_carrier_active;
	}

	/**
	 * Updates the active carrier if year and month match the specified year and month
	 * @param year
	 * @param month
	 */
	public void updateActiveCarrier(String year) 
	{
		if (!is_carrier_active && FlightUtils.ACTIVE_YEAR.equals(year))
		{
			this.is_carrier_active = true;
		}
	}
	
	public static final String ACTIVE_YEAR = "2015";
	public static final String ACTIVE_MONTH = "1";
}