package neu.mr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

/**
 * @author Akash Singh
 * 
 *  CarrierRecord data structure to process the carrier records
 *
 */
public class CarrierRecord // implements Comparable
{
	private String carrier;
	private ArrayList<Double> ticket_prices = new ArrayList<Double>();
	private double mean_ticket_price;
	long ticket_count;
	/**
	 * Constructor which inputs the carrier
	 * @param carrier
	 */
	public CarrierRecord(String carrier)
	{
		this.carrier = carrier;
	}

	/**
	 * Adds a ticket price value in the existing collection of ticket prices
	 * @param price
	 */
	public void addTicketPrice(double price) 
	{
		this.ticket_prices.add(price);
	}

	/**
	 * Computes the mean of ticket prices and stores it in the mean field
	 */
	public void computeMean() 
	{
		double sum = 0;
		long count = 0;
		
		for (Double ticket_price : this.ticket_prices)
		{
			sum += ticket_price;
			count++;
		}
		
		if (ticket_count != 0)
			this.mean_ticket_price = sum / this.ticket_count;
		else
			this.mean_ticket_price = sum / count;
	}

	/**
	 * Getter of mean ticket value
	 * @return Mean ticket price
	 */
	public double getMeanTicketPrice() 
	{
		return this.mean_ticket_price;
	}

	/**
	 * Getter of carrier field
	 * @return The carrier field value of the carrier record
	 */
	public String getCarrier() 
	{
		return this.carrier;
	}
	
	/** Updates the ticket count
	 * @param count
	 */
	public void updateTicketCount(long count)
	{
		this.ticket_count += count;
	}
}