package neu.mr;

public class A4MinCarrier {
	
	private String carrier;
	private double min_slope_intercept_val = 0;
	
	public A4MinCarrier(String carrier, double min_slope_intercept_val)
	{
		this.carrier = carrier;
		this.min_slope_intercept_val = min_slope_intercept_val;
	}
	
	// Getters and Setters
	public double getMin_slope_intercept_val() {
		return min_slope_intercept_val;
	}
	
	public void setMin_slope_intercept_val(double min_slope_intercept_val) {
		this.min_slope_intercept_val = min_slope_intercept_val;
	}
	
	public String getCarrier() {
		return carrier;
	}
	
	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}
}
