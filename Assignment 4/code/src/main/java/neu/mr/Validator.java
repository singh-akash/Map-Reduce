package neu.mr;

/**
 * @author Akash Singh
 * 
 * Basic Validators
 *
 */
public class Validator {

	/** Validates whether given string is a valid integer
	 * @param s
	 * @return true for valid integers
	 */
	public static boolean isValidInteger(String s)
	{
		try
		{
			Integer.parseInt(s);
		}
		catch (NumberFormatException e)
		{
			return false;
		}
		return true;
	}
	
	/** Validates whether given string is a valid Double
	 * @param s
	 * @return true for valid double values
	 */	
	public static boolean isValidDouble(String s)
	{
		try
		{
			Double.parseDouble(s);
		}
		catch (NumberFormatException e)
		{
			return false;
		}
		return true;
	}
	
	/** Evaluates whether given string is non null and non empty
	 * @param s
	 * @return true for non null and non empty strings
	 */
	public static boolean isNonEmptyString(String s)
	{
		return (s != null && s.trim().length() != 0);
	}
	
	/** Evaluates whether given string could be successfully converted to boolean or not
	 * @param s
	 * @return true for correct boolean values
	 */
	public static boolean isValidBooleanString(String s)
	{	
		try 
		{
			return (1 == (int) Double.parseDouble(s) || (1 == (int) Double.parseDouble(s)));
		}
		catch (NumberFormatException e)
		{
			return ("true".equals(s) || "false".equals(s));
		}
	}
}