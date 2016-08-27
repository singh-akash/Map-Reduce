package neu.mr;

public class WeekUtils {

	public static int getWeek(int m, int d, int y)
	{
		int curr_m = 1;
		int total_days = 0;
		while (curr_m < m)
		{
			total_days += getDays(curr_m, y);
			curr_m++;
		}
		
		total_days += d;
		
		return total_days/7;
	}
	
	public static int getDays(int m, int y)
	{
		if (m == 2)
		{
			if (y % 100 == 0 && y % 400 != 0)
			{
				return 28;
			}
			else if (y % 4 == 0)
			{
				return 29;
			}
			else 
			{
				return 28;
			}
		}
		else if (m == 1 ||
				m == 3 ||
				m == 5 ||
				m == 7 ||
				m == 8 ||
				m == 10 ||
				m == 12)
		{
			return 31;
		}
		else
		{
			return 30;
		}
	}
}