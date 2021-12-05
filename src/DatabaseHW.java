import java.sql.*;
import java.io.*;
import com.opencsv.*;

public class DatabaseHW {

	public static void main(String[] args) {
		String url = "jdbc:mysql://localhost:3306/TomasHW";
		String user = "root";
		String pw = "";
		String filePath = "/home/christian/Documents/Tomas/sql_hw/indiegogo2.csv";
		String tableName = "indiegogo2";
		String keywordColumn = "tags";
		
		try {
			Connection con = DriverManager.getConnection(url, user, pw);
			//createTargetTable(con, tableName);
			//populateTable(con, filePath, tableName);
			countKeywords(con, 2021, 2021, "yoga", keywordColumn, tableName);
			keywordRange(con, 2021, 7, 2021, 7, "hair", keywordColumn, tableName);
			con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
	}
	
	public static void createTargetTable(Connection con, String tableName)
	{
		String firstPart = "CREATE TABLE ";
		
		String mainPart = "(\n" + 
				"	bullet_point VARCHAR(255) NULL,\n" + 
				"	category VARCHAR(255) NULL,\n" + 
				"	category_url VARCHAR(255) NULL,\n" + 
				"	clickthrough_url VARCHAR(255) NULL,\n" + 
				"	close_date DATETIME NULL,\n" + 
				"	currency CHAR(3) NULL,\n" + 
				"	funds_raised_amount INT NULL,\n" + 
				"	funds_raised_percentage FLOAT(10) NULL,\n" + 
				"	image_url VARCHAR(255) NULL,\n" + 
				"	is_indemand BOOL NULL,\n" + 
				"	is_pre_launch BOOL NULL,\n" + 
				"	offered_by VARCHAR(255) NULL,\n" + 
				"	open_date DATETIME NULL,\n" + 
				"	perk_goal_percentage INT NULL,\n" + 
				"	perks_claimed INT NULL,\n" + 
				"	price_offered FLOAT(10) NULL,\n" + 
				"	price_retail FLOAT(10) NULL, \n" + 
				"	product_stage VARCHAR(255) NULL,\n" + 
				"	project_id INT NULL,\n" + 
				"	project_type VARCHAR(255) NULL,\n" + 
				"	source_url VARCHAR(255) NULL,\n" + 
				"	tagline VARCHAR(255) NULL,\n" + 
				"	tags VARCHAR(255) NULL,\n" + 
				"	title VARCHAR(255) NULL\n" + 
				")";
		String createStatement = firstPart + tableName + " " + mainPart;
		System.out.println(createStatement);
		
		try {
			Statement stmt = con.createStatement();
			stmt.execute(createStatement);
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
		
	}
	
	public static void populateTable(Connection con, String filePath, String tableName) 
	{
		String[] nextLine;
		int lineCount = 0;
		try {
			FileReader fr = new FileReader(filePath);
			System.out.println(fr);
			CSVReader reader= new CSVReader(fr);
			while ((nextLine = reader.readNext()) != null)
			{
				if (lineCount == 0) {
					lineCount++;
					continue;
				}
				String insertStatement = String.format("INSERT INTO %s VALUES ", tableName);
				
				for (int i = 0; i < nextLine.length; i++)
				{
					String value = (nextLine[i].equals("null") ? "NULL" : nextLine[i]); 
					if (value.equals("true")) value = "1";
					if (value.equals("false")) value = "0";
					if (!value.equals("NULL"))
					{
						if (value.indexOf('"') != -1)
						{
							value = value.replace('"', '\'');
						}
						value = '"' + value + '"';
					}
					
					
					if (i == 0)
					{
						insertStatement +=  "(" + value + ",";
					}
					else if (i == nextLine.length - 1) 
					{
						insertStatement += value + ")";
					}
					else
					{
						insertStatement += value + ",";
					}
				}
				//System.out.println(insertStatement);
				Statement stmt = con.createStatement();
				stmt.executeUpdate(insertStatement);
				lineCount++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	public static int countKeywords(Connection con, int minYear, int maxYear, String keyword, String keywordColumn, String tableName) throws SQLException, NumberFormatException
	{
		String startDate = minYear + "0101";
		String endDate = maxYear + "1231";
		String selectQuery = "SELECT COUNT(*) FROM " + tableName
							+ " WHERE (open_date BETWEEN " + startDate + " AND " + endDate 
							+ ") AND " + keywordColumn + " LIKE '%"+ keyword + "%'";
		//System.out.println(selectQuery);
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(selectQuery);
		rs.next();
		return Integer.parseInt(rs.getString(1));
		
		
	}
	
	public static ResultSet keywordRange(Connection con, int minYear, int minYearMonth, int maxYear, int maxYearMonth ,String keyword, String keywordColumn, String tableName) throws SQLException
	{
		String startDateMonth = minYearMonth < 10 ? ("0" + Integer.toString(minYearMonth)) : Integer.toString(minYearMonth);
		String startDate = minYear + startDateMonth + "01";
		maxYearMonth++; // increment maxYearmonth because we will be searching until 1st day of the month after that
		String endDateMonth = maxYearMonth < 10 ? ("0" + Integer.toString(maxYearMonth)) : Integer.toString(maxYearMonth);
		String endDate = maxYear + endDateMonth + "01";
		String selectQuery = "SELECT * FROM " + tableName
							+ " WHERE (open_date BETWEEN " + startDate + " AND " + endDate 
							+ ") AND " + keywordColumn + " LIKE '%"+ keyword + "%'";
		//System.out.println(selectQuery);
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(selectQuery);
		while (rs.next()) {
			System.out.println("row:" + rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4));
		}
		return rs;
		
	}
}
	
