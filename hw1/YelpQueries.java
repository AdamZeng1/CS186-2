import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class YelpQueries
{
  public static void main(String[] args) throws ClassNotFoundException
  {
    // load the sqlite-JDBC driver using the current class loader
    Class.forName("org.sqlite.JDBC");

    String dbLocation = "yelp_dataset.db"; 

    Connection connection = null;
    try
    {
      // create a database connection
      connection = DriverManager.getConnection("jdbc:sqlite:" + dbLocation);

      Statement statement = connection.createStatement();

      // Question 0
      statement.execute("DROP VIEW IF EXISTS q0"); // Clean out views
      String q0 = "CREATE VIEW q0 AS "
                   + "SELECT count(*) FROM reviews";
      statement.execute(q0);

      // Question 1
      statement.execute("DROP VIEW IF EXISTS q1");
      String q1 = "CREATE VIEW q1 AS " 
                  + "SELECT AVG(U.review_count) FROM users AS U WHERE U.review_count < 10";
      statement.execute(q1);

      // Question 2
      statement.execute("DROP VIEW IF EXISTS q2");
      String q2 = "CREATE VIEW q2 AS "
                  + "SELECT U.name FROM users AS U WHERE U.review_count > 50 AND U.yelping_since > '2014-11'";
      statement.execute(q2);

      // Question 3
      statement.execute("DROP VIEW IF EXISTS q3");
      String q3 = "CREATE VIEW q3 AS "
                  + "SELECT B.name, B.stars FROM businesses AS B WHERE B.city = 'Pittsburgh' AND B.stars > 3";
      statement.execute(q3);

      // Question 4
      statement.execute("DROP VIEW IF EXISTS q4");
      String q4 = "CREATE VIEW q4 AS "
                  + "SELECT B.name FROM businesses B WHERE B.review_count >= 500 AND B.city = 'Las Vegas' ORDER BY B.stars ASC LIMIT 1";
      statement.execute(q4);

      // Question 5
      statement.execute("DROP VIEW IF EXISTS q5");
      String q5 = "CREATE VIEW q5 AS "
                  + " SELECT B.name FROM businesses B, checkins C WHERE C.business_id = B.business_id AND C.day = 0 ORDER BY C.num_checkins DESC, B.name ASC LIMIT 5";
      statement.execute(q5);

      // Question 6
      statement.execute("DROP VIEW IF EXISTS q6");
      String q6 = "CREATE VIEW q6 AS "
                  + "SELECT day FROM checkins GROUP BY day ORDER BY SUM(num_checkins) DESC LIMIT 1";
      statement.execute(q6);

      // Question 7
      statement.execute("DROP VIEW IF EXISTS q7");
      String q7 = "CREATE VIEW q7 AS "
                  + "SELECT B.name FROM businesses AS B, reviews AS R, users AS U WHERE B.business_id = R.business_id AND R.user_id = U.user_id AND U.review_count = (SELECT MAX(review_count) FROM users) ORDER BY B.name ASC";
      statement.execute(q7);

      // Question 8
      statement.execute("DROP VIEW IF EXISTS q8");
      String q8 = "CREATE VIEW q8 AS "
                  + "SELECT AVG(stars) FROM (SELECT * FROM businesses WHERE city = 'Edinburgh' ORDER BY review_count DESC LIMIT (SELECT COUNT(*) FROM businesses WHERE city = 'Edinburgh')/10)";
      statement.execute(q8);

      // Question 9
      statement.execute("DROP VIEW IF EXISTS q9");
      String q9 = "CREATE VIEW q9 AS "
                  + "SELECT name FROM users WHERE name LIKE '%..%' ORDER BY name ASC";
      statement.execute(q9);

      // Question 10
      statement.execute("DROP VIEW IF EXISTS q10");
      String q10 = "CREATE VIEW q10 AS "
                  + "SELECT city FROM businesses B, reviews R, users U WHERE U.name LIKE '%..%' AND R.user_id = U.user_id AND B.business_id = R.business_id GROUP BY city ORDER BY COUNT(city) DESC LIMIT 1"; // Replace this line
      statement.execute(q10);

      connection.close();

    }
    catch(SQLException e)
    {
      // if the error message is "out of memory", 
      // it probably means no database file is found
      System.err.println(e.getMessage());
    }
    finally
    {
      try
      {
        if(connection != null)
          connection.close();
      }
      catch(SQLException e)
      {
        // connection close failed.
        System.err.println(e);
      }
    }
  }
}
