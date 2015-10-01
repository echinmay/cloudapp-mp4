import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class
      Configuration con = HBaseConfiguration.create();

      // Instaniate HBaseAdmin class
      HBaseAdmin admin = new HBaseAdmin(con);
      
      // Instantiate table descriptor class
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("powers"));

      // Add column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);

      // Instantiating HTable class
      HTable hTable = new HTable(con, "powers");
     
      String[][] arrvals = new String [3][5];
      arrvals[0][0] = "row1";
      arrvals[0][1] = "superman";
      arrvals[0][2] = "strength";
      arrvals[0][3] = "clark";
      arrvals[0][4] = "100";

      arrvals[1][0] = "row2";
      arrvals[1][1] = "batman";
      arrvals[1][2] = "money";
      arrvals[1][3] = "bruce";
      arrvals[1][4] = "50";

      arrvals[2][0] = "row3";
      arrvals[2][1] = "wolverine";
      arrvals[2][2] = "healing";
      arrvals[2][3] = "logan";
      arrvals[2][4] = "75";
      // Repeat these steps as many times as necessary
      for (int i = 0; i < 3; i++){
	      // Instantiating Put class
              // Hint: Accepts a row name
          Put p = new Put(Bytes.toBytes(arrvals[i][0]));

      	  // Add values using add() method
          p.add(Bytes.toBytes("personal"),
          Bytes.toBytes("hero"), Bytes.toBytes(arrvals[i][1]));
          // Hints: Accepts column family name, qualifier/row name ,value
          p.add(Bytes.toBytes("personal"),
          Bytes.toBytes("power"), Bytes.toBytes(arrvals[i][2]));

          p.add(Bytes.toBytes("professional"),
          Bytes.toBytes("name"), Bytes.toBytes(arrvals[i][3]));

          p.add(Bytes.toBytes("professional"),
          Bytes.toBytes("xp"), Bytes.toBytes(arrvals[i][4]));
          hTable.put(p);
       }
      // Save the table
      //hTable.put(p);
	
      // Close table
      hTable.close();

      HTable hTable2 = new HTable(con, "powers");
      // Instantiate the Scan class
      Scan scan = new Scan();
     
      // Scan the required columns
      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

      // Get the scan result
      ResultScanner scanner = hTable2.getScanner(scan);

      // Read values from scan result
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        System.out.println(result);
      }

      // Print scan result
 
      // Close the scanner
      scanner.close();
   
      // Htable closer
      hTable2.close();
   }
}

