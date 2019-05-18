package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Ex {

    private static final String TABLE_NAME = "user";
    private static final String PERSONAL = "per";
    private static final String PERSONAL_NAME = "n";
    private static final String PERSONAL_CITY = "c";
    private static final String PROFESSIONAL = "pro";
    private static final String PROFESSIONAL_DESIGNATION = "d";
    private static final String PROFESSIONAL_SALARY = "s";

    private static String[] cEmpId = {"1", "2", "3"};
    private static String[] cNames = {"John", "Marry", "Bob"};
    private static String[] cCities = {"Boston", "New York", "Fremont"};
    private static String[] cDesignation = {"Manager", "Sr.Engineer", "Jr.Engineer"};
    private static String[] cSalaries = {"150000", "130000", "90000"};

    public static void exec1(Table userTable) throws IOException {
        for (int i = 0; i < 3; i++) {
            Put putRow = new Put(Bytes.toBytes(cEmpId[i]));
            putRow.addColumn(PERSONAL.getBytes(), PERSONAL_NAME.getBytes(), Bytes.toBytes(cNames[i]));
            putRow.addColumn(PERSONAL.getBytes(), PERSONAL_CITY.getBytes(), Bytes.toBytes(cCities[i]));
            putRow.addColumn(PROFESSIONAL.getBytes(), PROFESSIONAL_DESIGNATION.getBytes(), Bytes.toBytes(cDesignation[i]));
            putRow.addColumn(PROFESSIONAL.getBytes(), PROFESSIONAL_SALARY.getBytes(), Bytes.toBytes(cSalaries[i]));
            userTable.put(putRow);
        }
    }

    public static void exec2(Table userTable) throws IOException {
        Get getRow = new Get("3".getBytes());
        Result result = userTable.get(getRow);
        double newSalary = (Double.parseDouble(Bytes.toString(result.getValue(PROFESSIONAL.getBytes(), PROFESSIONAL_SALARY.getBytes()))) * 1.05f);
        newSalary = Math.floor(newSalary * 100 + 0.5) / 100; // Avoid the dynamic points issue
        byte[] salary = String.valueOf(newSalary).getBytes();

        Put putRow = new Put(Bytes.toBytes("3"));
        putRow.addColumn(PROFESSIONAL.getBytes(), PROFESSIONAL_DESIGNATION.getBytes(), Bytes.toBytes("Sr.Engineer"));
        putRow.addColumn(PROFESSIONAL.getBytes(), PROFESSIONAL_SALARY.getBytes(), salary);
        userTable.put(putRow);
    }

    public static void main(String... args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("zookeeper.znode.parent", "/hbase");

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDesc.addFamily(new HColumnDescriptor(PERSONAL).setCompressionType(Algorithm.NONE));
            tableDesc.addFamily(new HColumnDescriptor(PROFESSIONAL));

            System.out.print("Creating table.... ");

            if (admin.tableExists(tableDesc.getTableName())) {
                admin.disableTable(tableDesc.getTableName());
                admin.deleteTable(tableDesc.getTableName());
            }
            admin.createTable(tableDesc);
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            exec1(table);
            exec2(table);

            System.out.println(" Done!");
        }
    }
}
