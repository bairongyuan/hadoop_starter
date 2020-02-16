package kkb.hbase_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTest {
    private Connection connection;
    private final String TABLE_NAME = "myuser";
    private Table table;

    @Test
    public void createTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        TableName myuser = TableName.valueOf("myuser");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(myuser);
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }

    @BeforeTest
    public void initTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    @AfterTest
    public void close() throws IOException {
        table.close();
        connection.close();
    }

    @Test
    public void addData() throws IOException {
        Put put = new Put("rk0001".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), "lisi".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(), "address".getBytes(), "地球人".getBytes());
        table.put(put);
    }

    @Test
    public void batchInsert() throws IOException {
        Put put = new Put("rk0002".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), "曹操".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes(1));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloworld"));

        Put put1 = new Put("rk0003".getBytes());
        put1.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put1.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
        put1.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
        put1.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes(1));
        put1.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("幽州涿郡涿县"));
        put1.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
        put1.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("talk is cheap, show me the code"));

        Put put2 = new Put("rk0004".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes(1));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("下邳"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("12888888888"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("what are you 弄啥嘞!"));

        Put put3 = new Put("rk0005".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("诸葛亮"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("四川隆中"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("14888888888"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("出师表你背了嘛"));

        Put put4 = new Put("rk0006".getBytes());
        put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("司马懿"));
        put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(27));
        put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("哪里人有待考究"));
        put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15888888888"));
        put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("跟诸葛亮死掐"));


        Put put5 = new Put("rk0007".getBytes());
        put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("xiaobubu—吕布"));
        put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("内蒙人"));
        put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15788888888"));
        put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("貂蝉去哪了"));

        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        puts.add(put5);
        table.put(puts);
    }

    @Test
    public void getFData() throws IOException {
        Get get = new Get(Bytes.toBytes("rk0003"));

        get.addFamily("f1".getBytes());
        get.addColumn("f2".getBytes(), "phone".getBytes());
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        for (Cell cell : cells) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] rowKey = CellUtil.cloneRow(cell);
            byte[] cellValue = CellUtil.cloneValue(cell);
            if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                System.out.println(Bytes.toString(family));
                System.out.println(Bytes.toString(qualifier));
                System.out.println(Bytes.toString(rowKey));
                System.out.println(Bytes.toInt(cellValue));
            } else {
                System.out.println(Bytes.toString(family));
                System.out.println(Bytes.toString(qualifier));
                System.out.println(Bytes.toString(rowKey));
                System.out.println(Bytes.toString(cellValue));
            }
        }
    }

    @Test
    public void scanData() throws IOException {
        Scan scan = new Scan();

        scan.addFamily("f1".getBytes());
        scan.addColumn("f2".getBytes(), "phone".getBytes());
        scan.setStartRow("rk0003".getBytes());
        scan.setStopRow("rk0007".getBytes());

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] rowKey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                    System.out.println("数据的rowKey为：" + Bytes.toString(rowKey)
                            + " 数据的列族为：" + Bytes.toString(family)
                            + " 数据的列名为：" + Bytes.toString(qualifier)
                            + " 数据的值为：" + Bytes.toInt(value));
                } else {
                    System.out.println("数据的rowKey为：" + Bytes.toString(rowKey)
                            + " 数据的列族为：" + Bytes.toString(family)
                            + " 数据的列名为：" + Bytes.toString(qualifier)
                            + " 数据的值为：" + Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void rowFilter () throws IOException {
        Scan scan = new Scan();

        BinaryComparator binaryComparator = new BinaryComparator("rk0003".getBytes());

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] rowKey = CellUtil.cloneRow(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                    System.out.println("数据的rowKey为：" + Bytes.toString(rowKey)
                            + " 数据的列族为：" + Bytes.toString(family)
                            + " 数据的列名为：" + Bytes.toString(qualifier)
                            + " 数据的值为int：" + Bytes.toInt(value));
//                            + " 数据的值为int：" + Bytes.toString(value));
                } else {
                    System.out.println("数据的rowKey为：" + Bytes.toString(rowKey)
                            + " 数据的列族为：" + Bytes.toString(family)
                            + " 数据的列名为：" + Bytes.toString(qualifier)
                            + " 数据的值为：" + Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void familyFilter () throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("f2");
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells){
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] rowKey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                    System.out.println("数据的rowKey为：" + Bytes.toString(rowKey)
                            + " 数据的列族为：" + Bytes.toString(family)
                            + " 数据的列名为：" + Bytes.toString(qualifier)
                            + " 数据的值为int：" + Bytes.toInt(value));
//                            + " 数据的值为int：" + Bytes.toString(value));
                } else {
                    System.out.println("数据的rowKey为：" + Bytes.toString(rowKey)
                            + " 数据的列族为：" + Bytes.toString(family)
                            + " 数据的列名为：" + Bytes.toString(qualifier)
                            + " 数据的值为：" + Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void qualifierFilter () throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("name");
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }
    private void printlReult(ResultScanner scanner) {
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if("age".equals(Bytes.toString(qualifier_name))  || "id".equals(Bytes.toString(qualifier_name))){
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)   +"======数据的列族为" +  Bytes.toString(family_name)+"======数据的列名为" +  Bytes.toString(qualifier_name) + "==========数据的值为" +Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void contains8 () throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("8");
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    @Test
    public void prefixFilter () throws IOException {
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    @Test
    public void singleColumnValueFilter () throws IOException {
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }
}
