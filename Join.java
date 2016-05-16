/*
Orders ( OrderKey, CustKey, OrderStatus, TotalPrice, OrderDate, OrderPriority, Clerk, ShipPriority, Comment )
You need to implement the following SQL query in the Map-Reduce Framework:

SELECT CustKey, SUM(TotalPrice) FROM orders GROUP BY CustKey
*/
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Orders implements WritableComparable<Orders> {
    public int orderKey;
    public int custKey;
    public String orderStatus;
    public double totalPrice;
    public String orderDate;
    public String orderPriority;
    public String clerk;
    public int shipPriority;
    public String comment;
    
    Orders () {}

    Orders ( int orderkey, int custkey, String orderstatus, double totalprice, String orderdate, String orderpriority, String clerk, int shippriority, String comment ) {
        orderKey = orderkey;
        custKey = custkey;
        orderStatus = orderstatus;
        totalPrice = totalprice;
        orderDate = orderdate;
        orderPriority = orderpriority;
        clerk = clerk;
        shipPriority = shippriority;
        comment = comment;
    }

    Orders ( int orderkey, int custkey, double totalprice ) {
        orderKey = orderkey;
        custKey = custkey;
        totalPrice = totalprice;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(orderKey);
        out.writeInt(custKey);
        out.writeDouble(totalPrice);

    }

    public void readFields ( DataInput in ) throws IOException {
        orderKey = in.readInt();
        custKey = in.readInt();
        totalPrice = in.readDouble();
    }
    
    public int compareTo(Orders o) {
        if (custKey == o.custKey)
            return 1;
        else
            return 0;
    }
}
/*
class DblArrayWritable extends ArrayWritable 
    { 
        public DblArrayWritable() 
        { 
            super(DoubleWritable.class); 
        }
    }
*/
public class Join {

    

    public static class OrdersMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\\|");
            int orderKey = Integer.parseInt(s.next());
            int custKey = Integer.parseInt(s.next());//s.nextInt();
            String orderStatus = s.next();

            Double totalPrice = s.nextDouble();

            //DoubleWritable[] values = new DoubleWritable[1];
            //values[0] = new DoubleWritable(totalPrice);
            
            Orders o = new Orders(orderKey,custKey,totalPrice);
            context.write(new IntWritable(o.custKey), new DoubleWritable(o.totalPrice));/*new DoubleWritable(o.totalPrice)*/
            s.close();
        }
    }

    public static class OrdersReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        static Vector<Orders> orders = new Vector<Orders>();

        public void reduce ( IntWritable key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
            double sum = 0.0;
            for(DoubleWritable i:values){
                sum = sum + i.get();
            }
            context.write(key, new DoubleWritable(sum));        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = new Job(new Configuration());
        job.setJobName("JoinJob");
        job.setJarByClass(Join.class);
        job.setMapperClass(OrdersMapper.class);
        job.setReducerClass(OrdersReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}