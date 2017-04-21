package com.test.Employee;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


//import com.cloudera.spark.hbase.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;

@SuppressWarnings("serial")
public class EmployeeInfo implements Serializable
{

	public static void main(String []args){
	//System.out.println("srikanth");
	//private static final Logger log = LoggerFactory.getLogger(SkillDemandResumeParser.class);
    SparkConf sparkConf = new SparkConf().setAppName("EmployeesApp").setMaster("local");
	JavaSparkContext jsc = new JavaSparkContext(sparkConf);

	
	
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quorum", "127.0.0.1");
	conf.set("hbase.zookeeper.property.clientPort", "2181");
	conf.set(TableInputFormat.INPUT_TABLE, "employees");
	conf.set(TableInputFormat.SCAN_COLUMN_FAMILY,"personal_details");
	JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc,conf);

	Integer batchSize =2;
	
	try {
	      List<byte[]> list = new ArrayList<byte[]>(5);
	     /* list.add(Bytes.toBytes("1"));
	      list.add(Bytes.toBytes("2"));
	      list.add(Bytes.toBytes("3"));*/
	      list.add( "emp1".getBytes());
	     
	 
	      JavaRDD<byte[]> rdd = jsc.parallelize(list);	 
	      System.out.println("Before Bulk Get");
	      EmployeeInfo employee = new EmployeeInfo();
		  JavaRDD resultRDD = (JavaRDD)hbaseContext.bulkGet(TableName.valueOf("employees"), batchSize, rdd, employee.new GetFunction(),employee.new ResultFunction());
	      System.out.println("Count in RDD"+resultRDD.count());
	      resultRDD.saveAsTextFile("file:///home/cloudera/output");
	    //rdd.saveAsObjectFile("file:///home/cloudera/output");
	      System.out.println("After Bulk Get");
	      //hbaseContext.bulkGet("employees", batchSize, rdd, employee.new GetFunction(),employee.new ResultFunction());
	} finally {
	      jsc.stop();
	    }

	  }
public class GetFunction implements Serializable, Function<byte[], Get> {

    private static final long serialVersionUID = 1L;

    public Get call(byte[] v) throws Exception {
    	//System.out.println("In Get function");
    	System.out.println(Bytes.toString(v));
      return new Get(v);
    }
  }

  public class ResultFunction implements Serializable, Function<Result, String> {

    private static final long serialVersionUID = 1L;

    public String call(Result result) throws Exception {
      Iterator<KeyValue> it = result.list().iterator();
      StringBuilder b = new StringBuilder();

      b.append(Bytes.toString(result.getRow()) + ":");

      while (it.hasNext()) {
        KeyValue kv = it.next();
        String q = Bytes.toString(kv.getQualifier());
        if (q.equals("name")) {
          b.append("(" + Bytes.toString(kv.getQualifier()) + ","
              + Bytes.toLong(kv.getValue()) + ")");
        } else {
          b.append("(" + Bytes.toString(kv.getQualifier()) + ","
              + Bytes.toString(kv.getValue()) + ")");
        }
      }
      //System.out.println("In result function");
     // System.out.println(b.toString());
      return b.toString();
    }
  }
  }