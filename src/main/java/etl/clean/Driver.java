package etl.clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import utils.HdfsUtil;

public class Driver {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            String HDFS = "hdfs://hadoop-master:9000/";
            HdfsUtil hdfsUtil = new HdfsUtil(HDFS, conf);
            // 指定带运行参数的目录为输入输出目录
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println("Usage: Data Deduplication <in> <out>");
				System.exit(2);
			}
            //修改传入参数
//            String inputPath = HDFS + "weblog/apachelogs/apache_simple.log";
//            String outputPath = HDFS + "user/hive/warehouse/testdb.db/clickstream_log/dt=2019-05-28";
            String inputPath = HDFS + otherArgs[0];
            String outputPath = HDFS +otherArgs[1];
            Job job = Job.getInstance(conf);
            job.setJobName("clickstream_etl");
            job.setJarByClass(Driver.class);
            job.setMapperClass(ClickStreamMapper.class);
            job.setReducerClass(ClickStreamReducer.class);
            //设置writable传入传出参数
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);
//            job.setOutputKeyClass(LongWritable.class);
//            job.setOutputValueClass(Text.class);
            //设置map reduce key value
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            job.setPartitionerClass(SessionIdPartitioner.class);
            job.setSortComparatorClass(SortComparator.class);
//			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            hdfsUtil.rmr(outputPath);
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


//1. 启动hadoop。先去web上8088和9870看一下是否正常.
//        2. 准备数据。创建文件夹hadoop fs -mkdir -p /weblog/coollogs/in
//        再创建文件夹hadoop fs -mkdir -p /weblog/coollogs/out
//        查看hadoop fs -ls /weblog/coollogs/in 是否有内容；
//        没有的话，把数据放进去hadoop fs -put coolshell_20140212.log /weblog/coollogs/in/
//        再次查看hadoop fs -ls /weblog/coollogs/in 是否有内容，也可以去web查看；
//        3. 开始写程序，利用idea生成jar包
//        4. 修改jar包zip -d weblog.jar 'META-INF/.SF' 'META-INF/.RSA' 'META-INF/*SF'
//        然后传到服务器的root，也可以先传到服务器再修改。
//        5. 在服务器上执行jar文件，hadoop jar weblog.jar weblog.WebLogDriver，过程比较慢，千万别着急，可以去8088看一下状态，千万要等map和reduce都100%之后才行。
//        6. 在web端的9870下看一下，是不是有_SUCCESS


// 1、先将Apache日志上传至HDFS的目录下，并按照时间进行存储
// 如：/tmp/apache_log/2017-07-12
//
// 2、将程序打包上传至Hadoop集群
// 如：/home/files/clickstream_etl.jar
//

//要选择数据库
// 3、如果是第一次执行，则需要先在Hive中创建clickstream_log表
// 如：
// hive> create table clickstream_log(
// > ipaddress string,
// > uniqueid string,
// > url string,
// > sessionid string,
// > sessiontimes int,
// > areaaddress string,
// > localaddress string,
// > browsertype string,
// > operationsys string,
// > referurl string,
// > receicetime bigint,
// > userid string,
// > csvp int)
// > partitioned by (dt string)
// > row format delimited fields terminated by '\t';
//
// 4、执行jar程序，将前一天的点击流日志进行数据清洗并输出至clickstream_log表相对应的HDFS路径下
// 如：hadoop jar /home/files/clickstream_etl.jar com.etl.clean.Driver
// 	  /tmp/apache_log/2017-07-12 /user/hive/warehouse/clickstream_log/dt=2017-07-12

//修改传入参数后修改路径
//hadoop jar clickstream_etl.jar etl.clean.Driver /tmp/apache_log/2019-06-12/ /user/hive/warehouse/clickstream_log/dt=2019-06-12
//hadoop jar clickstream_etl.jar etl.clean.Driver /tmp/apache_log/2019-06-13/ /user/hive/warehouse/clickstream_log/dt=2019-06-13