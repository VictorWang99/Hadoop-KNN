import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.ArrayList;
import java.util.Map;
import java.util.Comparator;
import java.util.TreeMap;
import java.io.*;
import java.util.Iterator;

public class KNN{
        private static int K = 10;
        private static ArrayList<Iris> testDatas = new ArrayList<Iris>();
        public static class Iris {
            // 属性
            public double sepalLength, sepalWidth, petalLength, petalWidth;
            // 类别
            public String label;
            public Iris(String[] data) {
                sepalLength = Double.parseDouble(data[0]);
                sepalWidth = Double.parseDouble(data[1]);
                petalLength = Double.parseDouble(data[2]);
                petalWidth = Double.parseDouble(data[3]);
                if(data.length==5){
                    label = data[4];
                }
            }
            // 计算两个样本的欧式距离
            public double distance(Iris another) {
                double sum = 0;
                sum += Math.pow(another.sepalLength - sepalLength, 2);
                sum += Math.pow(another.sepalWidth - sepalWidth, 2);
                sum += Math.pow(another.petalLength - petalLength, 2);
                sum += Math.pow(another.petalWidth - petalWidth, 2);
                return Math.sqrt(sum);
            }
        }
        public static class KNNMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
                @Override
                protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        String line = value.toString();
                        // 划分以空格分隔的数据
                        String[] data = line.split("\\s+");
                        // 构建训练样本
                        Iris trainData = new Iris(data);
                        // 对测试集中的所有样本，计算与该训练样本的欧式距离，将测试样本的编号作为key，将距离与训练样本的label的组合作为value传给reducer
                        for (int i = 0; i < testDatas.size(); i++) {
                            Iris testData = testDatas.get(i);
                            double distance = trainData.distance(testData);
                            Text valueInfo = new Text(Double.toString(distance)+','+trainData.label);
                            context.write(new IntWritable(i), valueInfo);
                        }
                }
        }
        
        public static class KNNReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
                @Override
                protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        TreeMap<Double, String> disMapToLabel = new TreeMap<>();
                        // 对于第key个测试样本，将与所有训练样本的距离以及训练样本的label放入TreeMap中，TreeMap会默认以距离升序排列。
                        for (Text value : values) {
                                double distance = Double.parseDouble(value.toString().split(",")[0]);
                                String label = value.toString().split(",")[1];
                                disMapToLabel.put(distance, label);
                        }
                        // 统计距离最小的前k个label的数量，以多数投票原则得到预测的label
                        int setosa_count=0;
                        int versicolor_count=0;
                        int virginica_count=0;
                        Iterator<Double> iterator =  disMapToLabel.keySet().iterator();
                        int i = 0;
                        while(iterator.hasNext()) {
                            double dis = iterator.next();
                            String label = disMapToLabel.get(dis);
                            if(label=="Iris-setosa") setosa_count++;
                            else if(label=="Iris-versicolor") versicolor_count++;
                            else if(label=="Iris-virginica") virginica_count++;
                            i++;
                            if(i>=K) break;
                        }
                        String testLabel;
                        if(setosa_count > versicolor_count && setosa_count > virginica_count){
                            testLabel = "Iris-setosa";
                        }
                        else if(versicolor_count > setosa_count && versicolor_count > virginica_count){
                            testLabel = "Iris-versicolor";
                        }
                        else testLabel = "Iris-virginica";
                        // 最后输出预测结果
                        context.write(key, new Text(testLabel));
                }
        }
        public static void main(String[] args) throws Exception {
                // 以下部分为HadoopMapreduce主程序的写法，对照即可
                // 创建配置对象
                Configuration conf = new Configuration();
                // 创建Job对象
                Job job = Job.getInstance(conf, "KNN");
                // 设置运行Job的类
                job.setJarByClass(KNN.class);
                // 设置Mapper类
                job.setMapperClass(KNNMapper.class);
                // 设置Reducer类
                job.setReducerClass(KNNReducer.class);
                // 设置Map输出的Key value
                job.setMapOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);
                // 设置Reduce输出的Key value
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);
                // 设置输入训练集路径
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                // 读取测试集文件并构造测试集
                FileReader fr=new FileReader(args[1]);
                BufferedReader br=new BufferedReader(fr);
                String line=null;
                while ((line=br.readLine())!=null) {
                    Iris testData = new Iris(line.split("\\s+"));
                    testDatas.add(testData);
                }
                br.close();
                fr.close();
                // 设置输出的路径
                FileOutputFormat.setOutputPath(job, new Path(args[2]));
                // 从第四个参数（如果有）读取指定的K，否则K默认为10
                if(args.length>=4)
                    K = Integer.parseUnsignedInt(args[3]);
                // 提交job
                boolean b = job.waitForCompletion(true);
                if(!b) {
                        System.out.println("KNN task fail!");
                }
        }
}