# Hadoop-KNN
云计算实践课程设计<br>
实现了基于Hadoop平台的KNN算法，使用的数据集是非常经典的KNN算法数据集—Iris数据集。<br>
## Mapper的实现
在Mapper中，我们每次读取训练集中的一行，即训练集的一个样本。对于测试集中的每个测试样本，计算与这个训练样本的欧式距离。之后，将测试样本的编号作为mapper输出的key，将距离以及训练样本的类别作为mapper输出的value。这样，我们就可以通过mapper过程，计算每个训练样本与每个测试样本的距离了。
'''
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
'''
## Reducer的实现
在这里，MapReduce框架会把key值相同的value合并。也就是说，我们得到了第key个测试样本与各个训练样本的距离以及训练样本的类别。那么我们需要按照距离从小到大排序。在排序这个过程，我们利用了TreeMap这个数据结构，该结构会对数据自动排序储存。
'''
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
'''
之后选取前K条记录的类别做多数投票，就可以得到这个测试样本分类的预测了。
'''
                        int setosa_count=0, versicolor_count=0, virginica_count=0;
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
'''
