package util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import java.util.concurrent.Future
import scala.collection.mutable

/**
 * kafka工具类，用于生产数据和消费数据
 */
object MykafkaUtils {

  /**
   * 消费者配置
   * 消费者配置类（ConsumerConfig）
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map( //scala Map()函数
    //kafka集群位置
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "master:9092,slave1:9092,slave2:9092",
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils("kafka.bootstrap-servers"),
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    /**
     * kv反序列化器,读（字节->对象）   序列化,写（对象->字节）
     * 1、kafka序列化消息是在生产端，序列化后，消息才能网络传输，以字节的形式传输
     * 2、kafka反序列化消息是在消费端。由于网络传输过来的是byte[]（字节），只有反序列化后才能得到生产者发送的真实的消息内容
     */
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //offset（消费者偏移量）提交 自动（true） 手动（false）
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //自动提交的时间间隔（毫秒）
    //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000",
    //offset重置 重置到头（earliest） 重置到尾（latest）
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    //groupId（消费者组）
  )

  /**
   * Consumer Group:消费者共用一个ID（Group ID），每个消费者组订阅的所有主题中，每个主题的每个分区只能由消费者组的一个消费者消费
   * 一个分区不能被同一个消费者组的多个消费者消费
   */

  /**
   * 基于SparkStreaming消费，获取到kafkaDStream,使用默认的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    //createDirectStream（sparkstreaming对接kafka，用direct方式消费数据的方法）
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, //ssc: StreamingContext（Spark Streaming所有流操作的主要入口）
      LocationStrategies.PreferConsistent, //Kafka消费者的分布策略, PreferConsistent（最常用的策略，当Spark机器和Kafka机器不属于同一组机器时使用）
      ConsumerStrategies.Subscribe(Array(topic), consumerConfigs)) //Kafka消费者配置
    kafkaDStream
  }

  /**
   * 基于SparkStreaming消费，获取到kafkaDStreqm,使用指定的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    //createDirectStream（sparkstreaming对接kafka，用direct方式消费数据的方法）
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, //ssc: StreamingContext（Spark Streaming所有流操作的主要入口）
      LocationStrategies.PreferConsistent, //Kafka消费者的分布策略, PreferConsistent（最常用的策略，当Spark机器和Kafka机器不属于同一组机器时使用）
      ConsumerStrategies.Subscribe(Array(topic), consumerConfigs, offsets)) //Kafka消费者配置
    kafkaDStream
  }

  /**
   * 生产者对象
   */
  val producer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   * 生产者配置类（ProducerConfig）
   */
  def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap()
    //kafka集群位置
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils("kafka.bootstrap-servers"))
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    /**
     * kv反序列化器,读（字节->对象）   序列化,写（对象->字节）
     * 1、kafka序列化消息是在生产端，序列化后，消息才能网络传输，以字节的形式传输
     * 2、kafka反序列化消息是在消费端。由于网络传输过来的是byte[]（字节），只有反序列化后才能得到生产者发送的真实的消息内容
     */
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //acks（消息发送确认机制）
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    //幂等配置
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    //KafkaProducer : kafka客户端,向kafka集群发送数据
    val producer: KafkaProducer[String, String] = new KafkaProducer(producerConfigs)
    producer
  }

  /**
   * kafka的分区策略 :
   * 1、轮询策略 : 如果key值为null，并且使用了默认的分区器，Kafka会根据轮训策略将消息均匀地分布到各个分区上,性能较低
   * 2、散列策略 : 如果键值不为null，并且使用了默认的分区器，Kafka会对键进行散列，然后根据散列值把消息映射到对应的分区上
   * 3、黏性分区策略 : 选择单个分区发送所有无Key的消息。一旦这个分区的batch已满或处于“已完成”状态
   * 黏性分区器会随机地选择另一个分区并会尽可能地坚持使用该分区——像黏住这个分区一样
   */

  /**
   * 生产【发送消息】（按照默认的黏性分区策略）
   */
  def send(topic: String, msg: String): Future[RecordMetadata] = (
    producer.send(new ProducerRecord[String, String](topic, msg))
    )

  /**
   * 生产【发送消息】（按照key分区）
   */
  def send(topic: String, key: String, msg: String): Future[RecordMetadata] = (

    //ProducerRecord（要发送到卡夫卡的键/值对，包括要将记录发送到的主题名称、可选分区号以及可选的键和值）
    //send : 发送消息给Kafka中broker(主题)
    producer.send(new ProducerRecord(topic, key, msg))
    )

  /**
   * 关闭生产者对象
   */
  def close() = {
    if (producer != null) producer.close()
  }

  /**
   *刷写，将缓冲区（batch）的数据刷写到磁盘（partition）
   */
  def flush(): Unit ={
    producer.flush()
  }
}
