package app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{MyOffsetsUtils, MyRedisUtils, MykafkaUtils}

import java.util


/**
 * ---业务数据的采集和分流---
 *
 * 1、准备实时环境
 *
 * 2、从redis中读取偏移量
 *
 * 3、从kafka中消费数据
 *
 * 4、提取偏移量结束点
 *
 * 5、数据处理
 * 5、1 转换数据结构
 * 5、2 分流
 * 事实数据 -> kafka
 * 维度数据 -> Redis
 * 6、flush（刷写）kafka的缓冲区
 *
 * 7、提交offset
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    //1、准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[3]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "ODS_BASE_DB"
    val groupId: String = "ODS_BASE_DB_GROUP"

    //2、从Redis中读取offset,指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3、从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      //指定offset进行消费
      kafkaDStream
      = MykafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      //默认offset进行消费
      kafkaDStream
      = MykafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //4、提取偏移量结束点  从当前消费到的数据中提取offsets,不对流中的数据做任何处理
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        //提取offset
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //不对流中的数据做如何处理，返回rdd
        rdd
      }
    )
    //5、处理数据
    //5、1 转换数据结构
    //map() : 对rdd中的每一个元素进行操作
    val jsonObjDStrem: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        //一个k/v对被发送到kafka，这包含被发送记录的主题名字，一个可选的分区编号，一个可选的key和value
        //获取value值,value就是日志数据
        val dataJson: String = consumerRecord.value()
        //将字符串转换成Json对象
        val jSONObject: JSONObject = JSON.parseObject(dataJson)
        //返回
        jSONObject
      }
    )
//        jsonObjDStrem.print(1000)

    //5.2 分流

    //事实表清单
    //val factTables: Array[String] = Array[String]("order_info", "order_detail" /*缺啥补啥*/)
    //维度表清单
    //val dimTables: Array[String] = Array[String]("user_info", "base_province" /*缺啥补啥*/)

    /**
     * Redis连接写到哪里???
     * foreachRDD外面:  driver  ，连接对象不能序列化，不能传输
     * foreachRDD里面, foreachPartition外面 : driver  ，连接对象不能序列化，不能传输
     * foreachPartition里面 , 循环外面：executor ， 每分区数据开启一个连接，用完关闭.
     * foreachPartition里面,循环里面:  executor ， 每条数据开启一个连接，用完关闭， 太频繁。
     */

    //foreachRDD的作用是对每个批次的RDD做自定义操作,action(行动)算子
    jsonObjDStrem.foreachRDD(
      rdd => {

        //配置动态表清单
        //将表清单维护到redis中，实时任务中动态的到redis中获取表清单
        //类型：set
        //key：FACT:TABLES DIM:TABLES
        //value：表名的集合
        //写入API：sadd
        //读取API：smembers
        //过期：不过期
        val redisFactKyes: String = "FACT:TABLES"   //事实表清单
        val redisDimKyes: String = "DIM:TABLES"     //维度表清单
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        // 事实表清单
        val factTables: util.Set[String] = jedis.smembers(redisFactKyes)
        println("factTables: " + factTables)
        //做成广播变量：
        //   广播变量是一个只读变量，通过它我们可以将一些共享数据集或者大变量缓存在Spark集群中的各个机器上而不用每个task都需要copy一个副本，后续计算可以重复使用，减少了数据传输时网络带宽的使用，提高效率
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
        // 维度表清单
        val dimTables: util.Set[String] = jedis.smembers(redisDimKyes)
        println("dimTables: " + dimTables)
        //做成广播变量：
        //   广播变量是一个只读变量，通过它我们可以将一些共享数据集或者大变量缓存在Spark集群中的各个机器上而不用每个task都需要copy一个副本，后续计算可以重复使用，减少了数据传输时网络带宽的使用，提高效率
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)

        //foreachPartition : 一个分区一个分区的拿数据，spark不存在分区，RDD才有分区
        rdd.foreachPartition(
          jsonObjIter => {
            // 开启redis连接（存储维度数据）
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()
            for (jsonObj <- jsonObjIter) {
              //提取操作类型
              val operType: String = jsonObj.getString("type")

              val opValue: String = operType match {
                case "bootstrap-insert" => "BI" //历史维度数据同步
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              //判断操作类型：1、明确什么操作 2、过滤不感兴趣的数据
              if (opValue != null) {
                //提取表名
                val tableName: String = jsonObj.getString("table")
                //value : 获取广播值   contains() : 方法用于判断字符串中是否包含指定的字符或字符串
                if (factTablesBC.value.contains(tableName)) {
                  //提取数据  事实数据
                  val data: String = jsonObj.getString("data")
                  //toUpperCase() 方法将字符串小写字符转换为大写
                  val dwdTopicName: String = s"DWD_${tableName.toUpperCase}_${opValue}"
                  MykafkaUtils.send(dwdTopicName, data)

                  //模拟数据延迟
                  if (tableName.equals("order_detail")) {
                    Thread.sleep(200)
                  }
                }

                if (dimTablesBC.value.contains(tableName)) {
                  //维度数据 -> Redis
                  //类型：String
                  //      hash：整个表存成一个hash，要考虑目前数据量大小和将来数据增长问题 及 高频访问问题
                  //      hash：一条数据存成一个hash
                  //      String：一条数据存成一个jsonString
                  //key（String）：DIM:表名:ID
                  //value（String）：整条数据的jsonString
                  //写入API：set
                  //读取API：get
                  //过期：不过期

                  //提取数据中的id
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey: String = s"DIM:${tableName.toUpperCase}:$id"
                  //往redis中存数据
                  jedis.set(redisKey, dataObj.toJSONString)
                }
              }
            }
            //关闭资源
            jedis.close()

            //foreachPartition里面：Executor段执行，每批次每分区执行一次
            //6、flush（刷写）kafka的缓冲区
            MykafkaUtils.flush()
          }
        )
        //7、提交offset foreachRDD里面，forech外面：提交offset？Driver段执行，一批次执行一批次（周期性）
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

}
