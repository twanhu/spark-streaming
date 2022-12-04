package app

import bean.{PageDisplayLog, PageLog}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import bean.{PageActionLog, StartLog}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{MyOffsetsUtils, MykafkaUtils}

/**
 * ---日志数据的采集和分流---
 *
 * 1.准备实时处理环境 StreamingContext
 *
 * 2.从kafka中消费数据
 *
 * 3.处理数据
 * 3.1 转换数据结构
 * 专用结构：Bean（对象）
 * 通用结构：map JsonObject
 * 3.2 分流
 *
 * 4.写出DWD层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    //TODO并行度与kafka中topic的分区个数对应
    /**
     * 任何Spark程序都是SparkContext开始的，SparkContext的初始化需要一个SparkConf对象，SparkConf包含了Spark集群配置的各种参数
     * StreamingContext（Spark Streaming所有流操作的主要入口）
     */
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[3]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5)) //Seconds():设置了StreamingContext多久（秒）读取一次数据流

    //2.从Kafka中消费数据
    val topicName: String = "ODS_BASE_LOG"
    val groupId: String = "ODS_BASE_LOG_GROUP"

    // 从Redis中读取offset,指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

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

    // 从当前消费到的数据中提取offsets结束点,不对流中的数据做任何处理
    var offsetRanges: Array[OffsetRange] = null
    //transform : 操作RDD，返回一个新的DStream
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        //提取offset
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //在driver执行
        //不对流中的数据做如何处理，返回rdd
        rdd
      }
    )

    //3.处理数据
    //3.1 转换数据结构
    //map() : 对rdd中的每一个元素进行操作
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecrd => {
        //一个k/v对被发送到kafka，这包含被发送记录的主题名字，一个可选的分区编号，一个可选的key和value
        //获取value值,value就是日志数据
        val log: String = consumerRecrd.value()
        //将字符串转换成Json对象
        val JSONObj: JSONObject = JSON.parseObject(log)
        //返回
        JSONObj
      }
    )
//    jsonObjDStream.print(1000)

    //3.2 分流
    //  日志数据：
    //    页面访问数据
    //        公共字段
    //           页面数据
    //              曝光数据
    //              事件数据
    //        错误数据
    //    启动数据
    //        公共字段
    //           启动数据
    //        错误数据
    val DWD_PAGE_LOG_TOPIC = "DWD_PAGE_LOG_TOPIC" //页面访问数据
    val DWD_PAGE_DISPLAY_TOPIC = "DWD_PAGE_DISPLAY_TOPIC" //曝光数据
    val DWD_PAGE_ACTION_TOPIC = "DWD_PAGE_ACTION_TOPIC" //事件数据
    val DWD_START_LOG_TOPIC = "DWD_START_LOG_TOPIC" //启动数据
    val DWD_ERROR_LOG_TOPIC = "DWD_ERROR_LOG_TOPIC" //错误数据

    //分流规则：
    //  错误数据：不做任何的拆分，只要包含错误字段，直接整条数据发送到对应的topic
    //  页面数据：拆分成页面访问、曝光、事件分别发送到对应的topic
    //  启动数据：发送到对应的topic

    //foreachRDD的作用是对每个批次的RDD做自定义操作,action(行动)算子
    jsonObjDStream.foreachRDD(
      rdd => {
        //foreachPartition : 一个分区一个分区的拿数据，spark不存在分区，RDD才有分区
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //分流过程
              //分流错误数据
              val errObj: JSONObject = jsonObj.getJSONObject("err")
              if (errObj != null) {
                //将错误数据发送到 DWD_ERROR_LOG_TOPIC    toJSONString（转换成JSON字符串）
                MykafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
              } else {
                //提取公共字段  getJSONObject（获取JSON对象）
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                val ar: String = commonObj.getString("ar")
                val uid: String = commonObj.getString("uid")
                val os: String = commonObj.getString("os")
                val ch: String = commonObj.getString("ch")
                val isNew: String = commonObj.getString("is_new")
                val md: String = commonObj.getString("md")
                val mid: String = commonObj.getString("mid")
                val vc: String = commonObj.getString("vc")
                val ba: String = commonObj.getString("ba")
                //提取时间戳
                val ts: Long = jsonObj.getLong("ts")

                //页面数据
                val pageObj: JSONObject = jsonObj.getJSONObject("page")
                if (pageObj != null) {
                  //提取page字段
                  val pageId: String = pageObj.getString("page_id")
                  val pageItem: String = pageObj.getString("item")
                  val pageItemType: String = pageObj.getString("item_type")
                  val duringTime: Long = pageObj.getLong("during_time")
                  val lastPageId: String = pageObj.getString("last_page_id")
                  val sourceType: String = pageObj.getString("soucre_type")

                  //封装成PageLog（公共字段+页面数据）
                  val pageLog =
                    PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
                  //将Bean对象转换成JSON对象，发送到DWD_PAGE_LOG_TOPIC
                  MykafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))
                  //提取曝光数据
                  val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                  if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                    //循环拿到每个曝光
                    for (i <- 0 until displaysJsonArr.size()) {
                      val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                      //提取曝光字段
                      val displayType: String = displayObj.getString("display_type")
                      val displayItem: String = displayObj.getString("item")
                      val displayItemType: String = displayObj.getString("item_type")
                      val posId: String = displayObj.getString("pos_id")
                      val order: String = displayObj.getString("order")

                      //封装成PageDisplayLog【（公共字段+页面数据）+曝光数据】先有页面数据才有曝光数据
                      val pageDisplayLog =
                        PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                      // 写到 DWD_PAGE_DISPLAY_TOPIC
                      MykafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                    }
                  }
                  //提取事件数据
                  val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                  if (actionJsonArr != null && actionJsonArr.size() > 0) {
                    //循环拿到每个事件
                    for (i <- 0 until actionJsonArr.size()) {
                      val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                      //提取字段
                      val actionId: String = actionObj.getString("action_id")
                      val actionItem: String = actionObj.getString("item")
                      val actionItemType: String = actionObj.getString("item_type")
                      val actionTs: Long = actionObj.getLong("ts")

                      //封装PageActionLog【（公共字段+页面数据）+事件数据】先有页面数据才有事件数据
                      var pageActionLog =
                        PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                      //写出到DWD_PAGE_ACTION_TOPIC
                      MykafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                    }
                  }
                }
                // 启动数据
                val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
                if (startJsonObj != null) {
                  //提取字段
                  val entry: String = startJsonObj.getString("entry")
                  val loadingTime: Long = startJsonObj.getLong("loading_time")
                  val openAdId: String = startJsonObj.getString("open_ad_id")
                  val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                  val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                  //封装StartLog（公共字段+启动数据）
                  val startLog =
                    StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                  //写出DWD_START_LOG_TOPIC
                  MykafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
                }
              }
            }
            //foreachPartition里面：Executor段执行，每批次每分区执行一次
            //刷写kafka,Kafka 消息的发送分为同步发送和异步发送。 Kafka 默认使用异步发送的方式
            MykafkaUtils.flush()
          }
        )

        /*
        //foreach 是一条一条的拿数据进行处理
        rdd.foreach(
          jsonObj => {

          }
        )
        */

        //foreachRDD里面，forech外面：提交offset？Driver段执行，一批次执行一批次（周期性）
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
        //foreachRDD里面，forech外面：刷写kafka缓冲区？Driver段执行，一批次执行一批次（周期性）分流是在executor端完成，driver端做刷写，刷的不是同一个对象的缓冲区
      }
    )
    // foreachRDD外面：提交offset？Driver段执行，每次启动程序执行一次
    // foreachRDD外面：刷写kafka缓冲区？Driver段执行，每次启动程序执行一次，分流是在executor端完成，driver端做刷写，刷的不是同一个对象的缓冲区

    // 开始计算
    ssc.start()
    //等待计算被中断
    ssc.awaitTermination()
  }
}
