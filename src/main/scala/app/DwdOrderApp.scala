package app

import com.alibaba.fastjson.{JSON, JSONObject}
import bean.{OrderDetail, OrderInfo, OrderWide}
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{MyOffsetsUtils, MyRedisUtils, MykafkaUtils, MyEsUtils}

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

/**
 * 订单宽表任务
 *
 * 1、准备实时环境
 *
 * 2、从Redis中读取offset * 2
 *
 * 3、从kafka中消费数据 * 2
 *
 * 4、提取offset * 2
 *
 * 5、数据处理
 * 5.1、转换结构
 * 5.2、维度关联
 * 5.3、双流join
 *
 * 6、写入ES
 *
 * 7、提交offset * 2
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    //1、准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[3]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //2、从Redis中读取offset,指定offset进行消费
    //order_info
    val orderInfoTopicName: String = "DWD_ORDER_INFO_I"
    val orderInfoGroup: String = "DWD_ORDER_INFO:GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderInfoTopicName, orderInfoGroup)
    //order_detail
    val orderDetailTopicName: String = "DWD_ORDER_DETAIL_I"
    val orderDetailGroup: String = "DWD_ORDER_DETAIL_GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    //3、从kafka中消费数据
    //order_info
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsets != null && orderInfoOffsets.nonEmpty) {
      //指定offset进行消费
      orderInfoKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup, orderInfoOffsets)
    } else {
      //默认offset进行消费
      orderInfoKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup)
    }

    //order_detail
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsets != null && orderInfoOffsets.nonEmpty) {
      //指定offset进行消费
      orderDetailKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    } else {
      //默认offset进行消费
      orderDetailKafkaDStream = MykafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    }

    //4、提取offset
    //order_info
    //从当前消费到的数据中提取offsets结束点,不对流中的数据做任何处理
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    //transform : 操作RDD，返回一个新的DStream
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        //提取offset
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //不对流中的数据做如何处理，返回rdd
        rdd
      }
    )

    //order_detail
    //从当前消费到的数据中提取offsets结束点,不对流中的数据做任何处理
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    //transform : 操作RDD，返回一个新的DStream
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        //提取offset
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //不对流中的数据做如何处理，返回rdd
        rdd
      }
    )

    //5、处理数据
    //5.1、转换结构（专用结构）
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      //一个k/v对被发送到kafka，这包含被发送记录的主题名字，一个可选的分区编号，一个可选的key和value
      //获取value值,value就是日志数据
      consumerRecord => {
        val value: String = consumerRecord.value()
        //专用结构：Bean(标准的类)（对象）  classOf[T]:获取类型T的Class对象
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
    //    orderInfoDStream.print(100)

    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      //一个k/v对被发送到kafka，这包含被发送记录的主题名字，一个可选的分区编号，一个可选的key和value
      //获取value值,value就是日志数据
      consumerRecord => {
        val value: String = consumerRecord.value()
        //专用结构：Bean(标准的类)（对象）  classOf[T]:获取类型T的Class对象
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )
    //    orderDetailDStream.print(100)

    //5.2、维度关联
    // order_info
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        //val orderInfoes: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        val orderInfos: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (orderInfo <- orderInfos) {
          //关联用户维度
          val uid: Long = orderInfo.user_id
          val redisUserKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          //换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          orderInfo.user_gender = gender
          orderInfo.user_age = age

          //关联地区维度
          val provinceID: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsCode: String = provinceJsonObj.getString("iso_code")

          //补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsCode

          //处理日期字段
          val createTime: String = orderInfo.create_time
          val createDtHr: Array[String] = createTime.split(" ")
          //提取年份
          val createDate: String = createDtHr(0)
          //提取小时
          val createHr: String = createDtHr(1).split(":")(0)
          //补充到对象中
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr

          //orderInfoes.append(orderInfo)
        }
        jedis.close()
        orderInfos.iterator
      }
    )
    //    orderInfoDimDStream.print(100)

    //5.3、双流Join（两张表关联）
    //内连接 join 结果集取交集
    //外连接
    //  左外连 leftOuterJoin 左表所有+右表的匹配，分析清楚主(驱动)表 从(匹配)表
    //  右外连 rightOuterJoin 左表的匹配+右表的所有，分析清楚主(驱动)表 从(匹配)表
    //  全外连 fullOuterJoin 两张表的所有 ✔

    //数据库层面：order_info表的数据和order_detail表的数据一定能关联成功
    //流处理层面：order_info和order_detail是两个流，流的join只能是同一个批次的数据才能进行join
    //          如果两个表的数据进入到不同批次中，就会join不成功
    //数据延迟导致的数据没有进入到一个批次，在实时处理中是正常现象，可以接受因为延迟导致最终的结果延迟，不能接受因为延迟导致的数据丢失
    //将流中的数据转换成(K，V)才能Join，通过K进行Join  （id 关联 order_id）
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] = orderInfoDimDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailKVDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    //    val orderJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoKVDStream.join(orderDetailKVDStream)

    //解决：首先使用fullOuterJoin（通过在此DStream的RDD和其他DStream之间应用“完全外部联接”，返回新的DStream），保证join成功或者没有成功的数据都出现到结果中，
    //     让双方都多两步操作：到缓存中找对的人、把自己写到缓存中
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)

    /**
     * 一个OI 对应 多个OD，一个OD 对应 一个OI
     */
    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          //orderInfo有，orderDetail有
          if (orderInfoOp.isDefined) {
            //取出orderInfo
            val orderInfo: OrderInfo = orderInfoOp.get
            //isDefined:如果选项是Some的实例，则返回true，否则返回false
            if (orderDetailOp.isDefined) {
              //取出orderDetail
              val orderDetail: OrderDetail = orderDetailOp.get
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              //放入到结果集中
              orderWides.append(orderWide)
            }
            //orderInfo有，orderDetail没有

            //orderInfo写缓存
            //类型：String
            //key: ORDERJOIN:ORDER_INFO:ID
            //value: json（一条数据）
            //写入API: set
            //读取API: get
            //是否过期：24小时
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            //写入redis
            //jedis.set(redisOrderInfoKey, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
            //设置过期时间
            //jedis.expire(redisOrderInfoKey, 24 * 3600)
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

            //orderInfo读缓存，找orderDetail
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              //asScala（将Java集合转换为相应的Scala集合）
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                //组装成orderWide
                val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
                //加入到结果集中
                orderWides.append(orderWide)
              }
            }

          } else {
            //orderInfo没有，orderDetail有
            val orderDetail: OrderDetail = orderDetailOp.get
            //orderDetail读缓存，找orderInfo
            val redisOrderInfoKey = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.nonEmpty) {
              //classOf[T]（获取类型T的Class对象）
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              //加入到结果集中
              orderWides.append(orderWide)
            } else {
              //orderDetail写缓存
              //类型：set  （将相同且能关联同一个的OI放在一个set中）
              //key: ORDERJOIN:ORDER_DETAIL:ORDER_ID
              //value: json ...
              //写入API: sadd
              //读取API: smembers
              //是否过期:24小时
              val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )
    //    orderWideDStream.print(1000)

    //写入ES
    //1、索引分割，通过索引模板控制mapping（表结构）、setting（分片和副本）、aliases（索引别名）
    //2、使用工具类将数据写入ES
    orderWideDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          orderWideIter => {
            val orderWides: List[(String, OrderWide)] = orderWideIter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
            if (orderWides.nonEmpty) {
              val head: (String, OrderWide) = orderWides.head   //head:返回第一个元素
              //如果是真实的实时环境，直接获取当前日期即可
              //当前是模拟数据，会生成不同天的数据
              //从第一条数据中获取日期
              //提取日期
              val date: String = head._2.create_date
              //索引名
              val indexName: String = s"gmall_order_wide_$date"
              //写入到ES
              MyEsUtils.bulkSave(indexName, orderWides)
            }
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(orderInfoTopicName, orderInfoGroup, orderInfoOffsetRanges)
        MyOffsetsUtils.saveOffset(orderDetailTopicName, orderDetailGroup, orderDetailOffsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }



}
