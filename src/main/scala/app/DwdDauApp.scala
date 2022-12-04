package app

import bean.{DauInfo, PageLog}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}
import util.{MyBeanUtils, MyEsUtils, MyOffsetsUtils, MyRedisUtils, MykafkaUtils}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.{lang, util}
import util.Date
import scala.collection.mutable.ListBuffer

/**
 * ---日活宽表（关联日志数据和业务数据的维度数据）---
 *
 * 1、准备实时数据
 * 2、从Redis中读取偏移量
 * 3、从kafka中消费数据
 * 4、提取偏移量结束点
 * 5、处理数据
 * 5.1、转换数据结构
 * 5.2、去重
 * 5.3、维度关联
 * 6、写入ES
 * 7、提交offsets
 */
object DwdDauApp {
  //RDD算子：转换算子（map、filter、flatMap、union、groupByKey、reduceBykey等）、行动算子（reduce、count、first、take、saveAsTextFile、foreach等）
  def main(args: Array[String]): Unit = {
    //0、还原状态
    reverState()

    //1、准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[3]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "DWD_PAGE_LOG_TOPIC"
    val gropId = "DWD_DAU_GROUP"
    //2、从Redis中读取offset,指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, gropId)

    //3、从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      //指定offset进行消费
      kafkaDStream = MykafkaUtils.getKafkaDStream(ssc, topicName, gropId, offsets)
    } else {
      //默认offset进行消费
      kafkaDStream = MykafkaUtils.getKafkaDStream(ssc, topicName, gropId)
    }

    //4、提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    //transform : 操作RDD，返回一个新的DStream
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        //提取offset
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //不对流中的数据做如何处理，返回rdd
        rdd
      }
    )

    //5.1、转换数据结构
    //map() : 对rdd中的每一个元素进行操作
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        //一个k/v对被发送到kafka，这包含被发送记录的主题名字，一个可选的分区编号，一个可选的key和value
        //获取value值,value就是日志数据
        val value: String = consumerRecord.value()
        //专用结构：Bean(标准的类)（对象）  classOf[T]:获取类型T的Class对象
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    //foreachRDD的作用是对每个批次的RDD做自定义操作,action(行动)算子
    pageLogDStream.foreachRDD(
      rdd => println("自我审查前：" + rdd.count()) //返回数据集中所有元素的个数
    )
    //5.2、去重
    //自我审查：过滤掉last_page_id不为空的页面访问数据（标记有上一级页面的过滤）
    //filter() : 过滤，保留满足条件的数据
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    //foreachRDD的作用是对每个批次的RDD做自定义操作,action(行动)算子
    filterDStream.foreachRDD(
      rdd => {
        println("自我审查后：" + rdd.count()) //返回数据集中所有元素的个数
      }
    )

    //第三方审查：通过Redis维护今日访问的mid ,自我审查后的数据再与Redis中维护的今日访问mid进行比对
    //redis中如何维护日活状态
    //类型： set
    //key：DAU:DATE
    //value：mid的集合
    //写入API：list（lpush/rpush） set（sadd）
    //读取API：list（lrange）  set（smembers）
    //过期：24小时过期

    //mapPartitions() : 对rdd中的每个分区的迭代器进行操作
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLogList: List[PageLog] = pageLogIter.toList
        println("第三方审查前：" + pageLogList.size)

        //存储要的数据（List：不可变列表，ListBuffer：可变列表）
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogList) {
          //提取每条数据的mid（日活的统计基于mid，也可以基于uid）
          val mid: String = pageLog.mid

          //获取日期，因为我们要测试不同天的数据，所以不能直接获取系统数据（在实时的生产环境中应获取系统数据）
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"

          //redis的判断是否包含操作
          /*
          下面代码在分布式环境中，存在并发问题，可能多个并行度同时进入到if中，导致最终保留多条相同一个mid的数据
          //list
          val mids: util.List[String] = jedis.lrange(redisDauKey, 0, -1)
          if (!mids.contains(mid)) {
            jedis.lpush(redisDauKey, mid)
            pageLogs.append(pageLog)
          }

          //set
          val setMids: util.Set[String] = jedis.smembers(redisDauKey)
          if (!setMids.contains(mid)) {       //contains () 方法用于判断字符串中是否包含指定的字符或字符串
            jedis.sadd(redisDauKey,mid)
            pageLogs.append(pageLog)
          }
           */

          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }

        }
        jedis.close()
        println("第三方审查后：" + pageLogs.size)
        pageLogs.iterator
      }
    )
    //    redisFilterDStream.print()

    //5.3 维度关联
    //map ：一次处理一个元素的数据，mapPartitions：一次处理一批数据
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter) {
          val dauInfo: DauInfo = new DauInfo()
          //1、将PageLog中的字段拷贝到DauInfo，通过对象拷贝来完成
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          //2、补充维度
          //2.1、用户信息维度
          val uid: String = pageLog.user_id
          val redisUidKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          //换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now() //获取当前系统日期
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          //2.2、地区信息维度
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")

          //补充到对象中
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          //2.3、日期字段处理
          val date: Date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          //提取年份
          val dt: String = dtHrArr(0)
          //提取小时
          val hr: String = dtHrArr(1).split(":")(0)
          //补充到对象中
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }
        jedis.close()
        dauInfos.iterator
      }
    )
    //    dauInfoDStream.print(100)

    //写入到OLAP中
    //按照天分割索引，通过索引模板控制mapping（表结构），settings（分片和副本数），aliases（起别名）等
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            if (docs.nonEmpty) {
              //如果是真实的实时环境，直接获取当前日期即可
              //当前是模拟数据，会生成不同天的数据
              //从第一条数据中获取日期
              val head: (String, DauInfo) = docs.head //head:返回第一个元素
              //提取时间戳
              val ts: Long = head._2.ts
              //设置日期格式
              val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              //将时间戳转换成对应的日期格式
              val dateStr: String = sdf.format(new Date(ts))
              val indexName: String = s"gmall_dau_info_$dateStr"
              //写入到ES中
              MyEsUtils.bulkSave(indexName, docs)
            }
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, gropId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 状态还原
   *
   * 在每次启动实时任务时，进行一次状态还原，以ES为准，将所有的mid提取出来，覆盖到Redis中
   */
  def reverState(): Unit = {
    //从ES中查询到所有的mid
    val date: LocalDate = LocalDate.now() //从默认时区的系统时钟获取当前日期
    val indexName: String = s"gmall_dau_info_$date"
    val fieldName: String = "mid"
    val mids: List[String] = MyEsUtils.searchField(indexName, fieldName)

    //删除redis中记录的状态（所有的mid）
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisDauKey: String = s"DAU:$date"
    jedis.del(redisDauKey)

    //将从ES中查询到的mid覆盖到Redis中
    if (mids != null && mids.nonEmpty) {
      //单条发送，影响性能，耗时
      //      for (mid <- mids) {
      //        jedis.sadd(redisDauKey, mid)
      //      }

      //批量发送，Pipeline:管道
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        //将数据写入管道
        pipeline.sadd(redisDauKey, mid) //不会直接到redis执行
      }
      //将管道的数据写入Redis
      pipeline.sync() //到redis执行
    }
    jedis.close() //关闭Redis连接
  }

}
