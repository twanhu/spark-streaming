package util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import scala.collection.mutable.ListBuffer

/**
 * ES工具类，用于对ES进行读写操作
 */
object MyEsUtils {

  /**
   * 客户端对象
   */
  val esClient: RestHighLevelClient = build()

  /**
   * 创建ES客户端对象
   */
  def build(): RestHighLevelClient = {
    val host: String = MyPropsUtils(MyConfig.ES_HOST)
    val port: String = MyPropsUtils(MyConfig.ES_PORT)

    //创建RestClient连接池
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host, port.toInt))
    //创建ES客户端对象
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  /**
   * 关闭ES对象
   */
  def close(): Unit = {
    if (esClient != null) esClient.close()
  }

  /**
   * 1、批量写入
   * 2、幂等写入，精确一次消费
   */
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]): BulkResponse = {
    //BulkRequest对象可以用来在一次请求中，执行多个索引、更新或删除操作
    val bulkRequest: BulkRequest = new BulkRequest()
    for ((docId, docObj) <- docs) {
      //指定索引
      val indexRequest: IndexRequest = new IndexRequest(indexName)
      //转换成JSON字符串
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      //指定doc（文档）
      indexRequest.source(dataJson, XContentType.JSON)
      //指定_id
      indexRequest.id(docId)
      //将indexRequest加入到bulk
      bulkRequest.add(indexRequest)
    }
    //将数据写入ES，批量写入
    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * 查询指定的字段
   */
  def searchField(indexName: String, fieldName: String): List[String] = {
    //获取索引请求
    val getIndexRequest: GetIndexRequest = new GetIndexRequest(indexName)
    //exists:检查索引是否存在
    val isExists: Boolean = esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!isExists) {
      return null
    }
    //正常从ES中提取指定的字段
    //从ES中查询到所有的mid
    val mids: ListBuffer[String] = ListBuffer[String]()
    //创建搜索请求对象
    val searchRequest: SearchRequest = new SearchRequest(indexName)
    //向搜索请求中可以添加搜索内容的特征参数
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    //fetchSource():筛选后返回fieldName字段，排除null（没有字段排除写null）字段
    searchSourceBuilder.fetchSource(fieldName, null).size(100000)
    //将SearchSourceBuilder对象添加到搜索请求中
    searchRequest.source(searchSourceBuilder)

    //search():执行搜索请求对象
    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    //在ES中通过DSL查询得出结果后，按照格式提取字段
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceMap: util.Map[String, AnyRef] = hit.getSourceAsMap
      val mid: String = sourceMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }

//  def main(args: Array[String]): Unit = {
//    println(searchField("gmall_dau_info_2022-03-30", "mid"))
//  }

}
