package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis:
 * Redis是一个开源的key-value内存数据库
 * Redis读写速度快,基于内存读写
 *
 * Redis工具类，用于获取Jedis连接，操作Redis
 */
object MyRedisUtils {

  var jedisPool: JedisPool = null

  def getJedisFromPool(): Jedis = {
    //jedisPool == null （为创建redis连接对象）, jedisPool != null （已创建redis连接对象）
    if (jedisPool == null) {
      //连接池配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长（毫秒）
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
      val host: String = MyPropsUtils(MyConfig.REDIS_HOST) //redis地址
      val port: String = MyPropsUtils(MyConfig.REDIS_PORT) //redis端口号

      /**
       * JedisPool（Jedis连接池，获取Jedis实例需要从JedisPool中获取）
       * 1->获取Jedis实例需要从JedisPool中获取；
       * 2->用完Jedis实例需要还给JedisPool；
       * 3->如果Jedis在使用过程中出错，则也需要还给JedisPool；
       */
      //创建连接池对象
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }

    //获取资源
    jedisPool.getResource
  }
}
