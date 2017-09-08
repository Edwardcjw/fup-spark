//package org.edward.cui.es
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.dstream.DStream
//
///**
//  * Created by Cui on 2017/8/7.
//  */
//object EsStreaming extends HbaseFunction{
//  val appName = "EsStreaming"
//  val seconds = 300
//  val brokers = "s5:6667,s3:6667,s1:6667,s6:6667,s2:6667,s4:6667,m1:6667"
//  val params = Map[String, String]("group.id" -> "EsStreaming", "metadata.broker.list" -> brokers, "fetch.message.max.bytes" -> "536870912")
//
//  def main(args: Array[String]): Unit = {
//    import org.apache.log4j._
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
//    val bsf = new BizvaneStreamingFunction(appName, seconds)
//    bsf.ssc.checkpoint(appName)
//    val kafkaParams = if (args.nonEmpty) params+("auto.offset.reset" -> "smallest") else params
//
//    InitEsData.initEsCorps(InitEsData.getEsCorps().map(i=>i._1).toVector)
//
//
//        val vipDStream=bsf.kafkaStringDStream(kafkaParams,Set("STANDARD_C_CLIENT_VIP"))
//    saveEs(vipDStream,bsf.sqlContext,"C_CLIENT_VIP")
//    saveEs(bsf.kafkaStringDStream(kafkaParams,Set("STANDARD_DIM_STORE")),bsf.sqlContext,"DIM_STORE")
//    saveEs(bsf.kafkaStringDStream(kafkaParams,Set("STANDARD_DIM_SKU")),bsf.sqlContext,"DIM_SKU")
//    saveEs(bsf.kafkaStringDStream(kafkaParams,Set("STANDARD_HR_EMPLOYEE")),bsf.sqlContext,"HR_EMPLOYEE")
////    bsf.kafkaStringDStream(kafkaParams,Set("STANDARD_FACT_RETAIL","EC_FACT_RETAIL")).repartition(100).foreachRDD(rdd=>{
////      if(!rdd.isEmpty()){
////        rdd.foreachPartition(it=>{
////          it.foreach(x=>{
////            val b = Random.nextInt(30)+65
////            val key = b.toChar.toString
////            BizKafkaProducer.send("fact_retail_test",key,x)
////          })
////        })
////      }
////    })
////    saveEs(bsf.kafkaStringDStream(kafkaParams,Set("fact_retail_test")).repartition(100),bsf.sqlContext,"FACT_RETAIL")
//    saveEs(bsf.kafkaStringDStream(kafkaParams,Set("STANDARD_FACT_RETAIL","EC_FACT_RETAIL")).repartition(100),bsf.sqlContext,"FACT_RETAIL")
//
//
////    val fr = bsf.kafkaDStream(kafkaParams,Set("STANDARD_FACT_RETAIL"))
////    testStreaming(fr,"STANDARD_FACT_RETAIL")
////    testStreaming(bsf.kafkaDStream(kafkaParams,Set("STANDARD_C_CLIENT_VIP")),"STANDARD_C_CLIENT_VIP")
////    testStreaming(bsf.kafkaDStream(kafkaParams,Set("STANDARD_C_VOUCHERS")),"STANDARD_C_VOUCHERS")
//
//    bsf.ssc.start()
//    bsf.ssc.awaitTermination()
//  }
//
//  def testStreaming(fr:DStream[(String, String)],topic:String)={
//    fr.foreachRDD(rdd=>{
//      if(!rdd.isEmpty()){
//        println(topic+" partitions===>"+rdd.partitions.map(i=>i.index).toVector)
//        val a = rdd.mapPartitions(it=>{
//          val map = new java.util.HashMap[String,Int]
//          var r1 = 0
//          it.foreach(x=>{
//            val old = map.get(x._1)
//            r1 = r1 + 1
//            map.put(x._1, if (old == null) 1 else old+1)
//            (x._1,1)
//          })
//          Iterator((r1,map))
//        })
//        a.collect().foreach(println)
//      }
//
//    })
//  }
//
//  import org.apache.spark.sql.functions._
//  val corpLow: (String => String) = (corp: String) => corp.toLowerCase
//  val corpLowFunc= udf(corpLow)
//  val map = InitEsData.initMap ++ InitEsData.upsert
//
//
//  def saveEs(dstream:DStream[String],sqLContext: SQLContext,table:String)={
//    dstream.foreachRDD(rdd=>
//      if(! rdd.isEmpty()){
//        var data = sqLContext.read.json(rdd)//.filter("CORP_ID='C10238'")
//        val esCorps = InitEsData.getEsCorps().map(s=>s._1).toVector
//        val filterEsCorp :(String => Boolean) = (corp:String)=>esCorps.contains(corp)
//        val udfFilter = udf(filterEsCorp)
//
//        data = data.filter(udfFilter(new org.apache.spark.sql.Column("CORP_ID")))
//
//        data = data.withColumn("corp_lower",corpLowFunc(new org.apache.spark.sql.Column("CORP_ID")))
//        InitEsData.saveData(data,table,"{corp_lower}",initMap_this = map)
//      }else  println(table+" No Data to Save:"+System.currentTimeMillis()))
//  }
//}
