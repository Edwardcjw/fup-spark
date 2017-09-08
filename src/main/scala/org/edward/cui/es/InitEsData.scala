package org.edward.cui.es

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.edward.cui.es.trans.DFUtil
import org.edward.cui.es.trans.DFUtil._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._



/**
  * Created by Cui on 2017/7/6.
  */
object InitEsData extends Serializable{
  def main(args: Array[String]): Unit = {
    val corpf = args(0)
    val appName = "InitEsData-" + args.reduce((a,b)=>a+","+b)
    val path = s"hdfs://$host:8020/user/developer/Standard/" + corpf + "/"
    val corp = if(corpf.contains("EC")) corpf.substring(0,6) else corpf

    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)

//    initEsCorps(Vector(corp))
//    if(args.length>1){
//      args.drop(1).foreach(f=>initDimData(path,f,corp))
//    }else
//      initAllData(path,corp)

    sc.stop()
  }

  //  val (nodes, host) = ("192.168.1.23", "test3")//test
//  val (n1,n2,n3,n4,n5,n6,n7,n8)=("10.47.57.45","10.47.57.63","10.47.57.108","10.47.103.197","10.24.17.232","10.25.169.252","10.47.57.63","")
  val (n1,n2,n3,n4,n5,n6,n7,n8)=("172.16.200.157","172.16.200.156","172.16.200.155","172.16.200.158","172.16.50.250","10.25.169.252","10.47.57.63","")
  val (nodes, host) = (s"$n1,$n2,$n3,$n4", "m1")

  val initMap = Map(("es.nodes", nodes),("pushdown", "true"),("es.port", "9200"),("es.index.auto.create" , "true"),
    ("es.nodes.wan.only" , "true")/**,("es.mapping.id" , "ID"),(" es.resource.write","test/salesDetail")*/)
  val upsert = Map(ES_WRITE_OPERATION->ES_OPERATION_UPSERT)

  val idKey = "rid"
  val fType = Map(
    "DIM_STORE" -> Map("index" -> "dim_store", "dest" -> "CORP","uniKey"->"STORE_ID,CORP_ID#T_IN","addColList"->s"""[["CORP_ID,STORE_ID",",","$idKey","_"]]"""),
    "DIM_SKU" -> Map("index" -> "dim_sku", "dest" -> "CORP","uniKey"->"SKU_ID,CORP_ID#T_IN","addColList"->s"""[["CORP_ID,SKU_ID",",","$idKey","_"]]"""),
    "C_CLIENT_VIP" -> Map("index" -> "vipprofile", "dest" -> "ALL","uniKey"->"VIP_ID,CORP_ID#T_IN","addColList"->s"""[["CORP_ID,VIP_ID",",","$idKey","_"],["T_CR_Y,T_CR_M,T_CR_D",",","T_CR_DATE","/"],["T_VB_Y,T_VB_M,T_VB_D",",","T_VB_DATE","/"],["T_DL_Y,T_DL_M,T_DL_D",",","T_DL_DATE","/"]]"""),
    "FACT_RETAIL" -> Map("index" -> "sales_detail", "dest" -> "CORP","uniKey"->"ID,CORP_ID#T_IN","checkCols"->"EMP_ID,ID,CORP_ID,NUM_SALES,AMT_TRADE,AMT_SUG,ORDER_SOURCE", "doubleCols" -> "NUM_SALES,AMT_TRADE,AMT_SUG","addColList"->s"""[["CORP_ID,ID,EMP_ID",",","$idKey","_",""],["T_BL_Y,T_BL_M,T_BL_D",",","T_BL_DATE","/"]]"""),
    "HR_EMPLOYEE" -> Map("index" -> "dim_emp", "dest" -> "CORP","uniKey"->"EMP_ID,CORP_ID#T_IN","addColList"->s"""[["CORP_ID,EMP_ID",",","$idKey","_"]]"""),
    "VIP_SALES_DETAIL" -> Map("index" -> "vipprofile", "dest" -> "ALL","addColList"->s"""[["CORP_ID,VIP_ID",",","$idKey","_"]]""")
  )


//  def initAllData(space:Space,corp:String){
//    Vector("HR_EMPLOYEE","DIM_STORE","DIM_SKU", "C_CLIENT_VIP","FACT_RETAIL").foreach(f => initDimData(space, f, corp))
//  }
//
//  def initDimData(space:Space,f:String,corp:String){
//    try{
//      val df = space.sqlContext.read.json(space.folder + f+"/*.json").filter(s"CORP_ID='$corp'").repartition(200)
//      saveData(df,f,corp)
//    }catch {case e:Exception=>println(s"saveToEs error $corp,$f");e.printStackTrace()}
//  }

  def saveData(dataFrame: DataFrame,f:String,corp:String,initMap_this:Map[String,String]= initMap,castString:Boolean=true){
    var df = dataFrame
    try{
      val t1 = System.currentTimeMillis()
        println(s"saveToEs begin ,$f, map:$initMap_this"+"\t"+new Date(t1))

      if(fType.contains(f)){
        if(castString)
          df = df.select(df.columns.map(c=>df.col(c).cast("string")):_*)

        val fMap = fType(f)

        if(fMap.contains("checkCols")){
          val checkCols = DFUtil.keyToArr(fMap("checkCols"),",")
          df = DFUtil.checkColumn(df,checkCols)
        }

        //drop duplicate
        if(fMap.contains("uniKey")){
          val uKey = DFUtil.keyToArr(fMap("uniKey"),"#")
          df = DFUtil.dropDuplicate(df,DFUtil.keyToArr(uKey(0),","),DFUtil.keyToArr(uKey(1),","))
        }

        if(fMap.contains("doubleCols")){
          val vec = DFUtil.keyToArr(fMap("doubleCols"),",")
          val checkDouble :(String => Boolean) = (ns:String)=>try{String.valueOf(ns).toDouble;true}catch{case _:Exception=>false}
          val udfCheckDouble = udf(checkDouble)
          df = df.filter(vec.map(s=>udfCheckDouble(new org.apache.spark.sql.Column(s))).reduce((a,b)=>a.and(b)))
          df = df.select(df.columns.map(c=>if(vec.contains(c)) df.col(c).cast("double") else new Column(c)) :_*)
        }

        if (f == "FACT_RETAIL") df = factRetail(df,corp,initMap_this)

        if(fMap.contains("addColList")){
//          val addColList = JSON.parseFull(fMap("addColList")).get.asInstanceOf[List[List[String]]]
//          df = DFUtil.addConcatColumns(df,addColList)
        }

        if(f == "FACT_RETAIL")       //update vipRetail
          vipRetail(df,corp,initMap_this)

        var saveMap = initMap_this
        val esMapId = if(fMap.contains("es.mapping.id")) fMap("es.mapping.id") else s"$idKey"

        saveMap = saveMap ++ Map("es.mapping.id" -> esMapId)
        var index = fMap("index")
        var dest = fMap("dest")
        if (dest == "CORP") index = index + "_" + corp
        else if (dest == "ALL") dest = corp
        val resouce = index+"/"+dest
        df.saveToEs(resouce.toLowerCase(),saveMap)
        val t2 = System.currentTimeMillis()
        println(s"saveToEs success,$resouce"+"\t"+new Date(t2)+"\t cost:"+(t2-t1))
      }
    }catch {case e:Exception=>println(s"saveToEs error $corp,$f");e.printStackTrace()}
  }

  def factRetail(dataFrame: DataFrame,corp:String,initMap_this:Map[String,String]= initMap)={
      var df = dataFrame.drop("NUM_TRADE")
      val sqlContext = df.sqlContext
      val schema = df.schema
      val conInd = Vector("ORDER_ID","CORP_ID").map(c=>try{schema.fieldIndex(c)}catch {case _:Exception=> -1}).filter(i=>i>=0)
      val numSalesInd = schema.fieldIndex("NUM_SALES")
      val amtSugInd = schema.fieldIndex("AMT_SUG")
      val amtTradeInd = schema.fieldIndex("AMT_TRADE")
      val empIdInd = schema.fieldIndex("EMP_ID")
      val escaleInd = try{schema.fieldIndex("EMP_SCALE")}catch {case _:Exception=> -1}

      val d1 = df.mapPartitions(it=>it.map(r => (conInd.map(i=>r(i)), Vector(r)))).reduceByKey((a, b) => a ++ b)
        .mapPartitions(it=>{
          it.flatMap(x => {
            var totalSales = 0d
            x._2.foreach(r => totalSales += {
              try {
                String.valueOf(r(numSalesInd)).toDouble
              } catch {
                case _: Exception => 0d
              }
            })
            x._2.flatMap(r => {
              val indexes = Vector(numSalesInd, amtSugInd, amtTradeInd)
              val rNumSales = {
                try {
                  String.valueOf(r(numSalesInd)).toDouble
                } catch {
                  case _: Exception => 0d
                }
              }
              val num_trade = if (Math.abs(totalSales) < 0.01) 0d else try {
                rNumSales / Math.abs(totalSales)
              } catch {
                case _: Exception => 0d
              }
              val empVec = try{DFUtil.keyToArr(r(empIdInd).toString(), ",")}catch {case _:Exception=> Array(Em)}
              val numEmp = empVec.length
              var escale = if (escaleInd < 0) null else try {
                DFUtil.keyToArr(r(escaleInd).toString, ",").map(i => try{i.toDouble}catch {case _:Exception=>0d})
              } catch {
                case _: Exception => null
              }
              if (escale != null && escale.length != empVec.length) escale = null
              empVec.indices.map(ei => {
                val emp = empVec(ei)
                val seq = r.toSeq
                val newSeq = seq.indices.map(i => {
                  if (indexes.contains(i)) {
                    val numC = try{String.valueOf(seq(i)).toDouble}catch{case _:Exception=>0d}
                    if (escale == null)  numC / numEmp else numC * escale(ei)
                  } else if (empIdInd == i) emp
                  else seq(i)
                })
                val nt = if (escale == null) num_trade / numEmp else num_trade * escale(ei)
                Row.fromSeq(newSeq ++ Seq(nt, String.valueOf(ei)))
              })
            })
          })
        })
      df = sqlContext.createDataFrame(d1, schema.add(StructField("NUM_TRADE", DoubleType, false)).add(StructField("E_INDEX", StringType, true)))
    df
  }

  def vipRetail(dataFrame: DataFrame,corp:String,initMap_this:Map[String,String]= initMap)={
    val df = dataFrame
    val sqlContext = df.sqlContext
    val schema = df.schema
    //vip salesdetail
    val vipKey:Vector[String]=Vector("VIP_ID","CORP_ID","corp_lower")
    val vipKeys = vipKey.map(c=>(try{schema.fieldIndex(c)}catch {case _:Exception=> -1},c)).filter(i=>i._1>=0)
    val vipKeyInd = vipKeys.map(c=>c._1)
    val vipKeySchema = vipKeys.map(c=>StructField(c._2,StringType,false))
    try{
      val isStreaming = try{schema.fieldIndex("corp_lower")>=0}catch {case _:Exception=>false}

      val nowVipSales = df.filter("VIP_ID!='~!@#$%&*'").mapPartitions(it=>it.map(x=>{
        (vipKeyInd.map(i=>x(i)),Vector(x))
      })).reduceByKey((a,b)=>a++b)

//      val vipSales = checkSaveVipRetail(nowVipSales,vipKeys,schema,isStreaming)

//      val vipSalesDetail = vipSales.mapPartitions(it=>it.map(x=>Row.fromSeq(x._1:+x._2.toArray)))
//      val vipSalesDetailDf = sqlContext.createDataFrame(vipSalesDetail,StructType(vipKeySchema:+StructField("sales_detail",ArrayType(df.schema,false))))
//      val saveVSDMap = initMap_this ++ Map(ES_WRITE_OPERATION->ES_OPERATION_UPSERT,ES_READ_FIELD_AS_ARRAY_INCLUDE->"nested.sales_detail")
//      saveData(vipSalesDetailDf,"VIP_SALES_DETAIL",corp,saveVSDMap,false)
    }catch {case e:Exception=> println("VIP_SALES_DETAIL error=>"+e.getMessage);e.printStackTrace()}
  }

//  def checkSaveVipRetail(nowVipSales: org.apache.spark.rdd.RDD[(Vector[Any], Vector[Row])],vipKeys:Vector[(Int, String)],salesSchema:StructType,isStreaming:Boolean):org.apache.spark.rdd.RDD[(Vector[Any], Vector[Row])]={
//    val salesIdCol = Vector("ID","EMP_ID")
//    val salesIdInd = salesIdCol.map(c=>salesSchema.fieldIndex(c))
//    val salesCols = salesSchema.fields.map(_.name)
//    val corpVipInd = Vector("CORP_ID","VIP_ID").map(s=>{
//      var sInd = -1
//      for(i<- vipKeys.indices if sInd == -1){
//        val keyI = vipKeys(i)
//        if(keyI._2==s) sInd = i
//      }
//      sInd
//    })
//    println("nowVipSales show 2==========>")
//    nowVipSales.take(2).foreach(println)
//
//    val name = "EsVipRetail"
//    //check
//    val res = if(!isStreaming) nowVipSales else
//      nowVipSales.mapPartitions(it=>{
//        val tn = TableName.valueOf(TRv2(name))
//        val table = Saving.conn.getTable(tn)
//
//        val r1 = it.map(x=>{
//          val vipProIdVed = corpVipInd.map(i=>String.valueOf(x._1(i)))
//          val vipProId = vipProIdVed.reduce((a,b)=>a+"_"+b)
//
//          val g = new Get(vipProId.getBytes)
//          val cells = table.get(g).rawCells()
//          val oldList = cells.map(i=>{
//              val cellMap:Map[String,Any] = try{
//                val col = JSON.parseFull(Bytes.toString(CellUtil.cloneQualifier(i))).get.asInstanceOf[Map[String, String]]
//                val value = JSON.parseFull(Bytes.toString(CellUtil.cloneValue(i))).get.asInstanceOf[Map[String, Any]]
//                col ++ value
//              }catch {case _:Exception=>Map.empty}
//              cellMap
//            }).filter(m=>m.nonEmpty).toList
//
//          //      val corp = vipProIdVed(0).toLowerCase()
//          //      val oldList:List[Map[String,Any]] = try{
//          //        val mes = scala.io.Source.fromURL(s"http://$n1:9200/vipprofile/$corp/$vipProId/_source/?_source=nested.sales_detail").mkString
//          //        val result = JSON.parseFull(mes).get.asInstanceOf[Map[String,Map[String,List[Map[String,Any]]]]]
//          //        result("nested")("sales_detail")
//          //      }catch {case e:Exception=> e.printStackTrace();List.empty }
//
//          var vec = x._2
//          val nSalesId = x._2.map(sr=>salesIdInd.map(i=>String.valueOf(sr(i))).reduce((a,b)=>a+"_"+b))
//          oldList.foreach(osr=>{
//            val sid = salesIdCol.map(c=>try{String.valueOf(osr(c))}catch {case _:Exception=>""}).reduce((a,b)=>a+"_"+b)
//            if(!nSalesId.contains(sid)){
//              val orow = Row.fromSeq(salesCols.map(col=>try{osr(col)}catch {case _:Exception=>null}).toSeq)
//              vec = vec:+orow
//            }
//          })
//          (x._1,vec)
//        })
//        try{table.close()}catch {case _:Exception=>}
//        r1
//      })
//
//    //save
//    println("Running WriteHbase to table: "+TRv2(name))
//    val job = new Job(Saving.conf)
//    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
//    job.setOutputValueClass(classOf[Result])
//    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
//    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, TRv2(name))
//    nowVipSales.mapPartitions(it=>{
//      it.map(x=>{
//        val vipProIdVed = corpVipInd.map(i=>String.valueOf(x._1(i)))
//        val vipProId = vipProIdVed.reduce((a,b)=>a+"_"+b)
//        val p = new Put(vipProId.getBytes)
//        x._2.foreach(sr=>{
//          val col = JSONObject(salesIdCol.zip(salesIdInd.map(i=>String.valueOf(sr(i)))).toMap).toString()
//          val value = JSONObject(salesCols.zip(sr.toSeq).filter(i=>i._2!=null).toMap).toString()
//          p.addColumn("basic".getBytes,col.getBytes,value.getBytes)
//        })
//        (new ImmutableBytesWritable, p)
//      })
//    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
//    println("Saving Successful")
//
//    res
//  }

//
//  def checkEsCorp(corpId:String)={
//    try{
//      getEsCorps.contains(corpId)
//    }catch {case _:Exception=>false}
//  }
//  def getEsCorps()={
//    try{
//      val esConfTab = Saving.conn.getTable(TableName.valueOf(TRv2("EsCorp")))
//      val g = new Get("CORP_ID".getBytes)
//      val r = esConfTab.get(g).rawCells().map(c=>(Bytes.toString(CellUtil.cloneQualifier(c)),Bytes.toString(CellUtil.cloneValue(c)))).toMap
//      esConfTab.close()
//      r
//    }catch {case _:Exception=>Map.empty[String,String]}
//  }
//  def initEsCorps(corpIds:Seq[String]): Unit ={
//    try{
//      val esConfTab = Saving.conn.getTable(TableName.valueOf(TRv2("EsCorp")))
//      val p = new Put("CORP_ID".getBytes)
////      val rmes1 = sendEsHttp(s"http://$n1:9200/vipprofile","")
////      println(s"create index vipprofile res:$rmes1")
//
//      corpIds.foreach(corpId=>{
//        val corp = corpId.toLowerCase()
////        val mes = sendEsHttp(s"http://$n1:9200/vipprofile/_mapping/$corp/",s"""{"properties":{"sales_detail":{"type":"nested"}}}""")
////        println(s"initEsCorp corp:$corpId \t mes:$mes")
//        p.addColumn("basic".getBytes ,corpId.getBytes,corpId.getBytes)
//      })
//      esConfTab.put(p)
//      esConfTab.close()
//    }catch {case _:Exception=>}
//  }
//
//  def sendEsHttp(urlStr:String,httpReqMes:String,method:String="PUT")={
//    var mes = ""
//    var httpCon:HttpURLConnection = null
//    try{
//      val url = new URL(urlStr)
//      httpCon = url.openConnection().asInstanceOf[HttpURLConnection]
//      httpCon.setRequestMethod("PUT")
//      if(StringUtils.isNotEmpty(httpReqMes)){
//        httpCon.setDoOutput(true)
//        val out = new OutputStreamWriter(httpCon.getOutputStream())
//        out.write(httpReqMes)
//        out.close()
//      }
//    }catch {case e :Exception=> mes = e.getMessage;e.printStackTrace()}
//    try{mes = scala.io.Source.fromInputStream(httpCon.getInputStream()).mkString}catch {case e:Exception=>println("getMes err:"+e.getMessage)}
//    mes
//  }

}
