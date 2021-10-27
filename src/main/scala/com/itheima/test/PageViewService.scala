package com.itheima.test

import java.util
import java.util.UUID

import com.itheima.bean.{PageViewsBeanCase, WebLogBean}
import com.itheima.util.DateUtil
import org.apache.spark.RangePartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PageViewService {


  def savePageViewToHdfs(filterStaticWeblogRdd: RDD[WebLogBean]) = {
    //抽取数据中uid+time_local作为key,先进行sortbykey进行排序，然后使用rangepartitioner进行重新分区
    //(uid&time,1)
    val tupleRdd: RDD[(String, WebLogBean)] = filterStaticWeblogRdd.map(bean => (bean.guid + "&" + bean.time_local, bean)).sortByKey()
    //使用rangepartitioner进行重新分区
    val rangeTupleRdd: RDD[(String, WebLogBean)] = tupleRdd.partitionBy(new RangePartitioner(1000, tupleRdd))
    //使用集合类型累加器,使用这种方式获取到正在运行的sparksession对象
    val spark: SparkSession = SparkSession.getDefaultSession.get
    val headTailListAcc: CollectionAccumulator[(String, String)] = spark.sparkContext.collectionAccumulator[(String, String)]("headTailList")
    //第一遍生成sessionid,准备一个累加器：盛放每个分区的第一条和最后一条数据
    //(uidtime,sessionid,step,staylong)
    val uidTimeSessionStepLongRdd: RDD[( WebLogBean, String, Int, Long)] = generateSessionid(rangeTupleRdd, headTailListAcc)
    uidTimeSessionStepLongRdd.cache() //缓存防止累加器中数据重复添加
    uidTimeSessionStepLongRdd.count() //触发计算

    //累加器中要有数据必须是触发计算之后，累加器还要小心数据重复添加的问题，记得调用cache算子缓存起来，触发一个action
    val headTailList: util.List[(String, String)] = headTailListAcc.value //driver端运行  元素：（string，string）:index+seq,uidTime
    println("累加器中数据："+headTailList)
    import collection.JavaConverters._
    val buffer: mutable.Buffer[(String, String)] = headTailList.asScala

//转为可变的map：这个map中存放的就是每个分区的首尾记录
    val map: mutable.HashMap[String, String] = mutable.HashMap(buffer.toMap.toSeq:_*)


    //判断哪些分区边界是有问题的;index+last-->timediff;index+first-->correctsession+correctstep+questionsessionid
    val questionBoundaryMap: mutable.HashMap[String, String] = newProcessBoundarySession(map)

    println("有边界问题的分区信息："+questionBoundaryMap)
    println("再次打印累加器中数据："+map)
    //什么时候使用广播变量？小表join大表-》广播小表；driver端准备了数据而在executor需要使用这些数据这时候就需要考虑广播出去这些数据
    //我们要根据questionBoundaryMap中的数据去修改uidTimeSessionStepLongRdd中的数据
    val questionBroadCast: Broadcast[mutable.HashMap[String, String]] = spark.sparkContext.broadcast(questionBoundaryMap)
    //经过修复过后的正确的rdd数据，（uidtime,sessionid,step,staylong）
    val correctRdd: RDD[(WebLogBean, String, Int, Long)] = repairBoundarySession(uidTimeSessionStepLongRdd, questionBroadCast)

    val pageviewRdd: RDD[PageViewsBeanCase] = correctRdd.map(
      t => {


        PageViewsBeanCase(
          t._2, t._1.remote_addr, t._1.time_local, t._1.request, t._3, t._4,
          t._1.http_referer, t._1.http_user_agent, t._1.body_bytes_sent, t._1.status, t._1.guid
        )
      }
    )
    pageviewRdd.saveAsTextFile("/pageviewrddtxt")
    //保存起来
    import spark.implicits._
    val pageviewDataSet: Dataset[PageViewsBeanCase] = pageviewRdd.toDS()

//    pageviewDataSet.write.mode("append").parquet("/pageviewcaserdd/")


  }

  //修复我们rdd边界处的数据
  def repairBoundarySession(uidTimeSessionStepLongRdd: RDD[( WebLogBean, String, Int, Long)],
                            questionBroadCast: Broadcast[mutable.HashMap[String, String]]) = {
    //key:index/first/last,value:last-->timediff,first-->correctsessionid+correctstep+quesitonsessionid
    val questionMap: mutable.HashMap[String, String] = questionBroadCast.value
    val correctRdd: RDD[(WebLogBean, String, Int, Long)] = uidTimeSessionStepLongRdd.mapPartitionsWithIndex(
      (index, iter) => {
        //uid&time,sessionid,step,staylong
        var orginList = iter.toList
        val firstLine: String = questionMap.getOrElse(index + "&first", "")
        val lastLine: String = questionMap.getOrElse(index + "&last", "")
        if (lastLine != "") {
          //当前这个分区最后一条数据他的停留时长需要修改
          val buffer: mutable.Buffer[(WebLogBean, String, Int, Long)] = orginList.toBuffer
          val lastTuple: (WebLogBean, String, Int, Long) = buffer.remove(buffer.size - 1) //只修改停留时长
          buffer += ((lastTuple._1, lastTuple._2, lastTuple._3, lastLine.toLong))
          orginList = buffer.toList
        }

        if (firstLine != "") {
          //分区第一条数据有问题，则需要修改：按照错误的sessionid找到所有需要修改的数据，改正sessionid和step
            val firstArr: Array[String] = firstLine.split("&")
          val tuples: List[(WebLogBean, String, Int, Long)] = orginList.map {
            t => {
              if (t._2.equals(firstArr(2))) {
                //错误的sessionid,修改为正确的sessionid和步长
                (t._1, firstArr(0), firstArr(1).toInt + t._3.toInt, t._4)
              } else {
                t
              }
            }
          }
          orginList=tuples
        }
        orginList.iterator
      }
    )
    correctRdd

  }



  //根据累加器中的数据来判断哪些边界有问题，处理一个会话落在多个分区的现象，累加器收集的数据改变：每个分区的最后一条
  //记录添加上当前分区的数量
  def newProcessBoundarySession(map: mutable.HashMap[String, String]) = {

    //准备一个map接收确实有问题的边界数据: uid,sessionid,step,sessionid
    val boundaryMap: mutable.HashMap[String, String] = new mutable.HashMap[String,String]()
    for (num <- 1 until (map.size/2 )) {
      //从1分区开始到最后一个分区
        //每次取到当前遍历序号的
        //获取到某个分区的首尾记录
      //&first,0381971c-e065-402a-84dd-f7b33a80c732&2019-12-01 00:13:36&e4f66780-67ae-47b7-88d5-795778640e82
      var numFirstMsg= map.get(num + "&first").get
      //&last,0381971c-e065-402a-84dd-f7b33a80c732&2019-12-01 00:20:25&e4f66780-67ae-47b7-88d5-795778640e82&723&723
      var numLastMsg= map.get(num + "&last").get
      //获取到下个分区的第一条记录和上个分区的最后一条记录
//      val nextPartFirstMsg: String = map.get((num+1)+"&first").get
      //获取上个分区的最后一条记录
      val lastPartLastMsg: String = map.get((num-1)+"&last").get
      //比较判断当前这个分区的第一条记录与上个分区的最后一条记录是不是同个会话
      val numFirstArr: Array[String] = numFirstMsg.split("&")
      val numLastArr: Array[String] = numLastMsg.split("&")
//      val nextPartFirstArr: Array[String] = nextPartFirstMsg.split("&")
      val lastPartLastArr: Array[String] = lastPartLastMsg.split("&")
      //比较当前分区与上个分区边界处是否存在问题
      if (numFirstArr(0).equals(lastPartLastArr(0))){
        //说明是同个用户，比较时间差是否小于30分钟
        val timeDiff: Long = DateUtil.getTimeDiff(lastPartLastArr(1),numFirstArr(1))
        if (timeDiff <=30*60*1000){
          //说明本分区第一条记录与上个分区最后一条数据属于同个会话，则需要记录上个分区的step以及sessionid数据，记录下当前分区第一条应该
          //修改的信息：sessionid与step信息，上个分区要修改的页面停留时长信息
          //当前分区第一条记录要修改的正确信息:uid,time,正确sessionid,step,错误sessionid
          //先修改上个分区的最后一条记录的停留时长信息：改为时间差
          boundaryMap.put((num-1)+"&last",timeDiff.toString)
          //当前这个分区的第一条数据（当前分区的第一个session需要修改）：正确的sessionid,step,错误的sessionid
          //需要判断上个分区最后一条数据是否添加了正确的sessionid和步长信息
          if (lastPartLastArr.size >5){
            boundaryMap.put(num+"&first", lastPartLastArr(5)+"&"+(lastPartLastArr(6).toInt )+"&"+numFirstArr(2))

          }else{
            boundaryMap.put(num+"&first", lastPartLastArr(2)+"&"+(lastPartLastArr(3).toInt )+"&"+numFirstArr(2))
          }

          //是否需要更新最后一条数据的sessionid和步长信息需要进行判断
          if (numFirstArr(2).equals(numLastArr(2))){ //比较sessionid
            //更新最后一条数据的：为正确的sessionid和正确step数据，下个分区就可以获取到正确sessionid和步长数据
            if (lastPartLastArr.size >5){
              map.put(num+"&last",numLastMsg+"&"+lastPartLastArr(lastPartLastArr.size-2)+"&"+
                (lastPartLastArr(lastPartLastArr.size-1).toInt +numLastArr(4).toInt))
            }else{
              map.put(num+"&last",numLastMsg+"&"+lastPartLastArr(2)+"&"+
                (lastPartLastArr(3).toInt +numLastArr(4).toInt))
            }

          }
        }
      }



    }


    boundaryMap
  }


  //第一遍按照分区生成sessionid,(uid&time,1)
  def generateSessionid(rangeTupleRdd: RDD[(String, WebLogBean)],
                        headTailListAcc: CollectionAccumulator[(String, String)]) = {
//    val uidTimeRdd: RDD[String] = rangeTupleRdd.map(_._1)
    //针对每个分区生成sessionid,(uidtime,sessionid,step,staylong),此rdd中分区边界处数据是可能有问题的，
    //无需再把这个rdd遍历一遍，只需要把累加器中的数据进行判断比较即可
    val uidTimeSessionStepLongRdd: RDD[( WebLogBean, String, Int, Long)] = rangeTupleRdd.mapPartitionsWithIndex(
      (index, item) => {
        //index:分区编号，item:这个分区所有的数据组成一个迭代器
        val sortWeblogBeanList: List[(String, WebLogBean)] = item.toList
        //遍历集合生成sessionid:准备装生成的sessionid的结果集合：（uid&time,sessionid,step,staylong）
        val resultTupleList: ListBuffer[( WebLogBean, String, Int, Long)] =
          new ListBuffer[( WebLogBean, String, Int, Long)]()
        import scala.util.control.Breaks._
        var sessionid: String = UUID.randomUUID().toString
        var step = 1
        var page_staylong = 60000
        breakable {
          for (num <- 0 until (sortWeblogBeanList.size)) { //装有uid&time的list进行遍历
            //一条访问记录:uid&time
            val currentUidTime: (String, WebLogBean) = sortWeblogBeanList(num)
            val arr = currentUidTime._1.split("&")
            //uid
            var uid = arr(0)
            var time = arr(1)
            if (num == 0) {
              //这个分区的第一条数据，装载到累加器中,数据类型：（index&seq,uid&time）
              headTailListAcc.add((index + "&first", currentUidTime._1 + "&" + sessionid))
            }
            //如果只由一条记录
            if (sortWeblogBeanList.size == 1) {
              //之前输出，现在则需要保存起来最后一起输出
              resultTupleList += ((currentUidTime._2, sessionid, step, page_staylong))
              //重新生成sessionid
              sessionid = UUID.randomUUID().toString
              //跳出循环
              break
            }
            //数量不止一条，本条来计算上一条的时长
            breakable {
              if (num == 0) {
                //continue:中止本次，进入下次循环
                break()
              }
              //不是第一条数据，获取到上一条记录
              val lastUidTime: (String, WebLogBean) = sortWeblogBeanList(num - 1)
              val lastArr: Array[String] = lastUidTime._1.split("&")
              var lastUid = lastArr(0)
              var lastTime = lastArr(1)
              //判断的是两个uidTime的时间差值
              val timeDiff: Long = DateUtil.getTimeDiff(lastTime, time)
              //毫秒值是否小于30分钟
              if (lastUid.equals(uid) && timeDiff <= 30 * 60 * 1000) {
                //属于同个session，你们俩共用一个sessionid,输出上一条
                resultTupleList += ((lastUidTime._2, sessionid, step, timeDiff))
                //sessionid是否重新生成：不需要，step如何处理？
                step += 1

              } else {
                //不属于一个sessionid,如何处理？不管是否属于同个会话，我们输出的都是上一条记录
                //属于同个session，你们俩共用一个sessionid,输出上一条
                resultTupleList += ((lastUidTime._2, sessionid, step, page_staylong))
                //sessionid是否重新生成：需要，step如何处理？
                sessionid = UUID.randomUUID().toString
                //step重置为1
                step = 1
              }

              //最后一条需要我们控制输出
              if (num == sortWeblogBeanList.size - 1) {
                resultTupleList += ((currentUidTime._2, sessionid, step, page_staylong))
                //加载到累加器中
                headTailListAcc.add((index + "&last", currentUidTime._1 + "&" + sessionid + "&" + step+"&"+sortWeblogBeanList.size))
                //重新生成sessionid
                sessionid = UUID.randomUUID().toString
              }
            }

          }
        }

        //返回装有uidtime,sessionid,step,staylong的集合
        resultTupleList.iterator
      }
    )
    uidTimeSessionStepLongRdd

  }
}
