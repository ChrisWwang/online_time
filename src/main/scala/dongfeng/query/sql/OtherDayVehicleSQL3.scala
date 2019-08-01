package dongfeng.query.sql

import dongfeng.query.tips.Daynum

object OtherDayVehicleSQL3 {


  //TODO 测试给sql传递动态参数---------------------
  val num: Int = Daynum.getNum()
  val num_add1: Int = num + 1
  val num_less1: Int = num - 1

  //TODO ---------------------------------------------


  lazy val driver_online_record_time_original =
    s"""
       |select
       |driverID as driver_id,
       |unix_timestamp(createTime) as time ,
       |state
       |from
       |driver_online_record
       |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(createTime)) = ${num}
    """.stripMargin

  // 获取mongoDB前一天的数据,使用driverID分组,排序分组内的时间
  lazy val driver_online_record_time_original_0_3000 =
    """
      |select
      |driver_id,
      |time ,
      |state
      |from
      |driver_online_record_time_original
      |where driver_id < 3000
    """.stripMargin
  //where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(createTime)) = 1


  lazy val driver_online_record_time_original_3000_6000 =
    """
      |select
      |driver_id,
      |time ,
      |state
      |from
      |driver_online_record_time_original
      |where driver_id >= 3000
      |and driver_id < 5000
    """.stripMargin


  // 当前未启用
  lazy val driver_online_record_time_original_4000_5000 =
    """
      |select
      |driver_id,
      |time ,
      |state
      |from
      |driver_online_record_time_original
      |where driver_id >= 3000
      |and driver_id < 5000
    """.stripMargin


  lazy val driver_online_record_time_original_6000_ =
    """
      |select
      |driver_id,
      |time ,
      |state
      |from
      |driver_online_record_time_original
      |where driver_id >= 5000
    """.stripMargin

  // 过滤order_info
  lazy val order_info_filter =
    s"""
       |select
       |driver_id,
       |TO_DATE(close_gps_time) close_gps_time,
       |TO_DATE(begin_time) begin_time,
       |UNIX_TIMESTAMP(close_gps_time) close_gps_time_s,
       |UNIX_TIMESTAMP(begin_time) begin_time_s
       |from (
       |select
       |driver_id,
       |close_gps_time,
       |begin_time
       |from
       |order_info
       |) tb
       |where
       |DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = $num_less1
       |or DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = $num
       |or DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(begin_time)) = $num
       |or DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(begin_time)) = $num_add1
    """.stripMargin


  //
  lazy val driver_online_record_time_state_0 =
    """
      |select
      |driver_id,
      |time,
      |state
      |from (
      |select
      |driver_id,
      |time,
      |state,
      |row_number() over(partition by driver_id,time order by time) as rank
      |from
      |driver_online_record_time_original
      |where
      |state = '0'
      |)
      |where
      |rank = '1'
    """.stripMargin


  //
  lazy val driver_online_record_time_state_1 =
    """
      |select
      |driver_id,
      |time,
      |state
      |from (
      |select
      |driver_id,
      |time,
      |state,
      |row_number() over(partition by driver_id,time order by time) as rank
      |from
      |driver_online_record_time_original
      |where
      |state = '1'
      |)
      |where
      |rank = '1'
    """.stripMargin


  //
  lazy val driver_online_record_time_all =
    """
      |select
      |*
      |from
      |driver_online_record_time_state_0
      |union all
      |select
      |*
      |from
      |driver_online_record_time_state_1
    """.stripMargin


  // 获取mongoDB前一天的数据,使用driverID分组,排序分组内的时间
  lazy val driver_online_record_time_filter =
    """
      |select
      |driver_id,
      |time
      |from (
      |select
      |driver_id ,
      |time,
      |count(1) count
      |from
      |driver_online_record_time_all
      |group by driver_id,time
      |)
      |where count > 1
    """.stripMargin


  //
  lazy val driver_online_record_time_rank =
    """
      |select
      |driver_id,
      |time,
      |state,
      |row_number() over(partition by driver_id order by time) as rank
      |from (
      |select
      |t1.driver_id driver_id,
      |t1.time time,
      |t1.state state
      |from
      |driver_online_record_time_all t1
      |left join
      |driver_online_record_time_filter t2
      |on t1.driver_id = t2.driver_id
      |and t1.time = t2.time
      |where
      |t2.driver_id is null
      |and
      |t2.time is null
      |) tb
    """.stripMargin
  //where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(createTime)) = 1


  // 倒排分组内的时间
  lazy val driver_online_record_time_rank_desc =
    """
      |select
      |driver_id ,
      |time,
      |state,
      |row_number() over(partition by driver_id order by time desc) as rank
      |from
      |driver_online_record_time_rank
    """.stripMargin


  // 将排序表自连接，连接条件t1.driver_id = t2.driver_id and t1.rank + 1 = t2.rank and t1.state - 1 = t2.state,用t2.time-t1.time得到online_time
  lazy val driver_online_record_sameday =
    """
      |select
      |t1.driver_id ,
      |t2.time - t1.time online_time
      |from
      |driver_online_record_time_rank t1
      |join
      |driver_online_record_time_rank t2
      |where t1.driver_id = t2.driver_id
      |and t1.rank + 1 = t2.rank
      |and t1.state - 1 = t2.state
    """.stripMargin


  // 筛选出顺排组内rank=1并且state为0的数据，此数据为跨前一天数据,用time-当天凌晨时间戳
  lazy val driver_online_record_yesterday =
    s"""
       |select
       |driver_id ,
       |time,
       |time - UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),$num),'yyyy-MM-dd') as online_time
       |from
       |driver_online_record_time_rank
       |where rank = 1
       |and state = 0
    """.stripMargin


  // 筛选出时间倒排组内rank=1并且state为1的数据，此数据为跨后一天数据,用当天最后时间戳-time
  lazy val driver_online_record_nextday =
    s"""
       |select
       |driver_id ,
       |time,
       |UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),$num_less1),'yyyy-MM-dd') - time as online_time
       |from
       |driver_online_record_time_rank_desc
       |where rank = 1
       |and state = 1
    """.stripMargin


  //  lazy val driver_online_record_time_theday =
  //    """
  //      |select
  //      |tb1.driver_id driver_id,
  //      |tb1.time time,
  //      |tb1.state state
  //      |from (
  //      |select
  //      |t1.driver_id driver_id,
  //      |t1.time time,
  //      |t1.state state
  //      |from
  //      |driver_online_record_time_rank t1
  //      |left join
  //      |driver_online_record_yesterday t2
  //      |on t1.driver_id = t2.driver_id
  //      |and t1.time = t2.time
  //      |where
  //      |t2.driver_id is null
  //      |and
  //      |t2.time is null
  //      |) tb1
  //      |left join
  //      |driver_online_record_nextday tb2
  //      |on tb1.driver_id = tb2.driver_id
  //      |and tb1.time = tb2.time
  //      |where
  //      |tb2.driver_id is null
  //      |and
  //      |tb2.time is null
  //    """.stripMargin
  //
  //
  //
  //  lazy val driver_online_record_sameday =
  //    """
  //      |select
  //      |t1.driver_id driver_id,
  //      |t1.state0_time - t2.state1_time online_time
  //      |from
  //      |(
  //      |select
  //      |driver_id,
  //      |sum(time) state0_time
  //      |from
  //      |driver_online_record_time_theday
  //      |where state = 0
  //      |group by driver_id
  //      |) t1
  //      |left join
  //      |(
  //      |select
  //      |driver_id,
  //      |sum(time) state1_time
  //      |from
  //      |driver_online_record_time_theday
  //      |where state = 1
  //      |group by driver_id
  //      |) t2
  //      |on t1.driver_id = t2.driver_id
  //    """.stripMargin


  // 得到hbase order_info中create_time与close_gps_time都是当天的数据，即非跨天单数据
  lazy val order_info_sameday =
    s"""
       |select
       |driver_id,
       |close_gps_time_s - begin_time_s as online_time
       |from
       |order_info_filter
       |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),close_gps_time) = $num
       |and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),begin_time) = $num
       |and close_gps_time_s - begin_time_s > 0
    """.stripMargin
  // where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 1
  // and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 1


  // 得到create_time是前一天,并且close_gps_time是当天的数据，即为跨前一天单数据
  lazy val order_info_yesterday =
    s"""
       |select
       |driver_id,
       |close_gps_time_s - UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),$num),'yyyy-MM-dd') as online_time
       |from
       |order_info_filter
       |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),close_gps_time) = $num
       |and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),begin_time) = $num_add1
    """.stripMargin

  // where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 1
  // and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 2


  // 得到create_time是当天,并且close_gps_time是后一天的数据，即为跨后一天单数据
  lazy val order_info_nextday =
    s"""
       |select
       |driver_id,
       |UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),$num_less1),'yyyy-MM-dd') - begin_time_s as online_time
       |from
       |order_info_filter
       |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),close_gps_time) = $num_less1
       |and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),begin_time) = $num
    """.stripMargin

  // where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 0
  // and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 1


  lazy val driver_online_mysql_oneday =
    """
      |select
      |*
      |from
      |order_info_sameday
      |union all
      |select
      |*
      |from
      |order_info_yesterday
      |union all
      |select
      |*
      |from
      |order_info_nextday
    """.stripMargin


  lazy val driver_online_mongo_oneday =
    """
      |select
      |*
      |from
      |driver_online_record_sameday
      |union all
      |select
      |driver_id,
      |online_time
      |from
      |driver_online_record_yesterday
      |union all
      |select
      |driver_id,
      |online_time
      |from
      |driver_online_record_nextday
    """.stripMargin


  lazy val driver_online_mongo_oneday_all =
    """
      |select
      |*
      |from
      |driver_online_mongo_oneday_0_3000
      |union all
      |select
      |*
      |from
      |driver_online_mongo_oneday_3000_6000
      |union all
      |select
      |*
      |from
      |driver_online_mongo_oneday_6000_
    """.stripMargin


  lazy val driver_online_oneday =
    """
      |select
      |*
      |from
      |driver_online_mongo_oneday
      |union all
      |select
      |*
      |from
      |driver_online_mysql_oneday
    """.stripMargin


  // 将六个表数据聚合
  lazy val order_info_oneday =
    """
      |select
      |*
      |from
      |driver_online_record_sameday
      |union all
      |select
      |*
      |from
      |driver_online_record_yesterday
      |union all
      |select
      |*
      |from
      |driver_online_record_nextday
      |union all
      |select
      |*
      |from
      |order_info_sameday
      |union all
      |select
      |*
      |from
      |order_info_yesterday
      |union all
      |select
      |*
      |from
      |order_info_nextday
    """.stripMargin


  // driver_info连接opt_alliance_business,获取driver_id对应的数据
  lazy val driver_info_join_opt_alliance_business =
    """
      |select
      |di.driver_type driver_type,
      |di.id_ driver_id,
      |di.mobile driver_mobile,
      |di.driver_name driver_name,
      |di.register_city city_code,
      |di.city_name city_name,
      |di.driver_management_id driver_company_id,
      |oab.alliance_name driver_company_name
      |from
      |driver_info di
      |left join
      |opt_alliance_business oab
      |on di.driver_management_id = oab.id_
    """.stripMargin


  // order_info连接driver_info，获取driver_id对应的分组数据
  lazy val order_info_join_driver_info =
    """
      |select
      |di.driver_type driver_type,
      |di.driver_id driver_id,
      |di.driver_mobile driver_mobile,
      |di.driver_name driver_name,
      |di.city_code city_code,
      |di.city_name city_name,
      |di.driver_company_id driver_company_id,
      |di.driver_company_name driver_company_name,
      |oi.online_time online_time
      |from
      |driver_online_oneday oi
      |left join
      |driver_info_join_opt_alliance_business di
      |on oi.driver_id = di.driver_id
    """.stripMargin
  // 正式上线时换成order_info_oneday


  // 以所有类别都作为分组条件，聚合online_time
  lazy val order_info_oneday_group1 =
    s"""
       |select
       |driver_type,
       |driver_id,
       |driver_mobile,
       |driver_name,
       |city_code,
       |city_name,
       |driver_company_id,
       |driver_company_name,
       |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),$num) online_date,
       |round((sum(online_time)/3600),1) online_time,
       |from_unixtime(unix_timestamp()) create_time
       |from
       |order_info_join_driver_info
       |where driver_id is not null
       |group by driver_id,driver_name,driver_mobile,driver_type,city_code,city_name,driver_company_id,driver_company_name
    """.stripMargin


}


//lazy val driver_online_record_time_original =
//"""
//  |select
//  |driverID as driver_id,
//  |unix_timestamp(createTime) as time ,
//  |state
//  |from
//  |driver_online_record
//  |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(createTime)) = 1
//""".stripMargin
//
//
//// 获取mongoDB前一天的数据,使用driverID分组,排序分组内的时间
//lazy val driver_online_record_time_original_0_3000 =
//"""
//  |select
//  |driver_id,
//  |time ,
//  |state
//  |from
//  |driver_online_record_time_original
//  |where driver_id < 3000
//""".stripMargin
////where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(createTime)) = 1
//
//
//lazy val driver_online_record_time_original_3000_6000 =
//"""
//  |select
//  |driver_id,
//  |time ,
//  |state
//  |from
//  |driver_online_record_time_original
//  |where
//  |driver_id >= 3000
//  |and
//  |driver_id < 5000
//""".stripMargin
//
//
//// 当前未启用
//lazy val driver_online_record_time_original_4000_5000 =
//"""
//  |select
//  |driver_id,
//  |time ,
//  |state
//  |from
//  |driver_online_record_time_original
//  |where
//  |driver_id >= 3000
//  |and
//  |driver_id < 5000
//""".stripMargin
//
//
//lazy val driver_online_record_time_original_6000_ =
//"""
//  |select
//  |driver_id,
//  |time ,
//  |state
//  |from
//  |driver_online_record_time_original
//  |where driver_id >= 5000
//""".stripMargin
//
//// 过滤order_info
//lazy val order_info_filter =
//"""
//  |select
//  |driver_id,
//  |TO_DATE(close_gps_time) close_gps_time,
//  |TO_DATE(begin_time) begin_time,
//  |UNIX_TIMESTAMP(close_gps_time) close_gps_time_s,
//  |UNIX_TIMESTAMP(begin_time) begin_time_s
//  |from (
//  |select
//  |driver_id,
//  |close_gps_time,
//  |begin_time
//  |from
//  |order_info
//  |) tb
//  |where
//  |DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 0
//  |or DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 1
//  |or DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(begin_time)) = 1
//  |or DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(begin_time)) = 2
//""".stripMargin
//
//
////
//lazy val driver_online_record_time_state_0 =
//"""
//  |select
//  |driver_id,
//  |time,
//  |state
//  |from (
//  |select
//  |driver_id,
//  |time,
//  |state,
//  |row_number() over(partition by driver_id,time order by time) as rank
//  |from
//  |driver_online_record_time_original
//  |where
//  |state = '0'
//  |)
//  |where
//  |rank = '1'
//""".stripMargin
//
//
////
//lazy val driver_online_record_time_state_1 =
//"""
//  |select
//  |driver_id,
//  |time,
//  |state
//  |from (
//  |select
//  |driver_id,
//  |time,
//  |state,
//  |row_number() over(partition by driver_id,time order by time) as rank
//  |from
//  |driver_online_record_time_original
//  |where
//  |state = '1'
//  |)
//  |where
//  |rank = '1'
//""".stripMargin
//
//
////
//lazy val driver_online_record_time_all =
//"""
//  |select
//  |*
//  |from
//  |driver_online_record_time_state_0
//  |union all
//  |select
//  |*
//  |from
//  |driver_online_record_time_state_1
//""".stripMargin
//
//
//// 获取mongoDB前一天的数据,使用driverID分组,排序分组内的时间
//lazy val driver_online_record_time_filter =
//"""
//  |select
//  |driver_id,
//  |time
//  |from (
//  |select
//  |driver_id ,
//  |time,
//  |count(1) count
//  |from
//  |driver_online_record_time_all
//  |group by driver_id,time
//  |)
//  |where count > 1
//""".stripMargin
//
//
//
////
//lazy val driver_online_record_time_rank =
//"""
//  |select
//  |driver_id,
//  |time,
//  |state,
//  |row_number() over(partition by driver_id order by time) as rank
//  |from (
//  |select
//  |t1.driver_id driver_id,
//  |t1.time time,
//  |t1.state state
//  |from
//  |driver_online_record_time_all t1
//  |left join
//  |driver_online_record_time_filter t2
//  |on t1.driver_id = t2.driver_id
//  |and t1.time = t2.time
//  |where
//  |t2.driver_id is null
//  |and
//  |t2.time is null
//  |) tb
//""".stripMargin
////where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(createTime)) = 1
//
//
//
//// 倒排分组内的时间
//lazy val driver_online_record_time_rank_desc =
//"""
//  |select
//  |driver_id ,
//  |time,
//  |state,
//  |row_number() over(partition by driver_id order by time desc) as rank
//  |from
//  |driver_online_record_time_rank
//""".stripMargin
//
//
//
//// 将排序表自连接，连接条件t1.driver_id = t2.driver_id and t1.rank + 1 = t2.rank and t1.state - 1 = t2.state,用t2.time-t1.time得到online_time
//lazy val driver_online_record_sameday =
//"""
//  |select
//  |t1.driver_id ,
//  |t2.time - t1.time online_time
//  |from
//  |driver_online_record_time_rank t1
//  |join
//  |driver_online_record_time_rank t2
//  |where t1.driver_id = t2.driver_id
//  |and t1.rank + 1 = t2.rank
//  |and t1.state - 1 = t2.state
//""".stripMargin
//
//
//// 筛选出顺排组内rank=1并且state为0的数据，此数据为跨前一天数据,用time-当天凌晨时间戳
//lazy val driver_online_record_yesterday =
//"""
//  |select
//  |driver_id ,
//  |time,
//  |time - UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1),'yyyy-MM-dd') as online_time
//  |from
//  |driver_online_record_time_rank
//  |where rank = 1
//  |and state = 0
//""".stripMargin
//
//
//// 筛选出时间倒排组内rank=1并且state为1的数据，此数据为跨后一天数据,用当天最后时间戳-time
//lazy val driver_online_record_nextday =
//"""
//  |select
//  |driver_id ,
//  |time,
//  |UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),0),'yyyy-MM-dd') - time as online_time
//  |from
//  |driver_online_record_time_rank_desc
//  |where rank = 1
//  |and state = 1
//""".stripMargin
//
//
//
////  lazy val driver_online_record_time_theday =
////    """
////      |select
////      |tb1.driver_id driver_id,
////      |tb1.time time,
////      |tb1.state state
////      |from (
////      |select
////      |t1.driver_id driver_id,
////      |t1.time time,
////      |t1.state state
////      |from
////      |driver_online_record_time_rank t1
////      |left join
////      |driver_online_record_yesterday t2
////      |on t1.driver_id = t2.driver_id
////      |and t1.time = t2.time
////      |where
////      |t2.driver_id is null
////      |and
////      |t2.time is null
////      |) tb1
////      |left join
////      |driver_online_record_nextday tb2
////      |on tb1.driver_id = tb2.driver_id
////      |and tb1.time = tb2.time
////      |where
////      |tb2.driver_id is null
////      |and
////      |tb2.time is null
////    """.stripMargin
////
////
////
////  lazy val driver_online_record_sameday =
////    """
////      |select
////      |t1.driver_id driver_id,
////      |t1.state0_time - t2.state1_time online_time
////      |from
////      |(
////      |select
////      |driver_id,
////      |sum(time) state0_time
////      |from
////      |driver_online_record_time_theday
////      |where state = 0
////      |group by driver_id
////      |) t1
////      |left join
////      |(
////      |select
////      |driver_id,
////      |sum(time) state1_time
////      |from
////      |driver_online_record_time_theday
////      |where state = 1
////      |group by driver_id
////      |) t2
////      |on t1.driver_id = t2.driver_id
////    """.stripMargin
//
//
//
//// 得到hbase order_info中create_time与close_gps_time都是当天的数据，即非跨天单数据
//lazy val order_info_sameday =
//"""
//  |select
//  |driver_id,
//  |close_gps_time_s - begin_time_s as online_time
//  |from
//  |order_info_filter
//  |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),close_gps_time) = 1
//  |and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),begin_time) = 1
//  |and close_gps_time_s - begin_time_s > 0
//""".stripMargin
//// where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 1
//// and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 1
//
//
//// 得到create_time是前一天,并且close_gps_time是当天的数据，即为跨前一天单数据
//lazy val order_info_yesterday =
//"""
//  |select
//  |driver_id,
//  |close_gps_time_s - UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1),'yyyy-MM-dd') as online_time
//  |from
//  |order_info_filter
//  |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),close_gps_time) = 1
//  |and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),begin_time) = 2
//""".stripMargin
//
//// where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 1
//// and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 2
//
//
//// 得到create_time是当天,并且close_gps_time是后一天的数据，即为跨后一天单数据
//lazy val order_info_nextday =
//"""
//  |select
//  |driver_id,
//  |UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),0),'yyyy-MM-dd') - begin_time_s as online_time
//  |from
//  |order_info_filter
//  |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),close_gps_time) = 0
//  |and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),begin_time) = 1
//""".stripMargin
//
//// where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 0
//// and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 1
//
//
//lazy val driver_online_mysql_oneday =
//"""
//  |select
//  |*
//  |from
//  |order_info_sameday
//  |union all
//  |select
//  |*
//  |from
//  |order_info_yesterday
//  |union all
//  |select
//  |*
//  |from
//  |order_info_nextday
//""".stripMargin
//
////  lazy val driver_online_mongo_oneday =
////    """
////      |select
////      |*
////      |from
////      |driver_online_record_sameday
////      |union all
////      |select
////      |*
////      |from
////      |driver_online_record_yesterday
////      |union all
////      |select
////      |*
////      |from
////      |driver_online_record_nextday
////    """.stripMargin
//
////
//lazy val driver_online_mongo_oneday =
//"""
//  |select
//  |*
//  |from
//  |driver_online_record_sameday
//  |union all
//  |select
//  |driver_id,
//  |online_time
//  |from
//  |driver_online_record_yesterday
//  |union all
//  |select
//  |driver_id,
//  |online_time
//  |from
//  |driver_online_record_nextday
//""".stripMargin
//
//
//
//lazy val driver_online_mongo_oneday_all =
//"""
//  |select
//  |*
//  |from
//  |driver_online_mongo_oneday_0_3000
//  |union all
//  |select
//  |*
//  |from
//  |driver_online_mongo_oneday_3000_6000
//  |union all
//  |select
//  |*
//  |from
//  |driver_online_mongo_oneday_6000_
//""".stripMargin
//
//
//lazy val driver_online_oneday =
//"""
//  |select
//  |*
//  |from
//  |driver_online_mongo_oneday
//  |union all
//  |select
//  |*
//  |from
//  |driver_online_mysql_oneday
//""".stripMargin
//
//
//// 将六个表数据聚合
//lazy val order_info_oneday =
//"""
//  |select
//  |*
//  |from
//  |driver_online_record_sameday
//  |union all
//  |select
//  |*
//  |from
//  |driver_online_record_yesterday
//  |union all
//  |select
//  |*
//  |from
//  |driver_online_record_nextday
//  |union all
//  |select
//  |*
//  |from
//  |order_info_sameday
//  |union all
//  |select
//  |*
//  |from
//  |order_info_yesterday
//  |union all
//  |select
//  |*
//  |from
//  |order_info_nextday
//""".stripMargin
//
//
//// driver_info连接opt_alliance_business,获取driver_id对应的数据
//lazy val driver_info_join_opt_alliance_business =
//"""
//  |select
//  |di.driver_type driver_type,
//  |di.id_ driver_id,
//  |di.mobile driver_mobile,
//  |di.driver_name driver_name,
//  |di.register_city city_code,
//  |di.city_name city_name,
//  |di.driver_management_id driver_company_id,
//  |oab.alliance_name driver_company_name
//  |from
//  |driver_info di
//  |left join
//  |opt_alliance_business oab
//  |on di.driver_management_id = oab.id_
//""".stripMargin
//
//
//// order_info连接driver_info，获取driver_id对应的分组数据
//lazy val order_info_join_driver_info =
//"""
//  |select
//  |di.driver_type driver_type,
//  |di.driver_id driver_id,
//  |di.driver_mobile driver_mobile,
//  |di.driver_name driver_name,
//  |di.city_code city_code,
//  |di.city_name city_name,
//  |di.driver_company_id driver_company_id,
//  |di.driver_company_name driver_company_name,
//  |oi.online_time online_time
//  |from
//  |driver_online_oneday oi
//  |left join
//  |driver_info_join_opt_alliance_business di
//  |on oi.driver_id = di.driver_id
//""".stripMargin
//// 正式上线时换成order_info_oneday
//
//
//// 以所有类别都作为分组条件，聚合online_time
//lazy val order_info_oneday_group1 =
//"""
//  |select
//  |driver_type,
//  |driver_id,
//  |driver_mobile,
//  |driver_name,
//  |city_code,
//  |city_name,
//  |driver_company_id,
//  |driver_company_name,
//  |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) online_date,
//  |round((sum(online_time)/3600),1) online_time,
//  |from_unixtime(unix_timestamp()) create_time
//  |from
//  |order_info_join_driver_info
//  |where driver_id is not null
//  |group by driver_id,driver_name,driver_mobile,driver_type,city_code,city_name,driver_company_id,driver_company_name
//""".stripMargin
