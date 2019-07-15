package dongfeng.query.sql

object VehicleSQL {

  // 获取前一天的数据,使用driverID分组,排序分组内的时间
  lazy val driver_online_record_time_rank =
    """
      |select
      |driver_id ,
      |createTime,
      |time,
      |state,
      |row_number() over(partition by driver_id order by time) as rank
      |from (
      |select
      |driverID driver_id,
      |createTime,
      |unix_timestamp(createTime) as time ,
      |state
      |from
      |driver_online_record
      |where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(createTime)) = 1
      |) tb
    """.stripMargin


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
    """
      |select
      |driver_id ,
      |time - UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1),'yyyy-MM-dd') as online_time
      |from
      |driver_online_record_time_rank
      |where rank = 1
      |and state = 0
    """.stripMargin


  // 筛选出时间倒排组内rank=1并且state为1的数据，此数据为跨后一天数据,用当天最后时间戳-time
  lazy val driver_online_record_nextday =
    """
      |select
      |driver_id ,
      |UNIX_TIMESTAMP(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),'yyyy-MM-dd') - time as online_time
      |from
      |driver_online_record_time_rank_desc
      |where rank = 1
      |and state = 1
    """.stripMargin

  // 得到create_time与close_gps_time都是当天的数据，即非跨天单数据
  lazy val order_info_sameday =
    """
      |select
      |driver_id,
      |UNIX_TIMESTAMP(close_gps_time) - UNIX_TIMESTAMP(create_time) as online_time
      |from
      |order_info
      |where 1=1
    """.stripMargin
  // where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 1
  // and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 1


  // 得到create_time是前一天,并且close_gps_time是当天的数据，即为跨前一天单数据
  lazy val order_info_yesterday =
    """
      |select
      |driver_id,
      |UNIX_TIMESTAMP(close_gps_time) - UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1),'yyyy-MM-dd') as online_time
      |from
      |order_info
      |where 1=1
    """.stripMargin
  // where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 1
  // and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 2


  // 得到create_time是当天,并且close_gps_time是后一天的数据，即为跨后一天单数据
  lazy val order_info_nextday =
    """
      |select
      |driver_id,
      |UNIX_TIMESTAMP(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),'yyyy-MM-dd') - UNIX_TIMESTAMP(create_time) as online_time
      |from
      |order_info
      |where 1=1
    """.stripMargin
  // where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 0
  // and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 1



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
      |order_info_oneday oi
      |left join
      |driver_info_join_opt_alliance_business di
      |on oi.driver_id = di.driver_id
    """.stripMargin
  // 正式上线时换成order_info_oneday


  //  以所有类别都作为分组条件，聚合online_time
  lazy val order_info_oneday_group =
    """
      |select
      |driver_type,
      |driver_id,
      |driver_mobile,
      |driver_name,
      |city_code,
      |city_name,
      |driver_company_id,
      |driver_company_name,
      |sum(online_time) online_time
      |from
      |order_info_join_driver_info
      |where driver_id is not null
      |group by driver_id,driver_name,driver_mobile,driver_type,city_code,city_name,driver_company_id,driver_company_name
    """.stripMargin


  // 以所有类别都作为分组条件，聚合online_time
  lazy val order_info_oneday_group1 =
    """
      |select
      |driver_type,
      |driver_id,
      |driver_mobile,
      |driver_name,
      |city_code,
      |city_name,
      |driver_company_id,
      |driver_company_name,
      |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) online_date,
      |round((sum(online_time)/3600),1) online_time,
      |from_unixtime(unix_timestamp()) create_time
      |from
      |order_info_join_driver_info
      |where driver_id is not null
      |group by driver_id,driver_name,driver_mobile,driver_type,city_code,city_name,driver_company_id,driver_company_name
      |limit 5
    """.stripMargin





}
