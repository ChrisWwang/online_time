package dongfeng.query.sql

object VehicleSQL {

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
      |UNIX_TIMESTAMP(close_gps_time) - UNIX_TIMESTAMP(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)) as online_time
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
      |UNIX_TIMESTAMP(from_unixtime(unix_timestamp(),'yyyy-MM-dd')) - UNIX_TIMESTAMP(create_time) as online_time
      |from
      |order_info
      |where 1=1
    """.stripMargin
  // where DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(close_gps_time)) = 0
  // and DATEDIFF(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),TO_DATE(create_time)) = 1


  // 将三个表数据聚合
  lazy val order_info_oneday =
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


  //将driver_info连接opt_alliance_business
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


  //将order_info连接driver_info
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
      |order_info_sameday oi
      |left join
      |driver_info_join_opt_alliance_business di
      |on oi.driver_id = di.driver_id
    """.stripMargin
  // 正式上线时换成order_info_oneday


  // 司机分组，聚合online_time
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


  // 司机分组，聚合online_time
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
      |limit 10
    """.stripMargin

}
