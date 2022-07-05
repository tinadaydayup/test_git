package com.farfetch.dragon.data.marketing.customerdata.task.customerbehavior;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.time.LocalDate;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

public class LostCustomerAnalyze {
       public static Dataset getLostCustomer(Dataset... inputs){
           LocalDate startDayTwoMonAgo = LocalDate.now().minusDays(60);//最近两个月
           LocalDate startDayTwoYearAgo = LocalDate.now().minusDays(730);//最近两年 365+365
           Dataset trackingData = inputs[0]; //行为
           Dataset orderData = inputs[1];//订单

           //最近两年浏览过的用户
           Dataset vistUserTwoYear = trackingData.filter(to_date(col("datepartition")).geq(startDayTwoYearAgo.toString()))
                                                 .filter(col("dimuser.userid").gt(0))
                                                 .select(col("dimuser.userid").as("user_id"))
                                                 .distinct();
           //最近两个月浏览过的用户
           Dataset vistUserTwoMonth = trackingData.filter(to_date(col("datepartition")).geq(startDayTwoMonAgo.toString()))
                   .filter(col("dimuser.userid").gt(0))
                   .select(col("dimuser.userid").as("user_id"))
                   .distinct();

           //最近两年下过单的用户
           Dataset orderUserTwoYear = orderData.filter(to_date(col("orderdate_gmt")).geq(startDayTwoYearAgo.toString()))
                   .filter(col("userid").gt(0))
                   .select(col("userid").as("user_d"))
                   .distinct();

           //最近两年访问且下过单且最近两个月没有访问过
           Dataset lostUser = vistUserTwoYear.join(orderUserTwoYear, "user_d")
                                             .join(vistUserTwoMonth, "user_id");

           //过滤最近两个月的数据 .filter(to_date(col("datepartition")).geq(startDate.toString()))
           //选择分区>=最近两个月的数据
           return lostUser;
       }
}

