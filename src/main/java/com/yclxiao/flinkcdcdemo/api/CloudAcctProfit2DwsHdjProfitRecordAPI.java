package com.yclxiao.flinkcdcdemo.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.yclxiao.flinkcdcdemo.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class CloudAcctProfit2DwsHdjProfitRecordAPI {
    private static final Logger LOG = LoggerFactory.getLogger(CloudAcctProfit2DwsHdjProfitRecordAPI.class);
    private static String MYSQL_HOST = "10.20.1.11";
    private static int MYSQL_PORT = 3306;
    private static String MYSQL_USER = "root";
    private static String MYSQL_PASSWD = "123456";
    private static String SYNC_DB = "cloud_test";
    private static List<String> SYNC_TABLES = Arrays.asList("cloud_test.acct_profit");

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .databaseList(SYNC_DB) // set captured database
                .tableList(String.join(",", SYNC_TABLES)) // set captured table
                .username(MYSQL_USER)
                .password(MYSQL_PASSWD)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        DataStreamSource<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CDC Source" + CloudAcctProfit2DwsHdjProfitRecordAPI.class.getName());

        List<String> tableList = getTableList();
        for (String tbl : tableList) {
            SingleOutputStreamOperator<String> filterStream = filterTableData(cdcSource, tbl);
            SingleOutputStreamOperator<String> cleanStream = clean(filterStream);
            // 流的数据sink出去
            cleanStream.addSink(new CustomDealDataSink())
                    .name("sink " + tbl);
        }
        env.execute(CloudAcctProfit2DwsHdjProfitRecordAPI.class.getName());
    }

    /**
     * 自定义sink
     */
    private static class CustomDealDataSink extends RichSinkFunction<String> {
        private transient Connection coalitiondbConnection;
        private transient Statement coalitiondbStatement;
        private transient Connection cloudConnection;
        private transient Statement cloudStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在这里初始化 JDBC 连接
            coalitiondbConnection = DriverManager.getConnection("jdbc:mysql://10.20.1.11:3306/coalitiondb", "root", "123456");
            coalitiondbStatement = coalitiondbConnection.createStatement();
            cloudConnection = DriverManager.getConnection("jdbc:mysql://10.20.1.11:3306/cloud_test", "root", "123456");
            cloudStatement = cloudConnection.createStatement();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // 解析拿到的CDC-JSON数据
            JSONObject rowJson = JSON.parseObject(value);
            String outNo = rowJson.getString("out_no");
            Integer userType = rowJson.getInteger("user_type");
            String id = rowJson.getString("id");
            String payOrderNo = rowJson.getString("pay_order_no");
            String title = rowJson.getString("title");
            String fromUserId = rowJson.getString("from_user_id");
            String fromAccountId = rowJson.getString("from_account_id");
            String userId = rowJson.getString("user_id");
            String accountId = rowJson.getString("account_id");
            Integer amount = rowJson.getInteger("amount");
            Integer profitState = rowJson.getInteger("profit_state");
            Date profitTime = rowJson.getTimestamp("profit_time");
            Integer refundState = rowJson.getInteger("refund_state");
            Date refundTime = rowJson.getTimestamp("refund_time");
            Date addTime = rowJson.getTimestamp("add_time");
            String remark = rowJson.getString("remark");
            String acctCircle = rowJson.getString("acct_circle");
            Integer fromUserType = rowJson.getInteger("from_user_type");
            String companyId = rowJson.getString("company_id");
            String bizCompanyId = rowJson.getString("biz_company_id");
            if (1 != profitState || !"PG11111".equals(acctCircle)) {
                return;
            }

            // 读取相关表的数据（与其他表进行关联）
            Integer bizType = null;
            String contributeUserId = null;
            String relationBrandOwnerId = null;
            ResultSet virtualOrderResultSet = coalitiondbStatement.executeQuery("select * from tc_virtual_order where order_type != 2 and id = '" + outNo + "'");
            // 如果是tc_virtual_order订单（上岗卡、安心卡、课程）
            if (virtualOrderResultSet.next()) {
                // 处理数据逻辑
                Integer virtualOrder4OrderType = virtualOrderResultSet.getInt("order_type");
                String virtualOrder4CompanyId = virtualOrderResultSet.getString("company_id");
                String virtualOrder4BrandId = virtualOrderResultSet.getString("brand_id");
                // 上岗卡订单排掉，因为已经有别的任务处理了
                if (virtualOrder4OrderType == 2) {
                    return;
                }
                // orderType转换
                if (virtualOrder4OrderType == 6) {
                    bizType = 10;
                } else if (virtualOrder4OrderType == 1) {
                    bizType = 11;
                } else if (virtualOrder4OrderType == 5) {
                    bizType = 12;
                }
                // userType转换
                if (virtualOrder4OrderType == 6 && userType == 92) {
                    contributeUserId = virtualOrder4CompanyId;
                } else if (virtualOrder4OrderType == 1 && userType == 92) {
                    contributeUserId = virtualOrder4CompanyId;
                } else if (virtualOrder4OrderType == 5 && userType == 92) {
                    contributeUserId = virtualOrder4CompanyId;
                }
                // relationBrandOwnerId转换
                if (virtualOrder4OrderType == 6 && userType == 90) {
                    relationBrandOwnerId = virtualOrder4BrandId;
                } else if (virtualOrder4OrderType == 1 && userType == 90) {
                    relationBrandOwnerId = virtualOrder4BrandId;
                } else if (virtualOrder4OrderType == 5 && userType == 90) {
                    relationBrandOwnerId = virtualOrder4BrandId;
                }
                // remark转换
                if (virtualOrder4OrderType == 1 || virtualOrder4OrderType == 5) {
                    remark = title;
                }
            } else {
                // 如果不是tc_virtual_order的数据，则可能是其他数据，此处只保留好到家实物商品数据
                if (StringUtils.isBlank(payOrderNo)) {
                    return;
                }
                ResultSet acctPayOrderResultSet = cloudStatement.executeQuery("select * from acct_pay_order t where t.id = '" + payOrderNo + "'");
                if (!acctPayOrderResultSet.next()) {
                    return;
                }
                Integer payCate = acctPayOrderResultSet.getInt("pay_cate");
                if (200100 != payCate) { // 好到家实物商品类型
                    return;
                }

                bizType = 20;
                if (userType == 92 && StringUtils.isNotBlank(bizCompanyId)) {
                    contributeUserId = bizCompanyId;
                } else if (userType == 90 && StringUtils.isNotBlank(bizCompanyId)) {
                    ResultSet brandOwnerIdResultSet = cloudStatement.executeQuery("select * from uc_brand_partner t where t.company_id = '" + bizCompanyId + "'");
                    if (brandOwnerIdResultSet.next()) {
                        relationBrandOwnerId = brandOwnerIdResultSet.getString("brand_owner_id");
                    }
                }
            }
            if (StringUtils.isBlank(remark)) {
                remark = title;
            }

            // 数据写入到mysql
            String insertSql = "INSERT INTO dws_profit_record_hdj_flink_api (id, show_profit_id, order_no, from_user_id, from_user_type, user_id,\n" +
                    "                                                    user_type, amount, profit_time, state, acct_circle, biz_type,\n" +
                    "                                                    contribute_user_id, relation_brand_owner_id, remark, add_time)\n" +
                    "VALUES ('" + id + "', '" + "JSD" + id + "', '" + outNo + "', '" + fromUserId + "', " + fromUserType + ", '" + userId + "', " + userType + ",\n" +
                    "        " + amount + ", '" + DateFormatUtils.format(profitTime.getTime(), "yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("GMT")) + "', " + profitState + ", '" + acctCircle + "', " + bizType + ", " + (StringUtils.isBlank(contributeUserId) ? null : "'" + contributeUserId + "'") + ", " + (StringUtils.isBlank(relationBrandOwnerId) ? null : "'" + relationBrandOwnerId + "'") + ", '" + remark + "',\n" +
                    "        '" + DateFormatUtils.format(profitTime.getTime(), "yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("GMT")) + "');";
            cloudStatement.execute("delete from dws_profit_record_hdj_flink_api where id = '" + id + "'");
            cloudStatement.execute(insertSql);
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 在这里关闭 JDBC 连接
            coalitiondbStatement.close();
            coalitiondbConnection.close();
            cloudStatement.close();
            cloudConnection.close();
        }
    }

    /**
     * 清晰数据
     *
     * @param source
     * @return
     */
    private static SingleOutputStreamOperator<String> clean(SingleOutputStreamOperator<String> source) {
        return source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String row, Collector<String> out) throws Exception {
                try {
                    LOG.info("============================row:{}", row);
                    JSONObject rowJson = JSON.parseObject(row);
                    String op = rowJson.getString("op");
                    //history,insert,update
                    if (Arrays.asList("r", "c", "u").contains(op)) {
                        out.collect(rowJson.getJSONObject("after").toJSONString());
                    } else {
                        LOG.info("filter other op:{}", op);
                    }
                } catch (Exception ex) {
                    LOG.warn("filter other format binlog:{}", row);
                }
            }
        });
    }

    /**
     * 过滤数据
     *
     * @param source
     * @param table
     * @return
     */
    private static SingleOutputStreamOperator<String> filterTableData(DataStreamSource<String> source, String table) {
        return source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String row) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    JSONObject source = rowJson.getJSONObject("source");
                    String tbl = source.getString("table");
                    return table.equals(tbl);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return false;
                }
            }
        });
    }

    private static List<String> getTableList() {
        List<String> tables = new ArrayList<>();
        String sql = "SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '" + SYNC_DB + "'";
        List<JSONObject> tableList = JdbcUtil.executeQuery(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWD, sql);
        for (JSONObject jsob : tableList) {
            String schemaName = jsob.getString("TABLE_SCHEMA");
            String tblName = jsob.getString("TABLE_NAME");
            String schemaTbl = schemaName + "." + tblName;
            if (SYNC_TABLES.contains(schemaTbl)) {
                tables.add(tblName);
            }
        }
        return tables;
    }
}
