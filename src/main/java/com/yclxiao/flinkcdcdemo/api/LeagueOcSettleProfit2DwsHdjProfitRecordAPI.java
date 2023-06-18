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
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/**
 * league.oc_settle_profit -> cloud.dws_profit_record_hdj
 * API方式
 */
public class LeagueOcSettleProfit2DwsHdjProfitRecordAPI {
    private static final Logger LOG = LoggerFactory.getLogger(LeagueOcSettleProfit2DwsHdjProfitRecordAPI.class);
    private static String MYSQL_HOST = "10.20.1.11";
    private static int MYSQL_PORT = 3306;
    private static String MYSQL_USER = "root";
    private static String MYSQL_PASSWD = "123456";
    private static String SYNC_DB = "league_test";
    private static List<String> SYNC_TABLES = Arrays.asList("league_test.oc_settle_profit");

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
        env.setParallelism(3);
        env.enableCheckpointing(5000);

        DataStreamSource<String> cdcSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CDC Source" + LeagueOcSettleProfit2DwsHdjProfitRecordAPI.class.getName());

        List<String> tableList = getTableList();
        for (String tbl : tableList) {
            SingleOutputStreamOperator<String> filterStream = filterTableData(cdcSource, tbl);
            SingleOutputStreamOperator<String> cleanStream = clean(filterStream);
            SingleOutputStreamOperator<String> logicStream = logic(cleanStream);
            logicStream.addSink(new CustomDealDataSink());
        }
        env.execute(LeagueOcSettleProfit2DwsHdjProfitRecordAPI.class.getName());
    }

    private static class CustomDealDataSink extends RichSinkFunction<String> {
        private transient Connection cloudConnection;
        private transient PreparedStatement cloudPreparedStatement;

        private String insertSql = "INSERT INTO dws_profit_record_hdj_flink_api (id, show_profit_id, order_no, from_user_id, from_user_type, user_id,\n" +
                "                                              user_type, amount, profit_time, state, acct_circle, biz_type,\n" +
                "                                              contribute_user_id, relation_brand_owner_id, remark, add_time)\n" +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?,\n" +
                "        ?, ?, ?, ?, ?, ?, ?, ?)";
        private String deleteSql = "delete from dws_profit_record_hdj_flink_api where id = '%s'";

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在这里初始化 JDBC 连接
            cloudConnection = DriverManager.getConnection("jdbc:mysql://10.20.1.11:3306/cloud_test", "root", "123456");
            cloudPreparedStatement = cloudConnection.prepareStatement(insertSql);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            JSONObject dataJson = JSON.parseObject(value);
            String id = dataJson.getString("id");
            String showProfitId = dataJson.getString("show_profit_id");
            String orderNo = dataJson.getString("order_no");
            String fromUserId = dataJson.getString("from_user_id");
            Integer fromUserType = dataJson.getInteger("from_user_type");
            String userId = dataJson.getString("user_id");
            Integer userType = dataJson.getInteger("user_type");
            Integer amount = dataJson.getInteger("amount");
            Timestamp addTime = dataJson.getTimestamp("add_time");
            Integer state = dataJson.getInteger("state");
            Timestamp profitTime = dataJson.getTimestamp("profit_time");
            String acctCircle = dataJson.getString("acct_circle");
            Integer bizType = dataJson.getInteger("biz_type");
            String remark = dataJson.getString("remark");
            String contributeUserId = dataJson.getString("contribute_user_id");
            String relationBrandOwnerId = dataJson.getString("relation_brand_owner_id");

            Timestamp profitTimeTimestamp = Timestamp.valueOf(DateFormatUtils.format(profitTime.getTime(), "yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("GMT")));
            Timestamp addTimeTimestamp = Timestamp.valueOf(DateFormatUtils.format(addTime.getTime(), "yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("GMT")));

            cloudPreparedStatement.setString(1, id);
            cloudPreparedStatement.setString(2, showProfitId);
            cloudPreparedStatement.setString(3, orderNo);
            cloudPreparedStatement.setString(4, fromUserId);
            cloudPreparedStatement.setInt(5, fromUserType);
            cloudPreparedStatement.setString(6, userId);
            cloudPreparedStatement.setInt(7, userType);
            cloudPreparedStatement.setInt(8, amount);
            cloudPreparedStatement.setTimestamp(9, profitTimeTimestamp);
            cloudPreparedStatement.setInt(10, state);
            cloudPreparedStatement.setString(11, StringUtils.isBlank(acctCircle) ? "PG11111" : acctCircle);
            cloudPreparedStatement.setInt(12, bizType);
            cloudPreparedStatement.setString(13, contributeUserId);
            cloudPreparedStatement.setString(14, relationBrandOwnerId);
            cloudPreparedStatement.setString(15, remark);
            cloudPreparedStatement.setTimestamp(16, addTimeTimestamp);

            cloudPreparedStatement.execute(String.format(deleteSql, id));
            cloudPreparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 在这里关闭 JDBC 连接
            cloudPreparedStatement.close();
            cloudConnection.close();
        }
    }

    /**
     * 处理逻辑：过滤掉部分数据
     *
     * @param cleanStream
     * @return
     */
    private static SingleOutputStreamOperator<String> logic(SingleOutputStreamOperator<String> cleanStream) {
        return cleanStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String data) throws Exception {
                try {
                    JSONObject dataJson = JSON.parseObject(data);
                    String id = dataJson.getString("id");
                    Integer bizType = dataJson.getInteger("biz_type");
                    if (StringUtils.isBlank(id) || bizType == null) {
                        return false;
                    }
                    // 只处理上岗卡数据
                    return bizType == 9;
                } catch (Exception ex) {
                    LOG.warn("filter other format binlog:{}", data);
                    return false;
                }
            }
        });
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
