package com.yclxiao.flinkcdcdemo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LeagueOcSettleProfit2DwsHdjProfitRecordSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(5000);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS cloud_test");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS league_test");

        // 动态表，此为source表
        tEnv.executeSql("CREATE TABLE league_test.oc_settle_profit (\n" +
                "    id                           STRING,\n" +
                "    show_profit_id               STRING,\n" +
                "    order_no                     STRING,\n" +
                "    from_user_id                 STRING,\n" +
                "    from_user_type               INT,\n" +
                "    user_id                      STRING,\n" +
                "    user_type                    INT,\n" +
                "    rate                         INT,\n" +
                "    amount                       INT,\n" +
                "    type                         INT,\n" +
                "    add_time                     TIMESTAMP,\n" +
                "    state                        INT,\n" +
                "    expect_profit_time           TIMESTAMP,\n" +
                "    profit_time                  TIMESTAMP,\n" +
                "    profit_mode                  INT,\n" +
                "    opt_code                     STRING,\n" +
                "    opt_name                     STRING,\n" +
                "    acct_circle                  STRING,\n" +
                "    process_state                INT,\n" +
                "    parent_id                    STRING,\n" +
                "    keep_account_from_user_id    STRING,\n" +
                "    keep_account_from_bm_user_id STRING,\n" +
                "    keep_account_user_id         STRING,\n" +
                "    keep_account_bm_user_id      STRING,\n" +
                "    biz_type                     INT,\n" +
                "    remark                       STRING,\n" +
                "    contribute_user_id           STRING,\n" +
                "    relation_brand_owner_id      STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '10.20.1.11',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'database-name' = 'league_test',\n" +
                "  'table-name' = 'oc_settle_profit',\n" +
                "  'scan.incremental.snapshot.enabled' = 'false'\n" +
                ")");

        // 动态表，此为sink表。sink表和source表的connector不一样
        tEnv.executeSql("CREATE TABLE cloud_test.dws_profit_record_hdj_flink (\n" +
                "    id                      STRING,\n" +
                "    show_profit_id          STRING,\n" +
                "    order_no                STRING,\n" +
                "    from_user_id            STRING,\n" +
                "    from_user_type          INT,\n" +
                "    user_id                 STRING,\n" +
                "    user_type               INT,\n" +
                "    amount                  INT,\n" +
                "    profit_time             TIMESTAMP,\n" +
                "    state                   INT,\n" +
                "    acct_circle             STRING,\n" +
                "    biz_type                INT,\n" +
                "    contribute_user_id      STRING,\n" +
                "    relation_brand_owner_id STRING,\n" +
                "    remark                  STRING,\n" +
                "    add_time                TIMESTAMP,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://10.20.1.11:3306/cloud_test',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = 'dws_profit_record_hdj_flink'\n" +
                ")");

        tEnv.executeSql("INSERT INTO cloud_test.dws_profit_record_hdj_flink (id, show_profit_id, order_no, from_user_id, from_user_type, user_id,\n" +
                "                                              user_type, amount, profit_time, state, acct_circle, biz_type,\n" +
                "                                              contribute_user_id, relation_brand_owner_id, remark, add_time)\n" +
                "select f.id,\n" +
                "       f.show_profit_id,\n" +
                "       f.order_no,\n" +
                "       f.from_user_id,\n" +
                "       f.from_user_type,\n" +
                "       f.user_id,\n" +
                "       f.user_type,\n" +
                "       f.amount,\n" +
                "       f.profit_time,\n" +
                "       f.state,\n" +
                "       f.acct_circle,\n" +
                "       f.biz_type,\n" +
                "       f.contribute_user_id,\n" +
                "       f.relation_brand_owner_id,\n" +
                "       f.remark,\n" +
                "       f.add_time\n" +
                "from league_test.oc_settle_profit f\n" +
                "where f.id is not null\n" +
                "  and f.biz_type is not null\n" +
                "  and f.biz_type = 9");
    }
}
