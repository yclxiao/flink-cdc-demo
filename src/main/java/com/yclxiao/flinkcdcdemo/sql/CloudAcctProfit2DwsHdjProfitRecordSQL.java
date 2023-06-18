package com.yclxiao.flinkcdcdemo.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CloudAcctProfit2DwsHdjProfitRecordSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(5000);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS cloud_test");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS coalitiondb");

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
        // 动态表，此为source表
        tEnv.executeSql("CREATE TABLE cloud_test.acct_profit (\n" +
                "    id                           STRING,\n" +
                "    pay_order_no                 STRING,\n" +
                "    out_no                       STRING,\n" +
                "    title                        STRING,\n" +
                "    from_user_id                 STRING,\n" +
                "    from_account_id              STRING,\n" +
                "    user_id                      STRING,\n" +
                "    account_id                   STRING,\n" +
                "    amount                       INT,\n" +
                "    profit_state                 INT,\n" +
                "    profit_time                  TIMESTAMP,\n" +
                "    refund_state                 INT,\n" +
                "    refund_time                  TIMESTAMP,\n" +
                "    add_time                     TIMESTAMP,\n" +
                "    remark                       STRING,\n" +
                "    acct_circle                  STRING,\n" +
                "    user_type                    INT,\n" +
                "    from_user_type               INT,\n" +
                "    company_id                   STRING,\n" +
                "    profit_mode                  INT,\n" +
                "    type                         INT,\n" +
                "    parent_id                    STRING,\n" +
                "    oc_profit_id                 STRING,\n" +
                "    keep_account_from_user_id    STRING,\n" +
                "    keep_account_from_bm_user_id STRING,\n" +
                "    keep_account_user_id         STRING,\n" +
                "    keep_account_bm_user_id      STRING,\n" +
                "    biz_company_id               STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '10.20.1.11',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'database-name' = 'cloud_test',\n" +
                "  'table-name' = 'acct_profit',\n" +
                "  'scan.incremental.snapshot.enabled' = 'false'\n" +
                ")");
        // 动态表，此为source表
        tEnv.executeSql("CREATE TABLE coalitiondb.tc_virtual_order (\n" +
                "    id                 STRING,\n" +
                "    title              STRING,\n" +
                "    prod_id            BIGINT,\n" +
                "    buy_num            INT,\n" +
                "    unit_price         INT,\n" +
                "    total_amount       INT,\n" +
                "    offer_amount       INT,\n" +
                "    use_brand_coin_num INT,\n" +
                "    pay_cash           INT,\n" +
                "    pay_state          INT,\n" +
                "    pay_time           TIMESTAMP,\n" +
                "    pay_way_id         INT,\n" +
                "    pay_order_no       STRING,\n" +
                "    state              INT,\n" +
                "    complete_time      TIMESTAMP,\n" +
                "    close_time         TIMESTAMP,\n" +
                "    close_reason       STRING,\n" +
                "    order_source       STRING,\n" +
                "    settle_state       INT,\n" +
                "    settle_time        TIMESTAMP,\n" +
                "    refund_state       INT,\n" +
                "    refund_time        TIMESTAMP,\n" +
                "    refund_amount      INT,\n" +
                "    coalition_id       STRING,\n" +
                "    seller_id          STRING,\n" +
                "    buyer_user_id      STRING,\n" +
                "    buyer_bm_user_id   STRING,\n" +
                "    buyer_user_type    INT,\n" +
                "    buyer_user_name    STRING,\n" +
                "    buyer_user_phone   STRING,\n" +
                "    require_info       STRING,\n" +
                "    remark             STRING,\n" +
                "    notify_state       INT, \n" +
                "    notify_time        TIMESTAMP,\n" +
                "    add_user_id        STRING,\n" +
                "    add_user_name      STRING,\n" +
                "    add_user_type      INT,\n" +
                "    add_time           TIMESTAMP,\n" +
                "    opt_time           TIMESTAMP,\n" +
                "    order_type         INT,\n" +
                "    company_id         STRING,\n" +
                "    brand_id           STRING,\n" +
                "    delete_at          BIGINT,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://10.20.1.11:3306/coalitiondb',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = 'tc_virtual_order'\n" +
                ")");
        // 动态表，此为source表
        tEnv.executeSql("CREATE TABLE cloud_test.acct_pay_order (\n" +
                "    id                      STRING,\n" +
                "    biz_order_id            STRING,\n" +
                "    company_id              STRING,\n" +
                "    title                   STRING,\n" +
                "    amount                  INT,\n" +
                "    pay_state               INT,\n" +
                "    pay_time                TIMESTAMP,\n" +
                "    pay_cert                STRING,\n" +
                "    way_id                  INT,\n" +
                "    pay_type                INT,\n" +
                "    pay_cate                INT,\n" +
                "    payer                   STRING,\n" +
                "    payer_account_id        BIGINT,\n" +
                "    payer_name              STRING,\n" +
                "    payee                   STRING,\n" +
                "    payee_account_id        BIGINT,\n" +
                "    payee_name              STRING,\n" +
                "    deposit_order_id        STRING,\n" +
                "    gateway_order_id        STRING,\n" +
                "    refund_state            INT,\n" +
                "    refund_time             TIMESTAMP,\n" +
                "    refund_amount           INT,\n" +
                "    notify_state            INT,\n" +
                "    notify_time             TIMESTAMP,\n" +
                "    next_notify_time        TIMESTAMP,\n" +
                "    notify_num              INT,\n" +
                "    notify_upper            INT,\n" +
                "    add_user_id             STRING,\n" +
                "    add_user_name           STRING,\n" +
                "    add_time                TIMESTAMP,\n" +
                "    remark                  STRING,\n" +
                "    acct_circle             STRING,\n" +
                "    keep_account_user_id    STRING,\n" +
                "    keep_account_bm_user_id STRING,\n" +
                "    biz_company_id          STRING,\n" +
                "    item_id                 BIGINT,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://10.20.1.11:3306/cloud_test',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = 'acct_pay_order'\n" +
                ")");
        // 动态表，此为source表
        tEnv.executeSql("CREATE TABLE cloud_test.uc_brand_partner (\n" +
                "    id             STRING,\n" +
                "    company_id     STRING,\n" +
                "    shop_id        STRING,\n" +
                "    brand_owner_id STRING,\n" +
                "    name           STRING,\n" +
                "    state          INT,\n" +
                "    expire_date    TIMESTAMP,\n" +
                "    effect_date    TIMESTAMP,\n" +
                "    order_no       STRING,\n" +
                "    remark         STRING,\n" +
                "    add_time       TIMESTAMP,\n" +
                "    add_user_id    STRING,\n" +
                "    add_user_name  STRING,\n" +
                "    delete_at      BIGINT,\n" +
                "    contact_name   STRING,\n" +
                "    contact_phone  STRING,\n" +
                "    is_owner       INT,\n" +
                "    type           INT,\n" +
                "    shop_type      INT,\n" +
                "    product_ids    STRING," +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://10.20.1.11:3306/cloud_test',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = 'uc_brand_partner'\n" +
                ")");

        tEnv.executeSql("INSERT INTO cloud_test.dws_profit_record_hdj_flink (id, show_profit_id, order_no, from_user_id, from_user_type, user_id,\n" +
                "                                                    user_type, amount, profit_time, state, acct_circle, biz_type,\n" +
                "                                                    contribute_user_id, relation_brand_owner_id, remark, add_time)\n" +
                "select *\n" +
                "from (\n" +
                "select f.id,\n" +
                "             concat('JSD', f.id) as show_profit_id,\n" +
                "             f.out_no,\n" +
                "             f.from_user_id,\n" +
                "             f.from_user_type,\n" +
                "             f.user_id,\n" +
                "             f.user_type,\n" +
                "             f.amount,\n" +
                "             f.profit_time,\n" +
                "             f.profit_state,\n" +
                "             f.acct_circle,\n" +
                "             CASE\n" +
                "                 when vo.order_type = 6 then 10\n" +
                "                 when vo.order_type = 1 then 11\n" +
                "                 when vo.order_type = 5 then 12\n" +
                "                 END             as biz_type,\n" +
                "             case\n" +
                "                 when vo.order_type = 6 and f.user_type = 92 then vo.company_id\n" +
                "                 when vo.order_type = 1 and f.user_type = 92 then vo.company_id\n" +
                "                 when vo.order_type = 5 and f.user_type = 92 then vo.company_id\n" +
                "                 end             as contribute_user_id,\n" +
                "             case\n" +
                "                 when vo.order_type = 6 and f.user_type = 90 then vo.brand_id\n" +
                "                 when vo.order_type = 1 and f.user_type = 90 then vo.brand_id\n" +
                "                 when vo.order_type = 5 and f.user_type = 90 then vo.brand_id\n" +
                "                 end             as relation_brand_owner_id,\n" +
                "             case\n" +
                "                 when vo.order_type = 1 then f.title\n" +
                "                 when vo.order_type = 5 then f.title\n" +
                "                 else f.remark\n" +
                "                 end             as remark,\n" +
                "             f.add_time\n" +
                "      from cloud_test.acct_profit as f\n" +
                "               inner join coalitiondb.tc_virtual_order as vo on f.out_no = vo.id\n" +
                "      where f.acct_circle = 'PG11111'\n" +
                "        and f.profit_state = 1\n" +
                "        and vo.order_type <> 2\n" +
                "      union all\n" +
                "      select f.id,\n" +
                "             concat('JSD', f.id) as show_profit_id,\n" +
                "             f.out_no,\n" +
                "             f.from_user_id,\n" +
                "             f.from_user_type,\n" +
                "             f.user_id,\n" +
                "             f.user_type,\n" +
                "             f.amount,\n" +
                "             f.profit_time,\n" +
                "             f.profit_state,\n" +
                "             f.acct_circle,\n" +
                "             20                  as biz_type,\n" +
                "             case\n" +
                "                 when f.user_type = 92 and f.biz_company_id <> null then f.biz_company_id\n" +
                "                 end             as contribute_user_id,\n" +
                "             case\n" +
                "                 when f.user_type = 90 and f.biz_company_id <> null then (select ubp.brand_owner_id\n" +
                "                                             from cloud_test.uc_brand_partner as ubp\n" +
                "                                             where ubp.company_id = f.biz_company_id)\n" +
                "                 end             as relation_brand_owner_id,\n" +
                "             f.remark            as remark,\n" +
                "             f.add_time\n" +
                "      from cloud_test.acct_profit as f\n" +
                "               inner join cloud_test.acct_pay_order as apo on f.pay_order_no = apo.id\n" +
                "      where f.acct_circle = 'PG11111'\n" +
                "        and f.profit_state = 1\n" +
                "        and apo.pay_cate = 200100\n" +
                ") as t\n" +
                "where t.add_time >= '2023-06-05 15:43:56'\n");
    }
}
