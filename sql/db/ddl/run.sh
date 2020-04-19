#!/bin/bash

hive -f ddl_ods_activity_info.sql;
hive -f ddl_ods_activity_order.sql;
hive -f ddl_ods_activity_rule.sql;
hive -f ddl_ods_base_dic.sql;
hive -f ddl_ods_cart_info.sql;
hive -f ddl_ods_comment_info.sql;
hive -f ddl_ods_coupon_info.sql;
hive -f ddl_ods_coupon_use.sql;
hive -f ddl_ods_favor_info.sql;
hive -f dd_ods_order_refund_info.sql;
hive -f ddl_ods_base_category1.sql;
hive -f ddl_ods_base_category2.sql;
hive -f ddl_ods_base_category3.sql;
hive -f ddl_ods_base_province.sql;
hive -f ddl_ods_base_region.sql;
hive -f ddl_ods_base_trademark.sql;
hive -f ddl_ods_order_detail.sql;
hive -f ddl_ods_order_info.sql;
hive -f ddl_ods_order_status_log.sql;
hive -f ddl_ods_payment_info.sql;
hive -f ddl_ods_sku_info.sql;
hive -f ddl_ods_spu_info.sql;
hive -f ddl_ods_user_info.sql;