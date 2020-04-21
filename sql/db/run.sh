#!/bin/bash

do_date=$1
##do_date=2020-04-07

## ./ods_activity_info.sh $do_date
## ./ods_activity_order.sh $do_date
## ./ods_activity_rule.sh $do_date
## ./ods_activity_sku.sh $do_date
## ./ods_base_category1.sh $do_date
## ./ods_base_category2.sh $do_date
## ./ods_base_category3.sh $do_date
## ./ods_base_dic.sh $do_date
## ./ods_base_province.sh $do_date
## ./ods_base_region.sh $do_date
## ./ods_base_trademark.sh $do_date
## ./ods_cart_info.sh $do_date
## ./ods_comment_info.sh $do_date
## ./ods_coupon_info.sh $do_date
## ./ods_coupon_use.sh $do_date
## ./ods_favor_info.sh $do_date
## ./ods_order_detail.sh $do_date
## ./ods_order_info.sh $do_date
## ./ods_order_refund_info.sh $do_date
## ./ods_order_status_log.sh $do_date
## ./ods_payment_info.sh $do_date
## ./ods_sku_info.sh $do_date
## ./ods_spu_info.sh $do_date
## ./ods_user_info.sh $do_date


./dwd_dim_activity_info.sh $do_date
./dwd_dim_base_province.sh $do_date
./dwd_dim_coupon_info.sh $do_date
./dwd_dim_sku_info.sh $do_date
./dwd_dim_user_info_his.sh $do_date
./dwd_fact_cart_info.sh $do_date
./dwd_fact_comment_info.sh $do_date
./dwd_fact_coupon_use.sh $do_date
./dwd_fact_favor_info.sh $do_date
./dwd_fact_order_detail.sh $do_date
./dwd_fact_order_info.sh $do_date
./dwd_fact_order_refund_info.sh $do_date
./dwd_fact_payment_info.sh $do_date

./dws_activity_info_daycount.sh $do_date
./dws_coupon_use_daycount.sh $do_date
./dws_sale_detail_daycount.sh $do_date
./dws_sku_action_daycount.sh $do_date
./dws_user_action_daycount.sh $do_date

./dwt_sku_topic.sh $do_date
./dwt_user_topic.sh $do_date

./ads_appraise_bad_topN.sh $do_date
./ads_order_daycount.sh $do_date
./ads_payment_daycount.sh $do_date
./ads_product_cart_topN.sh $do_date
./ads_product_favor_topN.sh $do_date
./ads_product_info.sh $do_date
./ads_product_refund_topN.sh $do_date
./ads_product_sale_topN.sh $do_date
./ads_sale_tm_category1_stat_mn.sh $do_date
./ads_user_action_convert_day.sh $do_date
./ads_user_topic.sh $do_date

