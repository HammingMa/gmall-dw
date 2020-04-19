#!/bin/bash

do_date=$1
##do_date=2020-04-07

./ods_activity_info.sh $do_date
./ods_activity_order.sh $do_date
./ods_activity_rule.sh $do_date
./ods_activity_sku.sh $do_date
./ods_base_category1.sh $do_date
./ods_base_category2.sh $do_date
./ods_base_category3.sh $do_date
./ods_base_dic.sh $do_date
./ods_base_province.sh $do_date
./ods_base_region.sh $do_date
./ods_base_trademark.sh $do_date
./ods_cart_info.sh $do_date
./ods_comment_info.sh $do_date
./ods_coupon_info.sh $do_date
./ods_coupon_use.sh $do_date
./ods_favor_info.sh $do_date
./ods_order_detail.sh $do_date
./ods_order_info.sh $do_date
./ods_order_refund_info.sh $do_date
./ods_order_status_log.sh $do_date
./ods_payment_info.sh $do_date
./ods_sku_info.sh $do_date
./ods_spu_info.sh $do_date
./ods_user_info.sh $do_date