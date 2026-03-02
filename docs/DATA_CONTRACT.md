
trakcing_id = 用户输入
shipperName = shipperName,
created_time = '第一个type=label时的tsMillis', 换算成当地时间
scanned_time = '第一个descriptionk开头=Scan at的tsMillis' 换算成当地时间，如果没有，则跳过
out_for_delivery_time = '第一个type=out-for-delivery的tsMillis", 换算成当地时间
attempted_time = '第一个type=fail的tsMillis', 换算成当地时间，如果没有，则跳过
failed_event_name = "第一个type=fail的event code 中的name', 如果没有，则跳过
failed_route = '第一个type=fail的description中route后面的内容
~~failed_driver = "第一个type=fail的generatedBy后面的邮箱，如果没有，则跳过~~
delivered_time = '第一个type=success的tsMillis', 换算成当地时间，如果没有，则跳过
success_name = '第一个type=success的event code中的name'，如果没有，则跳过
success_route = 第一个type=success的description中route后面的内容
~~success_driver = 第一个type=success的generatedBy后面的邮箱，如果没有，则跳过~~
创建到入库时间 =  scanned_time - created_time, 折算成小时展示，缺时间则跳过
库内停留时间 = out_for_delivery_time - scanned_time，折算成小时展示，缺时间则跳过
尝试配送时间 = attempted_time - out_for_delivery_time，折算成小时展示，缺时间则跳过
送达时间 = delivered_time - out_for_delivery_time，折算成小时展示，缺时间则跳过
整体配送时间 = delivered_time - created_time，折算成小时展示，缺时间则跳过