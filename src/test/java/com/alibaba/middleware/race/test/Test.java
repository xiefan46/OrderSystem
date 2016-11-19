package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.RaceUtil;




public class Test {
	@org.junit.Test
	public void testRowToString()
	{
		String line = "orderid:0	goodid:goodal_bb288141-585d-430f-bd06-e6f9d0207740	buyerid:ap_d63a5cf1-df29-4bc6-a49a-4b662081eeaf	createtime:1310604811324	done:true	amount:31	app_order_20_16:4952.635207348086	app_order_10_26:5884.834700906796	app_order_17_6:288.459476304056	app_order_8_30:8861.961283160806	app_order_22_4:2442.1932135608395";
		System.out.println(RaceUtil.createRowFromRowStr(line).toString());
	}
}
