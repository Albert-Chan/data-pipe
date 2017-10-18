package com.dataminer.datetime;

import org.junit.Test;

public class TimeWindowTest {
	@Test
	public void test() {

		long start, end;

		start = System.currentTimeMillis();
		for (int i = 0; i < 1000000L; i++) {
			TimeWindow.of("2017/05/15 11:01:12", "yyyy/MM/dd HH:mm:ss");
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);
		
		
		
		
//		assertEquals(LocalDateTime.of(2017, 5, 15, 11, 0, 0), window.toLeftEdge(15));
//		assertEquals(LocalDateTime.of(2017, 5, 15, 11, 15, 0), window.toRightEdge(15));
	}
}
