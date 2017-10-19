package com.dataminer.datetime;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TimeWindowTest {
	@Test
	public void test() {
		TimeWindow tw = TimeWindow.interval(Duration.ofMinutes(15));
		LocalDateTime aTime = LocalDateTime.of(2017, 5, 15, 11, 1, 12);

		LocalDateTime left = tw.toLeftEdge(aTime);
		LocalDateTime right = tw.toRightEdge(aTime);

		assertEquals(LocalDateTime.of(2017, 5, 15, 11, 0, 0), left);
		assertEquals(LocalDateTime.of(2017, 5, 15, 11, 15, 0), right);
		
		assertEquals(1660940, tw.getTimeWindowIndex(aTime));
		assertEquals(44, tw.getTimeWindowIndexInDayRange(aTime));
	}
}
