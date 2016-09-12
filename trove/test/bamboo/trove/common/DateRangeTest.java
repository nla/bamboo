package bamboo.trove.common;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

public class DateRangeTest{

	@Test
	public void testDateRange(){
		Date start = new Date(System.currentTimeMillis() - 1000);
		Date end = null;
		assertNotNull(new DateRange(start, end));
		end = new Date();
		assertNotNull(new DateRange(start, end));
		start = null;
		assertNotNull(new DateRange(start, end));
	}
	
	@Test(expected=NullPointerException.class)
	public void testDateRangeNullPointer(){
		new DateRange(null, null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testDateRangeInvalid(){
		Date start = new Date(System.currentTimeMillis());
		Date end = new Date(System.currentTimeMillis() - 10000);
		new DateRange(start, end);
	}
	
	@Test
	public void testIsDateInRange(){
		Date before = new Date(System.currentTimeMillis()- 30000);
		Date start = new Date(System.currentTimeMillis()- 20000);
		Date middle = new Date(System.currentTimeMillis()- 15000);
		Date end = new Date(System.currentTimeMillis()- 10000);
		Date after = new Date(System.currentTimeMillis());
		
		DateRange range = new DateRange(start, end);
		assertFalse(range.isDateInRange(before));
		assertTrue(range.isDateInRange(middle));
		assertFalse(range.isDateInRange(after));
		assertTrue(range.isDateInRange(start));
		assertTrue(range.isDateInRange(end));
	}

	@Test
	public void testIsDateInRangeOpenStart(){
		Date before = new Date(System.currentTimeMillis()- 30000);
		Date start = null;
		Date middle = new Date(System.currentTimeMillis()- 15000);
		Date end = new Date(System.currentTimeMillis()- 10000);
		Date after = new Date(System.currentTimeMillis());
		
		DateRange range = new DateRange(start, end);
		assertTrue(range.isDateInRange(before));
		assertTrue(range.isDateInRange(middle));
		assertFalse(range.isDateInRange(after));
		assertTrue(range.isDateInRange(end));
	}
	@Test
	public void testIsDateInRangeOpenEnd(){
		Date before = new Date(System.currentTimeMillis()- 30000);
		Date start = new Date(System.currentTimeMillis()- 20000);
		Date middle = new Date(System.currentTimeMillis()- 15000);
		Date end = null;
		Date after = new Date(System.currentTimeMillis());
		
		DateRange range = new DateRange(start, end);
		assertFalse(range.isDateInRange(before));
		assertTrue(range.isDateInRange(middle));
		assertTrue(range.isDateInRange(after));
		assertTrue(range.isDateInRange(start));
	}
	
	@Test(expected=NullPointerException.class)
	public void testIsDateInRangeNullPointer(){
		Date start = new Date(System.currentTimeMillis()- 20000);
		Date end = new Date(System.currentTimeMillis());
		
		DateRange range = new DateRange(start, end);
		range.isDateInRange(null);
	}
	
	@Test(expected=NullPointerException.class)
	public void testIsDateInRangeOpenStartNullPointer(){
		Date start = null;
		Date end = new Date(System.currentTimeMillis());
		
		DateRange range = new DateRange(start, end);
		range.isDateInRange(null);
	}
	
	@Test(expected=NullPointerException.class)
	public void testIsDateInRangeOpenEndNullPointer(){
		Date start = new Date(System.currentTimeMillis()- 20000);
		Date end = null;
		
		DateRange range = new DateRange(start, end);
		range.isDateInRange(null);		
	}
	@Test
	public void testIsValidRange(){
		Date start = new Date(System.currentTimeMillis()- 20000);
		Date end = new Date(System.currentTimeMillis()- 10000);
		
		assertFalse(DateRange.isValidRange(end, start));
		assertTrue(DateRange.isValidRange(start, end));
		assertTrue(DateRange.isValidRange(start, null));
		assertTrue(DateRange.isValidRange(null, end));
		assertFalse(DateRange.isValidRange(null, null));
	}

}
