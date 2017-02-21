package bamboo.trove.periodic;

import static org.junit.Assert.*;

import java.util.Calendar;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PeriodicWarcManagerTest{
	private static PeriodicWarcManager manager;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
		manager = new PeriodicWarcManager();
		manager.setLimitRunningTime(true);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStartHour(){
		manager.setLimitStartHour(24);
	}
	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStartHour2(){
		manager.setLimitStartHour(-1);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStopHour(){
		manager.setLimitStopHour(24);
	}
	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStopHour2(){
		manager.setLimitStopHour(-1);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStartMinute(){
		manager.setLimitStartMinute(60);
	}
	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStartMinute2(){
		manager.setLimitStartMinute(-1);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStopMinute(){
		manager.setLimitStopMinute(60);
	}
	@Test(expected=IllegalArgumentException.class)
	public void testSetLimitStopMinute2(){
		manager.setLimitStopMinute(-1);
	}

	@Test
	public void testCanRunTime(){
		
		checkStart(-2, -10, +2, +10, true);
		checkStart(-2, +10, +2, -10, true);
		checkStart(0, -10, 0, +10, true);
		checkStart(+2, -10, +2, +10, false);
		checkStart(-2, -10, -2, +10, false);
		checkStart(0, +10, 0, +15, false);
		checkStart(0, -15, 0, -10, false);
		checkStart(0, 0, +2, +10, true);
		checkStart(-2, -10, 0, 0, true);
		checkStart(0, 0, 0, +10, true);
		checkStart(0, -10, 0, 0, true);
		checkStart(0, +10, 0, +20, false);
		checkStart(0, -20, 0, -10, false);
	}

	@Test
	public void testCanRunTime2(){
		// start time after finish
		// may need to run for 22 hours or start time before midnight and finish after
		checkStart(4, 0, 1, 0, true);
		checkStart(0, 20, 0, 10, true);
		checkStart(4, 0, -1, 0, false);
		checkStart(0, 20, 0, -10, false);

		checkStart(2, 10, -2, -10, false);
		checkStart(-2, -10, -2, 10, false);
		checkStart(0, 10, 0, -10, false);
		checkStart(-2, 10, -2, -10, true);
		checkStart(2, 10, 2, -10, true);
		checkStart(0, -10, 0, -15, true);
		checkStart(0, 15, 0, 10, true);
		checkStart(0, 0, -2, -10, false);
		checkStart(2, 10, 0, 0, false);
		checkStart(0, 0, 0, -10, false);
		checkStart(0, 10, 0, 0, false);
		checkStart(0, -10, 0, -20, true);
		checkStart(0, 20, 0, 10, true);
}
	
	private void checkStart(int startHourMod, int startMinuteMod, int stopHourMod, int stopMinuteMod, boolean expected){
		Calendar time = Calendar.getInstance();
		time.add(Calendar.HOUR, startHourMod);
		time.add(Calendar.MINUTE, startMinuteMod);
		manager.setLimitStartHour(time.get(Calendar.HOUR_OF_DAY));
		manager.setLimitStartMinute(time.get(Calendar.MINUTE));
		time = Calendar.getInstance();
		time.add(Calendar.HOUR, stopHourMod);
		time.add(Calendar.MINUTE, stopMinuteMod);
		manager.setLimitStopHour(time.get(Calendar.HOUR_OF_DAY));
		manager.setLimitStopMinute(time.get(Calendar.MINUTE));
		String msg = "check time should " + (expected?"pass":"fail") + " h:" + startHourMod + " m:" + startMinuteMod + " h:" + stopHourMod + " m:" + stopMinuteMod;
		assertEquals(msg, expected, manager.canRunTime());

	}
}
