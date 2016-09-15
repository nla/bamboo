package bamboo.trove.common;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

public class RuleTest{

	@Test
	public void testRule(){
		Date start = new Date(System.currentTimeMillis()-1000000);
		Date end = new Date(System.currentTimeMillis()-1000);
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(com,", false));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 1000000, null, null, null, null , "(com,", false));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, null, null, null , "(com,", false));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, end, null, null , "(com,", false));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, end, null, null , "(com,", false));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, null , "(com,", false));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, end , "(com,", false));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, end , "(com,", false));
	}

	@Test(expected=NullPointerException.class)
	public void testRuleNull1(){
		new Rule(1, null, new Date(), 0, null, null, null, null , "(com,", false);
	}
	
	@Test(expected=NullPointerException.class)
	public void testRuleNull2(){
		new Rule(1, DocumentStatus.ACCEPTED, null, 0, null, null, null, null , "(com,", false);
	}

	@Test(expected=NullPointerException.class)
	public void testRuleNull3(){
		new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , null, false);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testRuleId(){
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleEmbargo(){
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, null, null, null , "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleDateRange1(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, end, start, null, null , "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleDateRange2(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, null, end, start, "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange1(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, null, end, null, "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange2(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, end, null, start, "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange3(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, null, end, start, "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange4(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, end, end, start, "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange5(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, end, start, end, "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange6(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, end, start, null, "(com,", false);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange7(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, end, null, end, "(com,", false);
	}

	@Test
	public void testMatches(){
		String url1 = "http://trove.nla.gov.au/index.html";
		String url2 = "http://www.biz.com/index.html";
		
		Date capture = new Date(System.currentTimeMillis()- 50000);
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		Rule r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,gov,nla,", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,gov,nla,", true);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "http://(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,gov,nla,trove,:80)/home.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));

		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 60000, null, null, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 20000, null, null, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));

		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, null, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, end, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, start, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, end, null, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, end, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, end, end, null, null , "(au,gov,nla,trove,)/index.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));

		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, null,  "(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null,null, end, "(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, start,  "(au,gov,nla,trove,)/index.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, end, null,"(au,gov,nla,trove,)/index.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, end, "(au,gov,nla,trove,)/index.html", false);
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, start, "(au,gov,nla,trove,)/index.html", false);
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
	}

	@Test
	public void testSort(){
		// array of rules that when sorted should be in id order.
		long nowLong = System.currentTimeMillis();
		Date now = new Date(nowLong);
		Date before = new Date(nowLong - 100000);
		Date after = new Date(nowLong + 100000);
		Rule[] rules = {new Rule(5, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(com,", false),
				new Rule(4, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,gov,", false),
				new Rule(8, DocumentStatus.ACCEPTED, new Date(), 0, before, before, now, after , "(com,aa,", false),
				new Rule(3, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,com,", false),
				new Rule(9, DocumentStatus.ACCEPTED, new Date(), 0, before, before, before, after , "(com,aa,", false),
				new Rule(2, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,com,", true),
				new Rule(6, DocumentStatus.ACCEPTED, new Date(), 0, now, now, before, before , "(com,aa,", false),
				new Rule(7, DocumentStatus.ACCEPTED, new Date(), 0, before, now, before, before , "(com,aa,", false),			
				new Rule(12, DocumentStatus.ACCEPTED, new Date(), 0, before, before, before, before , "(com,aa,", false),
				new Rule(10, DocumentStatus.ACCEPTED, new Date(), 10000, before, before, before, before , "(com,aa,", false),
				new Rule(11, DocumentStatus.ACCEPTED, new Date(), 1000, before, before, before, before , "(com,aa,", false),
				new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,", false),
				};
		Arrays.sort(rules);
//		for(Rule r : rules){
//			System.out.println(r.getId()+ " : "+r.getSurt()+" : "+r.getCapturedRange() + " : "+r.getRetrievedRange() + " : "+ r.getEmbargo());
//		}
		int i = 1;
		for(Rule r : rules){
			assertEquals(i++, r.getId());
		}
	}
}
