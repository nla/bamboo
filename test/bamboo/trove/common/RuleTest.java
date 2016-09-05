package bamboo.trove.common;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;

public class RuleTest{

	@Test
	public void testRule(){
		Date start = new Date(System.currentTimeMillis()-1000000);
		Date end = new Date(System.currentTimeMillis()-1000);
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(com,"));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 1000000, null, null, null, null , "(com,"));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, null, null, null , "(com,"));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, end, null, null , "(com,"));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, end, null, null , "(com,"));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, null , "(com,"));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, end , "(com,"));
		assertNotNull(new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, end , "(com,"));
	}

	@Test(expected=NullPointerException.class)
	public void testRuleNull1(){
		new Rule(1, null, new Date(), 0, null, null, null, null , "(com,");
	}
	
	@Test(expected=NullPointerException.class)
	public void testRuleNull2(){
		new Rule(1, DocumentStatus.ACCEPTED, null, 0, null, null, null, null , "(com,");
	}

	@Test(expected=NullPointerException.class)
	public void testRuleNull3(){
		new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , null);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testRuleId(){
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleEmbargo(){
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, null, null, null , "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleDateRange1(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, end, start, null, null , "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleDateRange2(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, null, end, start, "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange1(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, null, end, null, "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange2(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, end, null, start, "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange3(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, null, end, start, "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange4(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, null, end, end, start, "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange5(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, end, start, end, "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange6(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, end, start, null, "(com,");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRuleMultiRange7(){
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		new Rule(-1, DocumentStatus.ACCEPTED, new Date(), -1000, start, end, null, end, "(com,");
	}

	@Test
	public void testMatches(){
		String url1 = "http://trove.nla.gov.au/index.html";
		String url2 = "http://www.biz.com/index.html";
		
		Date capture = new Date(System.currentTimeMillis()- 50000);
		Date start = new Date(System.currentTimeMillis()- 80000);
		Date end = new Date(System.currentTimeMillis()+ 10000);
		Rule r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,gov,nla,");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(au,gov,nla,trove,)/home.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));

		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 60000, null, null, null, null , "(au,gov,nla,trove,)/index.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 20000, null, null, null, null , "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));

		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, null, null, null , "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, end, null, null , "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, start, null, null , "(au,gov,nla,trove,)/index.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, end, null, null, null , "(au,gov,nla,trove,)/index.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, start, end, null, null , "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, end, end, null, null , "(au,gov,nla,trove,)/index.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));

		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, null,  "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null,null, end, "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, start,  "(au,gov,nla,trove,)/index.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, end, null,"(au,gov,nla,trove,)/index.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, end, "(au,gov,nla,trove,)/index.html");
		assertTrue(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
		r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, start, start, "(au,gov,nla,trove,)/index.html");
		assertFalse(r.matches(url1, capture));
		assertFalse(r.matches(url2, capture));
	}

}
