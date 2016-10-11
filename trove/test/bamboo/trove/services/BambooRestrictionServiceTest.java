package bamboo.trove.services;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import bamboo.task.Document;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.Rule;

public class BambooRestrictionServiceTest{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
	}

	@Test
	public void testFilterDocument(){
		BambooRestrictionService service = new BambooRestrictionService();  
		service.currentRules = new ArrayList<Rule>();
		service.newRules = new ArrayList<Rule>();
		Date now = new Date();
		service.currentRules.add(new Rule(1, DocumentStatus.ACCEPTED, now, 0, null, null, null, null, "(", false));
		service.currentRules.add(new Rule(2, DocumentStatus.RESTRICTED_FOR_BOTH, now, 0, null, null, null, null, "(au,", false));
		service.currentRules.add(new Rule(3, DocumentStatus.RESTRICTED_FOR_DELIVERY, now, 0, null, null, null, null, "(au,gov,", false));
		service.currentRules.add(new Rule(4, DocumentStatus.NOT_APPLICABLE, now, 0, null, null, null, null, "(au,gov,nla,", false));
		service.currentRules.add(new Rule(5, DocumentStatus.ACCEPTED, now, 0, null, null, null, null, "(au,gov,nla,trove,", false));
		service.currentRules.add(new Rule(6, DocumentStatus.NOT_APPLICABLE, now, 0, null, null, null, null, "(au,gov,nla,trove,)/home.html", false));
		service.updateRulesBySiteList(service.currentRules);
		Document doc = new Document();
		doc.setUrl("http://trove.nla.gov.au/home.html");
		Rule r = service.filterDocument(doc);
		assertEquals(DocumentStatus.NOT_APPLICABLE, r.getPolicy());
		
		doc.setUrl("http://trove.nla.gov.au/home.xml");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.ACCEPTED, r.getPolicy());
		
		doc.setUrl("https://trove.nla.gov.au/index.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.ACCEPTED, r.getPolicy());
		
		doc.setUrl("ftp://dlir.aec.gov.au/home.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.RESTRICTED_FOR_DELIVERY, r.getPolicy());

		doc.setUrl("http://dlir.aec.com.au:8080/home.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, r.getPolicy());
		
		doc.setUrl("ftps://dlir.aec.com/home.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.ACCEPTED, r.getPolicy());

	}
	
	@Test
	public void testMatchesBadURL(){
		BambooRestrictionService service = new BambooRestrictionService();  
		service.currentRules = new ArrayList<Rule>();
		service.newRules = new ArrayList<Rule>();
		Rule r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(", false);
		service.currentRules.add(r);
		service.updateRulesBySiteList(service.currentRules);
		
		Date capture = new Date(System.currentTimeMillis()- 50000);
		Document doc = new Document();
		doc.setUrl("/losch.com.au/favicon.ico");
		doc.setDate(capture);
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());

		doc.setUrl("losch.com.au/favicon.ico");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
	}

	@Test
	public void testMatchesCatchAll(){
		BambooRestrictionService service = new BambooRestrictionService();  
		service.currentRules = new ArrayList<Rule>();
		service.newRules = new ArrayList<Rule>();
		Rule r = new Rule(1, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(", false);
		service.currentRules.add(r);
		service.updateRulesBySiteList(service.currentRules);
		Date capture = new Date(System.currentTimeMillis()- 50000);
		Document doc = new Document();
		doc.setDate(capture);

		doc.setUrl("http://mailto:linda@losch.com.au/favicon.ico");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
		doc.setUrl("http://mailto:gary.court@gmail.com/robots.txt");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
		doc.setUrl("http://user:pass@example.com/robots.txt");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
		doc.setUrl("http://user:pass@example.com/");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
		doc.setUrl("http://chrstian+1@cluebeck.de/");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());

		r = new Rule(2, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(com,", false);
		service.currentRules.add(r);
		service.updateRulesBySiteList(service.currentRules);

		doc.setUrl("http://mailto:linda@losch.com.au/favicon.ico");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
		doc.setUrl("http://mailto:gary.court@gmail.com/robots.txt");
		r = service.filterDocument(doc);
		assertEquals(2, r.getId());
		doc.setUrl("http://user:pass@example.com/robots.txt");
		r = service.filterDocument(doc);
		assertEquals(2, r.getId());
		doc.setUrl("http://user:pass@example.com/");
		r = service.filterDocument(doc);
		assertEquals(2, r.getId());
		doc.setUrl("http://chrstian+1@cluebeck.de/");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
		
		r = new Rule(3, DocumentStatus.ACCEPTED, new Date(), 0, null, null, null, null , "(com,gmail,)/robots.txt", false);
		service.currentRules.add(r);
		service.updateRulesBySiteList(service.currentRules);
		
		doc.setUrl("http://mailto:linda@losch.com.au/favicon.ico");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
		doc.setUrl("http://mailto:gary.court@gmail.com/robots.txt");
		r = service.filterDocument(doc);
		assertEquals(3, r.getId());
		doc.setUrl("http://user:pass@example.com/robots.txt");
		r = service.filterDocument(doc);
		assertEquals(2, r.getId());
		doc.setUrl("http://user:pass@example.com/");
		r = service.filterDocument(doc);
		assertEquals(2, r.getId());
		doc.setUrl("http://chrstian+1@cluebeck.de/");
		r = service.filterDocument(doc);
		assertEquals(1, r.getId());
	}
}
