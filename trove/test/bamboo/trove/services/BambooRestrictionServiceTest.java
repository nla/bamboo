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
		Date now = new Date();
		service.currentRules.add(new Rule(1, DocumentStatus.ACCEPTED, now, 0, null, null, null, null, "(", false));
		service.currentRules.add(new Rule(2, DocumentStatus.RESTRICTED_FOR_BOTH, now, 0, null, null, null, null, "(au,", false));
		service.currentRules.add(new Rule(3, DocumentStatus.RESTRICTED_FOR_DELIVERY, now, 0, null, null, null, null, "(au,gov,", false));
		service.currentRules.add(new Rule(4, DocumentStatus.REJECTED, now, 0, null, null, null, null, "(au,gov,nla,", false));
		service.currentRules.add(new Rule(5, DocumentStatus.ACCEPTED, now, 0, null, null, null, null, "(au,gov,nla,trove,", false));
		service.currentRules.add(new Rule(6, DocumentStatus.REJECTED, now, 0, null, null, null, null, "(au,gov,nla,trove,)/home.html", false));
		Document doc = new Document();
		doc.setUrl("trove.nla.gov.au/home.html");
		Rule r = service.filterDocument(doc);
		assertEquals(DocumentStatus.REJECTED, r.getPolicy());
		
		doc.setUrl("trove.nla.gov.au/home.xml");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.ACCEPTED, r.getPolicy());
		
		doc.setUrl("trove.nla.gov.au/index.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.ACCEPTED, r.getPolicy());
		
		doc.setUrl("dlir.aec.gov.au/home.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.RESTRICTED_FOR_DELIVERY, r.getPolicy());

		doc.setUrl("dlir.aec.com.au/home.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, r.getPolicy());
		
		doc.setUrl("dlir.aec.com/home.html");
		r = service.filterDocument(doc);
		assertEquals(DocumentStatus.ACCEPTED, r.getPolicy());

	}

}
