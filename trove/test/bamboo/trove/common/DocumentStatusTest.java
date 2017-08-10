package bamboo.trove.common;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DocumentStatusTest{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
	}

	/**
	 * Test for converting from solr doc back to policy.
	 */
	@Test
	public void testStatus(){
		assertEquals(DocumentStatus.ACCEPTED, DocumentStatus.status(null, null));
		assertEquals(DocumentStatus.RESTRICTED_FOR_BOTH, DocumentStatus.status(false, false));
		assertEquals(DocumentStatus.RESTRICTED_FOR_DELIVERY, DocumentStatus.status(false, null));
		assertEquals(DocumentStatus.RESTRICTED_FOR_DISCOVERY, DocumentStatus.status(null, false));
		assertEquals(DocumentStatus.ACCEPTED, DocumentStatus.status(true, true));
		assertEquals(DocumentStatus.ACCEPTED, DocumentStatus.status(true, null));
		assertEquals(DocumentStatus.ACCEPTED, DocumentStatus.status(null, true));
		assertEquals(DocumentStatus.RESTRICTED_FOR_DISCOVERY, DocumentStatus.status(true, false));
		assertEquals(DocumentStatus.RESTRICTED_FOR_DELIVERY, DocumentStatus.status(false, true));
	}

}
