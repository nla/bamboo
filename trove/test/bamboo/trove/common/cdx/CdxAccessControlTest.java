/*
 * Copyright 2016-2017 National Library of Australia
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bamboo.trove.common.cdx;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CdxAccessControlTest{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
	}

	@Test
	public void testGetSearchUrl(){
		assertEquals("trove.com.au/", CdxAccessControl.getSearchUrl("http://trove.com.au"));
		assertEquals("trove.com.au/", CdxAccessControl.getSearchUrl("http://www.trove.com.au"));
		assertEquals("trove.com.au/", CdxAccessControl.getSearchUrl("trove.com.au"));
		assertEquals("trove.com.au/", CdxAccessControl.getSearchUrl("www.trove.com.au"));
		assertEquals("trove.com.au/", CdxAccessControl.getSearchUrl("https://trove.com.au"));
		assertEquals("trove.com.au/", CdxAccessControl.getSearchUrl("ftp://trove.com.au"));
		assertEquals("trove.com.au/search", CdxAccessControl.getSearchUrl("http://trove.com.au/search"));
		assertEquals("trove.com.au/search", CdxAccessControl.getSearchUrl("http://trove.com.au/search#show"));
		assertEquals("trove.com.au/search", CdxAccessControl.getSearchUrl("http://trove.com.au/search?q=fred"));
		assertEquals("trove.com.au/search", CdxAccessControl.getSearchUrl("http://trove.com.au/search?q=fred#show"));
		assertEquals("trove.com.au/search", CdxAccessControl.getSearchUrl("http://trove.com.au/search?q=fred&name=mary"));
	}

}
