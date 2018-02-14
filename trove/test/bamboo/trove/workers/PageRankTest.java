package bamboo.trove.workers;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import bamboo.trove.common.SolrEnum;

public class PageRankTest{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
	}

	@Test
	public void testGetClassifications(){
		String[] txt = {"one"};
		float[] rank = {1.0f};
		
		PageRank pr = new PageRank(txt, rank, 0, 1.7f,  (byte)0b00000101);
		assertArrayEquals(new SolrEnum[]{SolrEnum.QUEUED_FOR_CLASSIFICATION, SolrEnum.IMAGE_HUMAN_SAFE}, 
				pr.getClassifications().toArray());
		
		pr = new PageRank(txt, rank, 0, 1.7f,  (byte)0b00100100);
		assertArrayEquals(new SolrEnum[]{SolrEnum.IMAGE_HUMAN_SAFE, SolrEnum.TEXT_HUMAN_SAFE}, 
				pr.getClassifications().toArray());
		pr = new PageRank(txt, rank, 0, 1.7f,  (byte)0b11100100);
		assertArrayEquals(new SolrEnum[]{SolrEnum.IMAGE_HUMAN_SAFE, SolrEnum.TEXT_HUMAN_UNSAFE}, 
				pr.getClassifications().toArray());
		pr = new PageRank(txt, rank, 0, 1.7f,  (byte)0b11101100);
		assertArrayEquals(new SolrEnum[]{SolrEnum.IMAGE_AUTO_MAYBE_SAFE, SolrEnum.TEXT_HUMAN_UNSAFE}, 
				pr.getClassifications().toArray());
		pr = new PageRank(txt, rank, 0, 1.7f,  (byte)0b11111000);
		assertArrayEquals(new SolrEnum[]{SolrEnum.IMAGE_HUMAN_UNSAFE, SolrEnum.TEXT_HUMAN_UNSAFE}, 
				pr.getClassifications().toArray());
	}

}
