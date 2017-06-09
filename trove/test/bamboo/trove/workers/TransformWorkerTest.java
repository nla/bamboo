package bamboo.trove.workers;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransformWorkerTest{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception{
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception{
	}

	private static final String text = "the quick brown fox jumps over the lazy dog";

	@Test
	public void testShortenText(){
		assertEquals("the quick", TransformWorker.shortenText(text, 11));
		assertEquals("the quick", TransformWorker.shortenText(text, 9));
		assertEquals("the quick", TransformWorker.shortenText(text, 14));
		assertEquals(text, TransformWorker.shortenText(text, 140));
		assertEquals(text, TransformWorker.shortenText(text, text.length()));
		assertEquals("the", TransformWorker.shortenText("the", 3));
		assertEquals("the", TransformWorker.shortenText("the ", 3));
		assertEquals("", TransformWorker.shortenText("then", 3));
		assertEquals("", TransformWorker.shortenText(text, 2));
		
	}

	@Test
	public void testRe(){
		assertEquals("the quick", TransformWorker.removeExtraSpaces("the   quick"));
		assertEquals("the quick", TransformWorker.removeExtraSpaces("the	quick"));
		assertEquals("the quick brown fox", TransformWorker.removeExtraSpaces("the   quick\nbrown\rfox"));
		assertEquals("the quick brown fox", TransformWorker.removeExtraSpaces("the \t\r\nquick\tbrown\rfox"));
		assertEquals("the quick", TransformWorker.removeExtraSpaces("the   \r\r\r\r\r\r\r\t\t\t\nquick"));
	}
}
