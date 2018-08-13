package bamboo.trove.workers;

import java.util.ArrayList;
import java.util.List;

import bamboo.trove.common.SolrEnum;

public class PageRank{

	public static final float PAGE_RANK_THRESHOLD = 0.8f;
	private static final byte QUEUED_FOR_CLASSIFICATION  = 0b00000001;
	private static final byte CLASSIFICATION_FAILED      = 0b00000010;
	private static final byte IMAGE_HUMAN_SAFE           = 0b00000100; // text inherits worst from contents
	private static final byte IMAGE_AUTO_SAFE            = 0b00001000;  // 8 
	private static final byte IMAGE_AUTO_MAYBE_SAFE      = 0b00001100;  // 12
	private static final byte IMAGE_AUTO_MAYBE_UNSAFE    = 0b00010000;  // 16
	private static final byte IMAGE_AUTO_UNSAFE          = 0b00010100;  // 20
	private static final byte IMAGE_HUMAN_UNSAFE         = 0b00011000;  // 24
	private static final byte TEXT_HUMAN_SAFE            = 0b00100000;  // 32
	private static final byte TEXT_AUTO_SAFE             = 0b01000000;  // 64
	private static final byte TEXT_AUTO_MAYBE_SAFE       = 0b01100000;  // 96
	private static final byte TEXT_AUTO_MAYBE_UNSAFE     = (byte) 0b10000000;  // 128 
	private static final byte TEXT_AUTO_UNSAFE           = (byte) 0b10100000;  // 160
	private static final byte TEXT_CONTENTKEEPER_UNSAFE  = (byte) 0b11000000;  // 192
	private static final byte TEXT_HUMAN_UNSAFE          = (byte) 0b11100000;  // 224
	
	private static byte[] CLASSIFICATIONS = {QUEUED_FOR_CLASSIFICATION, 
			CLASSIFICATION_FAILED,
			IMAGE_HUMAN_SAFE,
			IMAGE_AUTO_SAFE,
			IMAGE_AUTO_MAYBE_SAFE,
			IMAGE_AUTO_MAYBE_UNSAFE,
			IMAGE_AUTO_UNSAFE,
			IMAGE_HUMAN_UNSAFE,
			TEXT_HUMAN_SAFE,
			TEXT_AUTO_SAFE,
			TEXT_AUTO_MAYBE_SAFE,
			TEXT_AUTO_MAYBE_UNSAFE,
			TEXT_AUTO_UNSAFE,
			TEXT_CONTENTKEEPER_UNSAFE,
			TEXT_HUMAN_UNSAFE};
	private static SolrEnum[] CLASSIFICATIONS_SOLR = {
			SolrEnum.QUEUED_FOR_CLASSIFICATION, 
			SolrEnum.CLASSIFICATION_FAILED,
			SolrEnum.IMAGE_HUMAN_SAFE,
			SolrEnum.IMAGE_AUTO_SAFE,
			SolrEnum.IMAGE_AUTO_MAYBE_SAFE,
			SolrEnum.IMAGE_AUTO_MAYBE_UNSAFE,
			SolrEnum.IMAGE_AUTO_UNSAFE,
			SolrEnum.IMAGE_HUMAN_UNSAFE,
			SolrEnum.TEXT_HUMAN_SAFE,
			SolrEnum.TEXT_AUTO_SAFE,
			SolrEnum.TEXT_AUTO_MAYBE_SAFE,
			SolrEnum.TEXT_AUTO_MAYBE_UNSAFE,
			SolrEnum.TEXT_AUTO_UNSAFE,
			SolrEnum.TEXT_CONTENTKEEPER_UNSAFE,
			SolrEnum.TEXT_HUMAN_UNSAFE};

	private LinkTextScore[] linkText = null;
	private float ranking;
	private byte classification;
	private long siteHashAndYear;
	private boolean restricted;
	
	public PageRank(final String[] linkText, final float[] score, int size, float ranking, byte classification, long siteHashAndYear){
		this.ranking = ranking;
		restricted = ranking < PAGE_RANK_THRESHOLD;
		this.classification = classification;
		this.linkText = new LinkTextScore[size];
		for(int i=0;i<size;i++){
			this.linkText[i] = new LinkTextScore(linkText[i], score[i]);
		}
		this.siteHashAndYear = siteHashAndYear;
	}
	
	public LinkTextScore[] getLinkText(){
		return linkText;
	}
	public float getRanking(){
		return ranking;
	}
	public boolean isRestricted(){
		return restricted;
	}
	public byte getClassification(){
		return classification;
	}
	public long getSiteHashAndYear(){
		return siteHashAndYear;
	}
	
	public List<SolrEnum> getClassifications(){
		List<SolrEnum> list = new ArrayList<>();
		byte classifyBytes = (byte) ((byte)classification & (byte)0b00000011);
		decodeClassification(list, classifyBytes, 0, 2);

		byte imageBytes = (byte) ((byte)classification & (byte)0b00011100);
		decodeClassification(list, imageBytes, 2, 8);

		byte textBytes = (byte) ((byte)classification & (byte)0b11100000);
		decodeClassification(list, textBytes, 8, 15);

		return list;
	}
	
	private void decodeClassification(List<SolrEnum> list, byte b, int start, int end){
		for(int i=start;i<end;i++){
			if(CLASSIFICATIONS[i] == b){
				list.add(CLASSIFICATIONS_SOLR[i]);
				break; // should be only one from each group
			}
		}		
	}
	
	class LinkTextScore{
		private String linkText;
		private float score;
		
		private LinkTextScore(String text, float score){
			this.linkText = text;
			this.score = score;
		}
		
		public String getLinkText(){
			return linkText;
		}
		public float getScore(){
			return score;
		}
	}
}

