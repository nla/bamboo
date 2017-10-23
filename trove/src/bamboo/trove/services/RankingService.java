package bamboo.trove.services;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

@Service
public class RankingService{
	private String rankingServiceUrl; 

	public RankingService(){
		System.out.println("RankingService : start");
	}

	@PostConstruct
	public void init(){
		System.out.println("RankingService : init");
		//TODO open connection?
	}
	
	public RankingContainer getRanking(String url){
		String[] t = {"click here", "click there", "click everywhere"};
		return new RankingContainer(t, 2.7);
	}
	
	@Required
	public String setRankingServiceUrl(){
		return rankingServiceUrl;
	}


	public static class RankingContainer{
		private String[] linkText;
  	private double ranking;
  	
  	protected RankingContainer(String[] linkText, double ranking){
  		this.linkText = linkText;
  		this.ranking = ranking;
  	}
  	
  	public String[] getLinkText(){
  		return linkText;
  	}
  	public double getRanking(){
  		return ranking;
  	}
  }
}
