package bamboo.trove.services;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import bamboo.trove.common.BaseWarcDomainManager;
import lookupClient.LookupPageRankLinkTextClassification;

@Service
public class RankingService{
	private String rankingServiceUrl; 

	public LookupPageRankLinkTextClassification getLookupService() throws Exception{
		return new LookupPageRankLinkTextClassification(BaseWarcDomainManager.rankingService.getRankingServiceUrl());
	}
	
	public RankingService(){
	}

	@PostConstruct
	public void init(){
	}
	
	public String setRankingServiceUrl(){
		return rankingServiceUrl;
	}

	public String getRankingServiceUrl(){
		return rankingServiceUrl;
	}
}
