package bamboo.trove.services;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;

import bamboo.trove.common.BaseWarcDomainManager;
import lookupClient.LookupPageRankLinkTextClassification;

@Service
public class RankingService{
	private String rankingServiceUrl; 
	
	public LookupPageRankLinkTextClassification getLookupService() throws Exception{
		if(BaseWarcDomainManager.isDisableIndexing()){
			return null;
		}
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
