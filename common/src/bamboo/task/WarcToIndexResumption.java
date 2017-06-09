package bamboo.task;

public class WarcToIndexResumption extends WarcToIndex{
	private String resumptionToken;
	
	public String getResumptionToken(){
		return resumptionToken;
	}
	public void setResumptionToken(String resumptionToken){
		this.resumptionToken = resumptionToken;
	}
}
