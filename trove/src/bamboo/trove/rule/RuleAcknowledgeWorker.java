package bamboo.trove.rule;

import org.apache.solr.common.SolrInputDocument;

import au.gov.nla.trove.indexer.api.AcknowledgeWorker;

public class RuleAcknowledgeWorker implements AcknowledgeWorker{

	private String id;
	private RuleChangeUpdateManager manager;
	private boolean finishedSending = false;
	private int count = 0;
	private int sleepCount = 0;
	public RuleAcknowledgeWorker(String id, RuleChangeUpdateManager manager){
		this.id = id;
		this.manager = manager;
	}
	
	public void allSent(){
		finishedSending = true;
	}
	
	public synchronized void addSendCount(){
		count++;
	}
	
	@Override
	public void acknowledge(SolrInputDocument arg0){
		while(!finishedSending){
			try{
				Thread.sleep(10);
			}
			catch (InterruptedException e){
				// ignore
			}
		}
		synchronized (this){
			if(--count == 0){
				manager.acknowledge(id);
			}			
		}
	}

	@Override
	public synchronized void errorProcessing(SolrInputDocument arg0, Throwable error){
		manager.setError("Error processing document " + id, error);
		manager.stopProcessing();
	}

}
