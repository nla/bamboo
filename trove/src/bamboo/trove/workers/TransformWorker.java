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
package bamboo.trove.workers;

import static bamboo.trove.services.QualityControlService.DOCUMENT_CONTENT_TYPES;
import static bamboo.trove.services.QualityControlService.HTML_CONTENT_TYPES;
import static bamboo.trove.services.QualityControlService.PDF_CONTENT_TYPES;
import static bamboo.trove.services.QualityControlService.PRESENTATION_CONTENT_TYPES;
import static bamboo.trove.services.QualityControlService.SPREADSHEET_CONTENT_TYPES;

import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrInputDocument;
import org.netpreserve.urlcanon.Canonicalizer;
import org.netpreserve.urlcanon.ParsedUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import bamboo.trove.common.BaseWarcDomainManager;
import bamboo.trove.common.ContentThreshold;
import bamboo.trove.common.DocumentStatus;
import bamboo.trove.common.FilenameFinder;
import bamboo.trove.common.IndexerDocument;
import bamboo.trove.common.SearchCategory;
import bamboo.trove.common.SolrEnum;
import bamboo.trove.common.TitleTools;
import bamboo.trove.workers.PageRank.LinkTextScore;
//import lookupClient.LookupPageRankLinkTextClassification;

public class TransformWorker implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(TransformWorker.class);

  // Boost modifiers also used in RuleRecheckWorker.
  public static final float BONUS_GOV_SITE = 1.35f;
  public static final float BONUS_EDU_SITE = 1.1f;
  public static final float MALUS_SEARCH_CATEGORY = 0.9f;
  
  private static final float[] LINK_TEXT_SCORE_RANGE = {3.0f, 1.5f, 0.8f};
  
  public static final int TEXT_LIMIT = 1000;
  public static final int TEXT_LIMIT_SMALL = 50;

	private static final Pattern PATTERN_WHITE_SPACE = Pattern.compile("\\s");
	private static final Pattern PATTERN_MULTI_SPACE = Pattern.compile(" +");

	private final Canonicalizer CANON = Canonicalizer.AGGRESSIVE;
  private final boolean indexFullText;

  private Timer timer;
  private IndexerDocument lastJob = null;
  private IndexerDocument thisJob = null;
  
//  private LookupPageRankLinkTextClassification pageRanker;

  public TransformWorker(Timer timer, boolean indexFullText) {
    this.timer = timer;
    this.indexFullText = indexFullText;
//    try{
//			pageRanker = BaseWarcDomainManager.rankingService.getLookupService();
//		}
//		catch (Exception e){
//			log.error("Error starting Page Rank lookup service.", e);
//			if(!BaseWarcDomainManager.isDisableIndexing()){
//				System.exit(5);
//			}
//		}
  }

  @Override
  public void run() {
    while (loop()) {}
  }

  private boolean loop() {
    try {
      findJob();
      doJob();
      lastJob = thisJob;
      thisJob = null;

    } catch (InterruptedException ex) {
      log.error("Thread.Sleep() interrupted on worker that cannot find job. Resuming. MSG:'{}'", ex.getMessage());
      return true;

    } catch (Exception ex) {
      log.error("Unexpected error during TransformWorker execution: ", ex);
      if (thisJob != null) {
        thisJob.setTransformError(ex);
      }
      lastJob = thisJob;
      thisJob = null;
    }
    return true;
  }

  private void findJob() throws InterruptedException {
    thisJob = BaseWarcDomainManager.getNextTransformJob(lastJob);
    lastJob = null;
    while (thisJob == null) {
      Thread.sleep(1000);
      thisJob = BaseWarcDomainManager.getNextTransformJob(null);
    }
  }

  private void doJob() throws Exception{
    thisJob.transform.start(timer);
    transform(thisJob);
    thisJob.transform.finish();
  }

  private void transform(IndexerDocument document) throws Exception {
    // No indexing at all
    if (ContentThreshold.NONE.equals(document.getThreshold())) {
      return;
    }

    SolrInputDocument solr = new SolrInputDocument();
    
    int sec = (int)(document.getBambooDocument().getDate().getTime() / 1000);
//    pageRanker.lookupUrl(document.getBambooDocument().getUrl(), sec, document.getBambooDocument().getSite());
//    PageRank ranking = new PageRank(pageRanker.linkCaptionTexts, pageRanker.linkCaptionTextPageRanks, 
//    		pageRanker.linkCaptionTextCount, pageRanker.pageRank, pageRanker.classification, pageRanker.siteHashAndYear);
    float defaultPageRank = 0.847f;
    PageRank ranking = new PageRank(null, null, 
    		0, defaultPageRank, (byte)0, 0);

    basicMetadata(solr, document, ranking);
    classificationMetadata(solr, document, ranking);
    restrictions(solr, document);
    errorHandling(solr, document);
    fullText(solr, document, ranking);

    // We store the boost in the index too. It lets us look at it for debugging, but also restrictions
    // rules don't need to re-process boost constantly, they can just read it from Solr.
    // there are two fields to hold boost one from page rank and the other for page type boosting
    // we may need to try combination of these?
    solr.addField(SolrEnum.PAGERANK.toString(), ranking.getRanking());// field we are using for boost
    solr.setDocumentBoost(ranking.getRanking());

    document.converted(solr);
  }

  // Not SDF is not thread-safe, but this worker never allows
  // more than one thread into this method per instantiated worker
  private SimpleDateFormat dateYear = new SimpleDateFormat("yyyy");
  
  private void basicMetadata(SolrInputDocument solr, IndexerDocument document, PageRank ranking) {
    solr.addField(SolrEnum.ID.toString(), document.getDocId());

    // Display URL is the original provided by Bamboo
    String url = document.getBambooDocument().getUrl();
    solr.addField(SolrEnum.DISPLAY_URL.toString(), url);
    String deliveryUrl = document.getBambooDocument().getDeliveryUrl();
    if (deliveryUrl == null || "".equals(deliveryUrl)) {
      throw new IllegalArgumentException("Delivery URL is empty for document " + document.getDocId());
    }

    // In the vast majority of cases DELIVERY_URL == canon(DISPLAY_URL)
    // But we test for that because Pandora can throw a spanner in the works
    ParsedUrl parsedUrl = ParsedUrl.parseUrl(url);
    CANON.canonicalize(parsedUrl);
    if (!parsedUrl.toString().equals(deliveryUrl)) {
      // This is a pandora URL. To support exact match on both DISPLAY_URL and DELIVERY_URL
      // we need to store a canonicalized version of DISPLAY_URL
      solr.addField(SolrEnum.PANDORA_URL.toString(), deliveryUrl);
      solr.addField(SolrEnum.DELIVERY_URL.toString(), parsedUrl.toString());
    }
    else{
      solr.addField(SolrEnum.DELIVERY_URL.toString(), deliveryUrl);
    }

    // check if from the pandora collection.
    if(document.isPandora()){
    	solr.addField(SolrEnum.PANDORA.toString(), true);
    }
    
    String filename = FilenameFinder.getFilename(url);
    if (filename != null) {
      solr.addField(SolrEnum.FILENAME.toString(), filename);
    }

    solr.addField(SolrEnum.DATE.toString(), document.getBambooDocument().getDate());
    String year = dateYear.format(document.getBambooDocument().getDate());
    solr.addField(SolrEnum.DECADE.toString(), year.substring(0, 3));
    solr.addField(SolrEnum.YEAR.toString(), year);

    domainAndTitleMetadata(solr, document, ranking);

    // Optional metadata we _might_ get from html
    if(!ranking.isRestricted()){
      optionalMetadata(solr, document.getBambooDocument().getDescription());
      optionalMetadata(solr, document.getBambooDocument().getKeywords());
      optionalMetadata(solr, document.getBambooDocument().getPublisher());
      optionalMetadata(solr, document.getBambooDocument().getCreator());
      optionalMetadata(solr, document.getBambooDocument().getContributor());
      optionalMetadata(solr, document.getBambooDocument().getCoverage());
    }
  }
  
  private void domainAndTitleMetadata(SolrInputDocument solr, IndexerDocument document, PageRank ranking) {
    solr.addField(SolrEnum.SITE.toString(), document.getBambooDocument().getSite());
    solr.addField(SolrEnum.SITE_HASH.toString(), ranking.getSiteHashAndYear());
//    if(document.getBambooDocument().getOgSiteName() != null 
//    		&& !document.getBambooDocument().getOgSiteName().isEmpty()){
//    	solr.addField(SolrEnum.OG_SITE.toString(), document.getBambooDocument().getOgSiteName());
//    }
    solr.addField(SolrEnum.HOST.toString(), document.getBambooDocument().getHost());
    // We reverse the hostname (which is site + sub-domain) for efficient sub-domain wildcarding in Solr
    solr.addField(SolrEnum.HOST_REVERSED.toString(),
            (new StringBuffer(document.getBambooDocument().getHost())).reverse().toString());

    boolean softenSeoMalus = false;
    // If it is an AU gov website we index this
    if (document.getBambooDocument().getSite().endsWith(".gov.au")) {
      solr.addField(SolrEnum.AU_GOV.toString(), true);
      document.modifyBoost(BONUS_GOV_SITE);
      softenSeoMalus = true;
    }
    if (document.getBambooDocument().getSite().endsWith(".edu.au")) {
      document.modifyBoost(BONUS_EDU_SITE);
      softenSeoMalus = true;
    }

    String title = removeExtraSpaces(document.getBambooDocument().getTitle());
    if(!title.isEmpty())
    {    
    	solr.addField(SolrEnum.TITLE.toString(), title);
    }
//    if(document.getBambooDocument().getOgTitle() != null 
//    		&& !document.getBambooDocument().getOgTitle().isEmpty()){
//    	solr.addField(SolrEnum.OG_TITLE.toString(), document.getBambooDocument().getOgTitle());
//    }
    // add link text
    if(!ranking.isRestricted()){
      for(LinkTextScore t : ranking.getLinkText()){
      	if(t.getScore() >= LINK_TEXT_SCORE_RANGE[0]){
      		solr.addField(SolrEnum.LINK_TEXT_1.toString(), t.getLinkText());    	
      	}
      	else if(t.getScore() >= LINK_TEXT_SCORE_RANGE[1]){
      		solr.addField(SolrEnum.LINK_TEXT_2.toString(), t.getLinkText());
      	}
      	else if(t.getScore() >= LINK_TEXT_SCORE_RANGE[2]){
      		solr.addField(SolrEnum.LINK_TEXT_3.toString(), t.getLinkText());    	
      	}
      	else{
      		solr.addField(SolrEnum.LINK_TEXT_4.toString(), t.getLinkText());
        }
      }
    }
    
    if(document.getBambooDocument().getH1() != null 
    		&& !document.getBambooDocument().getH1().isEmpty()){
    	solr.addField(SolrEnum.H1.toString(), document.getBambooDocument().getH1());
    }
    document.modifyBoost(TitleTools.lengthMalus(title));

    float seoMalus = TitleTools.seoMalus(title);
    if (softenSeoMalus) {
      // We only apply 30% of the SEO malus to .gov and .edu sites
      seoMalus = (1.0F - (1.0F - seoMalus) * 0.3F);
    }
    document.modifyBoost(seoMalus);
  }

  /**
   * add classifications that contributed to ranking. 
   * @param solr
   * @param document
   * @param ranking
   */
  private void classificationMetadata(SolrInputDocument solr, IndexerDocument document, PageRank ranking) {
  	for(SolrEnum c : ranking.getClassifications()){
  		solr.addField(c.toString(), true);
  	}
  }
  
  private void optionalMetadata(SolrInputDocument solr, String optionalData) {
    if (optionalData != null && !"".equals(optionalData)) {
      String cleaned = removeExtraSpaces(optionalData).trim();
      solr.addField(SolrEnum.METADATA.toString(), cleaned);
    }
  }

  private void restrictions(SolrInputDocument solr, IndexerDocument document) {
    solr.addField(SolrEnum.RULE.toString(), document.getRuleId());
    // Don't populate if false
    if (DocumentStatus.RESTRICTED_FOR_BOTH.equals(document.getStatus())) {
      solr.addField(SolrEnum.DELIVERABLE.toString(), false);
      solr.addField(SolrEnum.DISCOVERABLE.toString(), false);
    }
    if (DocumentStatus.RESTRICTED_FOR_DELIVERY.equals(document.getStatus())) {
      solr.addField(SolrEnum.DELIVERABLE.toString(), false);
      //solr.addField(SolrEnum.DISCOVERABLE.toString(), true);
    }
    if (DocumentStatus.RESTRICTED_FOR_DISCOVERY.equals(document.getStatus())) {
      //solr.addField(SolrEnum.DELIVERABLE.toString(), true);
      solr.addField(SolrEnum.DISCOVERABLE.toString(), false);
    }
  }

  private void errorHandling(SolrInputDocument solr, IndexerDocument document) {
    if (document.getBambooDocument().getTextError() != null) {
      solr.addField(SolrEnum.TEXT_ERROR.toString(), true);
    }
  }

  private void fullText(SolrInputDocument solr, IndexerDocument document, PageRank pageRank) {
    if (ContentThreshold.METADATA_ONLY.equals(document.getThreshold())) {
      document.modifyBoost(MALUS_SEARCH_CATEGORY);
      solr.addField(SolrEnum.SEARCH_CATEGORY.toString(), SearchCategory.NONE.toString());
      return;
    }

    searchCategory(solr, document);

    String text = "";
    if (ContentThreshold.DOCUMENT_START_ONLY.equals(document.getThreshold())) {
      text = document.getBambooDocument().getText();
      if (text == null) return;
      
      text = removeExtraSpaces(text);
      text = text.trim();
      if(pageRank.isRestricted()){
      	text = shortenTextWords(text, TEXT_LIMIT_SMALL);
      }
      else{
      	text = shortenTextWords(text, TEXT_LIMIT);
      }
    }
    
    //if (ContentThreshold.UNIQUE_TERMS_ONLY.equals(document.getTheshold())) {
      // TODO: Full text == Only unique terms
    //}
    
    else if (ContentThreshold.FULL_TEXT.equals(document.getThreshold())) {
      text = document.getBambooDocument().getText();
      if (text == null) return;

      text = removeExtraSpaces(text);
      text = text.trim();
    }

    if (!"".equals(text)) {
      if (indexFullText) {
        solr.addField(SolrEnum.FULL_TEXT.toString(), text);
      }
    }
  }

  @VisibleForTesting
  public static String removeExtraSpaces(String text){
  	if(text == null){
  		return "";
  	}
  	
  	String txt = PATTERN_WHITE_SPACE.matcher(text).replaceAll(" ");
  	return PATTERN_MULTI_SPACE.matcher(txt).replaceAll(" ");
  }

  /**
   * Shorten the text to the size but keep full word. 
   * @param text
   * @param size
   * @return
   */
  protected static String shortenText(String text, int size){
  	if(text.length() <= size){
  		return text;
  	}
  	int pos = text.substring(0, size+1).lastIndexOf(" ");
  	if(pos < 0){
  		return "";
  	}
  	return text.substring(0, pos);
  }
  
  /**
   * Shorten the text to the size(words) but keep full word. 
   * @param text
   * @param size
   * @return
   */
  protected static String shortenTextWords(String text, int size){
  	if(size < 1){
  		return "";
  	}
  	String[] array = text.split("\\W");
  	if(array.length <= size){
  		return text;
  	}
  	StringBuilder sb = new StringBuilder();
		sb.append(array[0]);
		if(size == 1){
	  	return sb.toString();
		}
  	for(int i=1;i<size;i++){
  		sb.append(" ");
  		sb.append(array[i]);
  	}
  	return sb.toString();
  }
  
  private void searchCategory(SolrInputDocument solr, IndexerDocument document) {
    SearchCategory category = SearchCategory.NONE;
    String type = document.getBambooDocument().getContentType();

    if (DOCUMENT_CONTENT_TYPES.contains(type)) {
      document.modifyBoost(MALUS_SEARCH_CATEGORY);
      category = SearchCategory.DOCUMENT;
    }
    if (HTML_CONTENT_TYPES.contains(type)) {
      category = SearchCategory.HTML;
    }
    if (PDF_CONTENT_TYPES.contains(type)) {
      category = SearchCategory.PDF;
    }
    if (PRESENTATION_CONTENT_TYPES.contains(type)) {
      document.modifyBoost(MALUS_SEARCH_CATEGORY);
      category = SearchCategory.PRESENTATION;
    }
    if (SPREADSHEET_CONTENT_TYPES.contains(type)) {
      document.modifyBoost(MALUS_SEARCH_CATEGORY);
      category = SearchCategory.SPREADSHEET;
    }

    solr.addField(SolrEnum.SEARCH_CATEGORY.toString(), category.toString());
  }
}
