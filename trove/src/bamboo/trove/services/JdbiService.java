package bamboo.trove.services;

import javax.annotation.PostConstruct;

import bamboo.trove.db.DbPool;
import bamboo.trove.db.TroveDaoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

@Service
public class JdbiService {
  private static Logger log = LoggerFactory.getLogger(JdbiService.class);

  private String url;
  private String username;
  private String password;

  private TroveDaoRegistry dao;

  @Required
  public void setUrl(String url) {
    this.url = url;
  }

  @Required
  public void setUsername(String username) {
    this.username = username;
  }

  @Required
  public void setPassword(String password) {
    this.password = password;
  }

  @PostConstruct
  public void init() {
    DbPool db = new DbPool(url, username, password);
    dao = db.dao();
  }

  public TroveDaoRegistry getDao() {
    return dao;
  }
}