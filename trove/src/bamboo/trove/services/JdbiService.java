/*
 * Copyright 2016 National Library of Australia
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
package bamboo.trove.services;

import bamboo.trove.db.DbPool;
import bamboo.trove.db.TroveDaoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

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
