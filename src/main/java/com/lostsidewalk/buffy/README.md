
**This package contains data access classes for NewsGears:**

1. FeedDefinitionDao - JDBC-based class used to fetch data about RSS feeds (feed definitions) from *postgres*.

2. StagingPostDao - JDBC-based class used to fetch data about imported posts (the staging area) from *postgres*. 

3. RenderedFeedDao - Repository class used to fetch data about rendered (RSS published) feeds from *redis*. 

---

**Notes:** 

* All configuration is done via DataConfig. 

* For now, redis model entities are stored here, in the 'model' package; postgres model entities are 
stored in the newsgears-model project.  
