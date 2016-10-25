package com.broilogabriel

import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders


/**
  * Created by broilogabriel on 24/10/16.
  */
object Cluster {

  def getCluster(clusterName: String, address: String, port: Int): TransportClient = {
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build()
    new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(address, port))
  }

  def getScrollId(cluster: TransportClient, index: String) = {
    cluster.prepareSearch(index)
      .setSearchType(SearchType.SCAN)
      .setScroll(TimeValue.timeValueMinutes(5))
      .setQuery(QueryBuilders.matchAllQuery)
      .setSize(50)
      .execute().actionGet().getScrollId
  }

  def scroller(index: String, scrollId: String, cluster: TransportClient): Seq[Map[String, String]] = {
    val partial = cluster.prepareSearchScroll(scrollId)
      .setScroll(TimeValue.timeValueMinutes(20))
      .execute()
      .actionGet()
    if (partial.getHits.hits().length > 0) {
      partial.getHits.hits().map(hit => {
        //        val indexRequest = new IndexRequest(index, hit.getType, hit.getId)
        //        indexRequest.source(hit.getSourceRef)
        Map("index" -> index
          , "hitType" -> hit.getType
          , "hitId" -> hit.getId
          , "source" -> hit.getSourceAsString)
      })
    } else {
      Seq.empty
    }
  }

}
