/*
 * Copyright 2016 Target, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.openstack.provider.agent

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.cats.agent.AgentDataType
import com.netflix.spinnaker.cats.agent.CacheResult
import com.netflix.spinnaker.cats.cache.CacheData
import com.netflix.spinnaker.cats.cache.RelationshipCacheFilter
import com.netflix.spinnaker.cats.provider.ProviderCache
import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent
import com.netflix.spinnaker.clouddriver.cache.OnDemandAgent.OnDemandResult
import com.netflix.spinnaker.clouddriver.cache.OnDemandMetricsSupport
import com.netflix.spinnaker.clouddriver.openstack.cache.CacheResultBuilder
import com.netflix.spinnaker.clouddriver.openstack.cache.Keys
import com.netflix.spinnaker.clouddriver.openstack.cache.UnresolvableKeyException
import com.netflix.spinnaker.clouddriver.openstack.deploy.exception.OpenstackProviderException
import com.netflix.spinnaker.clouddriver.openstack.model.OpenstackFloatingIP
import com.netflix.spinnaker.clouddriver.openstack.model.OpenstackLoadBalancer
import com.netflix.spinnaker.clouddriver.openstack.model.OpenstackNetwork
import com.netflix.spinnaker.clouddriver.openstack.model.OpenstackPort
import com.netflix.spinnaker.clouddriver.openstack.model.OpenstackSubnet
import com.netflix.spinnaker.clouddriver.openstack.model.OpenstackVip
import com.netflix.spinnaker.clouddriver.openstack.security.OpenstackNamedAccountCredentials
import groovy.util.logging.Slf4j
import org.openstack4j.model.network.NetFloatingIP
import org.openstack4j.model.network.ext.HealthMonitor
import org.openstack4j.model.network.ext.HealthMonitorV2
import org.openstack4j.model.network.ext.LbPool
import org.openstack4j.model.network.ext.LbPoolV2
import org.openstack4j.model.network.ext.ListenerV2
import org.openstack4j.model.network.ext.LoadBalancerV2
import org.openstack4j.model.network.ext.Vip

import static com.netflix.spinnaker.cats.agent.AgentDataType.Authority.AUTHORITATIVE
import static com.netflix.spinnaker.clouddriver.cache.OnDemandAgent.OnDemandType.LoadBalancer
import static com.netflix.spinnaker.clouddriver.openstack.OpenstackCloudProvider.ID
import static com.netflix.spinnaker.clouddriver.openstack.cache.Keys.Namespace.FLOATING_IPS
import static com.netflix.spinnaker.clouddriver.openstack.cache.Keys.Namespace.LOAD_BALANCERS
import static com.netflix.spinnaker.clouddriver.openstack.cache.Keys.Namespace.NETWORKS
import static com.netflix.spinnaker.clouddriver.openstack.cache.Keys.Namespace.PORTS
import static com.netflix.spinnaker.clouddriver.openstack.cache.Keys.Namespace.SUBNETS
import static com.netflix.spinnaker.clouddriver.openstack.cache.Keys.Namespace.VIPS
import static com.netflix.spinnaker.clouddriver.openstack.provider.OpenstackInfrastructureProvider.ATTRIBUTES

@Slf4j
class OpenstackLoadBalancerCachingAgent extends AbstractOpenstackCachingAgent implements OnDemandAgent {

  final ObjectMapper objectMapper
  final OnDemandMetricsSupport metricsSupport

  Collection<AgentDataType> providedDataTypes = Collections.unmodifiableSet([
    AUTHORITATIVE.forType(LOAD_BALANCERS.ns)
  ] as Set)

  String agentType = "${accountName}/${region}/${OpenstackLoadBalancerCachingAgent.simpleName}"
  String onDemandAgentType = "${agentType}-OnDemand"

  OpenstackLoadBalancerCachingAgent(final OpenstackNamedAccountCredentials account,
                                    final String region,
                                    final ObjectMapper objectMapper,
                                    final Registry registry) {
    super(account, region)
    this.objectMapper = objectMapper
    this.metricsSupport = new OnDemandMetricsSupport(
      registry,
      this,
      "${ID}:${LoadBalancer}")
  }

  @Override
  CacheResult loadData(ProviderCache providerCache) {
    log.info("Describing items in ${agentType}")

    List<LoadBalancerV2> loadBalancers = clientProvider.getLoadBalancers(region)

    List<String> loadBalancerKeys = loadBalancers.collect { Keys.getLoadBalancerKey(it.name, it.id, accountName, region) }

    buildLoadDataCache(providerCache, loadBalancerKeys) { CacheResultBuilder cacheResultBuilder ->
      buildCacheResult(providerCache, loadBalancers, cacheResultBuilder)
    }
  }

  CacheResult buildCacheResult(ProviderCache providerCache, List<LoadBalancerV2> loadBalancers, CacheResultBuilder cacheResultBuilder) {
    loadBalancers?.collect { loadBalancer ->
      String loadBalancerKey = Keys.getLoadBalancerKey(loadBalancer.name, loadBalancer.id, accountName, region)

      if (shouldUseOnDemandData(cacheResultBuilder, loadBalancerKey)) {
        moveOnDemandDataToNamespace(objectMapper, typeReference, cacheResultBuilder, loadBalancerKey)
      } else {
        //listeners, pools, and health monitors get looked up
        Set<ListenerV2> listeners = [].toSet()
        Map<String, LbPoolV2> pools = [:]
        Map<String, HealthMonitorV2> healthMonitors = [:]
        loadBalancer.listeners.each { listenerListItem ->
          ListenerV2 listener = clientProvider.getLoadBalancerListener(region, listenerListItem.id)
          listeners << listener
          LbPoolV2 pool = clientProvider.client.networking().lbaasV2().lbPool().get(listener.defaultPoolId) //TODO
          pools << [(listener.id): pool]
          HealthMonitorV2 healthMonitor = clientProvider.client.networking().lbaasV2().healthMonitor().get(pool.healthMonitorId) //TODO
          healthMonitors << [(pool.id): healthMonitor]
        }

        //ips cached
        Collection<String> ipFilters = providerCache.filterIdentifiers(FLOATING_IPS.ns, Keys.getFloatingIPKey('*', accountName, region))
        Collection<CacheData> ipsData = providerCache.getAll(FLOATING_IPS.ns, ipFilters, RelationshipCacheFilter.none())
        CacheData ipCacheData = ipsData.find { i -> i.attributes?.fixedIpAddress == loadBalancer.vipAddress }
        String floatingIpKey = Keys.getFloatingIPKey(ipCacheData.id, accountName, region)
        //TODO view to view
//        Map<String, Object> ipAttributes = ipCacheData?.attributes
//        OpenstackFloatingIP ip = objectMapper.convertValue(ipAttributes, OpenstackFloatingIP)

        //subnets cached
        //TODO need to get subnet from pool after new version of openstack4j is published
        String subnetKey = Keys.getSubnetKey(pools.entrySet().first().value.subnetId, accountName, region)
        //TODO move to view. All pools will share the same subnet???
//        Map<String, Object> subnetMap = providerCache.get(SUBNETS.ns, Keys.getSubnetKey(loadBalancer.subnetId, accountName, region))?.attributes
//        OpenstackSubnet subnet = subnetMap ? objectMapper.convertValue(subnetMap, OpenstackSubnet) : null

        //networks cached
        String networkKey = Keys.getNetworkKey(ip.networkId, accountName, region)
        //TODO move to view
//        OpenstackNetwork network = null
//        if (ip) {
//          Map<String, Object> networkMap = providerCache.get(NETWORKS.ns, Keys.getNetworkKey(ip.networkId, accountName, region))?.attributes
//          network = networkMap ? objectMapper.convertValue(networkMap, OpenstackNetwork) : null
//        }

        //create load balancer. Server group relationships are not cached here as they are cached in the server group caching agent.
        OpenstackLoadBalancer openstackLoadBalancer = OpenstackLoadBalancer.from(loadBalancer, listeners, pools,
          healthMonitors, accountName, region)

        cacheResultBuilder.namespace(LOAD_BALANCERS.ns).keep(loadBalancerKey).with {
          attributes = objectMapper.convertValue(openstackLoadBalancer, ATTRIBUTES)
          relationships[FLOATING_IPS.ns] = [floatingIpKey]
          relationships[NETWORKS.ns] = [networkKey]
          relationships[SUBNETS.ns] = [subnetKey]
        }
      }
    }

    log.info("Caching ${cacheResultBuilder.namespace(LOAD_BALANCERS.ns).keepSize()} load balancers in ${agentType}")
    log.info("Caching ${cacheResultBuilder.onDemand.toKeep.size()} onDemand entries in ${agentType}")
    log.info("Evicting ${cacheResultBuilder.onDemand.toEvict.size()} onDemand entries in ${agentType}")

    cacheResultBuilder.build()
  }

  @Override
  boolean handles(OnDemandAgent.OnDemandType type, String cloudProvider) {
    type == LoadBalancer && cloudProvider == ID
  }

  @Override
  OnDemandResult handle(ProviderCache providerCache, Map<String, ? extends Object> data) {
    OnDemandResult result = null

    if (data.containsKey("loadBalancerName") && data.account == accountName && data.region == region) {
      String loadBalancerName = data.loadBalancerName.toString()

      LoadBalancerV2 loadBalancer = metricsSupport.readData {
        LoadBalancerV2 lbResult = null
        try {
          LoadBalancerV2 realtimeLoadBalancer = clientProvider.getLoadBalancerByName(region, loadBalancerName)
          lbResult = realtimeLoadBalancer ?: null
        } catch (OpenstackProviderException e) {
          //Do nothing ... Exception is thrown if a pool isn't found
        }
        lbResult
      }

      List<LoadBalancerV2> loadBalancers = []
      String loadBalancerKey = Keys.getLoadBalancerKey(loadBalancerName, '*', accountName, region)

      if (loadBalancer) {
        loadBalancers = [loadBalancer]
        loadBalancerKey = Keys.getLoadBalancerKey(loadBalancerName, loadBalancer.id, accountName, region)
      }

      CacheResult cacheResult = metricsSupport.transformData {
        buildCacheResult(providerCache, loadBalancers, new CacheResultBuilder(startTime: Long.MAX_VALUE))
      }

      String namespace = LOAD_BALANCERS.ns
      String resolvedKey = null
      try {
        resolvedKey = resolveKey(providerCache, namespace, loadBalancerKey)
        processOnDemandCache(cacheResult, objectMapper, metricsSupport, providerCache, resolvedKey)
      } catch (UnresolvableKeyException uke) {
        log.info("Load balancer ${loadBalancerName} is not resolvable", uke)
      }

      result = buildOnDemandCache(loadBalancer, onDemandAgentType, cacheResult, namespace, resolvedKey)
    }

    log.info("On demand cache refresh (data: ${data}) succeeded.")

    result
  }

  @Override
  Collection<Map> pendingOnDemandRequests(ProviderCache providerCache) {
    getAllOnDemandCacheByRegionAndAccount(providerCache, accountName, region)
  }
}
