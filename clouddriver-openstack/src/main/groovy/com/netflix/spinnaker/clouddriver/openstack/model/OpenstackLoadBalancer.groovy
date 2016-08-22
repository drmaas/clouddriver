/*
 * Copyright 2016 Target Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.openstack.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.netflix.spinnaker.clouddriver.model.LoadBalancer
import com.netflix.spinnaker.clouddriver.model.LoadBalancerServerGroup
import com.netflix.spinnaker.clouddriver.openstack.OpenstackCloudProvider
import com.netflix.spinnaker.clouddriver.openstack.domain.LoadBalancerResolver
import groovy.transform.Canonical
import org.openstack4j.model.network.ext.HealthMonitorV2
import org.openstack4j.model.network.ext.LbPoolV2
import org.openstack4j.model.network.ext.ListenerV2
import org.openstack4j.model.network.ext.LoadBalancerV2

@Canonical
@JsonIgnoreProperties(['portRegex','portPattern','createdRegex','createdPattern'])
class OpenstackLoadBalancer implements Serializable, LoadBalancerResolver {

  String type = OpenstackCloudProvider.ID
  String account
  String region
  String id
  String name
  String description
  String status
  String method
  Set<OpenstackLoadBalancerListener> listeners
  OpenstackHealthMonitor healthMonitor

  static OpenstackLoadBalancer from(LoadBalancerV2 loadBalancer, Set<ListenerV2> listeners, LbPoolV2 pool,
                                    HealthMonitorV2 healthMonitor, String account, String region) {
    if (!loadBalancer) {
      throw new IllegalArgumentException("Load balancer must not be null.")
    }
    Set<OpenstackLoadBalancerListener> openstackListeners = listeners?.collect { listener ->
      String internalPort = parseListenerKey(listener.description)['internalPort']
      new OpenstackLoadBalancerListener(externalProtocol: listener.protocol.toString(), externalPort: listener.protocolPort.toString(),
        internalProtocol: listener.protocol.toString(), internalPort: internalPort)
    }?.toSet() ?: [].toSet()
    OpenstackHealthMonitor openstackHealthMonitor = healthMonitor ? new OpenstackHealthMonitor(id: healthMonitor.id,
      adminStateUp: healthMonitor.adminStateUp, delay: healthMonitor.delay, maxRetries: healthMonitor.maxRetries,
      expectedCodes: healthMonitor.expectedCodes, httpMethod: healthMonitor.httpMethod) : null
    new OpenstackLoadBalancer(account: account, region: region, id: loadBalancer.id, name: loadBalancer.name,
      description: loadBalancer.description, status: loadBalancer.operatingStatus,
      method: pool?.lbMethod?.toString(), listeners: openstackListeners, healthMonitor: openstackHealthMonitor)
  }

  Long getCreatedTime() {
    parseCreatedTime(description)
  }

  @Canonical
  static class OpenstackLoadBalancerListener {
    String externalProtocol
    String externalPort
    String internalProtocol
    String internalPort
  }

  @Canonical
  static class OpenstackHealthMonitor {
    String id
    boolean adminStateUp
    Integer delay
    Integer maxRetries
    String expectedCodes
    String httpMethod
  }

  @Canonical
  @JsonIgnoreProperties(['portRegex','portPattern','createdRegex','createdPattern'])
  static class View extends OpenstackLoadBalancer implements LoadBalancer {
    String ip = ""
    String subnetId = ""
    String subnetName = ""
    String networkId = ""
    String networkName = ""
    Set<LoadBalancerServerGroup> serverGroups = [].toSet()
  }

  //TODO replace once lbaas upsert op is in place
  static Map<String, String> parseListenerKey(String key) {
    Map<String, String> result = [:]
    String[] parts = key.split(':')
    if (parts.length == 4) {
      result << [externalProtocol: parts[0], externalPort: parts[1], internalProtocol: parts[2], internalPort: parts[3]]
    }
    result
  }

}
