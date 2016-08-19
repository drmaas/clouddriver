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

package com.netflix.spinnaker.clouddriver.openstack.deploy.ops.loadbalancer

import com.netflix.spinnaker.clouddriver.data.task.Task
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository
import com.netflix.spinnaker.clouddriver.openstack.client.OpenstackClientProvider
import com.netflix.spinnaker.clouddriver.openstack.deploy.description.loadbalancer.DeleteOpenstackLoadBalancerDescription
import com.netflix.spinnaker.clouddriver.openstack.deploy.description.servergroup.MemberData
import com.netflix.spinnaker.clouddriver.openstack.deploy.description.servergroup.ServerGroupParameters
import com.netflix.spinnaker.clouddriver.openstack.deploy.exception.OpenstackOperationException
import com.netflix.spinnaker.clouddriver.openstack.deploy.exception.OpenstackProviderException
import com.netflix.spinnaker.clouddriver.openstack.deploy.ops.StackPoolMemberAware
import com.netflix.spinnaker.clouddriver.openstack.deploy.ops.servergroup.ServerGroupConstants
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperation
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperations
import groovy.util.logging.Slf4j
import org.openstack4j.model.heat.Stack
import org.openstack4j.model.network.ext.ListenerV2
import org.openstack4j.model.network.ext.LoadBalancerV2
import org.openstack4j.model.network.ext.status.LoadBalancerV2Status

/**
 * Removes an openstack load balancer.
 */
@Slf4j
class DeleteOpenstackLoadBalancerAtomicOperation implements AtomicOperation<Void>, StackPoolMemberAware {

  private final String BASE_PHASE = 'DELETE_LOAD_BALANCER'
  DeleteOpenstackLoadBalancerDescription description

  DeleteOpenstackLoadBalancerAtomicOperation(DeleteOpenstackLoadBalancerDescription description) {
    this.description = description
  }

  protected static Task getTask() {
    TaskRepository.threadLocalTask.get()
  }

  /*
   * curl -X POST -H "Content-Type: application/json" -d  '[ {  "deleteLoadBalancer": { "id": "6adc02a8-7b01-4f90-9e6f-9a4c3411e7ad", "region": "RegionOne", "account":  "test" } } ]' localhost:7002/openstack/ops
   */
  @Override
  Void operate(List priorOutputs) {
    String region = description.region
    String loadBalancerId = description.id
    OpenstackClientProvider provider = description.credentials.provider

    try {
      task.updateStatus BASE_PHASE, "Deleting load balancer ${loadBalancerId} in region ${region}..."

      task.updateStatus BASE_PHASE, "Fetching status tree..."
      LoadBalancerV2Status loadBalancerStatus = provider.getLoadBalancerStatusTree(region, loadBalancerId)?.loadBalancerV2Status
      task.updateStatus BASE_PHASE, "Fetched status tree."

      if (loadBalancerStatus) {
        if (loadBalancerStatus.provisioningStatus != "ACTIVE") {
          throw new OpenstackOperationException(AtomicOperations.DELETE_LOAD_BALANCER, "Load balancer $loadBalancerId must have ACTIVE provisioning status to be deleted. Current status is $loadBalancerStatus.provisioningStatus")
        }
        //step 1 - delete load balancer
        deleteLoadBalancer(loadBalancerStatus)

        //step 2 - update stack(s) that reference load balancer
        updateServerGroup(loadBalancerId)
      }
    } catch (OpenstackProviderException e) {
      task.updateStatus BASE_PHASE, "Failed deleting load balancer ${e.message}."
      throw new OpenstackOperationException(AtomicOperations.DELETE_LOAD_BALANCER, e)
    }

    task.updateStatus BASE_PHASE, "Finished deleting load balancer ${loadBalancerId}."
  }

  /**
   * Delete the load balancer and all subelements.
   * @param loadBalancerStatus
   */
  void deleteLoadBalancer(LoadBalancerV2Status loadBalancerStatus) {
    //remove elements
    loadBalancerStatus.listenerStatuses?.each { listenerStatus ->
      listenerStatus.lbPoolV2Statuses?.each { poolStatus ->
        //delete health
        if (poolStatus.heathMonitorStatus?.id) {
          task.updateStatus BASE_PHASE, "Deleting health monitor $poolStatus.heathMonitorStatus.id for pool $poolStatus.id on listener $listenerStatus.id..."
          provider.client.networking().lbaasV2().healthMonitor().delete(poolStatus.heathMonitorStatus.id) //TODO
          task.updateStatus BASE_PHASE, "Deleted health monitor $poolStatus.heathMonitorStatus.id for pool $poolStatus.id on listener $listenerStatus.id."
          //TODO wait until load balancer is in ACTIVE status before proceeding
          waitForLoadBalancer(loadBalancerStatus.id, 5000)
        }
        //delete pool
        task.updateStatus BASE_PHASE, "Deleting pool $poolStatus.id on listener $listenerStatus.id..."
        provider.client.networking().lbaasV2().lbPool().delete(poolStatus.id) //TODO
        task.updateStatus BASE_PHASE, "Deleted pool $poolStatus.id on listener $listenerStatus.id."
        //TODO wait until load balancer is in ACTIVE status before proceeding
        waitForLoadBalancer(loadBalancerStatus.id, 5000)
      }
      //delete listener
      task.updateStatus BASE_PHASE, "Deleting listener $listenerStatus.id..."
      provider.client.networking().lbaasV2().listener().delete(listenerStatus.id)
      task.updateStatus BASE_PHASE, "Deleted listener $listenerStatus.id."
      //TODO wait until load balancer is in ACTIVE status before proceeding
      waitForLoadBalancer(loadBalancerStatus.id, 5000)
    }
    //delete load balancer
    task.updateStatus BASE_PHASE, "Deleting load balancer..."
    provider.client.networking().lbaasV2().loadbalancer().delete(loadBalancerStatus.id)
    task.updateStatus BASE_PHASE, "Deleted load balancer."
    //TODO wait until load balancer is deleted before proceeding
    waitForNull(loadBalancerStatus.id, 5000)
  }

  /**
   * Update the server group to remove the given load balancer.
   * @param loadBalancerId
   */
  void updateServerGroup(String loadBalancerId) {
    task.updateStatus BASE_PHASE, "Updating server groups that reference load balancer $loadBalancerId..."
    provider.listStacksWithLoadBalancers(description.region, [loadBalancerId]).each { stackSummary ->
      //get stack details
      task.updateStatus BASE_PHASE, "Fetching stack details for server group $stackSummary.name..."
      Stack stack = provider.getStack(description.region, stackSummary.name)
      task.updateStatus BASE_PHASE, "Fetched stack details for server group $stackSummary.name."

      //update parameters
      ServerGroupParameters newParams = ServerGroupParameters.fromParamsMap(stack.parameters)
      println newParams.loadBalancers
      newParams.loadBalancers.remove(loadBalancerId)
      println newParams.loadBalancers

      //get the current template from the stack
      task.updateStatus BASE_PHASE, "Fetching current template for server group $stack.name..."
      String template = provider.getHeatTemplate(description.region, stack.name, stack.id)
      task.updateStatus BASE_PHASE, "Successfully fetched current template for server group $stack.name."

      //we need to store subtemplate in asg output from create, as it is required to do an update and there is no native way of
      //obtaining it from a stack
      task.updateStatus BASE_PHASE, "Fetching subtemplates for server group $stack.name..."
      List<Map<String, Object>> outputs = stack.outputs
      String subtemplate = outputs.find { m -> m.get("output_key") == ServerGroupConstants.SUBTEMPLATE_OUTPUT }.get("output_value")

      //rebuild memberTemplate
      String memberTemplate = buildPoolMemberTemplate(newParams.loadBalancers.collectMany { lbid ->
        task.updateStatus BASE_PHASE, "Looking up load balancer details for load balancer $lbid..."
        LoadBalancerV2 loadBalancer = provider.getLoadBalancer(description.region, lbid)
        task.updateStatus BASE_PHASE, "Found load balancer details for load balancer $lbid."
        loadBalancer.listeners.collect { item ->
          task.updateStatus BASE_PHASE, "Looking up load balancer listener details for listener $item.id..."
          ListenerV2 listener = provider.getLoadBalancerListener(description.region, item.id)
          String internalPort = parseListenerKey(listener.description).internalPort
          String poolId = listener.defaultPoolId
          task.updateStatus BASE_PHASE, "Found load balancer listener details (poolId=$poolId, internalPort=$internalPort) for listener $item.id."
          new MemberData(subnetId: loadBalancer.vipSubnetId, externalPort: listener.protocolPort.toString(), internalPort: internalPort, poolId: poolId)
        }
      })
      task.updateStatus BASE_PHASE, "Fetched subtemplates for server group $stack.name."

      //update stack
      task.updateStatus BASE_PHASE, "Updating server group $stack.name..."
      provider.updateStack(description.region, stack.name, stack.id, template, [(ServerGroupConstants.SUBTEMPLATE_FILE): subtemplate, (ServerGroupConstants.MEMBERTEMPLATE_FILE): memberTemplate], newParams, newParams.loadBalancers)
      task.updateStatus BASE_PHASE, "Successfully updated server group $stack.name."
    }

    task.updateStatus BASE_PHASE, "Updated server groups that reference load balancer $loadBalancerId."
  }

  /**
   * Utility method to get provider
   * @return
   */
  OpenstackClientProvider getProvider() {
    description.credentials.provider
  }

  //TODO will be replaced with BlockingStatusChecker
  void waitForLoadBalancer(String id, long time) {
    while (description.credentials.provider.getLoadBalancer(description.region, id).provisioningStatus.toString() != "ACTIVE") {
      sleep(time)
      println("waiting for " + id)
    }
  }

  //TODO will be replaced with BlockingStatusChecker
  void waitForNull(String id, long time) {
    while (description.credentials.provider.getLoadBalancer(description.region, id) != null) {
      sleep(time)
      println("waiting for " + id)
    }
  }

}
