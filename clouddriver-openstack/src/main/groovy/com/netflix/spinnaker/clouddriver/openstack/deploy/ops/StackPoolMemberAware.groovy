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

package com.netflix.spinnaker.clouddriver.openstack.deploy.ops

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.netflix.spinnaker.clouddriver.openstack.deploy.description.servergroup.MemberData

trait StackPoolMemberAware {

  /**
   * Convert a list of pool members to an embeddable heat template.
   * @param memberData
   * @return
   */
  String buildPoolMemberTemplate(List<MemberData> memberData) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
    Map<String, Object> parameters = [address: [type: "string", description: "Server address for autoscaling group resource"]]
    Map<String, Object> resources = memberData.collectEntries {
      [
        ("member$it.internalPort"): [
          type      : "OS::Neutron::LBaaS::PoolMember",
          properties: [
            address      : [get_param: "address"],
            pool         : it.poolId,
            protocol_port: it.internalPort,
            subnet       : it.subnetId
          ]
        ]
      ]
    }
    Map<String, Object> memberTemplate = [
      heat_template_version: "2016-04-08",
      description          : "Pool members for autoscaling group resource",
      parameters           : parameters,
      resources            : resources]
    mapper.writeValueAsString(memberTemplate)
  }

  /**
   * TODO this will move once the lbaasv2 operation is done
   * Generate key in the format externalProtocol:externalPort:internalProtocol:internalPort
   * @param port
   * @return
   */
  String getListenerKey(int externalPort, String externalProtocol, int internalPort, String internalProtocol) {
    "${externalProtocol}:${externalPort}:${internalProtocol}:${internalPort}"
  }

  /**
   * TODO this will move once the lbaasv2 operation is done
   * Parse the listener attributes from the key.
   * @param key
   * @return
   */
  Map<String, String> parseListenerKey(String key) {
    Map<String, String> result = [:]
    String[] parts = key.split(':')
    if (parts.length == 4) {
      result << [externalProtocol: parts[0], externalPort: parts[1], internalProtocol: parts[2], internalPort: parts[3]]
    }
    result
  }

}
