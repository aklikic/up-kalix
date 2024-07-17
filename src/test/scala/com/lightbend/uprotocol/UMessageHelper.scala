package com.lightbend.uprotocol

import com.google.protobuf.Message
import org.eclipse.uprotocol.core.usubscription.v3.{SubscriberInfo, SubscriptionRequest, UnsubscribeRequest}
import org.eclipse.uprotocol.uri.factory.UResourceBuilder
import org.eclipse.uprotocol.uuid.factory.UuidFactory
import org.eclipse.uprotocol.v1._

object UMessageHelper {

  def buildUAuthorityForVehicle(vin: String): UAuthority = UAuthority.newBuilder.setName(buildUAuthorityNameForVin(vin)).build

  def buildUAuthorityNameForVin(vin: String): String = "vcu.%s.veh.uprotocol.kalix.io".formatted(vin.toLowerCase)

  def buildUAuthorityForCloudService(): UAuthority = UAuthority.newBuilder.setName("cs.uprotocol.kalix.io").build

  def buildRpcCommand(rpcRequestMessageId: UUID, applicationTopicForRPC: UUri, serviceMethodTopic: UUri, payload: UPayload): UMessage = UMessage.newBuilder.setPayload(payload).setAttributes(UAttributes.newBuilder.setId(rpcRequestMessageId).setSource(applicationTopicForRPC).setSink(serviceMethodTopic).setPriority(UPriority.UPRIORITY_CS4).setPriorityValue(UPriority.UPRIORITY_CS4_VALUE).setType(UMessageType.UMESSAGE_TYPE_REQUEST).setTypeValue(UMessageType.UMESSAGE_TYPE_REQUEST_VALUE).setTtl(6000000).build).build

  def buildRpcCommand(rpcRequestMessageId: UUID, applicationTopicForRPC: UUri, serviceMethodTopic: UUri, protoPayload: Message): UMessage = buildRpcCommand(rpcRequestMessageId, applicationTopicForRPC, serviceMethodTopic, UPayload.newBuilder.setValue(protoPayload.toByteString).setFormat(UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF).build)

  def buildRpcCommand(applicationTopicForRPC: UUri, serviceMethodTopic: UUri, protoPayload: Message): UMessage = buildRpcCommand(UuidFactory.Factories.UUIDV6.factory.create, applicationTopicForRPC, serviceMethodTopic, UPayload.newBuilder.setFormat(UPayloadFormat.UPAYLOAD_FORMAT_PROTOBUF).setValue(protoPayload.toByteString).build)

  def buildPublishUMessage(source: UUri, uPayload: UPayload): UMessage =
    UMessage.newBuilder
      .setAttributes(UAttributes.newBuilder
                          .setSource(source)
                          .setId(UuidFactory.Factories.UUIDV6.factory.create)
                          .setPriority(UPriority.UPRIORITY_CS4)
                          .setPriorityValue(UPriority.UPRIORITY_CS4_VALUE)
                          .setType(UMessageType.UMESSAGE_TYPE_PUBLISH)
                          .setTypeValue(UMessageType.UMESSAGE_TYPE_PUBLISH_VALUE)
                          .setTtl(6000000).build
      ).setPayload(uPayload).build

  val SUBSCRIBE_METHOD = "Subscribe"
  val UNSUBSCRIBE_METHOD = "Unsubscribe"
  val SUBSCRIPTION_SERVICE = "core.usubscription"

  //    UEntity U_SUBSCRIPTION_SERVICE_V2 = UEntity.newBuilder().setName(SUBSCRIPTION_SERVICE).setVersionMajor(2).build();
  val U_SUBSCRIPTION_SERVICE_V3: UEntity = UEntity.newBuilder.setName(SUBSCRIPTION_SERVICE).setVersionMajor(3).build

  private def buildSubscriptionRequestV3(subscriberUri: UUri, whatToSubscribeToUri: UUri) = {
    // subscriber information
    val subscriber = SubscriberInfo.newBuilder.setUri(UUri.newBuilder.setAuthority(subscriberUri.getAuthority).setEntity(subscriberUri.getEntity).build).build
    // topic indicating the resource of the deployed service that will send the change events to the subscriber.
    val topic = UUri.newBuilder.setAuthority(whatToSubscribeToUri.getAuthority).setEntity(whatToSubscribeToUri.getEntity).setResource(whatToSubscribeToUri.getResource).build
    // build subscription request payload
    SubscriptionRequest.newBuilder.setSubscriber(subscriber).setTopic(topic).build
  }


  private def buildUnSubscribeRequestV3(subscriberUri: UUri, whatToUnSubscribeFromUri: UUri) = {
    // subscriber information
    val subscriber = SubscriberInfo.newBuilder.setUri(UUri.newBuilder.setAuthority(subscriberUri.getAuthority).setEntity(subscriberUri.getEntity).build).build
    // topic indicating the resource of the deployed service whose change events we no longer want.
    val topic = UUri.newBuilder.setAuthority(whatToUnSubscribeFromUri.getAuthority).setEntity(whatToUnSubscribeFromUri.getEntity).setResource(whatToUnSubscribeFromUri.getResource).build
    // build the un-subscribe request payload
    UnsubscribeRequest.newBuilder.setSubscriber(subscriber).setTopic(topic).build
  }

  def createSubscriptionRpcUMessageV3(subscriberUri: UUri, whatToSubscribeToUri: UUri): UMessage = {
    val messageId = UuidFactory.Factories.UUIDV6.factory.create
    val subscriptionRequest = buildSubscriptionRequestV3(subscriberUri, whatToSubscribeToUri)
    // build the subscription command topic
    val subscribeCommandTopic = UUri.newBuilder.setAuthority(whatToSubscribeToUri.getAuthority).setEntity(U_SUBSCRIPTION_SERVICE_V3).setResource(UResourceBuilder.forRpcRequest(SUBSCRIBE_METHOD)).build
    // RPC Uri for the application requesting to subscribe, subscriber is the caller of the RPC to subscribe.
    val subscriberTopicForSubscriptionResult = UUri.newBuilder.setAuthority(subscriberUri.getAuthority).setEntity(subscriberUri.getEntity).build
    buildRpcCommand(messageId, subscriberTopicForSubscriptionResult, subscribeCommandTopic, subscriptionRequest)
  }

  def createUnSubscriptionRpcUMessageV3(subscriberUri: UUri, whatToUnSubscribeToUri: UUri): UMessage = {
    val messageId = UuidFactory.Factories.UUIDV6.factory.create
    val unSubscriptionRequest = buildUnSubscribeRequestV3(subscriberUri, whatToUnSubscribeToUri)
    // build the subscription command topic
    val unSubscribeCommandTopic = UUri.newBuilder.setAuthority(subscriberUri.getAuthority).setEntity(U_SUBSCRIPTION_SERVICE_V3).setResource(UResourceBuilder.forRpcRequest(UNSUBSCRIBE_METHOD)).build
    // RPC Uri for the application requesting to subscribe, subscriber is the caller of the RPC to subscribe.
    val subscriberTopicForSubscriptionResult = UUri.newBuilder.setAuthority(subscriberUri.getAuthority).setEntity(subscriberUri.getEntity).build
    buildRpcCommand(messageId, subscriberTopicForSubscriptionResult, unSubscribeCommandTopic, unSubscriptionRequest)
  }

  def buildWhatToSubscribeToUri(vin: String, serviceName: String, serviceVersion: Int, resourceName: String, resourceMessage: String): UUri = UUri.newBuilder.setAuthority(UAuthority.newBuilder.setName(buildUAuthorityNameForVin(vin)).build).setEntity(UEntity.newBuilder.setName(serviceName).setVersionMajor(serviceVersion).build).setResource(UResource.newBuilder.setName(resourceName).setMessage(resourceMessage).build).build

  def buildWhatToSubscribeToUri(vin: String, serviceName: String, serviceVersion: Int, resourceName: String, resourceInstance: String, resourceMessage: String): UUri = UUri.newBuilder.setAuthority(UAuthority.newBuilder.setName(buildUAuthorityNameForVin(vin)).build).setEntity(UEntity.newBuilder.setName(serviceName).setVersionMajor(serviceVersion).build).setResource(UResource.newBuilder.setName(resourceName).setInstance(resourceInstance).setMessage(resourceMessage).build).build
}
