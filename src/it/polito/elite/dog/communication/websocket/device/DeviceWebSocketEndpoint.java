/*
 * Dog - WebSocket Device Endpoint
 * 
 * Copyright (c) 2014 Luigi De Russis
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
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package it.polito.elite.dog.communication.websocket.device;

import it.polito.elite.dog.communication.rest.device.api.DeviceRESTApi;
import it.polito.elite.dog.communication.websocket.annotation.WebSocketPath;
import it.polito.elite.dog.communication.websocket.api.WebSocketConnector;
import it.polito.elite.dog.communication.websocket.device.json.WebSocketNotification;
import it.polito.elite.dog.communication.websocket.message.InvocationResult;
import it.polito.elite.dog.core.library.model.notification.Notification;
import it.polito.elite.dog.core.library.util.LogHelper;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.measure.Measure;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.osgi.framework.BundleContext;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.osgi.service.log.LogService;

/**
 * Represent the WebSocket endpoint for handling device-related information.
 * 
 * @author <a href="mailto:teo.montanaro@gmail.com">Teodoro Montanaro</a>
 * @author <a href="mailto:luigi.derussis@polito.it">Luigi De Russis</a>
 * @see <a href="http://elite.polito.it">http://elite.polito.it</a>
 * 
 */
// TODO Add the possibility to update an existing subscription (PUT)
@WebSocketPath("/api/v1/subscriptions/devices")
public class DeviceWebSocketEndpoint implements EventHandler
{
	// reference for the DeviceRESTApi
	private AtomicReference<DeviceRESTApi> restApi;
	// reference for the WebSocket connector
	private AtomicReference<WebSocketConnector> websocketConnector;
	
	// the bundle context reference
	private BundleContext context;
	
	// the service logger
	private LogHelper logger;
	
	// the instance-level mapper
	private ObjectMapper mapper;
	
	// store all the subscribed notifications for each client
	// key: notification, value: client IDs
	Map<String, HashSet<String>> notification2clients = new ConcurrentHashMap<String, HashSet<String>>();
	
	/**
	 * Default constructor
	 */
	public DeviceWebSocketEndpoint()
	{
		// init
		this.restApi = new AtomicReference<DeviceRESTApi>();
		this.websocketConnector = new AtomicReference<WebSocketConnector>();
		
		// initialize the instance-wide object mapper
		this.mapper = new ObjectMapper();
		// set the mapper pretty printing
		this.mapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
		// avoid empty arrays and null values
		this.mapper.configure(SerializationConfig.Feature.WRITE_EMPTY_JSON_ARRAYS, false);
		this.mapper.setSerializationInclusion(Inclusion.NON_NULL);
	}
	
	/**
	 * Bundle activation, stores a reference to the context object passed by the
	 * framework to get access to system data, e.g., installed bundles, etc.
	 * 
	 * @param context
	 *            the OSGi bundle context
	 */
	public void activate(BundleContext context)
	{
		// store the bundle context
		this.context = context;
		
		// init the logger
		this.logger = new LogHelper(this.context);
		
		// log the activation
		this.logger.log(LogService.LOG_INFO, "Activated....");
		
		// register the endpoint to the connector
		this.websocketConnector.get().registerEndpoint(this, this.restApi.get());
	}
	
	/**
	 * Bind the DeviceRESTApi service (before the bundle activation)
	 * 
	 * @param deviceRestApi
	 *            the DeviceRestApi service to add
	 */
	public void addedRESTApi(DeviceRESTApi deviceRestApi)
	{
		// store a reference to the DeviceRESTApi service
		this.restApi.set(deviceRestApi);
	}
	
	/**
	 * Unbind the DeviceRESTApi service
	 * 
	 * @param deviceRestApi
	 *            the DeviceRESTApi service to remove
	 */
	public void removedRESTApi(DeviceRESTApi deviceRestApi)
	{
		this.restApi.compareAndSet(deviceRestApi, null);
	}
	
	/**
	 * Bind the WebSocketConnector service (before the bundle activation)
	 * 
	 * @param websocket
	 *            the websocket connector service to add
	 */
	public void addedWebSocketConnector(WebSocketConnector websocket)
	{
		// store a reference to the WebSocketEndPoint service
		this.websocketConnector.set(websocket);
	}
	
	/**
	 * Unbind the WebSocketConnecto service
	 * 
	 * @param websocket
	 *            the websocket connector service to remove
	 */
	public void removedWebSocketConnector(WebSocketConnector websocket)
	{
		this.websocketConnector.compareAndSet(websocket, null);
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.osgi.service.event.EventHandler#handleEvent(org.osgi.service.event
	 * .Event)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void handleEvent(Event event)
	{
		// get number of connected clients
		int connectedClients = this.websocketConnector.get().getConnectedClients().size();
		
		// handle an event only if at least one client is connected...
		if (this.websocketConnector.get().isWebSocketAvailable() && connectedClients > 0)
		{
			// we obtain the parts of the URI used by the notification as name
			String[] topicParts = event.getTopic().split("/");
			String eventName = topicParts[topicParts.length - 1];
			// create the variable that will contain all the fields contained in
			// the notification (deviceUri, notificationTopic ...)
			HashMap<String, String> notificationContent = new HashMap<String, String>();
			
			// get the necessary information about the notification
			Object eventContent = event.getProperty("event");
			if (eventContent != null)
			{
				// we can received a single notification or more than one
				// notification so we create a list in both cases
				Set<Notification> notificationList = new HashSet<Notification>();
				// multiple notifications
				if (eventContent instanceof HashSet)
					notificationList.addAll((HashSet<Notification>) eventContent);
				// single notification
				else
					notificationList.add((Notification) eventContent);
				
				// we scroll through all the items of the received list of
				// notifications to store all the fields contained in it
				for (Notification singleNotification : notificationList)
				{
					WebSocketNotification notificationResponse = new WebSocketNotification();
					// cause the notification could contain a lot of fields
					// (that we cannot know in advance) we have to scroll
					// through all of them storing them in an map
					// (notificationContent)
					for (Field notificationField : singleNotification.getClass().getDeclaredFields())
					{
						notificationField.setAccessible(true);
						String notificationFieldName = notificationField.getName();
						Object notificationFieldValue = null;
						try
						{
							String notificationFieldValueFinal = "";
							notificationFieldValue = notificationField.get(singleNotification);
							// the content could be a measure or a string only,
							// because if we want to send more than one
							// notification, we have to send a packet with an
							// array of single notifications
							if (notificationFieldValue instanceof Measure<?, ?>)
								notificationFieldValueFinal = notificationFieldValue.toString();
							else if (notificationFieldValue instanceof String)
								notificationFieldValueFinal = (String) notificationFieldValue;
							// use the notificationTopic to get the content of
							// the notification, but the received
							// notificationTopic contains more information than
							// needed (it/polito/elite/...) so we take
							// only the last part
							if (notificationFieldName.equals("notificationTopic"))
							{
								String[] fieldValueFinalParts = notificationFieldValueFinal.split("/");
								notificationFieldValueFinal = fieldValueFinalParts[fieldValueFinalParts.length - 1];
							}
							// insert the acquired information in the map
							notificationContent.put(notificationFieldName, notificationFieldValueFinal);
						}
						catch (Exception e)
						{
							// if something went wrong we want to continue with
							// the other notification fields
							this.logger.log(LogService.LOG_WARNING,
									"Ops! Something goes wrong in parsing a notification... skip!", e);
						}
					}
					
					// check and send the right notification to the proper
					// client
					if (!notificationContent.isEmpty())
					{
						HashSet<String> sendToClients = new HashSet<String>();
						
						if ((!notificationContent.get("deviceUri").isEmpty()) && (!eventName.isEmpty()))
						{
							// get all the possible keys in the format
							// deviceUri-notificationTopic
							Set<String> keys = this.prepareNotificationKeys(notificationContent.get("deviceUri"),
									eventName);
							
							// for each key, look for the corresponding clients
							for (String key : keys)
							{
								if (this.notification2clients.containsKey(key))
								{
									// get the clients set
									sendToClients.addAll(this.notification2clients.get(key));
								}
							}
						}
						
						if (!sendToClients.isEmpty())
						{
							for (String client : this.websocketConnector.get().getConnectedClients())
							{
								// send the notification only if the registered
								// client is still connected
								if (sendToClients.contains(client))
								{
									// transform the notification in JSON
									// format, with clientId, messageType, type
									notificationResponse.setNotification(notificationContent);
									notificationResponse.setClientId(client);
									notificationResponse.setMessageType("notification");
									
									String notificationToSend;
									try
									{
										notificationToSend = this.mapper.writeValueAsString(notificationResponse);
										// send the message
										this.websocketConnector.get().sendMessage(notificationToSend, client);
									}
									catch (Exception e)
									{
										this.logger.log(LogService.LOG_ERROR, "Exception in sending the notification"
												+ eventName, e);
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * Register a notification subscription for all the controllables.
	 * 
	 * @param notifications
	 *            List of notifications the client want to subscribe
	 * @param clientId
	 *            the client unique ID
	 * @return a {@link String} containing the result of the registration
	 * 
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 */
	@POST
	@Path("/notifications")
	public String notificationRegistrationWithoutControllable(Object notifications, String clientId)
			throws JsonParseException, JsonMappingException
	{
		return this.notificationRegistration(clientId, "*", notifications);
	}
	
	/**
	 * Remove a notification subscription for all the controllables.
	 * 
	 * @param notification
	 *            the notification whose subscription is to remove
	 * @param clientId
	 *            the client ID
	 * @return a {@link String} representing the result of the operation
	 */
	@DELETE
	@Path("/notifications/{notification}")
	public String notificationUnregistrationWithoutControllable(@PathParam("notification") String notification,
			String clientId)
	{
		return this.notificationUnregistration(clientId, "*", notification);
	}
	
	/**
	 * Register a notification subscription for a specific controllable.
	 * 
	 * @param controllable
	 *            device to which we want to subscribe a list of notifications
	 * @param notifications
	 *            list of notifications the client want to subscribe
	 * @param clientId
	 *            the client unique ID
	 * @return a {@link String} result containing the result of the registration
	 * 
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 */
	@POST
	@Path("/{controllable}/notifications")
	public String notificationRegistrationWithControllable(@PathParam("controllable") String controllable,
			Object notifications, String clientId) throws JsonParseException, JsonMappingException
	{
		return this.notificationRegistration(clientId, controllable, notifications);
	}
	
	/**
	 * Remove a notification subscription for a specific controllable.
	 * 
	 * @param controllable
	 *            device for which unsubscribe a notification
	 * @param notification
	 *            the notification whose subscription is to remove
	 * @param clientId
	 *            the client ID
	 * @return a {@link String} representing the result of the operation
	 */
	@DELETE
	@Path("/{controllable}/notifications/{notification}")
	public String notificationUnregistrationWithControllable(@PathParam("controllable") String controllable,
			@PathParam("notification") String notification, String clientId)
	{
		return this.notificationUnregistration(clientId, "*", notification);
	}
	
	/**
	 * Register a notification (main method)
	 * 
	 * @param clientId
	 *            ID of the client that is requiring the subscription
	 * @param controllable
	 *            the URI of the device for which we want to subscribe the
	 *            notifications
	 * @param notifications
	 *            list of notifications from which the user want to be
	 *            subscribed
	 * 
	 * @return a {@link String} containing the result of the registration
	 */
	private String notificationRegistration(String clientId, String controllable, Object notificationsAcquired)
			throws JsonParseException, JsonMappingException
	{
		// set the default result value
		String result = "Registration failed";
		
		Object notifications;
		// check if the notifications are in JSON format or not
		try
		{
			notifications = this.mapper.readTree((String) notificationsAcquired);
		}
		catch (Exception e)
		{
			notifications = notificationsAcquired;
		}
		try
		{
			// list of notification that has to be subscribed
			Set<String> notificationsList = new HashSet<String>();
			
			// we insert each notification only once, so if the client send the
			// same notification name twice (or more) we insert only the last
			// one
			if (notifications instanceof String)
			{
				// if we receive only a single notification we can add it
				// directly to the list of notifications
				notificationsList.add((String) notifications);
			}
			else if (notifications instanceof ObjectNode)
			{
				// get the principal JSON node
				ObjectNode notificationsNode = (ObjectNode) notifications;
				
				// try to get the "notifications" field, in the case of multiple
				// notification registrations
				JsonNode notificationsField = notificationsNode.get("notifications");
				
				if (notificationsField != null && notificationsField.size() == 1)
				{
					JsonNode notificationElements = notificationsField.getElements().next();
					if (notificationElements.isArray())
					{
						// get the array containing the notification names
						ArrayNode notificationArray = (ArrayNode) notificationElements;
						Iterator<JsonNode> iterator = notificationArray.getElements();
						
						// store all the notifications to subscribe
						while (iterator.hasNext())
						{
							JsonNode current = iterator.next();
							notificationsList.add(current.getTextValue());
						}
					}
					
				}
				else
				{
					throw new Exception("Malformed JSON Request.");
				}
			}
			
			// TODO remove, it is here for backward compatibility
			// if the list is empty, it assumes that the client wants to
			// subscribe all notifications
			if (notificationsList.isEmpty())
			{
				notificationsList.add("*");
			}
			
			// at the end of the process that chooses which notifications have
			// to be subscribed we can call the method that does the real
			// subscription
			if (this.registerNotifications(clientId, controllable, notificationsList))
				result = "Registration completed successfully";
		}
		catch (Exception e)
		{
			this.logger.log(LogService.LOG_ERROR, "Notification registration failed");
			result = "Registration failed";
		}
		
		// try to convert the result in JSON format
		try
		{
			InvocationResult jsonResult = new InvocationResult();
			jsonResult.setResult(result);
			return this.mapper.writeValueAsString(jsonResult);
		}
		catch (Exception e)
		{
			this.logger.log(LogService.LOG_WARNING, "Impossible to convert an object in JSON", e);
			// prepare the return message in JSON format anyway
			return "{\"result\":\"" + result + "\"}";
		}
		
	}
	
	/**
	 * Add one or more notifications to the list of notifications subscribed by
	 * a client for a specific controllable.
	 * 
	 * @param clientId
	 *            the id of a user (it is the last part of the instance (after
	 *            the @))
	 * @param controllable
	 *            the URI of the device for which we want to subscribe the
	 *            notifications
	 * @param notifications
	 *            the list of notifications to subscribe
	 * @return true if the registration succeeded, false otherwise
	 */
	private boolean registerNotifications(String clientId, String controllable, Set<String> notifications)
	{
		boolean result = true;
		try
		{
			// first case: the map between notifications and clients already
			// exists
			if (notification2clients != null && !notification2clients.isEmpty())
			{
				// for each notification+device URI, store the client id
				for (String notification : notifications)
				{
					String key = notification + "-" + controllable;
					if (this.notification2clients.containsKey(key))
					{
						// associate the client to the pair
						// "notification-device URI"
						// since it is a set, multiple client registration for
						// the same pair notification-device is ignored
						this.notification2clients.get(key).add(clientId);
					}
					else
					{
						// no client registered for the given
						// notification+device URI
						HashSet<String> clientSet = new HashSet<String>();
						clientSet.add(clientId);
						this.notification2clients.put(key, clientSet);
					}
				}
			}
			// second case: it is the first subscription ever
			else
			{
				// no notifications registered
				this.notification2clients = new ConcurrentHashMap<String, HashSet<String>>();
				HashSet<String> clients = new HashSet<String>();
				clients.add(clientId);
				
				for (String notification : notifications)
				{
					this.notification2clients.put(notification + "-" + controllable, clients);
				}
			}
		}
		catch (Exception e)
		{
			this.logger.log(LogService.LOG_ERROR, "Exception in registering a new subscription for device "
					+ controllable + "from " + clientId, e);
			result = false;
		}
		
		return result;
	}
	
	/**
	 * Unregister a notification subscription for a given controllable.
	 * 
	 * @param clientId
	 *            ID of the client that requires the unsubscription
	 * 
	 * @param controllable
	 *            URI of the device for which we want to unsubscribe the
	 *            notification
	 * 
	 * @param notificationsToRemove
	 *            the notification from which the user want to be unsubscribed
	 * 
	 * @return a {@link String} containing the result of the unregistration
	 */
	private String notificationUnregistration(String clientId, String controllable, String notificationToRemove)
	{
		// set default result
		String result = "Unregistration failed";
		
		// the notification is a simple string (i.e., only one notification)
		if (this.removeNotification(clientId, controllable, notificationToRemove))
			result = "Unregistration completed successfully";
		
		try
		{
			// we try to set the result as JSON but if it generates any kind of
			// exception we will set it directly as a string
			InvocationResult jsonResult = new InvocationResult();
			jsonResult.setResult(result);
			return this.mapper.writeValueAsString(jsonResult);
		}
		catch (Exception e)
		{
			// if it is not possible to parse the result we send it as string
			// (i.e., create the JSON structure by hand)
			this.logger.log(LogService.LOG_WARNING, "Impossible to parse the unsubscription result", e);
			return "{\"result\":\"" + result + "\"}";
		}
	}
	
	/**
	 * Remove a subscribed notification from the list of the client
	 * subscriptions.
	 * 
	 * @param clientId
	 *            the client unique ID
	 * @param controllableToRemove
	 *            the URI of the device for which unsubscribe the notification
	 * @param notificationToRemove
	 *            the notification to unsubscribe
	 * @return true if the registration succeeded, false otherwise
	 */
	private boolean removeNotification(String clientId, String controllableToRemove, String notificationToRemove)
	{
		// set the default result value
		boolean result = false;
		
		try
		{
			// compose the pair device/notification
			String key = controllableToRemove + "-" + notificationToRemove;
			
			// if it is in the map and the client has been subscribed to that
			// pair, remove it
			if (this.notification2clients.containsKey(key))
			{
				HashSet<String> clients = this.notification2clients.get(key);
				if (clients.contains(clientId))
					clients.remove(clientId);
				
				// if no client are registered, remove the entry
				if (clients.isEmpty())
					this.notification2clients.remove(key);
			}
			
			result = true;
		}
		catch (Exception e)
		{
			this.logger.log(LogService.LOG_ERROR, "Exception in unsubscribing a notification", e);
			
			result = false;
		}
		return result;
	}
	
	/**
	 * Prepare all the needed keys for the notification2clients map, in the
	 * format deviceUri-notificationName
	 * 
	 * @param device
	 *            the device URI
	 * @param notification
	 *            the notification name (last part of the topic)
	 * @return a {@link Set} of generated keys
	 */
	private Set<String> prepareNotificationKeys(String device, String notification)
	{
		// prepare all the possible key associated to the
		// given pair device-notification
		Set<String> keys = new HashSet<String>();
		keys.add(notification + "-" + device);
		keys.add(notification + "-*");
		keys.add("*-" + device);
		keys.add("*-*");
		
		return keys;
	}
}
