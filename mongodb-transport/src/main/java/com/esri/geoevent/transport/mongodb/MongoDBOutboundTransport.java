/*
  Copyright 1995-2013 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
 */

package com.esri.geoevent.transport.mongodb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;

import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class MongoDBOutboundTransport extends OutboundTransportBase
{
	private static final BundleLogger	LOGGER										= BundleLoggerFactory.getLogger(MongoDBOutboundTransport.class);

	private static final String				HOST_NAME_PROPERTY				= "outputHostName";
	private static final String				PORT_PROPERTY							= "outputPort";
	private static final String				DATABASE_NAME_PROPERTY		= "outputDatabaseName";
	private static final String				USER_NAME_PROPERTY				= "outputUserName";
	private static final String				PASSWORD_PROPERTY					= "outputPassword";
	private static final String				COLLECTION_NAME_PROPERTY	= "outputCollectionName";
	private static final String				WRITE_CONCERN_PROPERTY		= "outputWriteConcern";

	private String										host											= "localhost";
	private int												port											= 27017;
	private String										databaseName							= "db";
	private String										userName									= "";
	private String										password									= "";
	private String										collectionName						= "test";
	private WriteConcern							writeConcern;
	private String										errorMessage;
	private MongoClient								mongoClient;
	private DB												db;

	private DBCollection							collection;

	public MongoDBOutboundTransport(TransportDefinition definition) throws ComponentException
	{
		super(definition);
	}

	protected void readProperties() throws ConfigurationException
	{
		if (hasProperty(HOST_NAME_PROPERTY))
			host = getProperty(HOST_NAME_PROPERTY).getValueAsString();
		else
			host = "localhost";

		if (hasProperty(PORT_PROPERTY))
			port = ((Integer) getProperty(PORT_PROPERTY).getValue());
		else
			port = 27017;

		if (hasProperty(DATABASE_NAME_PROPERTY))
			databaseName = getProperty(DATABASE_NAME_PROPERTY).getValueAsString();
		else
			databaseName = "db";

		if (hasProperty(USER_NAME_PROPERTY))
			userName = getProperty(USER_NAME_PROPERTY).getValueAsString();
		else
			userName = "";

		if (hasProperty(PASSWORD_PROPERTY))
			password = getProperty(PASSWORD_PROPERTY).getValueAsString();
		else
			password = "";

		if (hasProperty(COLLECTION_NAME_PROPERTY))
			collectionName = getProperty(COLLECTION_NAME_PROPERTY).getValueAsString();
		else
			collectionName = "GeoEvents";

		if (hasProperty(WRITE_CONCERN_PROPERTY))
		{
			String writeConcernString = getProperty(WRITE_CONCERN_PROPERTY).getValueAsString();
			writeConcern = WriteConcern.valueOf(writeConcernString);
		}
	}

	private void applyProperties() throws IOException
	{
		mongoClient = new MongoClient(host, port);
		mongoClient.setWriteConcern(writeConcern);
		db = mongoClient.getDB(databaseName);

		boolean auth = true;
		if (userName != null && userName.length() > 0 && password != null && password.length() > 0)
		{
			auth = db.authenticate(userName, password.toCharArray());
		}
		if (!auth)
		{
			throw new IOException(LOGGER.translate("AUTHENTICATION_ERROR", host, userName));
		}

		collection = db.getCollection(collectionName);
	}

	@Override
	public void afterPropertiesSet()
	{
		try
		{
			readProperties();
			if (getRunningState() == RunningState.STARTED)
			{
				cleanup();
				applyProperties();
			}
		}
		catch (Exception error)
		{
			errorMessage = error.getMessage();
			LOGGER.error(errorMessage, error);
			setRunningState(RunningState.ERROR);
		}
	}

	@Override
	public void receive(ByteBuffer buffer, String channelId)
	{
		if (this.getRunningState() == RunningState.STARTED)
		{
			try
			{
				String json = convertToString(buffer);
				DBObject dbObj = (DBObject) com.mongodb.util.JSON.parse(json);
				collection.insert(dbObj);
			}
			catch (Exception error)
			{
				LOGGER.error("RECEIVE_ERROR", error.getMessage());
				LOGGER.info(error.getMessage(), error);
			}
		}
	}

	private String convertToString(ByteBuffer buffer)
	{
		try
		{
			CharsetDecoder decoder = getCharsetDecoder();
			CharBuffer charBuffer = decoder.decode(buffer);
			String decodedBuffer = charBuffer.toString();
			return decodedBuffer;
		}
		catch (CharacterCodingException error)
		{
			LOGGER.error("DECODE_ERROR", error.getMessage());
			LOGGER.info(error.getMessage(), error);
			buffer.clear();
			return null;
		}
	}

	private void cleanup()
	{
		errorMessage = "";
		if (mongoClient != null)
		{
			mongoClient.close();
			mongoClient = null;
		}
	}

	@Override
	public synchronized void start()
	{
		if (isRunning())
			return;
		try
		{
			this.setRunningState(RunningState.STARTING);
			applyProperties();
			this.setRunningState(RunningState.STARTED);
		}
		catch (IOException error)
		{
			String errorMsg = LOGGER.translate("START_ERROR", error.getMessage());
			LOGGER.error(errorMsg);
			LOGGER.info(error.getMessage(), error);
			errorMessage = error.getMessage();
			this.setRunningState(RunningState.ERROR);
		}
	}

	@Override
	public synchronized void stop()
	{
		this.setRunningState(RunningState.STOPPING);
		cleanup();
		this.setRunningState(RunningState.STOPPED);
	}

	@Override
	public String getStatusDetails()
	{
		return errorMessage;
	}

	public enum CollectionNameMethod
	{
		Static, ByField;
	}
}
