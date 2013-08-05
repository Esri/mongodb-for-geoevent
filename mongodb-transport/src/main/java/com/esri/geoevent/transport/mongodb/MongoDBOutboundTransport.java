package com.esri.geoevent.transport.mongodb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class MongoDBOutboundTransport extends OutboundTransportBase
{
  private static final Log    log                      = LogFactory.getLog(MongoDBOutboundTransport.class);

  private static final String HOST_NAME_PROPERTY       = "outputHostName";
  private static final String PORT_PROPERTY            = "outputPort";
  private static final String DATABASE_NAME_PROPERTY   = "outputDatabaseName";
  private static final String USER_NAME_PROPERTY       = "outputUserName";
  private static final String PASSWORD_PROPERTY        = "outputPassword";
  private static final String COLLECTION_NAME_PROPERTY = "outputCollectionName";
  private static final String WRITE_CONCERN_PROPERTY   = "outputWriteConcern";

  private String              host                     = "localhost";
  private int                 port                     = 27017;
  private String              databaseName             = "db";
  private String              userName                 = "";
  private String              password                 = "";
  private String              collectionName           = "test";
  private WriteConcern        writeConcern;

  private String              errorMessage;

  private MongoClient         mongoClient;
  private DB                  db;

  private Charset             charset                  = Charset.forName("UTF-8");
  private CharsetDecoder      decoder;

  private DBCollection        collection;

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
      throw new IOException("Failed to authenticate to the Mongo Database Server at " + host + " using the name " + userName + " and the configured password.");
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
    catch (Exception ex)
    {
      errorMessage = ex.getMessage();
      log.error(errorMessage);
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
      catch (Exception e)
      {
        log.error(e.getMessage(), e);
      }
    }
  }

  private String convertToString(ByteBuffer buffer)
  {
    if (decoder == null)
      decoder = charset.newDecoder();
    try
    {
      CharBuffer charBuffer = decoder.decode(buffer);
      String decodedBuffer = charBuffer.toString();
      return decodedBuffer;
    }
    catch (CharacterCodingException e)
    {
      log.warn("Could not decode the incoming buffer - " + e);
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
    catch (IOException e)
    {
      log.error("Could not start the Mongo DB output transport : " + e.getMessage());
      errorMessage = e.getMessage();
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
