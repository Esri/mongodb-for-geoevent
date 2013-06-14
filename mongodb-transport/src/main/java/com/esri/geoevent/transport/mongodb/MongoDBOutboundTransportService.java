package com.esri.geoevent.transport.mongodb;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.transport.Transport;
import com.esri.ges.transport.TransportServiceBase;
import com.esri.ges.transport.util.XmlTransportDefinition;

public class MongoDBOutboundTransportService extends TransportServiceBase
{
  public MongoDBOutboundTransportService()
  {
    definition = new XmlTransportDefinition(getResourceAsStream("outboundtransport-definition.xml"));
  }

  @Override
  public Transport createTransport() throws ComponentException
  {
    return new MongoDBOutboundTransport(definition);
  }
}
