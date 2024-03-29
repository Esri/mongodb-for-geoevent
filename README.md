# mongodb-for-geoevent
**This item has been deprecated. Please consider contributing an idea to the [Esri Community](https://community.esri.com/t5/arcgis-geoevent-server-ideas/idb-p/arcgis-geoevent-server-ideas) if you need similar functionality.**

ArcGIS 10.4 GeoEvent Extension for Server sample MongoDB Ouptut Connector for sending GeoEvents to MongoDB.

![App](mongodb-for-geoevent.png?raw=true)

## Features
* MongoDB Outbound Transport

## Instructions

Building the source code:

1. Make sure Maven and ArcGIS GeoEvent Extension SDK are installed on your machine.
2. Run 'mvn install -Dcontact.address=[YourContactEmailAddress]'

Installing the transport and connector:

1. Copy the *.jar files under the 'target' sub-folder(s) into the [ArcGIS-GeoEvent-Extension-Install-Directory]/deploy folder.
2. Open GeoEvent Manager, go to the 'Site -> Configuration Store' page and import the Mongo_Connector.xml file.

## Requirements

* ArcGIS GeoEvent Extension for Server.
* ArcGIS GeoEvent Extension SDK.
* Java JDK 1.7 or greater.
* Maven.

## Resources

* [Download the connector's tutorial](http://www.arcgis.com/home/item.html?id=0f246f7e9f074b3f80b24724b460e82f) from the ArcGIS GeoEvent Extension Gallery
* [ArcGIS GeoEvent Extension for Server Resources](http://links.esri.com/geoevent)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing
Copyright 2013 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt](license.txt?raw=true) file.
