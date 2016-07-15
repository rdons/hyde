using System;
using System.Collections.Concurrent;
using System.Net;
using TechSmith.Hyde.Table.Azure;

namespace TechSmith.Hyde.Table
{
   public class AzureTableStorageProvider : TableStorageProvider
   {
      public AzureTableStorageProvider( ICloudStorageAccount cloudStorageAccount )
         : base ( new AzureTableEntityTableContext( cloudStorageAccount ) )
      {
      }
   }
}