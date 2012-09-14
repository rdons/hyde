using System;
using System.Net;
using TechSmith.Hyde.Table.Azure;

namespace TechSmith.Hyde.Table
{
   public class AzureTableStorageProvider : TableStorageProvider
   {
      private readonly ICloudStorageAccount _cloudStorageAccount;

      public AzureTableStorageProvider( ICloudStorageAccount cloudStorageAccount )
      {
         _cloudStorageAccount = cloudStorageAccount;
         ServicePoint servicePoint = ServicePointManager.FindServicePoint( new Uri( _cloudStorageAccount.TableEndpoint ) );
         servicePoint.Expect100Continue = false;
         servicePoint.ConnectionLimit = 48;
      }

      protected override ITableContext GetContext()
      {
         return new AzureTableContext( _cloudStorageAccount );
      }
   }
}