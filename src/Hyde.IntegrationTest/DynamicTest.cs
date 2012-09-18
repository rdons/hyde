using System;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.StorageClient;
using TechSmith.Hyde.Table;

namespace TechSmith.Hyde.IntegrationTest
{
   [TestClass]
   public class DynamicTest
   {
      readonly ICloudStorageAccount _storageAccount = new ConnectionStringCloudStorageAccount( ConfigurationManager.AppSettings["storageConnectionString"] );
      private static readonly string _baseTableName = "DynamicIntegrationTest";

      private TableStorageProvider _tableStorageProvider;
      private string _tableName;

      [TestInitialize]
      public void TestInitialize()
      {

         _tableStorageProvider = new AzureTableStorageProvider( _storageAccount );

         _tableName = _baseTableName + Guid.NewGuid().ToString().Replace( "-", string.Empty );

         var client = new CloudTableClient( _storageAccount.TableEndpoint,
                                         _storageAccount.Credentials );
         client.CreateTable( _tableName );
      }

      [ClassCleanup]
      public static void ClassCleanup()
      {
         var storageAccountProvider = new ConnectionStringCloudStorageAccount( ConfigurationManager.AppSettings["storageConnectionString"] );

         var client = new CloudTableClient( storageAccountProvider.TableEndpoint,
                                         storageAccountProvider.Credentials );

         var orphanedTables = client.ListTables( _baseTableName );
         foreach ( var orphanedTableName in orphanedTables )
         {
            client.DeleteTableIfExist( orphanedTableName );
         }
      }

      [TestMethod]
      public void Get_RetrievingObjectViaDynamic_ShouldHydrateEntityWithAllProperties()
      {
         var simpleEntity = new DecoratedItem
         {
            Id = string.Format("Dynamic{0}", DateTime.Now.Ticks),
            Name = "Test",
            Age = 1
         };

         _tableStorageProvider.Add( _tableName, simpleEntity );
         _tableStorageProvider.Save();

         var retrievedObject = _tableStorageProvider.Get( _tableName, simpleEntity.Id, simpleEntity.Name );

         Assert.AreEqual( simpleEntity.Age, (int) retrievedObject.Age );
         Assert.AreEqual( simpleEntity.Id, retrievedObject.PartitionKey );
         Assert.AreEqual( simpleEntity.Name, retrievedObject.RowKey );
      }
   }
}
