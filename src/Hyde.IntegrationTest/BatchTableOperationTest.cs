using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Table;

namespace TechSmith.Hyde.IntegrationTest
{
   [TestClass]
   public class BatchTableOperationTest
   {
      private static readonly string _baseTableName = "BatchTestTable";
      private AzureTableStorageProvider _tableStorageProvider;
      private CloudTableClient _client;
      private string _tableName;

      [TestInitialize]
      public void TestInitialize()
      {
         ICloudStorageAccount storageAccount = new ConnectionStringCloudStorageAccount( ConfigurationManager.AppSettings["storageConnectionString"] );

         _tableStorageProvider = new AzureTableStorageProvider( storageAccount );

         _client = new CloudTableClient( new Uri( storageAccount.TableEndpoint ), storageAccount.Credentials );

         _tableName = _baseTableName + Guid.NewGuid().ToString().Replace( "-", string.Empty );

         var table = _client.GetTableReference( _tableName );
         table.Create();
      }

      [TestCleanup]
      public void TestCleanup()
      {
         var table = _client.GetTableReference( _tableName );
         table.Delete();
      }

      [ClassCleanup]
      public static void ClassCleanup()
      {
         var storageAccountProvider = new ConnectionStringCloudStorageAccount( ConfigurationManager.AppSettings["storageConnectionString"] );

         var client = new CloudTableClient( new Uri( storageAccountProvider.TableEndpoint ), storageAccountProvider.Credentials );

         var orphanedTables = client.ListTables( _baseTableName );
         foreach ( var orphanedTableName in orphanedTables )
         {
            var table = client.GetTableReference( orphanedTableName.Name );
            table.DeleteIfExists();
         }
      }

      [TestMethod]
      public void Insert_101EntitiesInTheSamePartition_ShouldSucceed()
      {
         string partitionKey = "123";
         int expectedCount = 101;
         for ( int i = 0; i < expectedCount; i++ )
         {
            var item = new DecoratedItem
                       {
                          Id = partitionKey,
                          Name = i.ToString( CultureInfo.InvariantCulture )
                       };
            _tableStorageProvider.Add( _tableName, item );
         }


         _tableStorageProvider.Save();


         IEnumerable<DecoratedItem> items = _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, partitionKey );
         Assert.AreEqual( expectedCount, items.Count() );
      }

      [TestMethod]
      public void Update_101EntitiesInTheSamePartition_ShouldSucceed()
      {
         string partitionKey = "123";
         int expectedCount = 101;
         for ( int i = 0; i < expectedCount; i++ )
         {
            var item = new DecoratedItem
                       {
                          Id = partitionKey,
                          Name = i.ToString( CultureInfo.InvariantCulture )
                       };
            _tableStorageProvider.Add( _tableName, item );
         }
         _tableStorageProvider.Save();

         for ( int i = 0; i < expectedCount; i++ )
         {
            var item = _tableStorageProvider.Get<DecoratedItem>( _tableName, partitionKey, i.ToString( CultureInfo.InvariantCulture ) );
            item.Age = 101;
            _tableStorageProvider.Update( _tableName, item );
         }


         _tableStorageProvider.Save();


         IEnumerable<DecoratedItem> items = _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, partitionKey ).ToList();
         Assert.AreEqual( expectedCount, items.Count() );
         Assert.IsFalse( items.Any( i => i.Age != 101 ) );
      }

      [TestMethod]
      public void Upsert_101EntitiesInTheSamePartition_ShouldSucceed()
      {
         string partitionKey = "123";
         int expectedCount = 101;
         for ( int i = 0; i < expectedCount; i++ )
         {
            var item = new DecoratedItem
            {
               Id = partitionKey,
               Name = i.ToString( CultureInfo.InvariantCulture )
            };
            _tableStorageProvider.Upsert( _tableName, item );
         }


         _tableStorageProvider.Save();


         IEnumerable<DecoratedItem> items = _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, partitionKey ).ToList();
         Assert.AreEqual( expectedCount, items.Count() );
      }

      [TestMethod]
      public void Delete_101EntitiesInTheSamePartition_ShouldSucceed()
      {
         string partitionKey = "123";
         int expectedCount = 101;
         for ( int i = 0; i < expectedCount; i++ )
         {
            var item = new DecoratedItem
            {
               Id = partitionKey,
               Name = i.ToString( CultureInfo.InvariantCulture )
            };
            _tableStorageProvider.Add( _tableName, item );
         }
         _tableStorageProvider.Save();

         for ( int i = 0; i < expectedCount; i++ )
         {
            _tableStorageProvider.Delete( _tableName, partitionKey, i.ToString( CultureInfo.InvariantCulture ) );
         }


         _tableStorageProvider.Save();


         IEnumerable<DecoratedItem> items = _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, partitionKey );
         Assert.IsFalse( items.Any() );
      }

      [TestMethod]
      [ExpectedException( typeof( EntityAlreadyExistsException ) )]
      public void Insert_EntityAlreadyExists_ShouldSucceed()
      {
         string partitionKey = "123";
         string rowKey = "abc";
         var decoratedItem = new DecoratedItem
                             {
                                Id = partitionKey,
                                Name = rowKey
                             };
         _tableStorageProvider.Add( _tableName, decoratedItem );
         _tableStorageProvider.Add( _tableName, decoratedItem );


         _tableStorageProvider.Save();
      }
   }
}
