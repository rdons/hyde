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
   public class SaveInBatchesTest
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


         _tableStorageProvider.Save( Execute.InBatches );


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
         _tableStorageProvider.Save( Execute.InBatches );

         for ( int i = 0; i < expectedCount; i++ )
         {
            var item = _tableStorageProvider.Get<DecoratedItem>( _tableName, partitionKey, i.ToString( CultureInfo.InvariantCulture ) );
            item.Age = 101;
            _tableStorageProvider.Update( _tableName, item );
         }


         _tableStorageProvider.Save( Execute.InBatches );


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


         _tableStorageProvider.Save( Execute.InBatches );


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
         _tableStorageProvider.Save( Execute.InBatches );

         for ( int i = 0; i < expectedCount; i++ )
         {
            _tableStorageProvider.Delete( _tableName, partitionKey, i.ToString( CultureInfo.InvariantCulture ) );
         }


         _tableStorageProvider.Save( Execute.InBatches );


         IEnumerable<DecoratedItem> items = _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, partitionKey );
         Assert.IsFalse( items.Any() );
      }

      [TestMethod]
      public void Save_AllInsertsOnSamePartition_ShouldExecuteInEntityGroupTransaction()
      {
         // We can't tell directly whether an EGT is used, but we can infer it by setting up
         // an EGT to fail, and verifying that no actions were committed.
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "one" } );
         _tableStorageProvider.Save();

         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "zero" } );
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "one" } );
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "two" } );
         try
         {
            _tableStorageProvider.Save( Execute.InBatches );
            Assert.Fail( "Should have thrown exception" );
         }
         catch ( EntityAlreadyExistsException )
         {
         }

         Assert.AreEqual( 1, _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, "123" ).Count() );
      }

      [TestMethod]
      public void Save_MultipleOperationTypesOnSamePartition_ShouldExecuteInDifferentEntityGroupTransactions()
      {
         // As in the previous test, we can tell the operations were executed in different requests by
         // setting one up to fail and verifying that the other completed.
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "one" } );
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "two" } );
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "three" } );
         _tableStorageProvider.Save();

         _tableStorageProvider.Update( _tableName, new DecoratedItem { Id = "123", Name = "one", Age = 42 } );
         _tableStorageProvider.Delete( _tableName, new DecoratedItem { Id = "123", Name = "three" } );
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "two" } );

         try
         {
            _tableStorageProvider.Save( Execute.InBatches );
            Assert.Fail( "Should have thrown exception" );
         }
         catch ( EntityAlreadyExistsException )
         {
         }

         var results = _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, "123" ).ToList();
         Assert.AreEqual( 2, results.Count() );
         Assert.AreEqual( 42, _tableStorageProvider.Get<DecoratedItem>( _tableName, "123", "one" ).Age );
         Assert.IsFalse( results.Any( i => i.Name == "three" ) );
      }

      [TestMethod]
      public void Save_SameRowInsertedTwice_InsertsDoneInSeparateTransactions()
      {
         // Inserting the same row twice in the same EGT causes Table Storage to return 400 Bad Request.
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "abc" } );
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "abc" } );

         try
         {
            _tableStorageProvider.Save( Execute.InBatches );
            Assert.Fail( "Should have thrown exception" );
         }
         catch ( EntityAlreadyExistsException )
         {
         }

         Assert.AreEqual( 1, _tableStorageProvider.GetCollection<DecoratedItem>( _tableName, "123" ).Count() );
      }

      [TestMethod]
      public void Save_SameRowUpdatedTwice_UpdatesDoneInDifferentTransactions()
      {
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "abc", Age = 30 } );
         _tableStorageProvider.Save();

         _tableStorageProvider.Update( _tableName, new DecoratedItem { Id = "123", Name = "abc", Age = 40 } );
         _tableStorageProvider.Update( _tableName, new DecoratedItem { Id = "123", Name = "abc", Age = 50 } );
         _tableStorageProvider.Save( Execute.InBatches );

         Assert.AreEqual( 50, _tableStorageProvider.Get<DecoratedItem>( _tableName, "123", "abc" ).Age );
      }

      [TestMethod]
      public void Save_SameRowDeletedTwice_DeletesDoneInDifferentTransactions()
      {
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "abc", Age = 30 } );
         _tableStorageProvider.Save();

         _tableStorageProvider.Delete( _tableName, new DecoratedItem { Id = "123", Name = "abc" } );
         _tableStorageProvider.Delete( _tableName, new DecoratedItem { Id = "123", Name = "abc" } );
         _tableStorageProvider.Save( Execute.InBatches );

         Assert.IsNull( _tableStorageProvider.Get<DecoratedItem>( _tableName, "123", "abc" ) );
      }

      [TestMethod]
      public void Save_SameRowAddedAndUpdated_OperationsDoneInDifferentTransactions()
      {
         _tableStorageProvider.Add( _tableName, new DecoratedItem { Id = "123", Name = "abc", Age = 30 } );
         _tableStorageProvider.Update( _tableName, new DecoratedItem { Id = "123", Name = "abc", Age = 40 } );
         // next operation will be done in same transaction with previous update will fail. If previous operations were
         // correctly done in different transactions, the add will have completed.
         _tableStorageProvider.Update( _tableName, new DecoratedItem { Id = "123", Name = "not found", Age = 40 } );
         try
         {
            _tableStorageProvider.Save( Execute.InBatches );
            Assert.Fail( "Should have thrown exception" );
         }
         catch ( EntityDoesNotExistException )
         {
         }

         Assert.IsNotNull(  _tableStorageProvider.Get<DecoratedItem>( _tableName, "123", "abc" ) );
      }
   }
}
