using System;
using System.Collections.Generic;

namespace TechSmith.Hyde.Table.Azure
{
   internal class AzureTableEntityTableContext : ITableContext
   {
      public AzureTableEntityTableContext( ICloudStorageAccount storageAccount )
      {

      }

      public T GetItem<T>( string tableName, string partitionKey, string rowKey ) where T : new()
      {
         throw new NotImplementedException();
      }

      public IEnumerable<T> GetCollection<T>( string tableName ) where T : new()
      {
         throw new NotImplementedException();
      }

      public IEnumerable<T> GetCollection<T>( string tableName, string partitionKey ) where T : new()
      {
         throw new NotImplementedException();
      }

      public IEnumerable<T> GetRangeByPartitionKey<T>( string tableName, string partitionKeyLow, string partitionKeyHigh ) where T : new()
      {
         throw new NotImplementedException();
      }

      public IEnumerable<T> GetRangeByRowKey<T>( string tableName, string partitionKey, string rowKeyLow, string rowKeyHigh ) where T : new()
      {
         throw new NotImplementedException();
      }

      public dynamic GetItem( string tableName, string partitionKey, string rowKey )
      {
         throw new NotImplementedException();
      }

      public IEnumerable<dynamic> GetCollection( string tableName )
      {
         throw new NotImplementedException();
      }

      public IEnumerable<dynamic> GetCollection( string tableName, string partitionKey )
      {
         throw new NotImplementedException();
      }

      public IEnumerable<dynamic> GetRangeByPartitionKey( string tableName, string partitionKeyLow, string partitionKeyHigh )
      {
         throw new NotImplementedException();
      }

      public IEnumerable<dynamic> GetRangeByRowKey( string tableName, string partitionKey, string rowKeyLow, string rowKeyHigh )
      {
         throw new NotImplementedException();
      }

      public void AddNewItem( string tableName, dynamic itemToAdd, string partitionKey, string rowKey )
      {
         throw new NotImplementedException();
      }

      public void Upsert( string tableName, dynamic itemToUpsert, string partitionKey, string rowKey )
      {
         throw new NotImplementedException();
      }

      public void Update( string tableName, dynamic item, string partitionKey, string rowKey )
      {
         throw new NotImplementedException();
      }

      public void DeleteItem( string tableName, string partitionKey, string rowKey )
      {
         throw new NotImplementedException();
      }

      public void DeleteCollection( string tableName, string partitionKey )
      {
         throw new NotImplementedException();
      }

      public void Save()
      {
         throw new NotImplementedException();
      }

      public IEnumerable<T> GetRange<T>( string tableName, string partitionKeyLow, string partitionKeyHigh ) where T : new()
      {
         throw new NotImplementedException();
      }
   }
}