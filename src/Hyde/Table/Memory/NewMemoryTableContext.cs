using System;
using System.Collections.Generic;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Table.Azure;

namespace TechSmith.Hyde.Table.Memory
{
   internal class NewMemoryTableContext : ITableContext
   {
      private class Table
      {
         private readonly Dictionary<Tuple<string, string>, GenericTableEntity> _entities = new Dictionary<Tuple<string, string>, GenericTableEntity>();

         public GenericTableEntity Get( string partitionKey, string rowKey )
         {
            var key = new Tuple<string, string>( partitionKey, rowKey );
            if ( ! _entities.ContainsKey( key ) )
            {
               throw new EntityDoesNotExistException();
            }
            return _entities[key];
         }

         public void Add( GenericTableEntity entity )
         {
            _entities[new Tuple<string, string>( entity.PartitionKey, entity.RowKey )] = entity;
         }
      }

      private class StorageAccount
      {
         private readonly Dictionary<string, Table> _tables = new Dictionary<string, Table>();

         public Table GetTable( string tableName )
         {
            if ( !_tables.ContainsKey( tableName ) )
            {
               _tables.Add( tableName, new Table() );
            }
            return _tables[tableName];
         }
      }

      private static StorageAccount _tables = new StorageAccount();

      private readonly Queue<Action<StorageAccount>> _pendingActions = new Queue<Action<StorageAccount>>();

      public static void ResetAllTables()
      {
         _tables = new StorageAccount();
      }

      public T GetItem<T>( string tableName, string partitionKey, string rowKey ) where T : new()
      {
         return _tables.GetTable( tableName ).Get( partitionKey, rowKey ).ConvertTo<T>();
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
         var entity = GenericTableEntity.HydrateFrom( itemToAdd, partitionKey, rowKey );
         _pendingActions.Enqueue( tables => tables.GetTable( tableName ).Add( entity ) );
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
         foreach ( var action in _pendingActions )
         {
            action( _tables );
         }
      }

      public IEnumerable<T> GetRange<T>( string tableName, string partitionKeyLow, string partitionKeyHigh ) where T : new()
      {
         throw new NotImplementedException();
      }
   }
}
