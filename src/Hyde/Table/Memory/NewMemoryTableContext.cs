using System;
using System.Collections.Generic;
using System.Linq;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Table.Azure;

namespace TechSmith.Hyde.Table.Memory
{
   internal class NewMemoryTableContext : ITableContext
   {
      private class Partition
      {
         private readonly Dictionary<string, GenericTableEntity> _entities = new Dictionary<string, GenericTableEntity>();

         public GenericTableEntity GetEntity( string rowKey )
         {
            lock ( _entities )
            {
               if ( ! _entities.ContainsKey( rowKey ) )
               {
                  throw new EntityDoesNotExistException();
               }
               return _entities[rowKey];
            }
         }

         public void Add( GenericTableEntity entity )
         {
            lock ( _entities )
            {
               if ( _entities.ContainsKey( entity.RowKey ) )
               {
                  throw new EntityAlreadyExistsException();
               }
               _entities[entity.RowKey] = entity;
            }
         }

         public IEnumerable<GenericTableEntity> GetAll()
         {
            lock ( _entities )
            {
               return new List<GenericTableEntity>( _entities.Values );
            }
         }
      }

      private class Table
      {
         private readonly Dictionary<string, Partition> _partitions = new Dictionary<string, Partition>();

         public Partition GetPartition( string partitionKey )
         {
            lock ( _partitions )
            {
               if ( !_partitions.ContainsKey( partitionKey ) )
               {
                  _partitions[partitionKey] = new Partition();
               }
               return _partitions[partitionKey];
            }
         }
      }

      private class StorageAccount
      {
         private readonly Dictionary<string, Table> _tables = new Dictionary<string, Table>();

         public Table GetTable( string tableName )
         {
            lock ( _tables )
            {
               if ( !_tables.ContainsKey( tableName ) )
               {
                  _tables.Add( tableName, new Table() );
               }
               return _tables[tableName];
            }
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
         return _tables.GetTable( tableName ).GetPartition( partitionKey ).GetEntity( rowKey ).ConvertTo<T>();
      }

      public IEnumerable<T> GetCollection<T>( string tableName ) where T : new()
      {
         throw new NotImplementedException();
      }

      public IEnumerable<T> GetCollection<T>( string tableName, string partitionKey ) where T : new()
      {
         return _tables.GetTable( tableName ).GetPartition( partitionKey ).GetAll().Select( e => e.ConvertTo<T>() );
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
         return _tables.GetTable( tableName ).GetPartition( partitionKey ).GetEntity( rowKey ).ConvertToDynamic();
      }

      public IEnumerable<dynamic> GetCollection( string tableName )
      {
         throw new NotImplementedException();
      }

      public IEnumerable<dynamic> GetCollection( string tableName, string partitionKey )
      {
         return _tables.GetTable( tableName ).GetPartition( partitionKey ).GetAll().Select( e => e.ConvertToDynamic() );
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
         _pendingActions.Enqueue( tables => tables.GetTable( tableName ).GetPartition( entity.PartitionKey ).Add( entity ) );
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
         _pendingActions.Clear();
      }

      public IEnumerable<T> GetRange<T>( string tableName, string partitionKeyLow, string partitionKeyHigh ) where T : new()
      {
         throw new NotImplementedException();
      }
   }
}
