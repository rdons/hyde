using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace TechSmith.Hyde.Table.Azure
{
   public class AzureQuery<T> : IQuery<T> where T : new()
   {
      private readonly CloudTable _table;
      private TableQuery<GenericTableEntity> _query;

      public AzureQuery( CloudTable table, string filter )
      {
         _table = table;
         _query = new TableQuery<GenericTableEntity>().Where( filter );
      }

      private AzureQuery( CloudTable table, TableQuery<GenericTableEntity> query )
      {
         _table = table;
         _query = query;
      }

      public IEnumerator<T> GetEnumerator()
      {
         return _table.ExecuteQuery( _query ).Select( e => e.ConvertTo<T>() ).GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return GetEnumerator();
      }

      public IQuery<T> Take( int count )
      {
         return new AzureQuery<T>( _table, _query.Take( count ) );
      }

   }
}
