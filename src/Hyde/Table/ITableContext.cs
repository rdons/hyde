using System.Threading.Tasks;

namespace TechSmith.Hyde.Table
{
   public interface ITableContext
   {
      // Implementation using generics and reflection.
      IFilterable<T> CreateQuery<T>( string tableName ) where T : new();

      // Implemntation using dynamics.
      IFilterable<dynamic> CreateQuery( string tableName, bool includeETagForDynamic );

      // Shared implementation between generics and dynamics.
      void AddNewItem( string tableName, TableItem tableItem );
      void Upsert( string tableName, TableItem tableItem );
      void Update( string tableName, TableItem tableItem, ConflictHandling conflictHandling );
      void Merge( string tableName, TableItem tableItem, ConflictHandling conflictHandling );
      void DeleteItem( string tableName, string partitionKey, string rowKey );
      void DeleteItem( string tableName, TableItem tableItem, ConflictHandling conflictHandling );
      Task SaveAsync( Execute executeMethod );
   }
}
