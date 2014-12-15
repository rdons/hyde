using TechSmith.Hyde.Table.Memory;

namespace TechSmith.Hyde.Table
{
   public class InMemoryTableStorageProvider : TableStorageProvider
   {
      public InMemoryTableStorageProvider( MemoryStorageAccount account = null )
         : base( new MemoryTableContext( account ) )
      {
      }

      public static void ResetAllTables()
      {
         MemoryTableContext.ResetAllTables();
      }
   }
}