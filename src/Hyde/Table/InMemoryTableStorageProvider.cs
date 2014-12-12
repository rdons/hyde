using TechSmith.Hyde.Table.Memory;

namespace TechSmith.Hyde.Table
{
   public class InMemoryTableStorageProvider : TableStorageProvider
   {
      public InMemoryTableStorageProvider(bool useInstancePrivateAccount = false)
         : base(new MemoryTableContext(useInstancePrivateAccount))
      {
      }

      public static void ResetAllTables()
      {
         MemoryTableContext.ResetAllTables();
      }
   }
}