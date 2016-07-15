using TechSmith.Hyde.Common.DataAnnotations;

namespace TechSmith.Hyde.IntegrationTest
{
   internal class DecoratedItemWithETag
   {
      [PartitionKey]
      public string Id
      {
         get;
         set;
      }

      [RowKey]
      public string Name
      {
         get;
         set;
      }

      [ETag]
      public object ETag
      {
         get;
         set;
      }

      public int Age
      {
         get;
         set;
      }
   }
}
