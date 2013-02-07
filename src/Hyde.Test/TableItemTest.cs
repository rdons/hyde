using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Common.DataAnnotations;
using TechSmith.Hyde.Table;

namespace TechSmith.Hyde.Test
{
   [TestClass]
   public class TableItemTest
   {
      [TestMethod]
      public void Create_ValidEntity_PropertiesSetCorrectly()
      {
         var item = TableItem.Create( new SimpleDataItem { FirstType = "Joe", SecondType = 34 }, "pk", "rk", true );

         Assert.AreEqual( 3, item.Properties.Count );
         Assert.AreEqual( "Joe", item.Properties["FirstType"].Item1 );
         Assert.AreEqual( 34, item.Properties["SecondType"].Item1 );
         Assert.IsNull( item.Properties["UriTypeProperty"].Item1 );
      }

      [TestMethod]
      public void CreateAndThrowOnReservedProperty_KeysProvidedAndNoReservedProperties_KeysSetCorrectly()
      {
         var item = TableItem.Create( new SimpleDataItem { FirstType = "Joe", SecondType = 34 }, "pk", "rk", true );

         Assert.AreEqual( "pk", item.PartitionKey );
         Assert.AreEqual( "rk", item.RowKey );
      }

      [TestMethod]
      [ExpectedException( typeof( InvalidEntityException ) )]
      public void CreateAndThrowOnReservedProperty_KeysProvidedAndHasReservedProperty_ThrowsInvalidEntityException()
      {
         TableItem.Create( new ClassWithTimestamp { Name = "Joe", Timestamp = DateTime.Now }, "pk", "rk", true );
      }

      [TestMethod]
      public void CreateAndIgnoreReservedProperty_KeysProvidedAndHasReservedProperty_IgnoresReservedProperty()
      {
         var item = TableItem.Create( new ClassWithUndecoratedPartitionKey { Name = "Joe", PartitionKey = "should be ignored" }, "pk", "rk", false );

         Assert.AreEqual( 1, item.Properties.Count );
         Assert.AreEqual( "Joe", item.Properties["Name"].Item1 );
         Assert.AreEqual( "pk", item.PartitionKey );
      }
   }

   class ClassWithTimestamp
   {
      public string Name { get; set; }

      public DateTime Timestamp { get; set; }
   }

   class ClassWithUndecoratedPartitionKey
   {
      public string Name { get; set; }

      public string PartitionKey { get; set; }
   }
}
