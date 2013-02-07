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

      [TestMethod]
      [ExpectedException( typeof( ArgumentException ) )]
      public void Create_KeysProvidedAndEntityHasDecoratedPartitionKeyPropertyWithDifferentValue_ThrowsArgumentException()
      {
         TableItem.Create( new DecoratedItem { Id = "pk1", Name = "rk", Age = 34 }, "pk2", "rk", true );
      }

      [TestMethod]
      [ExpectedException( typeof( ArgumentException ) )]
      public void Create_KeysProvidedAndEntityHasDecoratedRowKeyPropertyWithDifferentValue_ThrowsArgumentException()
      {
         TableItem.Create( new DecoratedItem { Id = "pk", Name = "rk1", Age = 34 }, "pk", "rk2", true );
      }

      [TestMethod]
      public void Create_KeysProvidedAndEntityHasDecoratedKeyPropertiesWithMatchingValues_ItemCreatedWithKeys()
      {
         var item = TableItem.Create( new DecoratedItem { Id = "pk", Name = "rk", Age = 34 }, "pk", "rk", true );

         Assert.AreEqual( "pk", item.PartitionKey );
         Assert.AreEqual( "rk", item.RowKey );
      }

      [TestMethod]
      [ExpectedException( typeof( ArgumentException ) )]
      public void Create_KeysNotProvidedAndEntityHasNoPartitionKey_ThrowsArgumentException()
      {
         TableItem.Create( new ClassWithoutDecoratedPartitionKey { Name = "Joe" }, true );
      }

      [TestMethod]
      [ExpectedException( typeof( ArgumentException ) )]
      public void Create_KeysNotProvidedAndEntityHasNoRowKey_ThrowsArgumentException()
      {
         TableItem.Create( new ClassWithoutDecoratedRowKey { Name = "Joe" }, true );
      }

      [TestMethod]
      public void Create_KeysNotProvidedAndEntityHasDecoratedKeyProperties_ItemCreatedWithKeys()
      {
         var item = TableItem.Create( new DecoratedItem { Id = "pk", Name = "rk", Age = 34 }, true );

         Assert.AreEqual( "pk", item.PartitionKey );
         Assert.AreEqual( "rk", item.RowKey );
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

   class ClassWithoutDecoratedRowKey
   {
      [PartitionKey]
      public string Name { get; set; }
   }

   class ClassWithoutDecoratedPartitionKey
   {
      [RowKey]
      public string Name { get; set; }
   }
}
