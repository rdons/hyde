using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TechSmith.Hyde.Table;

namespace TechSmith.Hyde.Test
{
   [TestClass]
   public class DynamicTest
   {
      private TableStorageProvider _tableStorageProvider;
      private static readonly string _tableName = "DynamicTable";

      [TestInitialize]
      public void SetUp()
      {
         InMemoryTableStorageProvider.ResetAllTables();
         _tableStorageProvider = new InMemoryTableStorageProvider();
      }

      [TestMethod]
      public void Get_ObjectInsertedWithClassAndRetrievedViaDynamic_ShouldReturnFullyHydratedObject()
      {
         var simpleEntity = new DecoratedItem
         {
            Id = string.Format("Dynamic{0}", DateTime.Now.Ticks),
            Name = "Test",
            Age = 1
         };

         _tableStorageProvider.Add( _tableName, simpleEntity );

         _tableStorageProvider.Save();

         var result = _tableStorageProvider.Get( _tableName, simpleEntity.Id, simpleEntity.Name );

         Assert.AreEqual( simpleEntity.Age, result.Age );
         Assert.AreEqual( simpleEntity.Id, result.PartitionKey );
         Assert.AreEqual( simpleEntity.Name, result.RowKey );
      }
   }
}
