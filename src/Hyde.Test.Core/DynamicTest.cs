using System;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TechSmith.Hyde.Common;
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
         _tableStorageProvider.ShouldThrowForReservedPropertyNames = false;
      }

      [TestMethod]
      public async Task Get_ObjectInsertedIsInheritsDynamicObject_RetrievedProperly()
      {
         dynamic item = new DynamicPropertyBag();
         item.Foo = "test";
         item.Bar = 1;

         string partitionKey = "partitionKey";
         string rowKey = "rowKey";
         _tableStorageProvider.Add( _tableName, item, partitionKey, rowKey );
         await _tableStorageProvider.SaveAsync();

         dynamic result = await _tableStorageProvider.GetAsync( _tableName, partitionKey, rowKey );

         Assert.AreEqual( item.Foo, result.Foo );
         Assert.AreEqual( item.Bar, result.Bar );
      }

      [TestMethod]
      public async Task Get_ObjectInsertedWithClassAndRetrievedViaDynamic_ShouldReturnFullyHydratedObject()
      {
         var simpleEntity = new DecoratedItem
         {
            Id = string.Format( "Dynamic{0}", DateTime.Now.Ticks ),
            Name = "Test",
            Age = 1
         };

         _tableStorageProvider.Add( _tableName, simpleEntity );

         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, simpleEntity.Id, simpleEntity.Name );

         Assert.AreEqual( simpleEntity.Age, result.Age );
         Assert.AreEqual( simpleEntity.Id, result.PartitionKey );
         Assert.AreEqual( simpleEntity.Name, result.RowKey );
      }

      [TestMethod]
      public async Task GetCollection_ObjectInsertedWithClassAndRetrievedViaDynamic_ShouldReturnFullyHydratedObjects()
      {
         string partitionKey = "Test";
         foreach( var i in Enumerable.Range( 0, 10 ) )
         {
            var simpleEntity = new DecoratedItem
            {
               Id = partitionKey,
               Name = string.Format( "Dynamic{0}_{1}", DateTime.Now.Ticks, i ),
               Age = 1
            };

            _tableStorageProvider.Add( _tableName, simpleEntity );

            await _tableStorageProvider.SaveAsync();
         }

         var result = await _tableStorageProvider.CreateQuery( _tableName ).PartitionKeyEquals( partitionKey ).Async();

         Assert.AreEqual( 10, result.Count() );
         Assert.AreEqual( 1, result.First().Age );
      }

      [TestMethod]
      public async Task Get_AddAndGetDynamic_DynamicIsReturnedWithAllProperties()
      {
         dynamic dyn = new ExpandoObject();

         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn, "pk", "rk" );

         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );

         Assert.AreEqual( "this is the first item.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public async Task UpsertDynamic_TheDynamicWasntInsertedYet_DynamicIsReturnedWithAllProperties()
      {
         dynamic dyn = new ExpandoObject();
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Upsert( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );
         Assert.AreEqual( "this is the first item.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public async Task UpsertDynamic_TheDynamicAlreadyInsertedAndNeedsToBeUpdated_DynamicIsReturnedWithAllProperties()
      {
         dynamic dyn = new ExpandoObject();
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();
         dyn.FirstItem = "this text is changed.";
         _tableStorageProvider.Upsert( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );
         Assert.AreEqual( "this text is changed.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public async Task UpdateDynamic_TheDynamicAlreadyInsertedAndNeedsToBeUpdated_DynamicIsReturnedWithAllProperties()
      {
         dynamic dyn = new ExpandoObject();
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();
         dyn.FirstItem = "this text is changed.";
         _tableStorageProvider.Update( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );
         Assert.AreEqual( "this text is changed.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public async Task AddDynamic_TheDynamicContainPartitionKeyAndRowKey_DynamicIsAddedWithAllProperties()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";
         dyn.RowKey = "rk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn );
         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );
         Assert.AreEqual( "pk", result.PartitionKey );
         Assert.AreEqual( "rk", result.RowKey );
         Assert.AreEqual( "this is the first item.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public async Task AddDynamic_TheDynamicContainPartitionKeyAndRowKeyThatMatchPartionKeyAndRowKeyArguments_DynamicIsAddedWithAllProperties()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";
         dyn.RowKey = "rk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );
         Assert.AreEqual( "pk", result.PartitionKey );
         Assert.AreEqual( "rk", result.RowKey );
         Assert.AreEqual( "this is the first item.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public void AddDynamic_TheDynamicDoesNotContainPartitionKey_ShouldThrowArgumentException()
      {
         dynamic dyn = new ExpandoObject();
         dyn.RowKey = "rk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         Assert.ThrowsException<ArgumentException>( (Action) ( () => _tableStorageProvider.Add( _tableName, dyn ) ) );
      }

      [TestMethod]
      public void AddDynamic_TheDynamicDoesNotContainRowKey_ShouldThrowArgumentException()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         Assert.ThrowsException<ArgumentException>( (Action) ( () => _tableStorageProvider.Add( _tableName, dyn ) ) );
      }

      [TestMethod]
      public void AddDynamic_TheDynamicContainPartitionKeyAndRowKeyThatDoNotMatchPartionKeyAndRowKeyArguments_ShouldThrowArgumentException()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "partitionKey";
         dyn.RowKey = "rowKey";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         Assert.ThrowsException<ArgumentException>( (Action) ( () => _tableStorageProvider.Add( _tableName, dyn, "pk", "rk" ) ) );
      }

      [TestMethod]
      public void AddDynamic_TheDynamicContainsReservedPropertyAndShouldThrowForReservedPropertyNamesIsTrue_ShouldThrowInvalidEntityException()
      {
         _tableStorageProvider.ShouldThrowForReservedPropertyNames = true;
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";

         Assert.ThrowsException<InvalidEntityException>( (Action) ( () => _tableStorageProvider.Add( _tableName, dyn ) ) );
      }

      [TestMethod]
      public async Task UpdateDynamic_TheDynamicContainsPartitionAndRowKey_DynamicIsUpdatedProperly()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";
         dyn.RowKey = "rk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn );
         await _tableStorageProvider.SaveAsync();
         dyn.FirstItem = "this text is changed.";
         _tableStorageProvider.Update( _tableName, dyn );
         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );
         Assert.AreEqual( "pk", result.PartitionKey );
         Assert.AreEqual( "rk", result.RowKey );
         Assert.AreEqual( "this text is changed.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public async Task UpdateDynamic_TheDynamicContainPartitionKeyAndRowKeyThatMatchPartionKeyAndRowKeyArguments_DynamicIsUpdatedProperly()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";
         dyn.RowKey = "rk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn );
         await _tableStorageProvider.SaveAsync();
         dyn.FirstItem = "this text is changed.";
         _tableStorageProvider.Update( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();

         var result = await _tableStorageProvider.GetAsync( _tableName, "pk", "rk" );
         Assert.AreEqual( "pk", result.PartitionKey );
         Assert.AreEqual( "rk", result.RowKey );
         Assert.AreEqual( "this text is changed.", result.FirstItem );
         Assert.AreEqual( 2, result.SecondItem );
      }

      [TestMethod]
      public async Task UpdateDynamic_TheDynamicContainPartitionKeyAndRowKeyThatDoNotMatchPartionKeyAndRowKeyArguments_ShouldThrowArgumentException()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";
         dyn.RowKey = "rk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn );
         await _tableStorageProvider.SaveAsync();
         dyn.FirstItem = "this text is changed.";

         Assert.ThrowsException<ArgumentException>( (Action) ( () => _tableStorageProvider.Update( _tableName, dyn, "partionKey", "rowKey" ) ) );
      }

      [TestMethod]
      public async Task UpdateDynamic_TheDynamicDoesNotContainPartitionKey_ShouldThrowArgumentException()
      {
         dynamic dyn = new ExpandoObject();
         dyn.RowKey = "rk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();
         dyn.FirstItem = "this text is changed.";
         Assert.ThrowsException<ArgumentException>( (Action) ( () => _tableStorageProvider.Update( _tableName, dyn ) ) );
      }

      [TestMethod]
      public async Task UpdateDynamic_TheDynamicDoesNotContainRowKey_ShouldThrowArgumentException()
      {
         dynamic dyn = new ExpandoObject();
         dyn.PartitionKey = "pk";
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Add( _tableName, dyn, "pk", "rk" );
         await _tableStorageProvider.SaveAsync();
         dyn.FirstItem = "this text is changed.";
         Assert.ThrowsException<ArgumentException>( (Action) ( () => _tableStorageProvider.Update( _tableName, dyn ) ) );
      }

      [TestMethod]
      public async Task UpdateDynamic_ItemDoesNotExist_ShouldThrowEntityDoesNotExistException()
      {
         dynamic dyn = new ExpandoObject();
         dyn.FirstItem = "this is the first item.";
         dyn.SecondItem = 2;

         _tableStorageProvider.Update( _tableName, dyn, "pk", "rk" );
         await AsyncAssert.ThrowsAsync<EntityDoesNotExistException>( () => _tableStorageProvider.SaveAsync() );
      }
   }
}
