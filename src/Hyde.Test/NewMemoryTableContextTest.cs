using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Table.Memory;

namespace TechSmith.Hyde.Test
{
   [TestClass]
   public class NewMemoryTableContextTest
   {
      private NewMemoryTableContext _context;

      [TestInitialize]
      public void Initialize()
      {
         NewMemoryTableContext.ResetAllTables();
         _context = new NewMemoryTableContext();
      }

      [TestMethod]
      [ExpectedException( typeof( EntityDoesNotExistException ) )]
      public void GetItem_ItemNotFound_ThrowsEntityNotFoundException()
      {
         _context.GetItem<DecoratedItem>( "foo", "pk", "rk" );
      }

      [TestMethod]
      public void GetItem_ItemAddedButSaveNotCalled_ThrowsEntityDoesNotExistException()
      {
         _context.AddNewItem( "table", new DecoratedItem { Id = "abc", Name = "123", Age = 50 }, "abc", "123" );
         try
         {
            _context.GetItem<DecoratedItem>( "table", "abc", "123" );
            Assert.Fail( "Should have thrown exception" );
         }
         catch ( EntityDoesNotExistException )
         {
         }
      }

      [TestMethod]
      public void Save_ItemAdded_GetItemReturnsTheItem()
      {
         var addedItem = new DecoratedItem { Id = "abc", Name = "123", Age = 50 };
         _context.AddNewItem( "table", addedItem, "abc", "123" );
         _context.Save();

         var returnedItem = _context.GetItem<DecoratedItem>( "table", "abc", "123" );
         Assert.AreEqual( addedItem.Id, returnedItem.Id );
         Assert.AreEqual( addedItem.Name, returnedItem.Name );
         Assert.AreEqual( addedItem.Age, returnedItem.Age );
      }

      [TestMethod]
      public void GetItem_ItemAddedAndSavedWithDifferentContext_ReturnsItem()
      {
         var addedItem = new DecoratedItem { Id = "abc", Name = "123", Age = 50 };
         _context.AddNewItem( "table", addedItem, "abc", "123" );
         _context.Save();

         var returnedItem = new NewMemoryTableContext().GetItem<DecoratedItem>( "table", "abc", "123" );
         Assert.AreEqual( addedItem.Id, returnedItem.Id );
         Assert.AreEqual( addedItem.Name, returnedItem.Name );
         Assert.AreEqual( addedItem.Age, returnedItem.Age );
      }

      [TestMethod]
      public void GetItem_ItemAddedToDifferentTable_ThrowsEntityDoesNotExistException()
      {
         var addedItem = new DecoratedItem { Id = "abc", Name = "123", Age = 50 };
         _context.AddNewItem( "table", addedItem, "abc", "123" );
         _context.Save();

         try
         {
            _context.GetItem<DecoratedItem>( "diffTable", "abc", "123" );
            Assert.Fail( "Should have thrown exception" );
         }
         catch ( EntityDoesNotExistException )
         {
         }
      }

      [TestMethod]
      public void AddNewItem_ItemAlreadyExistsAndSaveIsNotCalled_NoExceptionThrown()
      {
         _context.AddNewItem( "table", new DecoratedItem { Id = "abc", Name = "123" }, "abc", "123" );
         _context.Save();

         _context.AddNewItem( "table", new DecoratedItem { Id = "abc", Name = "123" }, "abc", "123" );
      }

      [TestMethod]
      public void AddNewItem_ItemAlreadyExistsAndSaveIsCalled_ThrowsEntityAlreadyExistsException()
      {
         _context.AddNewItem( "table", new DecoratedItem { Id = "abc", Name = "123" }, "abc", "123" );
         _context.Save();

         _context.AddNewItem( "table", new DecoratedItem { Id = "abc", Name = "123" }, "abc", "123" );
         try
         {
            _context.Save();
            Assert.Fail( "Should have thrown exception" );
         }
         catch ( EntityAlreadyExistsException )
         {
         }
      }
   }
}
