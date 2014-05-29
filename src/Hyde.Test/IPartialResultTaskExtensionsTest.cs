using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TechSmith.Hyde.Common;
using TechSmith.Hyde.Table;

namespace TechSmith.Hyde.Test
{
   /// <summary>
   /// Summary description for IPartialResultTaskExtensionsTest
   /// </summary>
   [TestClass]
   public class IPartialResultTaskExtensionsTest
   {
      [TestMethod]
      public void Flatten_FlattenedListIsSameAsManuallyResolvingPartialResults()
      {
         var list1 = new List<int> { 1 };
         var list2 = new List<int> { 2 };
         var list3 = new List<int> { 3 };

         var partialResult1 = new Mock<IPartialResult<int>>();
         var partialResult2 = new Mock<IPartialResult<int>>();
         var partialResult3 = new Mock<IPartialResult<int>>();
         partialResult3.Setup( res => res.GetEnumerator() ).Returns( () => list3.GetEnumerator() );
         partialResult3.Setup( res => res.HasMoreResults ).Returns( false );

         partialResult2.Setup( res => res.GetEnumerator() ).Returns( () => list2.GetEnumerator() );
         partialResult2.Setup( res => res.HasMoreResults ).Returns( true );
         partialResult2.Setup( res => res.GetNextAsync() ).Returns( Task.Factory.StartNew( () => partialResult3.Object ) );

         partialResult1.Setup( res => res.GetEnumerator() ).Returns( () => list1.GetEnumerator() );
         partialResult1.Setup( res => res.HasMoreResults ).Returns( true );
         partialResult1.Setup( res => res.GetNextAsync() ).Returns( Task.Factory.StartNew( () => partialResult2.Object ) );

         var flattened = Task.Factory.StartNew( () => partialResult1.Object ).FlattenAsync().Result.ToArray();

         Assert.AreEqual( partialResult1.Object.First(), flattened.ElementAt( 0 ) );
         Assert.AreEqual( partialResult2.Object.First(), flattened.ElementAt( 1 ) );
         Assert.AreEqual( partialResult3.Object.First(), flattened.ElementAt( 2 ) );
      }

      [TestMethod]
      public void Flatten_ExceptionThrownSynchronously_ExceptionThrownIsPropagatedToCaller()
      {
         var list1 = new List<int> { 1 };
         var list2 = new List<int> { 2 };

         var partialResult1 = new Mock<IPartialResult<int>>();
         var partialResult2 = new Mock<IPartialResult<int>>();

         partialResult2.Setup( res => res.GetEnumerator() ).Returns( () => list2.GetEnumerator() );
         partialResult2.Setup( res => res.HasMoreResults ).Returns( true );
         partialResult2.Setup( res => res.GetNextAsync() ).Throws( new InvalidOperationException( "test" ) );

         partialResult1.Setup( res => res.GetEnumerator() ).Returns( () => list1.GetEnumerator() );
         partialResult1.Setup( res => res.HasMoreResults ).Returns( true );
         partialResult1.Setup( res => res.GetNextAsync() ).Returns( () => Task.Factory.StartNew( () => partialResult2.Object ) );

         try
         {
            Task.Factory.StartNew( () => partialResult1.Object ).FlattenAsync().Wait();
         }
         catch ( AggregateException exception )
         {
            var inner = exception.InnerExceptions.Single() as InvalidOperationException;
            Assert.IsNotNull( inner, "Inner exception was not an InvalidOperationException." );
            Assert.IsTrue( inner.Message == "test", "Inner exception did not contain our message." );
         }
      }

      [TestMethod]
      public void Flatten_ExceptionThrownAsynchronously_ExceptionThrownIsPropagatedToCaller()
      {
         var list1 = new List<int> { 1 };
         var list2 = new List<int> { 2 };

         var partialResult1 = new Mock<IPartialResult<int>>();
         var partialResult2 = new Mock<IPartialResult<int>>();

         partialResult2.Setup( res => res.GetEnumerator() ).Returns( () => list2.GetEnumerator() );
         partialResult2.Setup( res => res.HasMoreResults ).Returns( true );
         partialResult2.Setup( res => res.GetNextAsync() ).Returns( () => Task.Factory.StartNew<IPartialResult<int>>( () => { throw new InvalidOperationException( "test" ); } ) );

         partialResult1.Setup( res => res.GetEnumerator() ).Returns( () => list1.GetEnumerator() );
         partialResult1.Setup( res => res.HasMoreResults ).Returns( true );
         partialResult1.Setup( res => res.GetNextAsync() ).Returns( () => Task.Factory.StartNew( () => partialResult2.Object ) );

         try
         {
            Task.Factory.StartNew( () => partialResult1.Object ).FlattenAsync().Wait();
         }
         catch ( AggregateException exception )
         {
            var inner = exception.InnerExceptions.Single() as InvalidOperationException;
            Assert.IsNotNull( inner, "Inner exception was not an InvalidOperationException." );
            Assert.IsTrue( inner.Message == "test", "Inner exception did not contain our message." );
         }
      }
   }
}
