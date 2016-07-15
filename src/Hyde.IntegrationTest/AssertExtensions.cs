using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TechSmith.Hyde.IntegrationTest
{
   public static class AsyncAssert
   {
      public static async Task ThrowsAsync<TException>( Func<Task> action )
      {
         try
         {
            await action();
            Assert.Fail( "Delegate did not throw expected exception " + typeof( TException ).Name + "." );
         }
         catch ( Exception ex )
         {
            if ( !( ex is TException ) )
            {
               Assert.Fail( "Delegate threw exception of type " + ex.GetType().Name + ", but " + typeof( TException ).Name + " or a derived type was expected." );
            }
            if ( ex.GetType() != typeof( TException ) )
            {
               Assert.Fail( "Delegate threw exception of type " + ex.GetType().Name + ", but " + typeof( TException ).Name + " was expected." );
            }
         }
      }
   }
}
