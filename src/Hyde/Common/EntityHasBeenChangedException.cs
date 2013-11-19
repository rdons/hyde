using System;
using System.Runtime.Serialization;

namespace TechSmith.Hyde.Common
{
   [Serializable]
   public class EntityHasBeenChangedException : Exception
   {
      public EntityHasBeenChangedException()
      {
      }

      public EntityHasBeenChangedException( string partitionKey, string rowKey, Exception inner )
         : base( string.Format( "Entity with partition key {0} and row key {1} was not found.", partitionKey, rowKey ), inner )
      {
      }

      public EntityHasBeenChangedException( string message )
         : base( message )
      {
      }

      public EntityHasBeenChangedException( string message, Exception inner )
         : base( message, inner )
      {
      }

      protected EntityHasBeenChangedException( SerializationInfo info, StreamingContext context )
         : base( info, context )
      {
      }
   }
}
