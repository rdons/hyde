using System;

namespace TechSmith.Hyde.Common
{
   public class EntityAlreadyExistsException : Exception
   {
      public EntityAlreadyExistsException()
      {
      }

      public EntityAlreadyExistsException( string partitionKey, string rowKey, Exception inner )
         : base( string.Format( "Entity with partition key {0} and row key {1} was not found.", partitionKey, rowKey ), inner )
      {
      }

      public EntityAlreadyExistsException( string message )
         : base( message )
      {
      }

      public EntityAlreadyExistsException( string message, Exception inner )
         : base( message, inner )
      {
      }
   }
}
