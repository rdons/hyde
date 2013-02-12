using System.Collections.Generic;

namespace TechSmith.Hyde.Table
{
   public interface IQuery<T> : IEnumerable<T> where T : new()
   {
      IQuery<T> Take( int count );
   }
}
